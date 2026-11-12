#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unit & Integration Test Suite cho Tuần 1 (p1_source_and_mirror & design/):
Kiểm chứng toàn diện:
1. Bộ tài liệu Basic Design (15 sheets) & Detail Design (11 sheets) bao gồm Sheet QA.
2. Nạp 3 bảng Danh mục thực tế (59 Dân tộc, 243 Quốc tịch, 15.325 Địa bàn hành chính: 36 Tỉnh & 3.322 Xã).
3. Script 1 (Initial Seed Generator) bao phủ đầy đủ các kịch bản dữ liệu sạch/bẩn/biên.
4. Full Load JDBC (phân mảnh theo MA_CD, ép kiểu tường minh, Drop/Rebuild Unique Index UK_MIR_NK, Idempotent 100%, Đối soát Nguồn == Mirror).
5. Script 2 (Change/Delta Generator) & CDC Upsert/Delete đồng bộ sang Mirror (INSERT, UPDATE, UPDATE MA_CD có MA_CD_OLD, DELETE).
"""

import sys
from pathlib import Path

import pytest
from openpyxl import load_workbook

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from p1_source_and_mirror.db_env import get_connection
from p1_source_and_mirror.debezium.register_connectors import build_connectors_payload, get_handoff_scn
from p1_source_and_mirror.full_load_jdbc import run_full_load_all
from p1_source_and_mirror.gen_delta_changes import apply_delta_changes
from p1_source_and_mirror.gen_seed_nhankhau import generate_seed_dataset
from p1_source_and_mirror.load_danhmuc import run_load_danhmuc


def test_01_design_documents_and_excel_sheets():
    """Kiểm tra bộ tài liệu Thiết kế cơ bản (15 sheets) và Thiết kế chi tiết (11 sheets)."""
    design_dir = PROJECT_ROOT / "design"
    bd_md = design_dir / "ThietKeCoBan_BasicDesign_HGD_C360.md"
    bd_xlsx = design_dir / "ThietKeCoBan_BasicDesign_HGD_C360.xlsx"
    dd_md = design_dir / "ThietKeChiTiet_DetailDesign_HGD_C360.md"
    dd_xlsx = design_dir / "ThietKeChiTiet_DetailDesign_HGD_C360.xlsx"

    assert bd_md.exists(), "Thiếu file ThietKeCoBan_BasicDesign_HGD_C360.md"
    assert bd_xlsx.exists(), "Thiếu file ThietKeCoBan_BasicDesign_HGD_C360.xlsx"
    assert dd_md.exists(), "Thiếu file ThietKeChiTiet_DetailDesign_HGD_C360.md"
    assert dd_xlsx.exists(), "Thiếu file ThietKeChiTiet_DetailDesign_HGD_C360.xlsx"

    wb_bd = load_workbook(bd_xlsx, read_only=True)
    assert len(wb_bd.sheetnames) == 15
    assert "QA" in wb_bd.sheetnames
    wb_bd.close()

    wb_dd = load_workbook(dd_xlsx, read_only=True)
    assert len(wb_dd.sheetnames) == 11
    assert "QA" in wb_dd.sheetnames
    wb_dd.close()


def test_02_week1_end_to_end_pipeline(tmp_path: Path):
    """Kiểm thử toàn trình Tuần 1 trên DB kiểm thử độc lập."""
    test_db = tmp_path / "test_week1_c360.db"

    # Bước 1: Nạp 3 bảng Danh mục từ CSV thực tế
    dm_summary = run_load_danhmuc(sqlite_path=test_db, inject_moho=True)
    assert dm_summary["VSS_DM_DANTOC"] == 59
    assert dm_summary["VSS_DM_QUOC_TICH"] == 243
    assert dm_summary["VSS_DM_DBHC"] == 15325
    assert dm_summary["active_provinces_2025"] == 36
    assert dm_summary["active_wards_2025"] == 3322

    # Bước 2: Sinh dữ liệu giả lập ban đầu (Script 1) với 5.000 công dân
    conn = get_connection(sqlite_path=test_db)
    seed_stats = generate_seed_dataset(conn, num_persons=5000, seed=42, batch_size=2500, out_dir=tmp_path)
    assert seed_stats["num_persons_total"] == 5000
    assert seed_stats["num_rows_total"] > 5000
    assert seed_stats["multi_household_persons"] > 0
    assert seed_stats["multi_household_tie_id_persons"] > 0
    assert seed_stats["filtered_out_persons_typeid_d"] > 0
    assert seed_stats["filtered_out_persons_unapproved"] > 0
    assert seed_stats["addr_5char_tinh_ttt"] > 0
    assert seed_stats["addr_sap_nhap_1to1"] > 0
    assert seed_stats["addr_moho_same_province_00999"] > 0
    assert seed_stats["addr_moho_cross_province_00998"] > 0
    assert seed_stats["addr_mismatch_tinh_xa"] > 0
    assert seed_stats["addr_infer_tinh_from_xa"] > 0
    assert seed_stats["addr_invalid_unmapped"] > 0

    # Bước 3: Chạy Full Load lần 1 và chạy lại lần 2 để kiểm chứng tính Idempotent + Rebuild Unique Index
    fl_report_1 = run_full_load_all(sqlite_path=test_db)
    fl_report_2 = run_full_load_all(sqlite_path=test_db)

    assert fl_report_2["nhankhau_full_load"]["reconciliation_status"] == "PASSED"
    assert fl_report_2["nhankhau_full_load"]["diff"] == 0
    assert fl_report_2["nhankhau_full_load"]["mirror_total"] == seed_stats["num_rows_total"]
    assert len(fl_report_2["nhankhau_full_load"]["mismatched_provinces"]) == 0

    # Kiểm chứng Unique Index UK_MIR_NK (MA_CD, MA_HO_GD) đã được bật lại trên bảng Mirror
    cur = conn.cursor()
    cur.execute("PRAGMA index_list('MIR_VSS_HO_GD_NHANKHAU')")
    indexes = {row[1]: row[2] for row in cur.fetchall()}  # {index_name: is_unique}
    assert "UK_MIR_NK" in indexes and indexes["UK_MIR_NK"] == 1

    # Kiểm chứng mốc SCN bàn giao cho Debezium CDC
    scn = get_handoff_scn(sqlite_path=test_db)
    assert scn is not None and scn > 0
    payloads = build_connectors_payload(scn=scn)
    assert payloads["source"]["config"]["log.mining.scn.start"] == str(scn)
    assert payloads["sink"]["config"]["primary.key.fields"] == "MA_CD,MA_HO_GD"

    # Bước 4: Chạy Script 2 (Sinh dữ liệu biến động Delta/CDC) và đồng bộ sang Mirror
    delta_manifest = apply_delta_changes(conn, sync_to_mirror=True, out_dir=tmp_path)
    assert len(delta_manifest["inserted_new_citizens"]) == 5
    assert len(delta_manifest["inserted_new_household_for_existing"]) == 3
    assert len(delta_manifest["updated_dirty_to_clean"]) == 5
    assert len(delta_manifest["updated_status_to_filtered_out"]) == 4
    assert len(delta_manifest["updated_status_to_deceased"]) == 2
    assert len(delta_manifest["updated_ma_cd_reissued"]) == 3
    assert len(delta_manifest["deleted_latest_household_fallback"]) == 3
    assert len(delta_manifest["deleted_single_household_citizen"]) == 3

    # Kiểm chứng sau khi áp dụng CDC (Upsert + Delete + Đổi khóa chính MA_CD), Nguồn và Mirror vẫn khớp 100%
    cur.execute("SELECT COUNT(*) FROM VSS_HO_GD_NHANKHAU")
    src_after_cdc = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM MIR_VSS_HO_GD_NHANKHAU")
    mir_after_cdc = cur.fetchone()[0]
    assert src_after_cdc == mir_after_cdc

    # Kiểm chứng các bản ghi đổi MA_CD không còn mã cũ trên Mirror và đã có mã mới kèm MA_CD_OLD
    for item in delta_manifest["updated_ma_cd_reissued"]:
        cur.execute("SELECT COUNT(*) FROM MIR_VSS_HO_GD_NHANKHAU WHERE MA_CD = ?", (item["MA_CD_OLD"],))
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT MA_CD_OLD FROM MIR_VSS_HO_GD_NHANKHAU WHERE MA_CD = ?", (item["MA_CD_NEW"],))
        r = cur.fetchone()
        assert r is not None and r[0] == item["MA_CD_OLD"]

    conn.close()

