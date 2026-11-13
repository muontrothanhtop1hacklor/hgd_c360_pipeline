#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
End-to-End (E2E) Test Suite Toàn Trình (Tuần 4 - Nghiệm thu Toàn diện Hệ thống):
Kiểm chứng 8 bước khép kín của chu trình dữ liệu:
  Bước 1: Nạp 3 bảng danh mục & Khởi tạo 2 bảng Map DBHC sau sáp nhập 2025.
  Bước 2: Sinh dữ liệu mẫu ban đầu (5.000 công dân) bao phủ 100% kịch bản sạch, bẩn, biên.
  Bước 3: Chạy Full Load JDBC sang Mirror (phân mảnh 10 partitions, SCN handoff, Unique Index).
  Bước 4: Chạy Full Load C360 (Khử trùng MA_CD, lọc nghiệp vụ, gán 11 cờ DQ_FLAGS).
  Bước 5: Đối soát dữ liệu 3 tầng lần 1 (Trước biến động): Nguồn == Mirror == C360 (Khớp 100%).
  Bước 6: Giả lập biến động Delta ở Nguồn (INSERT, UPDATE sạch/bẩn, đổi MA_CD có MA_CD_OLD, DELETE).
  Bước 7: Kích hoạt đồng bộ CDC & Near Real-time Worker xử lý Queue (MERGE / DELETE).
  Bước 8: Kiểm chứng qua 4 Dịch vụ REST APIs và Đối soát lần 2 (Sau biến động: Bảo toàn 100% số liệu).
"""

import os
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from p1_source_and_mirror.db_env import (
    get_connection,
    init_c360_schema,
    init_source_and_mirror_schema,
)
from p1_source_and_mirror.full_load_jdbc import run_full_load_all
from p1_source_and_mirror.gen_delta_changes import apply_delta_changes
from p1_source_and_mirror.gen_seed_nhankhau import generate_seed_dataset
from p1_source_and_mirror.load_danhmuc import run_load_danhmuc
from p2_c360_standardization.build_map_dbhc import build_and_refresh_dbhc_maps
from p2_c360_standardization.c360_etl_full_load import run_c360_full_load
from p2_c360_standardization.c360_nrt_worker import run_c360_nrt_batch
from p3_reconciliation_bi_api.api.main import app
from p3_reconciliation_bi_api.reconcile_3_tiers import run_3tier_reconciliation

DEMO_API_KEY = "c360_secret_key_demo_2026"


def test_week4_end_to_end_lifecycle(tmp_path: Path):
    """Kiểm thử toàn trình 8 bước theo đúng tiêu chuẩn nghiệm thu dự án."""
    e2e_db = tmp_path / "e2e_c360_test.db"

    # =========================================================================
    # BƯỚC 1: Nạp 3 bảng danh mục & Xây dựng 2 Bảng Map DBHC
    # =========================================================================
    dm_summary = run_load_danhmuc(sqlite_path=e2e_db, inject_moho=True)
    assert dm_summary["active_provinces_2025"] == 36
    assert dm_summary["active_wards_2025"] == 3322

    map_summary = build_and_refresh_dbhc_maps(sqlite_path=e2e_db)
    assert map_summary["status"] == "SUCCESS"

    # =========================================================================
    # BƯỚC 2: Sinh dữ liệu mẫu ban đầu (5.000 công dân)
    # =========================================================================
    conn = get_connection(sqlite_path=e2e_db)
    seed_stats = generate_seed_dataset(
        conn, num_persons=5000, seed=999, batch_size=2500, out_dir=tmp_path
    )
    assert seed_stats["num_persons_total"] == 5000
    assert seed_stats["num_rows_total"] > 5000
    assert seed_stats["expected_valid_c360_persons"] == 4800

    # =========================================================================
    # BƯỚC 3: Full Load JDBC từ Nguồn sang vùng Mirror
    # =========================================================================
    fl_mirror_report = run_full_load_all(sqlite_path=e2e_db)
    assert fl_mirror_report["nhankhau_full_load"]["reconciliation_status"] == "PASSED"
    assert fl_mirror_report["nhankhau_full_load"]["diff"] == 0
    assert fl_mirror_report["nhankhau_full_load"]["mirror_total"] == seed_stats["num_rows_total"]

    # =========================================================================
    # BƯỚC 4: Full Load C360 từ Mirror sang C360_THONG_TIN_CA_NHAN
    # =========================================================================
    fl_c360_report = run_c360_full_load(sqlite_path=e2e_db, batch_size=2500)
    assert fl_c360_report["status"] == "SUCCESS"
    assert fl_c360_report["total_citizens_loaded"] == seed_stats["expected_valid_c360_persons"]

    # =========================================================================
    # BƯỚC 5: Đối soát dữ liệu 3 tầng lần 1 (Trước biến động)
    # =========================================================================
    recon_1 = run_3tier_reconciliation(sqlite_path=e2e_db, out_path=tmp_path / "recon_pre.json")
    assert recon_1["overall_status"] == "PASSED"
    assert recon_1["tier1_source_vs_mirror"]["status"] == "PASSED"
    assert recon_1["tier2_mirror_vs_c360"]["golden_equation_diff"] == 0
    assert recon_1["tier2_mirror_vs_c360"]["leaked_invalid_records"] == 0
    assert recon_1["tier2_mirror_vs_c360"]["multi_household_sample_test"]["passed"] is True

    # =========================================================================
    # BƯỚC 6: Giả lập biến động Delta tại Nguồn & Đồng bộ CDC sang Mirror
    # =========================================================================
    delta_stats = apply_delta_changes(conn, sync_to_mirror=True, out_dir=tmp_path)
    assert len(delta_stats["inserted_new_citizens"]) > 0
    assert len(delta_stats["updated_ma_cd_reissued"]) > 0
    assert len(delta_stats["deleted_single_household_citizen"]) > 0

    # Kiểm tra Trigger trên Mirror đã tự động bắt sự kiện và đẩy vào C360_SYNC_QUEUE
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM C360_SYNC_QUEUE;")
    queue_event_count = cur.fetchone()[0]
    assert queue_event_count > 0

    # =========================================================================
    # BƯỚC 7: Worker Near Real-time xử lý hàng đợi Queue (MERGE & DELETE)
    # =========================================================================
    nrt_result = run_c360_nrt_batch(sqlite_path=e2e_db)
    assert nrt_result["status"] == "SUCCESS"
    assert nrt_result["merge_count"] > 0

    # =========================================================================
    # BƯỚC 8: Kiểm chứng qua REST APIs & Đối soát lần 2 (Sau biến động)
    # =========================================================================
    # 8.1. Kiểm tra qua REST APIs
    os.environ["SQLITE_DB_PATH"] = str(e2e_db)
    client = TestClient(app)
    headers = {"X-API-Key": DEMO_API_KEY}

    # Kiểm tra tìm thấy công dân mới chèn
    new_cd = delta_stats["inserted_new_citizens"][0]["MA_CD"]
    resp_new = client.get(f"/api/v1/ca-nhan/{new_cd}", headers=headers)
    assert resp_new.status_code == 200
    assert resp_new.json()["matched_by"] == "MA_SO_BHXH"

    # Kiểm tra công dân đổi mã: tìm thấy qua mã mới và mã cũ
    renamed_info = delta_stats["updated_ma_cd_reissued"][0]
    old_code = renamed_info["MA_CD_OLD"]
    new_code = renamed_info["MA_CD_NEW"]

    # Tra theo mã mới -> Tầng 1
    resp_renamed_new = client.get(f"/api/v1/ca-nhan/{new_code}", headers=headers)
    assert resp_renamed_new.status_code == 200
    assert resp_renamed_new.json()["matched_by"] == "MA_SO_BHXH"

    # Tra theo mã cũ -> Tầng 2
    resp_renamed_old = client.get(f"/api/v1/ca-nhan/{old_code}", headers=headers)
    assert resp_renamed_old.status_code == 200
    assert resp_renamed_old.json()["matched_by"] == "MA_SO_BHXH_CU"

    # Kiểm tra công dân bị xóa hoàn toàn khỏi hệ thống -> 404
    deleted_cd = delta_stats["deleted_single_household_citizen"][0]["MA_CD"]
    resp_deleted = client.get(f"/api/v1/ca-nhan/{deleted_cd}", headers=headers)
    assert resp_deleted.status_code == 404

    # 8.2. Chạy đối soát lần 2: Đảm bảo công thức vàng vẫn khớp 100% sau biến động
    recon_2 = run_3tier_reconciliation(sqlite_path=e2e_db, out_path=tmp_path / "recon_post.json")
    assert recon_2["overall_status"] == "PASSED"
    assert recon_2["tier2_mirror_vs_c360"]["golden_equation_diff"] == 0
    assert recon_2["tier2_mirror_vs_c360"]["leaked_invalid_records"] == 0

    conn.close()
