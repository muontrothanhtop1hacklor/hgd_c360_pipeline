#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unit & Integration Test Suite cho Tuần 2 (p2_c360_standardization):
Kiểm chứng toàn diện:
1. Bảng Map DBHC Tỉnh & Xã (36 Tỉnh, 3.322 Xã, Sáp nhập 1-1, Xã mơ hồ cùng tỉnh 00999 & khác tỉnh 00998, Guardrail chống xóa nhầm).
2. Thư viện chuẩn hóa dùng chung (db_c360_cleaner.py: Họ tên, Ngày sinh, CCCD 12 số + chặn '000000000000',
   SĐT + fallback SO_DIEN_THOAI + chặn '0000000000', Dân tộc, Quốc tịch mã/ID/tên 'Việt Nam', ĐC TT/LH/KS + *_GOC).
3. Job Full Load C360 (Streaming fetchmany, khử trùng MA_CD, lọc nghiệp vụ, khớp 100% số lượng công dân hợp lệ, khóa Sync Control).
4. Worker Near Real-time (Trigger Mirror -> Queue, MERGE công dân mới/sửa, DELETE công dân bị loại/xóa, đổi MA_CD,
   cắt lô max_batch_size KHÔNG xóa nhầm sự kiện chưa xử lý, PAUSE_NRT=1 trả SKIPPED, tích hợp gen_delta_changes).
"""

import sys
from pathlib import Path

import pytest

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
from p2_c360_standardization.db_c360_cleaner import (
    clean_cccd,
    clean_dantoc,
    clean_hoten,
    clean_ngaysinh,
    clean_phone,
    clean_quoctich,
    load_standardization_lookups,
    transform_citizen_record,
)


def test_01_map_dbhc_tables(tmp_path: Path):
    """Kiểm tra xây dựng 2 Bảng Map DBHC (Tỉnh & Xã), View hiệu lực và phân tầng ưu tiên (UT-001..003)."""
    test_db = tmp_path / "test_map_dbhc.db"
    run_load_danhmuc(sqlite_path=test_db, inject_moho=True)
    summary = build_and_refresh_dbhc_maps(sqlite_path=test_db)

    assert summary["status"] == "SUCCESS"
    assert summary["active_provinces"] == 36
    assert summary["active_wards"] == 3322

    conn = get_connection(sqlite_path=test_db)
    cur = conn.cursor()

    # View V_MIR_DM_DBHC_HIEU_LUC đã được khởi tạo trong schema
    cur.execute("SELECT name FROM sqlite_master WHERE type='view' AND name='V_MIR_DM_DBHC_HIEU_LUC';")
    assert cur.fetchone() is not None

    # UT-001: Tra cứu Tỉnh hiện hành và Tỉnh sáp nhập
    cur.execute("SELECT MA_TINH_MOI, LOAI_MAP FROM C360_MAP_DBHC_TINH WHERE MA_TINH_LOOKUP = '01'")
    assert cur.fetchone() == ("001", "HIEN_HANH")

    cur.execute("SELECT MA_TINH_MOI, LOAI_MAP FROM C360_MAP_DBHC_TINH WHERE MA_TINH_LOOKUP = '01TTT'")
    assert cur.fetchone()[0] == "001"

    cur.execute("SELECT MA_TINH_MOI, LOAI_MAP FROM C360_MAP_DBHC_TINH WHERE MA_TINH_LOOKUP = '34'")
    assert cur.fetchone() == ("033", "SAP_NHAP")

    # UT-002: Tra cứu Xã hiện hành và Xã sáp nhập 1-1
    cur.execute("SELECT MA_XA_MOI, MA_TINH_MOI, LOAI_MAP FROM C360_MAP_DBHC_XA WHERE MA_XA_LOOKUP = '00004'")
    assert cur.fetchone() == ("00004", "001", "HIEN_HANH")

    cur.execute("SELECT MA_XA_MOI, MA_TINH_MOI, LOAI_MAP FROM C360_MAP_DBHC_XA WHERE MA_XA_LOOKUP = '00013'")
    assert cur.fetchone() == ("00004", "001", "SAP_NHAP")

    cur.execute("SELECT MA_XA_MOI, MA_TINH_MOI, LOAI_MAP FROM C360_MAP_DBHC_XA WHERE MA_XA_LOOKUP = '13'")
    assert cur.fetchone() == ("00004", "001", "SAP_NHAP")

    # UT-003: Xã mơ hồ cùng tỉnh (00999) và khác tỉnh (00998)
    cur.execute("SELECT MA_XA_MOI, MA_TINH_MOI, LOAI_MAP FROM C360_MAP_DBHC_XA WHERE MA_XA_LOOKUP = '00999'")
    assert cur.fetchone() == (None, "001", "MO_HO")

    cur.execute("SELECT MA_XA_MOI, MA_TINH_MOI, LOAI_MAP FROM C360_MAP_DBHC_XA WHERE MA_XA_LOOKUP = '00998'")
    assert cur.fetchone() == (None, None, "MO_HO")

    conn.close()

    # Guardrail: Nếu DBHC rỗng -> phải raise RuntimeError, không xóa nhầm bảng Map
    empty_db = tmp_path / "empty_dbhc.db"
    with pytest.raises(RuntimeError):
        build_and_refresh_dbhc_maps(sqlite_path=empty_db)


def test_02_cleaner_functions(tmp_path: Path):
    """Kiểm tra chi tiết từng hàm chuẩn hóa, edge cases và bắt lỗi DQ_FLAGS."""
    # 1. Họ tên
    c_name, kd_name, extra_sp = clean_hoten("  Nguyễn   Văn   An  ")
    assert c_name == "Nguyễn Văn An"
    assert kd_name == "NGUYEN VAN AN"
    assert extra_sp is True

    # 2. Ngày sinh (UT-008)
    assert clean_ngaysinh("19950815")[:2] == ("19950815", "19950815")
    assert clean_ngaysinh("19950800") == ("199508", "19950800", False)
    assert clean_ngaysinh("19950000") == ("1995", "19950000", False)
    assert clean_ngaysinh("1995") == ("1995", "1995", False)
    assert clean_ngaysinh("19950230") == (None, "19950230", True)
    assert clean_ngaysinh("17500101") == (None, "17500101", True)

    # 3. CCCD / CMTND (CHK-07 + chặn toàn số 0)
    assert clean_cccd("001095012345", None) == ("001095012345", "001095012345", None, False, False)
    assert clean_cccd(None, "001095012345") == ("001095012345", None, "001095012345", False, False)
    assert clean_cccd("000000000000", None) == (None, "000000000000", None, True, False)
    assert clean_cccd("000000000000", "001095012345") == ("001095012345", "000000000000", "001095012345", False, False)
    assert clean_cccd(None, "123456789") == (None, None, "123456789", True, True)
    assert clean_cccd(None, None) == (None, None, None, True, False)

    # 4. Số điện thoại (UT-009 + chặn toàn số 0 / sai đầu số)
    assert clean_phone("840912 345 678") == ("0912345678", False)
    assert clean_phone("+84912-345-678") == ("0912345678", False)
    assert clean_phone("02438251234") == ("02438251234", False)
    assert clean_phone("0000000000") == (None, True)
    assert clean_phone("0123456789") == (None, True)
    assert clean_phone("12345") == (None, True)

    # 5. Dân tộc (hết hạn vs hợp lệ)
    mock_dantoc = {171: ("01", "Kinh", True), 248: ("98", "Hết hạn", False)}
    assert clean_dantoc(171, mock_dantoc) == ("01", "Kinh", False)
    assert clean_dantoc(248, mock_dantoc) == (None, None, True)

    # 6. Quốc tịch (mã, ID và tên tiếng Việt)
    mock_qt_code = {"VN": ("VN", "Việt Nam")}
    mock_qt_id = {"233": ("VN", "Việt Nam")}
    mock_qt_name = {"VIỆT NAM": ("VN", "Việt Nam"), "VIET NAM": ("VN", "Việt Nam")}
    assert clean_quoctich("VN", mock_qt_code, mock_qt_id, mock_qt_name) == ("VN", "Việt Nam", False)
    assert clean_quoctich("233", mock_qt_code, mock_qt_id, mock_qt_name) == ("VN", "Việt Nam", False)
    assert clean_quoctich("Việt Nam", mock_qt_code, mock_qt_id, mock_qt_name) == ("VN", "Việt Nam", False)
    assert clean_quoctich("XX", mock_qt_code, mock_qt_id, mock_qt_name) == (None, None, True)

    # 7. Kiểm tra transform_citizen_record: fallback SO_DIEN_THOAI, cột *_GOC, NAM_SINH, cờ _LH/_KS
    test_db = tmp_path / "test_transform.db"
    run_load_danhmuc(sqlite_path=test_db, inject_moho=True)
    build_and_refresh_dbhc_maps(sqlite_path=test_db)
    conn = get_connection(sqlite_path=test_db)
    lookups = load_standardization_lookups(conn)
    conn.close()

    rec = transform_citizen_record(
        {
            "MA_CD": "0123456789",
            "HO_TEN": "Nguyễn Văn Test",
            "GIOI_TINH": 1,
            "NGAY_SINH": 19950815,
            "SO_CCCD": "001095012345",
            "PHONE_NUMBER": None,
            "SO_DIEN_THOAI": "+84912345678",
            "MA_TINH": "01",
            "MA_XA": "00004",
            "MA_TINH_LH": "34",
            "MA_XA_LH": "00004",  # Tỉnh LH (033) lệch với Tỉnh của Xã LH (001) -> TINH_LH_LECH_XA
            "MA_TINH_KS": None,
            "MA_XA_KS": None,  # Không nhập khai sinh -> không spam cờ lỗi KS
            "DM_DAN_TOC_ID": 171,
            "QUOC_TICH": "Việt Nam",
            "MA_HO_GD": "HGD001",
        },
        lookups,
    )
    assert rec["SO_DIEN_THOAI"] == "0912345678"
    assert rec["SO_DIEN_THOAI_GOC"] == "+84912345678"
    assert rec["NAM_SINH"] == 1995
    assert rec["MA_TINH_GOC"] == "01"
    assert rec["MA_XA_GOC"] == "00004"
    assert rec["MA_QUOC_TICH"] == "VN"
    assert "TINH_LH_LECH_XA" in (rec["DQ_FLAGS"] or "")
    assert "KS_KHONG_MAP" not in (rec["DQ_FLAGS"] or "")
    assert "SDT_LOI" not in (rec["DQ_FLAGS"] or "")
    assert "QUOC_TICH_LOI" not in (rec["DQ_FLAGS"] or "")


def test_03_week2_full_load_and_nrt_pipeline(tmp_path: Path):
    """Kiểm thử toàn trình Tuần 2: Danh mục -> Seed -> Full Load -> NRT Worker -> Partial Batch -> Delta CDC."""
    test_db = tmp_path / "test_week2_c360.db"

    # Bước 1: Nạp danh mục & Xây dựng bảng Map
    run_load_danhmuc(sqlite_path=test_db, inject_moho=True)
    build_and_refresh_dbhc_maps(sqlite_path=test_db)

    # Bước 2: Sinh dữ liệu mẫu (5.000 công dân) vào nguồn VSS và nạp sang Mirror
    conn = get_connection(sqlite_path=test_db)
    seed_stats = generate_seed_dataset(conn, num_persons=5000, seed=123, batch_size=2500, out_dir=tmp_path)
    cur = conn.cursor()
    run_full_load_all(sqlite_path=test_db)

    # Kiểm tra View V_MIR_DM_DBHC_HIEU_LUC trên Mirror có đủ 36 tỉnh + 3.322 xã hiệu lực
    cur.execute("SELECT COUNT(*) FROM V_MIR_DM_DBHC_HIEU_LUC;")
    assert cur.fetchone()[0] == 36 + 3322

    # Bước 3: Chạy Full Load C360 (streaming)
    fl_res = run_c360_full_load(sqlite_path=test_db, batch_size=1000)
    assert fl_res["status"] == "SUCCESS"
    assert fl_res["total_citizens_loaded"] == seed_stats["expected_valid_c360_persons"]

    # Kiểm tra phương trình vàng: COUNT(*) == COUNT(DISTINCT MA_SO_BHXH) == valid distinct MA_CD trên Mirror
    cur.execute("SELECT COUNT(*), COUNT(DISTINCT MA_SO_BHXH) FROM C360_THONG_TIN_CA_NHAN;")
    cnt_total, cnt_distinct = cur.fetchone()
    assert cnt_total == seed_stats["expected_valid_c360_persons"]
    assert cnt_total == cnt_distinct

    cur.execute(
        """
        SELECT COUNT(*)
          FROM C360_THONG_TIN_CA_NHAN
         WHERE MA_SO_BHXH NOT IN (
             SELECT DISTINCT MA_CD
               FROM MIR_VSS_HO_GD_NHANKHAU
              WHERE COALESCE(TYPEID, 'ZZ') NOT IN ('GT', 'GM', 'GG', 'GK')
                AND COALESCE(TRANG_THAI_DUYET, 0) NOT IN (1, 3, 4)
         );
        """
    )
    assert cur.fetchone()[0] == 0

    # Bước 4: Kiểm thử PAUSE_NRT = 1 khiến NRT Worker trả SKIPPED
    cur.execute("UPDATE C360_SYNC_CONTROL SET PAUSE_NRT = 1 WHERE PROCESS_NAME = 'C360_SYNC';")
    conn.commit()
    skipped = run_c360_nrt_batch(sqlite_path=test_db)
    assert skipped["status"] == "SKIPPED"
    cur.execute("UPDATE C360_SYNC_CONTROL SET PAUSE_NRT = 0 WHERE PROCESS_NAME = 'C360_SYNC';")
    conn.commit()

    # Bước 5: Kiểm thử Near Real-time Worker (INSERT -> UPDATE đổi MA_CD -> DELETE)
    cur.execute(
        """
        INSERT INTO MIR_VSS_HO_GD_NHANKHAU (
            ID, MA_CD, MA_HO_GD, HO_TEN, GIOI_TINH, NGAY_SINH, MA_TINH, MA_XA,
            SO_CCCD, TYPEID, TRANG_THAI_DUYET, NGAY_TAO, NGAY_SUA
        ) VALUES (
            999901, 'CD_NEW_TEST_01', 'HGD_TEST_01', 'ĐẶNG VĂN MỚI', 1, 19980512, '01', '00004',
            '001098012345', 'TM', 2, '2026-11-12 10:00:00', '2026-11-12 10:00:00'
        );
        """
    )
    conn.commit()

    nrt_res = run_c360_nrt_batch(sqlite_path=test_db)
    assert nrt_res["status"] == "SUCCESS"
    assert nrt_res["merge_count"] >= 1

    cur.execute(
        "SELECT HO_TEN, MA_TINH_THUONG_TRU, MA_XA_THUONG_TRU, NAM_SINH FROM C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH = 'CD_NEW_TEST_01';"
    )
    assert cur.fetchone() == ("ĐẶNG VĂN MỚI", "001", "00004", 1998)

    # UT-010: UPDATE đổi MA_CD trên Mirror
    cur.execute(
        """
        UPDATE MIR_VSS_HO_GD_NHANKHAU
           SET MA_CD = 'CD_NEW_TEST_RENAMED',
               MA_CD_OLD = 'CD_NEW_TEST_01',
               NGAY_SUA = '2026-11-12 10:05:00'
         WHERE MA_CD = 'CD_NEW_TEST_01';
        """
    )
    conn.commit()
    run_c360_nrt_batch(sqlite_path=test_db)

    cur.execute("SELECT COUNT(*) FROM C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH = 'CD_NEW_TEST_01';")
    assert cur.fetchone()[0] == 0
    cur.execute("SELECT MA_SO_BHXH_CU FROM C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH = 'CD_NEW_TEST_RENAMED';")
    assert cur.fetchone()[0] == "CD_NEW_TEST_01"

    # UT-012: DELETE khỏi Mirror -> C360 tự động xóa
    cur.execute("DELETE FROM MIR_VSS_HO_GD_NHANKHAU WHERE MA_CD = 'CD_NEW_TEST_RENAMED';")
    conn.commit()
    run_c360_nrt_batch(sqlite_path=test_db)
    cur.execute("SELECT COUNT(*) FROM C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH = 'CD_NEW_TEST_RENAMED';")
    assert cur.fetchone()[0] == 0

    # Bước 6: Kiểm thử chống mất dữ liệu Queue khi pending > max_batch_size
    for idx in range(5):
        cur.execute(
            """
            INSERT INTO MIR_VSS_HO_GD_NHANKHAU (
                ID, MA_CD, MA_HO_GD, HO_TEN, GIOI_TINH, NGAY_SINH, MA_TINH, MA_XA,
                SO_CCCD, TYPEID, TRANG_THAI_DUYET, NGAY_TAO, NGAY_SUA
            ) VALUES (
                ?, ?, 'HGD_PARTIAL', 'CÔNG DÂN LÔ NHỎ', 1, 19950101, '01', '00004',
                '001095099999', 'TM', 0, '2026-11-12 11:00:00', '2026-11-12 11:00:00'
            );
            """,
            (999910 + idx, f"CD_PARTIAL_{idx}"),
        )
    conn.commit()

    # Lô 1 chỉ lấy 2 dòng -> 3 dòng còn lại PHẢI vẫn còn trong Queue
    b1 = run_c360_nrt_batch(sqlite_path=test_db, max_batch_size=2)
    assert b1["status"] == "SUCCESS"
    assert b1["deleted_queue_rows"] == 2
    cur.execute("SELECT COUNT(*) FROM C360_SYNC_QUEUE WHERE STATUS = 'PENDING';")
    assert cur.fetchone()[0] == 3

    # Lô 2 lấy nốt 3 dòng còn lại -> cả 5 công dân đều có mặt trên C360
    b2 = run_c360_nrt_batch(sqlite_path=test_db, max_batch_size=10)
    assert b2["status"] == "SUCCESS"
    assert b2["deleted_queue_rows"] == 3
    cur.execute("SELECT COUNT(*) FROM C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH LIKE 'CD_PARTIAL_%';")
    assert cur.fetchone()[0] == 5

    # Bước 7: Tích hợp toàn bộ 6 kịch bản biến động CDC (gen_delta_changes -> c360_nrt_worker)
    delta_manifest = apply_delta_changes(conn, sync_to_mirror=True, out_dir=tmp_path)
    assert len(delta_manifest["inserted_new_citizens"]) >= 1
    assert len(delta_manifest["updated_ma_cd_reissued"]) >= 1
    assert len(delta_manifest["deleted_latest_household_fallback"]) >= 1

    # Xử lý sạch hàng đợi sau biến động CDC
    while True:
        res = run_c360_nrt_batch(sqlite_path=test_db, max_batch_size=500)
        if res["status"] == "EMPTY":
            break
        assert res["status"] == "SUCCESS"

    # Kiểm chứng từng trường hợp đặc biệt sau biến động CDC:
    # - Đổi MA_CD: mã cũ biến mất khỏi C360, mã mới xuất hiện với MA_SO_BHXH_CU đúng mã cũ
    for item in delta_manifest["updated_ma_cd_reissued"]:
        cur.execute("SELECT COUNT(*) FROM C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH = ?;", (item["MA_CD_OLD"],))
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT MA_SO_BHXH_CU FROM C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH = ?;", (item["MA_CD_NEW"],))
        r_new = cur.fetchone()
        assert r_new is not None and r_new[0] == item["MA_CD_OLD"]

    # - Xóa hộ mới nhất của công dân có 2 hộ -> C360 tự động lùi về hộ cũ
    for item in delta_manifest["deleted_latest_household_fallback"]:
        cur.execute("SELECT SOURCE_MA_HO_GD FROM C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH = ?;", (item["MA_CD"],))
        r_fb = cur.fetchone()
        assert r_fb is not None and r_fb[0] == item["FALLBACK_MA_HO_GD"]

    # - Kiểm chứng phương trình vàng sau toàn bộ biến động CDC:
    # COUNT(C360) == COUNT(DISTINCT MA_CD hợp lệ trên Mirror) và sai lệch = 0
    cur.execute(
        """
        SELECT COUNT(DISTINCT MA_CD)
          FROM MIR_VSS_HO_GD_NHANKHAU
         WHERE COALESCE(TYPEID, 'ZZ') NOT IN ('GT', 'GM', 'GG', 'GK')
           AND COALESCE(TRANG_THAI_DUYET, 0) NOT IN (1, 3, 4);
        """
    )
    mir_valid_distinct = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM C360_THONG_TIN_CA_NHAN;")
    c360_after_delta = cur.fetchone()[0]
    assert c360_after_delta == mir_valid_distinct

    cur.execute(
        """
        SELECT COUNT(*)
          FROM (
              SELECT MA_SO_BHXH FROM C360_THONG_TIN_CA_NHAN
              EXCEPT
              SELECT DISTINCT MA_CD
                FROM MIR_VSS_HO_GD_NHANKHAU
               WHERE COALESCE(TYPEID, 'ZZ') NOT IN ('GT', 'GM', 'GG', 'GK')
                 AND COALESCE(TRANG_THAI_DUYET, 0) NOT IN (1, 3, 4)
          );
        """
    )
    assert cur.fetchone()[0] == 0

    conn.close()
