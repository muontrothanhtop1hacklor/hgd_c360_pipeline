#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unit & Integration Test Suite cho Tuần 2 (p2_c360_standardization):
Kiểm chứng toàn diện:
1. Bảng Map DBHC Tỉnh & Xã (36 Tỉnh, 3.322 Xã, Sáp nhập 1-1, Xã mơ hồ cùng tỉnh 00999 & khác tỉnh 00998).
2. Thư viện chuẩn hóa dùng chung (db_c360_cleaner.py: Họ tên, Ngày sinh, CCCD 12 số, SĐT, Dân tộc, Quốc tịch, ĐC).
3. Job Full Load C360 (Khử trùng MA_CD, lọc nghiệp vụ, 48.000 công dân hợp lệ, quản lý khóa Sync Control).
4. Worker Near Real-time (Bắt sự kiện qua Trigger trên Mirror, MERGE công dân mới/sửa, DELETE công dân bị loại/xóa, đổi MA_CD).
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
from p1_source_and_mirror.gen_seed_nhankhau import generate_seed_dataset
from p1_source_and_mirror.load_danhmuc import run_load_danhmuc
from p2_c360_standardization.build_map_dbhc import build_and_refresh_dbhc_maps
from p2_c360_standardization.c360_etl_full_load import run_c360_full_load
from p2_c360_standardization.c360_nrt_worker import run_c360_nrt_batch
from p2_c360_standardization.db_c360_cleaner import (
    clean_address_group,
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
    """Kiểm tra xây dựng 2 Bảng Map DBHC (Tỉnh & Xã) và phân tầng ưu tiên (UT-001, UT-002, UT-003)."""
    test_db = tmp_path / "test_map_dbhc.db"
    run_load_danhmuc(sqlite_path=test_db, inject_moho=True)
    summary = build_and_refresh_dbhc_maps(sqlite_path=test_db)

    assert summary["status"] == "SUCCESS"
    assert summary["active_provinces"] == 36
    assert summary["active_wards"] == 3322

    conn = get_connection(sqlite_path=test_db)
    cur = conn.cursor()

    # UT-001: Tra cứu Tỉnh hiện hành và Tỉnh sáp nhập
    cur.execute("SELECT MA_TINH_MOI, LOAI_MAP FROM C360_MAP_DBHC_TINH WHERE MA_TINH_LOOKUP = '01'")
    r01 = cur.fetchone()
    assert r01 == ("001", "HIEN_HANH")

    cur.execute("SELECT MA_TINH_MOI, LOAI_MAP FROM C360_MAP_DBHC_TINH WHERE MA_TINH_LOOKUP = '01TTT'")
    assert cur.fetchone()[0] == "001"

    cur.execute("SELECT MA_TINH_MOI, LOAI_MAP FROM C360_MAP_DBHC_TINH WHERE MA_TINH_LOOKUP = '34'")
    r34 = cur.fetchone()
    assert r34 == ("033", "SAP_NHAP")  # Thái Bình cũ sáp nhập vào Hưng Yên 033

    # UT-002: Tra cứu Xã hiện hành và Xã sáp nhập 1-1
    cur.execute("SELECT MA_XA_MOI, MA_TINH_MOI, LOAI_MAP FROM C360_MAP_DBHC_XA WHERE MA_XA_LOOKUP = '00004'")
    r_xa_act = cur.fetchone()
    assert r_xa_act == ("00004", "001", "HIEN_HANH")

    cur.execute("SELECT MA_XA_MOI, MA_TINH_MOI, LOAI_MAP FROM C360_MAP_DBHC_XA WHERE MA_XA_LOOKUP = '00013'")
    r_xa_sn = cur.fetchone()
    assert r_xa_sn == ("00004", "001", "SAP_NHAP")

    # Mất số 0 ở đầu do ép kiểu số
    cur.execute("SELECT MA_XA_MOI, MA_TINH_MOI, LOAI_MAP FROM C360_MAP_DBHC_XA WHERE MA_XA_LOOKUP = '13'")
    assert cur.fetchone() == ("00004", "001", "SAP_NHAP")

    # UT-003: Xã mơ hồ cùng tỉnh (00999) -> Xã = NULL, Tỉnh = '001', LOAI_MAP = 'MO_HO'
    cur.execute("SELECT MA_XA_MOI, MA_TINH_MOI, LOAI_MAP FROM C360_MAP_DBHC_XA WHERE MA_XA_LOOKUP = '00999'")
    r_moho_same = cur.fetchone()
    assert r_moho_same[0] is None
    assert r_moho_same[1] == "001"
    assert r_moho_same[2] == "MO_HO"

    # Xã mơ hồ khác tỉnh (00998) -> Xã = NULL, Tỉnh = NULL, LOAI_MAP = 'MO_HO'
    cur.execute("SELECT MA_XA_MOI, MA_TINH_MOI, LOAI_MAP FROM C360_MAP_DBHC_XA WHERE MA_XA_LOOKUP = '00998'")
    r_moho_diff = cur.fetchone()
    assert r_moho_diff[0] is None
    assert r_moho_diff[1] is None
    assert r_moho_diff[2] == "MO_HO"

    conn.close()


def test_02_cleaner_functions():
    """Kiểm tra chi tiết từng hàm chuẩn hóa và bắt lỗi chất lượng dữ liệu DQ_FLAGS."""
    # 1. Họ tên
    c_name, kd_name, extra_sp = clean_hoten("  Nguyễn   Văn   An  ")
    assert c_name == "Nguyễn Văn An"
    assert kd_name == "NGUYEN VAN AN"
    assert extra_sp is True

    # 2. Ngày sinh (UT-008)
    # Đầy đủ
    dob, goc, err = clean_ngaysinh("19950815")
    assert dob == "19950815" and not err
    # Chỉ năm tháng
    dob, goc, err = clean_ngaysinh("19950800")
    assert dob == "199508" and not err
    # Chỉ năm
    dob, goc, err = clean_ngaysinh("19950000")
    assert dob == "1995" and not err
    dob, goc, err = clean_ngaysinh("1995")
    assert dob == "1995" and not err
    # Sai lịch (30/02)
    dob, goc, err = clean_ngaysinh("19950230")
    assert dob is None and err is True
    # Ngoài khoảng (< 1850)
    dob, goc, err = clean_ngaysinh("17500101")
    assert dob is None and err is True

    # 3. CCCD / CMTND (CHK-07)
    sdd, cccd, cmtnd, khong_sdd, cmnd9 = clean_cccd("001095012345", None)
    assert sdd == "001095012345" and not khong_sdd and not cmnd9

    sdd, cccd, cmtnd, khong_sdd, cmnd9 = clean_cccd(None, "001095012345")
    assert sdd == "001095012345" and not khong_sdd and not cmnd9

    sdd, cccd, cmtnd, khong_sdd, cmnd9 = clean_cccd(None, "123456789")
    assert sdd is None and khong_sdd and cmnd9

    sdd, cccd, cmtnd, khong_sdd, cmnd9 = clean_cccd(None, None)
    assert sdd is None and khong_sdd and not cmnd9

    # 4. Số điện thoại (UT-009)
    p, p_err = clean_phone("840912 345 678")
    assert p == "0912345678" and not p_err

    p, p_err = clean_phone("+84912-345-678")
    assert p == "0912345678" and not p_err

    p, p_err = clean_phone("12345")
    assert p is None and p_err is True

    # 5. Dân tộc (hết hạn vs hợp lệ)
    mock_dantoc = {171: ("01", "Kinh", True), 248: ("98", "Hết hạn", False)}
    ma, ten, err = clean_dantoc(171, mock_dantoc)
    assert ma == "01" and ten == "Kinh" and not err

    ma, ten, err = clean_dantoc(248, mock_dantoc)
    assert ma is None and err is True  # Hết hạn -> DAN_TOC_LOI

    # 6. Quốc tịch (mã vs ID)
    mock_qt_code = {"VN": ("VN", "Việt Nam")}
    mock_qt_id = {"233": ("VN", "Việt Nam")}
    ma, ten, err = clean_quoctich("VN", mock_qt_code, mock_qt_id)
    assert ma == "VN" and not err
    ma, ten, err = clean_quoctich("233", mock_qt_code, mock_qt_id)
    assert ma == "VN" and not err
    ma, ten, err = clean_quoctich("XX", mock_qt_code, mock_qt_id)
    assert ma is None and err is True


def test_03_week2_full_load_and_nrt_pipeline(tmp_path: Path):
    """Kiểm thử toàn trình Tuần 2: Nạp Danh mục -> Seed -> Full Load C360 -> NRT Worker."""
    test_db = tmp_path / "test_week2_c360.db"

    # Bước 1: Nạp danh mục & Xây dựng bảng Map
    run_load_danhmuc(sqlite_path=test_db, inject_moho=True)
    build_and_refresh_dbhc_maps(sqlite_path=test_db)

    # Bước 2: Sinh dữ liệu mẫu (5.000 công dân) vào nguồn VSS và sao chép sang Mirror
    conn = get_connection(sqlite_path=test_db)
    seed_stats = generate_seed_dataset(conn, num_persons=5000, seed=123, batch_size=2500, out_dir=tmp_path)
    cur = conn.cursor()

    # Sao chép sang Mirror để chuẩn bị Full Load C360
    cur.execute("DELETE FROM MIR_VSS_HO_GD_NHANKHAU;")
    cols = [c[1] for c in cur.execute("PRAGMA table_info(VSS_HO_GD_NHANKHAU)").fetchall()]
    cur.execute(f"INSERT INTO MIR_VSS_HO_GD_NHANKHAU ({', '.join(cols)}) SELECT * FROM VSS_HO_GD_NHANKHAU;")
    conn.commit()

    # Bước 3: Chạy Full Load C360
    fl_res = run_c360_full_load(sqlite_path=test_db, batch_size=2500)
    assert fl_res["status"] == "SUCCESS"
    assert fl_res["total_citizens_loaded"] == seed_stats["expected_valid_c360_persons"]

    # Kiểm tra tính duy nhất: COUNT(*) == COUNT(DISTINCT MA_SO_BHXH)
    cur.execute("SELECT COUNT(*), COUNT(DISTINCT MA_SO_BHXH) FROM C360_THONG_TIN_CA_NHAN;")
    cnt_total, cnt_distinct = cur.fetchone()
    assert cnt_total == seed_stats["expected_valid_c360_persons"]
    assert cnt_total == cnt_distinct

    # Kiểm tra không còn bản ghi bị loại trừ
    cur.execute("SELECT COUNT(*) FROM C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH IN (SELECT MA_CD FROM MIR_VSS_HO_GD_NHANKHAU WHERE TYPEID = 'D');")
    assert cur.fetchone()[0] == 0

    # Bước 4: Kiểm thử Near Real-time Worker qua Queue (UT-010, UT-011, UT-012)
    # 4.1. Thêm công dân mới vào Mirror -> Trigger đẩy vào Queue
    cur.execute(
        """
        INSERT INTO MIR_VSS_HO_GD_NHANKHAU (
            ID, MA_CD, MA_HO_GD, HO_TEN, GIOI_TINH, NGAY_SINH, MA_TINH, MA_XA,
            SO_CCCD, TYPEID, TRANG_THAI_DUYET, NGAY_TAO, NGAY_SUA
        ) VALUES (
            999901, 'CD_NEW_TEST_01', 'HGD_TEST_01', 'ĐẶNG VĂN MỚI', 1, '19980512', '01', '00004',
            '001098012345', '1', 1, '2026-11-12 10:00:00', '2026-11-12 10:00:00'
        );
        """
    )
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM C360_SYNC_QUEUE WHERE MA_CD = 'CD_NEW_TEST_01';")
    assert cur.fetchone()[0] >= 1

    # Chạy worker NRT
    nrt_res = run_c360_nrt_batch(sqlite_path=test_db)
    assert nrt_res["status"] == "SUCCESS"
    assert nrt_res["merge_count"] >= 1

    # Kiểm tra công dân mới xuất hiện trên C360
    cur.execute("SELECT HO_TEN, MA_TINH_THUONG_TRU, MA_XA_THUONG_TRU FROM C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH = 'CD_NEW_TEST_01';")
    new_citizen = cur.fetchone()
    assert new_citizen is not None
    assert new_citizen[0] == "ĐẶNG VĂN MỚI"
    assert new_citizen[1] == "001"
    assert new_citizen[2] == "00004"

    # 4.2. UT-010: UPDATE đổi MA_CD trên Mirror
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

    # Trigger đẩy cả mã cũ và mã mới vào Queue
    cur.execute("SELECT COUNT(*) FROM C360_SYNC_QUEUE WHERE MA_CD IN ('CD_NEW_TEST_01', 'CD_NEW_TEST_RENAMED');")
    assert cur.fetchone()[0] >= 2

    # Chạy worker NRT
    run_c360_nrt_batch(sqlite_path=test_db)

    # Kiểm tra mã cũ bị xóa sạch, mã mới xuất hiện với MA_SO_BHXH_CU
    cur.execute("SELECT COUNT(*) FROM C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH = 'CD_NEW_TEST_01';")
    assert cur.fetchone()[0] == 0

    cur.execute("SELECT MA_SO_BHXH, MA_SO_BHXH_CU FROM C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH = 'CD_NEW_TEST_RENAMED';")
    renamed_cit = cur.fetchone()
    assert renamed_cit is not None
    assert renamed_cit[1] == "CD_NEW_TEST_01"

    # 4.3. UT-012: DELETE khỏi Mirror -> C360 tự động xóa công dân
    cur.execute("DELETE FROM MIR_VSS_HO_GD_NHANKHAU WHERE MA_CD = 'CD_NEW_TEST_RENAMED';")
    conn.commit()

    run_c360_nrt_batch(sqlite_path=test_db)

    cur.execute("SELECT COUNT(*) FROM C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH = 'CD_NEW_TEST_RENAMED';")
    assert cur.fetchone()[0] == 0

    conn.close()
