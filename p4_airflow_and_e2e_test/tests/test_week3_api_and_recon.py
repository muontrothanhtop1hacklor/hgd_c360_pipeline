#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unit & Integration Test Suite cho Tuần 3 (p3_reconciliation_bi_api):
Thiết kế tự chủ 100% (tự khởi tạo DB kiểm thử riêng qua fixture, không phụ thuộc file DB có sẵn trên máy):
1. Job Đối soát Tự động 3 Tầng (Nguồn <-> Mirror <-> C360, Công thức vàng, Đối chiếu sâu 100 mẫu nhiều hộ,
   kiểm thử âm phát hiện ngay bản ghi rò rỉ).
2. 3 Data Mart Views BI & Script trích xuất/provisioning Metabase (V_BI_THONG_KE_DIA_BAN,
   V_BI_CHAT_LUONG_DU_LIEU, V_BI_GIAM_SAT_DONG_BO).
3. 4 REST APIs Tra cứu C360 (Xác thực X-API-Key SHA-256 401/403, Cascade dò 3 tầng + 409 Conflict,
   Tra cứu lô > 500 mã & chặn > 1.000 mã với 400 MSG-E003, Tìm kiếm 2 chiều ngày sinh rút gọn UT-014,
   Change Feed Cursor Base64 với độ trễ an toàn 120s UT-015, và Async Batch Audit Log).
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict

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
    init_api_and_bi_schema,
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
from p3_reconciliation_bi_api.metabase_setup import build_bi_datamart_and_snapshot
from p3_reconciliation_bi_api.reconcile_3_tiers import run_3tier_reconciliation

FULL_SCOPE_KEY = "c360_secret_key_demo_2026"
SINGLE_ONLY_KEY = "c360_single_only_key_2026"
REVOKED_KEY = "c360_revoked_key_2026"


@pytest.fixture(scope="module")
def week3_env(tmp_path_factory: pytest.TempPathFactory) -> Dict[str, Any]:
    """Khởi tạo DB kiểm thử độc lập xuyên suốt Tuần 1 -> Tuần 2 -> Tuần 3."""
    base_dir = tmp_path_factory.mktemp("week3_suite")
    db_path = base_dir / "test_week3_e2e.db"

    prev_db_env = os.environ.get("SQLITE_DB_PATH")
    os.environ["SQLITE_DB_PATH"] = str(db_path)

    run_load_danhmuc(sqlite_path=db_path, inject_moho=True)
    conn = get_connection(sqlite_path=db_path)
    seed_stats = generate_seed_dataset(conn, num_persons=3000, seed=99, batch_size=1500, out_dir=base_dir)
    conn.close()

    run_full_load_all(sqlite_path=db_path)
    build_and_refresh_dbhc_maps(sqlite_path=db_path)
    fl_res = run_c360_full_load(sqlite_path=db_path, batch_size=1500)

    conn = get_connection(sqlite_path=db_path)
    init_api_and_bi_schema(conn)
    conn.close()

    yield {
        "db_path": db_path,
        "base_dir": base_dir,
        "expected_valid_citizens": seed_stats["expected_valid_c360_persons"],
        "loaded_citizens": fl_res["total_citizens_loaded"],
    }

    if prev_db_env is None:
        os.environ.pop("SQLITE_DB_PATH", None)
    else:
        os.environ["SQLITE_DB_PATH"] = prev_db_env


def test_01_3tier_reconciliation(week3_env: Dict[str, Any], tmp_path: Path):
    """Kiểm tra đối soát tự động 3 tầng (Tích cực & Tiêu cực)."""
    db_path = week3_env["db_path"]
    out_json = tmp_path / "recon_report.json"

    report = run_3tier_reconciliation(sqlite_path=db_path, out_path=out_json, sample_size=100)
    assert out_json.exists()
    assert report["overall_status"] == "PASSED"

    # Tầng 1: Nguồn <-> Mirror
    t1 = report["tier1_source_vs_mirror"]
    assert t1["status"] == "PASSED"
    assert t1["tables"]["HO_GD_NHANKHAU"]["diff"] == 0
    assert t1["mismatched_provinces"] == {}

    # Tầng 2: Mirror <-> C360 & kiểm tra mẫu sâu 100 công dân nhiều hộ
    t2 = report["tier2_mirror_vs_c360"]
    assert t2["status"] == "PASSED"
    assert t2["golden_equation_diff"] == 0
    assert t2["leaked_invalid_records"] == 0
    assert t2["missing_valid_records"] == 0
    assert t2["multi_household_sample_test"]["samples_checked"] == 100
    assert t2["multi_household_sample_test"]["passed"] is True

    # Tầng 3: Ma trận 11 mã cờ chất lượng dữ liệu DQ_FLAGS
    t3 = report["tier3_data_quality"]
    assert t3["total_citizens"] == week3_env["expected_valid_citizens"]
    assert t3["clean_citizens"] + t3["dirty_citizens"] == t3["total_citizens"]
    assert len(t3["flag_breakdown"]) == 11

    # Kiểm thử âm (Negative Test): Nếu cố tình chèn 1 bản ghi không có trên Mirror vào C360 -> Đối soát phải FAILED
    conn = get_connection(sqlite_path=db_path)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO C360_THONG_TIN_CA_NHAN (MA_SO_BHXH, HO_TEN, HO_TEN_KHONG_DAU)
        VALUES ('GHOST_LEAK_999', 'BẢN GHI RÒ RỈ', 'BAN GHI RO RI');
        """
    )
    conn.commit()
    bad_report = run_3tier_reconciliation(sqlite_path=db_path, out_path=tmp_path / "bad_recon.json")
    assert bad_report["overall_status"] == "FAILED"
    assert bad_report["tier2_mirror_vs_c360"]["leaked_invalid_records"] == 1

    # Dọn lại bản ghi giả để trả trạng thái chuẩn
    cur.execute("DELETE FROM C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH = 'GHOST_LEAK_999';")
    conn.commit()
    conn.close()


def test_02_bi_datamart_views_and_dashboard_snapshot(week3_env: Dict[str, Any], tmp_path: Path):
    """Kiểm tra 3 Data Mart Views BI và trình xuất Dashboard Metabase/HTML."""
    db_path = week3_env["db_path"]
    expected_total = week3_env["expected_valid_citizens"]
    html_out = tmp_path / "bi_preview.html"

    snap = build_bi_datamart_and_snapshot(sqlite_path=db_path, html_out_path=html_out)
    assert html_out.exists()
    assert snap["group1_demographics"]["total_citizens"] == expected_total

    conn = get_connection(sqlite_path=db_path)
    cur = conn.cursor()

    # View 1: Tổng số công dân & tổng các nhóm tuổi phải khớp tuyệt đối 100%
    cur.execute(
        """
        SELECT SUM(TONG_SO_CONG_DAN),
               SUM(DUOI_18_TUOI + TU_18_DEN_35 + TU_36_DEN_60 + TREN_60_TUOI + KHONG_RO_TUOI),
               SUM(CO_CCCD_12_SO + CHI_CO_CMND_9_SO + KHONG_DINH_DANH)
          FROM V_BI_THONG_KE_DIA_BAN;
        """
    )
    v1_total, v1_age_sum, v1_id_sum = cur.fetchone()
    assert v1_total == expected_total
    assert v1_age_sum == expected_total
    assert v1_id_sum == expected_total

    # View 2: Tổng hồ sơ sạch + có lỗi khớp tuyệt đối
    cur.execute("SELECT SUM(TONG_SO_HO_SO), SUM(SO_HO_SO_SACH + SO_HO_SO_CO_LOI) FROM V_BI_CHAT_LUONG_DU_LIEU;")
    v2_total, v2_split = cur.fetchone()
    assert v2_total == expected_total
    assert v2_split == expected_total

    # View 3: Đã ghi nhận lô FULL_LOAD trong C360_SYNC_LOG
    cur.execute("SELECT COUNT(*) FROM V_BI_GIAM_SAT_DONG_BO WHERE BATCH_TYPE = 'FULL_LOAD' AND STATUS = 'SUCCESS';")
    assert cur.fetchone()[0] >= 1

    # View 4 (Nhóm dân tộc) & View 5 (Tỉnh thường trú vs liên hệ)
    cur.execute("SELECT SUM(SO_NGUOI) FROM V_BI_NHOM_DAN_TOC;")
    assert cur.fetchone()[0] == expected_total
    cur.execute("SELECT SUM(SO_THUONG_TRU), SUM(SO_LIEN_HE) FROM V_BI_TINH_THUONG_TRU_LIEN_HE;")
    tt_sum, lh_sum = cur.fetchone()
    assert tt_sum == expected_total
    assert lh_sum == expected_total

    conn.close()


def test_03_rest_api_auth_cascade_search_feed_and_audit(week3_env: Dict[str, Any]):
    """Kiểm thử toàn diện Bảo mật X-API-Key, phân quyền Scope, 4 REST APIs (UT-013..015) & Async Audit Log."""
    db_path = week3_env["db_path"]
    conn = get_connection(sqlite_path=db_path)
    cur = conn.cursor()

    # Chuẩn bị dữ liệu test đặc thù cho UT-013, UT-014, UT-015:
    old_sync_ts = (datetime.now() - timedelta(seconds=300)).strftime("%Y-%m-%d %H:%M:%S")
    fresh_sync_ts = (datetime.now() - timedelta(seconds=10)).strftime("%Y-%m-%d %H:%M:%S")

    # Đặt các bản ghi hiện có về mốc 300s trước để đủ điều kiện qua watermark 120s
    cur.execute("UPDATE C360_THONG_TIN_CA_NHAN SET SYNC_DATE = ?;", (old_sync_ts,))

    # Chèn 3 công dân phục vụ kiểm thử:
    # - A1: Có MA_SO_BHXH_CU duy nhất, ngày sinh rút gọn '199508', SYNC_DATE = 300s trước
    # - B1 & B2: Cùng trùng SO_DINH_DANH = '099095888888' để test 409 Conflict; B2 vừa đồng bộ 10s trước (chưa qua 120s lag)
    cur.execute(
        """
        INSERT INTO C360_THONG_TIN_CA_NHAN (
            MA_SO_BHXH, MA_SO_BHXH_CU, SO_DINH_DANH, HO_TEN, HO_TEN_KHONG_DAU,
            GIOI_TINH, NGAY_SINH_CHUAN, NAM_SINH, MA_TINH_THUONG_TRU, SYNC_DATE
        ) VALUES
        ('API_TEST_A1', 'OLD_BHXH_A1', '001095777777', 'Lê Thị Kiểm Thử', 'LE THI KIEM THU', 2, '199508', 1995, '001', ?),
        ('API_TEST_B1', 'OLD_DUP_99', '099095888888', 'Trần Trùng Lặp Một', 'TRAN TRUNG LAP MOT', 1, '19900115', 1990, '079', ?),
        ('API_TEST_B2', 'OLD_DUP_99', '099095888888', 'Trần Trùng Lặp Hai', 'TRAN TRUNG LAP HAI', 1, '19920520', 1992, '079', ?);
        """,
        (old_sync_ts, old_sync_ts, fresh_sync_ts),
    )
    conn.commit()
    conn.close()

    headers_full = {"X-API-Key": FULL_SCOPE_KEY}
    headers_single = {"X-API-Key": SINGLE_ONLY_KEY}
    headers_revoked = {"X-API-Key": REVOKED_KEY}

    # Dùng context manager để kích hoạt lifespan (khởi chạy & xả sạch Async Batch Audit Worker)
    with TestClient(app) as client:
        # 0. Kiểm tra Live BI Dashboard HTML (/dashboard) & dữ liệu JSON (/api/v1/bi/dashboard-data)
        r_dash = client.get("/dashboard")
        assert r_dash.status_code == 200
        assert "Thông tin cá nhân – Gold" in r_dash.text
        r_dash_json = client.get("/api/v1/bi/dashboard-data")
        assert r_dash_json.status_code == 200
        assert r_dash_json.json()["group1_demographics"]["total_citizens"] >= week3_env["expected_valid_citizens"]

        # 1. Kiểm tra Xác thực 401 & Phân quyền Scope 403 (CHK-11)
        r_no_key = client.get("/api/v1/ca-nhan/API_TEST_A1")
        assert r_no_key.status_code == 401
        assert r_no_key.json()["detail"]["code"] == "MSG-E001"

        r_wrong = client.get("/api/v1/ca-nhan/API_TEST_A1", headers={"X-API-Key": "WRONG_SECRET"})
        assert r_wrong.status_code == 401

        r_revoked = client.get("/api/v1/ca-nhan/API_TEST_A1", headers=headers_revoked)
        assert r_revoked.status_code == 401

        # Client chỉ có 'read:single': gọi API 1 được 200, nhưng gọi API 2/3/4 bị 403 MSG-E002
        assert client.get("/api/v1/ca-nhan/API_TEST_A1", headers=headers_single).status_code == 200
        r_forbidden_batch = client.post(
            "/api/v1/ca-nhan/tra-cuu-lo", headers=headers_single, json={"ma_list": ["API_TEST_A1"]}
        )
        assert r_forbidden_batch.status_code == 403
        assert r_forbidden_batch.json()["detail"]["code"] == "MSG-E002"
        assert client.get("/api/v1/ca-nhan/tim-kiem", headers=headers_single, params={"ho_ten": "Le Thi"}).status_code == 403
        assert client.get("/api/v1/ca-nhan/thay-doi", headers=headers_single).status_code == 403

        # 2. API 1 (UT-013): Cascade 3 tầng & 409 Conflict & 404 Not Found
        # Tầng 1: MA_SO_BHXH
        r_t1 = client.get("/api/v1/ca-nhan/API_TEST_A1", headers=headers_full)
        assert r_t1.status_code == 200
        assert r_t1.json()["matched_by"] == "MA_SO_BHXH"

        # Tầng 2: MA_SO_BHXH_CU duy nhất
        r_t2 = client.get("/api/v1/ca-nhan/OLD_BHXH_A1", headers=headers_full)
        assert r_t2.status_code == 200
        assert r_t2.json()["matched_by"] == "MA_SO_BHXH_CU"
        assert r_t2.json()["data"]["ma_so_bhxh"] == "API_TEST_A1"

        # Tầng 3: SO_DINH_DANH duy nhất
        r_t3 = client.get("/api/v1/ca-nhan/001095777777", headers=headers_full)
        assert r_t3.status_code == 200
        assert r_t3.json()["matched_by"] == "SO_DINH_DANH"
        assert r_t3.json()["data"]["ma_so_bhxh"] == "API_TEST_A1"

        # Trùng >= 2 người ở Tầng 2 (MA_SO_BHXH_CU) & Tầng 3 (SO_DINH_DANH) -> 409 Conflict kèm candidates
        r_conf_cu = client.get("/api/v1/ca-nhan/OLD_DUP_99", headers=headers_full)
        assert r_conf_cu.status_code == 409
        assert r_conf_cu.json()["detail"]["code"] == "MSG-E004"
        assert r_conf_cu.json()["detail"]["candidate_count"] == 2

        r_conf_cccd = client.get("/api/v1/ca-nhan/099095888888", headers=headers_full)
        assert r_conf_cccd.status_code == 409
        assert r_conf_cccd.json()["detail"]["matched_tier"] == "SO_DINH_DANH"
        assert len(r_conf_cccd.json()["detail"]["candidates"]) == 2

        # Không tìm thấy ở cả 3 tầng -> 404 MSG-I001
        r_404 = client.get("/api/v1/ca-nhan/NOT_FOUND_999999", headers=headers_full)
        assert r_404.status_code == 404
        assert r_404.json()["detail"]["code"] == "MSG-I001"

        # 3. API 2: Tra cứu lô > 500 mã (kiểm tra chia chunk) + Validation 400 MSG-E003
        big_batch = ["API_TEST_A1", "OLD_BHXH_A1", "099095888888"] + [f"FAKE_{i}" for i in range(600)]
        r_batch = client.post("/api/v1/ca-nhan/tra-cuu-lo", headers=headers_full, json={"ma_list": big_batch})
        assert r_batch.status_code == 200
        b_json = r_batch.json()
        assert b_json["total_requested"] == 603
        assert b_json["found_count"] == 2
        assert b_json["conflict_count"] == 1
        assert b_json["not_found_count"] == 600

        # Vượt 1.000 mã hoặc rỗng -> 400 MSG-E003
        r_over_1000 = client.post(
            "/api/v1/ca-nhan/tra-cuu-lo", headers=headers_full, json={"ma_list": [f"K{i}" for i in range(1001)]}
        )
        assert r_over_1000.status_code == 400
        assert r_over_1000.json()["detail"]["code"] == "MSG-E003"

        r_empty_batch = client.post("/api/v1/ca-nhan/tra-cuu-lo", headers=headers_full, json={"ma_list": []})
        assert r_empty_batch.status_code == 400
        assert r_empty_batch.json()["detail"]["code"] == "MSG-E003"

        # 4. API 3 (UT-014): Tìm kiếm không dấu/thừa khoảng trắng + Ngày sinh 2 chiều + Chặn ngày sinh bẩn 400
        # DB lưu '199508' -> tìm với '19950815' vẫn khớp; tìm với '1995' cũng khớp!
        r_s1 = client.get(
            "/api/v1/ca-nhan/tim-kiem",
            headers=headers_full,
            params={"ho_ten": "  lê   thị kiểm thử ", "ngay_sinh": "19950815", "ma_tinh": "01"},
        )
        assert r_s1.status_code == 200
        assert r_s1.json()["total_matched"] == 1
        assert r_s1.json()["data"][0]["ma_so_bhxh"] == "API_TEST_A1"

        r_s2 = client.get(
            "/api/v1/ca-nhan/tim-kiem",
            headers=headers_full,
            params={"ho_ten": "LE THI KIEM THU", "ngay_sinh": "1995"},
        )
        assert r_s2.status_code == 200
        assert r_s2.json()["total_matched"] == 1

        # Ngày sinh sai định dạng / sai lịch -> 400 MSG-E003 (không được lọt qua lọc!)
        for bad_dob in ("abcd", "19950", "19950230", "17000101"):
            r_bad_dob = client.get(
                "/api/v1/ca-nhan/tim-kiem",
                headers=headers_full,
                params={"ho_ten": "LE THI KIEM THU", "ngay_sinh": bad_dob},
            )
            assert r_bad_dob.status_code == 400
            assert r_bad_dob.json()["detail"]["code"] == "MSG-E003"

        # 5. API 4 (UT-015): Phân trang Cursor Base64 & Độ trễ an toàn 120s
        # API_TEST_B2 có SYNC_DATE mới 10s trước -> KHÔNG ĐƯỢC xuất hiện trong feed!
        seen_bhxh = set()
        cursor = None
        while True:
            params = {"limit": 1000}
            if cursor:
                params["cursor"] = cursor
            r_feed = client.get("/api/v1/ca-nhan/thay-doi", headers=headers_full, params=params)
            assert r_feed.status_code == 200
            f_body = r_feed.json()
            for item in f_body["data"]:
                seen_bhxh.add(item["ma_so_bhxh"])
            if not f_body["has_more"]:
                break
            cursor = f_body["next_cursor"]
            assert cursor is not None

        assert "API_TEST_A1" in seen_bhxh
        assert "API_TEST_B1" in seen_bhxh
        assert "API_TEST_B2" not in seen_bhxh  # Bị chặn đúng bởi safe watermark lag 120s!

        # Cursor sai định dạng -> 400 MSG-E003
        r_bad_cur = client.get("/api/v1/ca-nhan/thay-doi", headers=headers_full, params={"cursor": "NOT_BASE64!!!"})
        assert r_bad_cur.status_code == 400
        assert r_bad_cur.json()["detail"]["code"] == "MSG-E003"

    # Sau khi thoát khối `with TestClient(app)`, Async Audit Worker đã tự động xả sạch log xuống DB
    conn = get_connection(sqlite_path=db_path)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM C360_API_AUDIT_LOG;")
    audit_count = cur.fetchone()[0]
    assert audit_count >= 15

    # Kiểm tra có đủ log cho cả 200, 400, 401, 403, 404, 409
    cur.execute("SELECT DISTINCT RESPONSE_STATUS FROM C360_API_AUDIT_LOG;")
    logged_statuses = {r[0] for r in cur.fetchall()}
    assert {200, 400, 401, 403, 404, 409}.issubset(logged_statuses)

    # Kiểm tra Query Plan đảm bảo 100% truy vấn API đều dùng Index (không Full Table Scan)
    index_check_queries = [
        "SELECT * FROM C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH = '0000000001'",
        "SELECT * FROM C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH_CU = '0000000001'",
        "SELECT * FROM C360_THONG_TIN_CA_NHAN WHERE SO_DINH_DANH = '001095012345'",
        "SELECT * FROM C360_THONG_TIN_CA_NHAN WHERE HO_TEN_KHONG_DAU = 'LE THI KIEM THU' AND NGAY_SINH_CHUAN LIKE '1995%'",
        "SELECT * FROM C360_THONG_TIN_CA_NHAN WHERE SYNC_DATE <= '2026-11-26 09:00:00' AND SYNC_DATE > '2026-11-20 00:00:00' ORDER BY SYNC_DATE ASC, ID ASC LIMIT 100",
    ]
    for q in index_check_queries:
        plan_rows = cur.execute(f"EXPLAIN QUERY PLAN {q}").fetchall()
        plan_text = " ".join(str(r[-1]) for r in plan_rows).upper()
        assert "USING INDEX" in plan_text, f"Truy vấn không dùng Index: {q} -> {plan_text}"

    # Dọn 3 bản ghi test API để giữ DB sạch
    cur.execute("DELETE FROM C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH IN ('API_TEST_A1', 'API_TEST_B1', 'API_TEST_B2');")
    conn.commit()
    conn.close()


def test_04_bulk_delta_cdc_to_bi_dashboard_flow(week3_env: Dict[str, Any], tmp_path: Path):
    """
    Kiểm chứng đúng kịch bản 4 bước Before -> Script Delta -> Kafka CDC -> After trên BI:
      1. Đọc tổng số người ban đầu trên Dashboard BI (Before).
      2. Bơm lô biến động lớn (2.000 công dân mới + UPDATE/DELETE) vào nguồn -> CDC đẩy sang Mirror.
      3. Kiểm tra Topic Kafka CDC (MIR_CDC_TOPIC_METRICS) & Queue Pending đã ghi nhận đủ > 2.000 messages.
      4. Chạy Near Real-time Worker -> Kiểm tra số liệu trên Dashboard BI tự động nhảy lên khớp 100% đối soát.
    """
    db_path = week3_env["db_path"]

    with TestClient(app) as client:
        before_data = client.get("/api/v1/bi/dashboard-data").json()
        total_before = before_data["group1_demographics"]["total_citizens"]

        conn = get_connection(sqlite_path=db_path)
        apply_delta_changes(conn, sync_to_mirror=True, out_dir=tmp_path, bulk_insert_count=2000)
        conn.close()

        # Bước 3: Kiểm tra trạng thái trung gian (CDC Topic đã nhận > 2.000 messages, Queue đang chờ xử lý)
        mid_data = client.get("/api/v1/bi/dashboard-data").json()
        assert mid_data["group3_sync_monitoring"]["cdc_topic"]["total_messages"] >= 2000
        assert mid_data["group3_sync_monitoring"]["queue_pending"] >= 2000

        # Bước 4: Kích hoạt luồng Near Real-time chuẩn hóa & cập nhật C360
        while True:
            nrt = run_c360_nrt_batch(sqlite_path=db_path, max_batch_size=5000)
            if nrt["status"] == "EMPTY":
                break

        after_data = client.get("/api/v1/bi/dashboard-data").json()
        total_after = after_data["group1_demographics"]["total_citizens"]
        assert total_after > total_before + 1850  # ~96% của 2.000 dòng mới là công dân hợp lệ
        assert after_data["group3_sync_monitoring"]["queue_pending"] == 0

    # Đối soát 3 tầng sau biến động lô lớn phải PASSED 100% (golden_equation_diff == 0)
    final_recon = run_3tier_reconciliation(sqlite_path=db_path, out_path=tmp_path / "after_delta_recon.json")
    assert final_recon["overall_status"] == "PASSED"
    assert final_recon["tier2_mirror_vs_c360"]["golden_equation_diff"] == 0
