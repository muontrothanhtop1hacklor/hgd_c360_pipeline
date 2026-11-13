#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unit & Integration Test Suite cho Tuần 3 (p3_reconciliation_bi_api):
Kiểm chứng:
1. Job Đối soát Tự động 3 Tầng (Nguồn <-> Mirror <-> C360, Công thức vàng, Kiểm tra mẫu nhiều hộ).
2. Data Mart Views phục vụ Báo cáo BI Metabase (V_BI_THONG_KE_DIA_BAN, V_BI_CHAT_LUONG_DU_LIEU, V_BI_GIAM_SAT_DONG_BO).
3. 4 REST APIs Tra cứu C360 qua FastAPI TestClient (Xác thực X-API-Key SHA-256, Scopes, Cascade Dò 3 tầng, Tìm kiếm linh hoạt, Change Feed).
"""

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

from p1_source_and_mirror.db_env import get_connection, init_c360_schema, init_source_and_mirror_schema
from p3_reconciliation_bi_api.api.main import app
from p3_reconciliation_bi_api.reconcile_3_tiers import run_3tier_reconciliation

DEMO_API_KEY = "c360_secret_key_demo_2026"


def test_01_3tier_reconciliation(tmp_path: Path):
    """Kiểm tra đối soát 3 tầng toàn vẹn số liệu."""
    report = run_3tier_reconciliation(out_path=tmp_path / "test_recon.json")
    assert report["overall_status"] == "PASSED"

    # Tầng 1
    t1 = report["tier1_source_vs_mirror"]
    assert t1["status"] == "PASSED"
    assert t1["tables"]["HO_GD_NHANKHAU"]["diff"] == 0

    # Tầng 2
    t2 = report["tier2_mirror_vs_c360"]
    assert t2["status"] == "PASSED"
    assert t2["golden_equation_diff"] == 0
    assert t2["leaked_invalid_records"] == 0
    assert t2["multi_household_sample_test"]["passed"] is True

    # Tầng 3
    t3 = report["tier3_data_quality"]
    assert t3["total_citizens"] == 48000
    assert t3["clean_citizens"] > 0
    assert "NGAY_SINH_LOI" in t3["flag_breakdown"]


def test_02_bi_datamart_views():
    """Kiểm tra tạo và truy vấn 3 Data Mart Views phục vụ Dashboard BI."""
    conn = get_connection()
    cur = conn.cursor()

    views_sql_path = PROJECT_ROOT / "p3_reconciliation_bi_api" / "views_bi_datamart.sql"
    cur.executescript(views_sql_path.read_text(encoding="utf-8"))
    conn.commit()

    # View 1: Thống kê địa bàn
    cur.execute("SELECT COUNT(*), SUM(TONG_SO_CONG_DAN) FROM V_BI_THONG_KE_DIA_BAN;")
    v1_rows, v1_total = cur.fetchone()
    assert v1_rows > 0
    assert v1_total == 48000

    # View 2: Báo cáo chất lượng dữ liệu
    cur.execute("SELECT COUNT(*), SUM(SO_HO_SO_SACH) FROM V_BI_CHAT_LUONG_DU_LIEU;")
    v2_rows, v2_sach = cur.fetchone()
    assert v2_rows > 0
    assert v2_sach > 0

    # View 3: Giám sát đồng bộ
    cur.execute("SELECT COUNT(*) FROM V_BI_GIAM_SAT_DONG_BO;")
    assert cur.fetchone()[0] >= 1

    conn.close()


def test_03_rest_api_endpoints():
    """Kiểm tra 4 Dịch vụ REST APIs Tra cứu C360 (FastAPI TestClient)."""
    client = TestClient(app)

    # 1. Kiểm tra xác thực X-API-Key
    # Thiếu key -> 401
    resp_no_key = client.get("/api/v1/ca-nhan/0000000001")
    assert resp_no_key.status_code == 401

    # Key sai -> 401
    resp_bad_key = client.get("/api/v1/ca-nhan/0000000001", headers={"X-API-Key": "WRONG_KEY"})
    assert resp_bad_key.status_code == 401

    headers = {"X-API-Key": DEMO_API_KEY}

    # 2. Kiểm tra Healthcheck
    resp_health = client.get("/api/health")
    assert resp_health.status_code == 200
    assert resp_health.json()["status"] == "HEALTHY"

    # Lấy 1 công dân mẫu có sẵn trong DB để kiểm tra
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT MA_SO_BHXH, HO_TEN, SO_DINH_DANH FROM C360_THONG_TIN_CA_NHAN LIMIT 1;")
    sample = cur.fetchone()
    sample_ma = sample[0]
    sample_name = sample[1]
    sample_cccd = sample[2]
    conn.close()

    # 3. API 1: GET /api/v1/ca-nhan/{ma}
    # Tầng 1: Tra theo MA_SO_BHXH hiện hành
    resp_api1 = client.get(f"/api/v1/ca-nhan/{sample_ma}", headers=headers)
    assert resp_api1.status_code == 200
    body1 = resp_api1.json()
    assert body1["matched_by"] == "MA_SO_BHXH"
    assert body1["data"]["ma_so_bhxh"] == sample_ma
    assert body1["data"]["ho_ten"] == sample_name

    # Tầng 3: Tra theo SO_DINH_DANH (nếu có)
    if sample_cccd:
        resp_cccd = client.get(f"/api/v1/ca-nhan/{sample_cccd}", headers=headers)
        assert resp_cccd.status_code in (200, 409)

    # Mã không tồn tại -> 404
    resp_404 = client.get("/api/v1/ca-nhan/999999999999999", headers=headers)
    assert resp_404.status_code == 404

    # 4. API 2: POST /api/v1/ca-nhan/tra-cuu-lo
    resp_batch = client.post(
        "/api/v1/ca-nhan/tra-cuu-lo",
        headers=headers,
        json={"ma_list": [sample_ma, "NON_EXISTING_CODE_999"]},
    )
    assert resp_batch.status_code == 200
    batch_res = resp_batch.json()
    assert batch_res["total_requested"] == 2
    assert batch_res["found_count"] >= 1
    assert batch_res["not_found_count"] >= 1

    # 5. API 3: GET /api/v1/ca-nhan/tim-kiem
    resp_search = client.get(
        "/api/v1/ca-nhan/tim-kiem",
        headers=headers,
        params={"ho_ten": sample_name},
    )
    assert resp_search.status_code == 200
    search_res = resp_search.json()
    assert search_res["total_matched"] >= 1
    assert any(c["ma_so_bhxh"] == sample_ma for c in search_res["data"])

    # 6. API 4: GET /api/v1/ca-nhan/thay-doi
    resp_feed = client.get(
        "/api/v1/ca-nhan/thay-doi",
        headers=headers,
        params={"limit": 50},
    )
    assert resp_feed.status_code == 200
    feed_res = resp_feed.json()
    assert "safe_watermark" in feed_res
    assert isinstance(feed_res["data"], list)
