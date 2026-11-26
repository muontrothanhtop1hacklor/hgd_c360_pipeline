#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Khởi tạo Data Mart Views & Provisioning 3 Nhóm Dashboard Báo cáo BI (Metabase / HTML Snapshot):
  - Nhóm 1: Thống kê Nhân khẩu & Định danh theo Địa bàn 2025 (V_BI_THONG_KE_DIA_BAN)
  - Nhóm 2: Báo cáo Chất lượng Dữ liệu Data Quality 11 cờ DQ_FLAGS (V_BI_CHAT_LUONG_DU_LIEU)
  - Nhóm 3: Giám sát Vận hành Đồng bộ Near Real-time (V_BI_GIAM_SAT_DONG_BO)

Hỗ trợ 2 chế độ:
  1. Local Lightweight Mode (mặc định): Tạo 3 Data Mart Views trong DB và kết xuất Dashboard
     trực quan ra HTML độc lập (`data/bi_dashboard_preview.html`) để xem ngay trên trình duyệt
     mà không tốn 1GB RAM bật container Metabase.
  2. Metabase API Provisioning (`--push-metabase`): Tự động đăng nhập Metabase REST API
     (`METABASE_URL`, mặc định http://localhost:3000) và khởi tạo Collection + 3 nhóm Cards.
"""

import argparse
import html
import json
import os
import sys
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from p1_source_and_mirror.db_env import (
    get_connection,
    init_api_and_bi_schema,
    init_c360_schema,
    init_source_and_mirror_schema,
)


METABASE_CARDS_SPEC = [
    {
        "group": "Nhóm 1 - Nhân khẩu & Định danh 2025",
        "name": "1.1 Phân bổ dân số & Tỷ lệ phủ CCCD 12 số theo 36 Tỉnh/TP",
        "display": "bar",
        "sql": (
            "SELECT MA_TINH, TEN_TINH, SUM(TONG_SO_CONG_DAN) AS TONG_CONG_DAN, "
            "SUM(CO_CCCD_12_SO) AS CO_CCCD_12_SO, SUM(CHI_CO_CMND_9_SO) AS CHI_CO_CMND_9_SO, "
            "ROUND(100.0 * SUM(CO_CCCD_12_SO) / SUM(TONG_SO_CONG_DAN), 2) AS TY_LE_CCCD_PCT "
            "FROM V_BI_THONG_KE_DIA_BAN GROUP BY MA_TINH, TEN_TINH ORDER BY TONG_CONG_DAN DESC;"
        ),
    },
    {
        "group": "Nhóm 1 - Nhân khẩu & Định danh 2025",
        "name": "1.2 Cơ cấu độ tuổi, giới tính & tình trạng còn sống",
        "display": "pie",
        "sql": (
            "SELECT SUM(SO_NAM) AS NAM, SUM(SO_NU) AS NU, SUM(CON_SONG) AS CON_SONG, SUM(DA_MAT) AS DA_MAT, "
            "SUM(DUOI_18_TUOI) AS DUOI_18, SUM(TU_18_DEN_35) AS TU_18_35, "
            "SUM(TU_36_DEN_60) AS TU_36_60, SUM(TREN_60_TUOI) AS TREN_60 "
            "FROM V_BI_THONG_KE_DIA_BAN;"
        ),
    },
    {
        "group": "Nhóm 2 - Chất lượng dữ liệu (Data Quality)",
        "name": "2.1 Xếp hạng Tỉnh/TP theo tỷ lệ dữ liệu bẩn cần làm sạch",
        "display": "table",
        "sql": (
            "SELECT MA_TINH, TEN_TINH, TONG_SO_HO_SO, SO_HO_SO_SACH, SO_HO_SO_CO_LOI, TY_LE_LOI_PCT "
            "FROM V_BI_CHAT_LUONG_DU_LIEU ORDER BY TY_LE_LOI_PCT DESC, SO_HO_SO_CO_LOI DESC;"
        ),
    },
    {
        "group": "Nhóm 2 - Chất lượng dữ liệu (Data Quality)",
        "name": "2.2 Tổng hợp 11 mã cờ lỗi chất lượng dữ liệu DQ_FLAGS",
        "display": "bar",
        "sql": (
            "SELECT SUM(LOI_TINH_KHONG_MAP) AS TINH_KHONG_MAP, SUM(LOI_XA_KHONG_MAP) AS XA_KHONG_MAP, "
            "SUM(LOI_XA_MO_HO) AS XA_MO_HO, SUM(CANH_BAO_TINH_LECH_XA) AS TINH_LECH_XA, "
            "SUM(LOI_NGAY_SINH) AS NGAY_SINH_LOI, SUM(LOI_SO_DIEN_THOAI) AS SDT_LOI, "
            "SUM(THIEU_SO_DINH_DANH) AS KHONG_SO_DINH_DANH, SUM(CANH_BAO_CMND_9_SO) AS CANH_BAO_CMND9, "
            "SUM(LOI_DAN_TOC) AS DAN_TOC_LOI, SUM(LOI_QUOC_TICH) AS QUOC_TICH_LOI, "
            "SUM(THUA_KHOANG_TRANG_HO_TEN) AS HOTEN_THUA_KHOANG_TRANG FROM V_BI_CHAT_LUONG_DU_LIEU;"
        ),
    },
    {
        "group": "Nhóm 3 - Giám sát vận hành đồng bộ (Sync Monitoring)",
        "name": "3.1 Lịch sử các lô chạy Full Load & Near Real-time (MERGE / DELETE / Latency)",
        "display": "line",
        "sql": "SELECT * FROM V_BI_GIAM_SAT_DONG_BO LIMIT 50;",
    },
]


def build_bi_datamart_and_snapshot(
    sqlite_path: Optional[Path] = None,
    html_out_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Khởi tạo 3 Data Mart Views và trích xuất số liệu tổng hợp cho 3 nhóm Dashboard BI."""
    conn = get_connection(sqlite_path=sqlite_path)
    init_source_and_mirror_schema(conn)
    init_c360_schema(conn)
    init_api_and_bi_schema(conn)

    cur = conn.cursor()

    # Nhóm 1: Tổng quan nhân khẩu & Top 10 Tỉnh theo quy mô dân số
    cur.execute(
        """
        SELECT COALESCE(SUM(TONG_SO_CONG_DAN), 0),
               COALESCE(SUM(SO_NAM), 0),
               COALESCE(SUM(SO_NU), 0),
               COALESCE(SUM(CON_SONG), 0),
               COALESCE(SUM(DA_MAT), 0),
               COALESCE(SUM(DUOI_18_TUOI), 0),
               COALESCE(SUM(TU_18_DEN_35), 0),
               COALESCE(SUM(TU_36_DEN_60), 0),
               COALESCE(SUM(TREN_60_TUOI), 0),
               COALESCE(SUM(CO_CCCD_12_SO), 0),
               COALESCE(SUM(CHI_CO_CMND_9_SO), 0),
               COALESCE(SUM(KHONG_DINH_DANH), 0)
          FROM V_BI_THONG_KE_DIA_BAN;
        """
    )
    g1 = cur.fetchone()
    total_pop = g1[0] or 0

    cur.execute(
        """
        SELECT MA_TINH, TEN_TINH,
               SUM(TONG_SO_CONG_DAN) AS TONG_CD,
               SUM(CO_CCCD_12_SO) AS CO_CCCD,
               ROUND(100.0 * SUM(CO_CCCD_12_SO) / SUM(TONG_SO_CONG_DAN), 2) AS PCT_CCCD
          FROM V_BI_THONG_KE_DIA_BAN
         GROUP BY MA_TINH, TEN_TINH
         ORDER BY TONG_CD DESC
         LIMIT 10;
        """
    )
    top_provinces_pop = [
        {"ma_tinh": r[0], "ten_tinh": r[1], "total_citizens": r[2], "cccd_12_count": r[3], "cccd_12_pct": r[4]}
        for r in cur.fetchall()
    ]

    # Nhóm 2: Chất lượng dữ liệu theo tỉnh & 11 cờ lỗi
    cur.execute(
        """
        SELECT COALESCE(SUM(SO_HO_SO_SACH), 0),
               COALESCE(SUM(SO_HO_SO_CO_LOI), 0),
               COALESCE(SUM(LOI_TINH_KHONG_MAP), 0),
               COALESCE(SUM(LOI_XA_KHONG_MAP), 0),
               COALESCE(SUM(LOI_XA_MO_HO), 0),
               COALESCE(SUM(CANH_BAO_TINH_LECH_XA), 0),
               COALESCE(SUM(LOI_NGAY_SINH), 0),
               COALESCE(SUM(LOI_SO_DIEN_THOAI), 0),
               COALESCE(SUM(THIEU_SO_DINH_DANH), 0),
               COALESCE(SUM(CANH_BAO_CMND_9_SO), 0),
               COALESCE(SUM(LOI_DAN_TOC), 0),
               COALESCE(SUM(LOI_QUOC_TICH), 0),
               COALESCE(SUM(THUA_KHOANG_TRANG_HO_TEN), 0)
          FROM V_BI_CHAT_LUONG_DU_LIEU;
        """
    )
    g2 = cur.fetchone()

    cur.execute(
        """
        SELECT MA_TINH, TEN_TINH, TONG_SO_HO_SO, SO_HO_SO_CO_LOI, TY_LE_LOI_PCT
          FROM V_BI_CHAT_LUONG_DU_LIEU
         ORDER BY TY_LE_LOI_PCT DESC, SO_HO_SO_CO_LOI DESC
         LIMIT 10;
        """
    )
    top_dirty_provinces = [
        {"ma_tinh": r[0], "ten_tinh": r[1], "total": r[2], "dirty": r[3], "dirty_pct": r[4]}
        for r in cur.fetchall()
    ]

    # Nhóm 3: Giám sát vận hành đồng bộ
    cur.execute(
        """
        SELECT LOG_ID, BATCH_TYPE, START_TIME, END_TIME,
               SO_KHOA_BIEN_DONG, SO_DONG_MERGE, SO_DONG_DELETE, STATUS, THOI_GIAN_XU_LY_GIAY
          FROM V_BI_GIAM_SAT_DONG_BO
         LIMIT 20;
        """
    )
    sync_logs = [
        {
            "log_id": r[0],
            "batch_type": r[1],
            "start_time": r[2],
            "end_time": r[3],
            "affected_keys": r[4],
            "merge_count": r[5],
            "delete_count": r[6],
            "status": r[7],
            "duration_sec": r[8],
        }
        for r in cur.fetchall()
    ]
    conn.close()

    snapshot = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "cards_configured": len(METABASE_CARDS_SPEC),
        "group1_demographics": {
            "total_citizens": total_pop,
            "male": g1[1],
            "female": g1[2],
            "alive": g1[3],
            "deceased": g1[4],
            "age_buckets": {
                "under_18": g1[5],
                "from_18_to_35": g1[6],
                "from_36_to_60": g1[7],
                "over_60": g1[8],
            },
            "id_coverage": {
                "has_cccd_12": g1[9],
                "only_cmnd_9": g1[10],
                "no_id": g1[11],
                "cccd_12_pct": round(100.0 * g1[9] / total_pop, 2) if total_pop else 0.0,
            },
            "top_provinces": top_provinces_pop,
        },
        "group2_data_quality": {
            "clean_records": g2[0],
            "dirty_records": g2[1],
            "clean_pct": round(100.0 * g2[0] / total_pop, 2) if total_pop else 0.0,
            "flags": {
                "TINH_KHONG_MAP": g2[2],
                "XA_KHONG_MAP": g2[3],
                "XA_MO_HO": g2[4],
                "TINH_LECH_XA": g2[5],
                "NGAY_SINH_LOI": g2[6],
                "SDT_LOI": g2[7],
                "KHONG_SO_DINH_DANH": g2[8],
                "CANH_BAO_CMND9": g2[9],
                "DAN_TOC_LOI": g2[10],
                "QUOC_TICH_LOI": g2[11],
                "HOTEN_THUA_KHOANG_TRANG": g2[12],
            },
            "top_dirty_provinces": top_dirty_provinces,
        },
        "group3_sync_monitoring": {
            "recent_batches": sync_logs,
        },
    }

    target_html = Path(html_out_path) if html_out_path else PROJECT_ROOT / "data" / "bi_dashboard_preview.html"
    target_html.parent.mkdir(parents=True, exist_ok=True)
    target_html.write_text(_render_html_dashboard(snapshot), encoding="utf-8")

    return snapshot


def _render_html_dashboard(snap: Dict[str, Any]) -> str:
    g1 = snap["group1_demographics"]
    g2 = snap["group2_data_quality"]
    g3 = snap["group3_sync_monitoring"]

    prov_rows = "".join(
        f"<tr><td>{html.escape(str(p['ma_tinh']))}</td><td>{html.escape(str(p['ten_tinh']))}</td>"
        f"<td>{p['total_citizens']:,}</td><td>{p['cccd_12_count']:,}</td><td>{p['cccd_12_pct']}%</td></tr>"
        for p in g1["top_provinces"]
    )
    flag_rows = "".join(
        f"<tr><td><code>{k}</code></td><td>{v:,}</td></tr>"
        for k, v in g2["flags"].items()
    )
    sync_rows = "".join(
        f"<tr><td>{s['log_id']}</td><td>{html.escape(str(s['batch_type']))}</td>"
        f"<td>{html.escape(str(s['start_time']))}</td><td>{s['affected_keys']:,}</td>"
        f"<td>{s['merge_count']:,}</td><td>{s['delete_count']:,}</td>"
        f"<td><b>{html.escape(str(s['status']))}</b></td><td>{s['duration_sec']}s</td></tr>"
        for s in g3["recent_batches"]
    )

    return f"""<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="utf-8">
<title>HGD C360 BI Dashboard Preview</title>
<style>
body {{ font-family: Segoe UI, Arial, sans-serif; margin: 24px; background: #f8fafc; color: #0f172a; }}
h1 {{ margin-bottom: 4px; }}
.kpi-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin: 20px 0; }}
.card {{ background: #fff; border: 1px solid #e2e8f0; border-radius: 8px; padding: 16px; }}
.kpi-val {{ font-size: 24px; font-weight: 700; color: #0284c7; }}
table {{ width: 100%; border-collapse: collapse; margin-top: 8px; font-size: 14px; }}
th, td {{ border-bottom: 1px solid #e2e8f0; padding: 8px; text-align: left; }}
th {{ background: #f1f5f9; }}
.grid-2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 20px; }}
</style>
</head>
<body>
<h1>Báo cáo BI Customer 360 (Hộ gia đình - Nhân khẩu 2025)</h1>
<p>Thời điểm trích xuất: <b>{snap['generated_at']}</b></p>
<div class="kpi-grid">
  <div class="card"><div>Tổng công dân hợp lệ C360</div><div class="kpi-val">{g1['total_citizens']:,}</div></div>
  <div class="card"><div>Tỷ lệ phủ CCCD 12 số</div><div class="kpi-val">{g1['id_coverage']['cccd_12_pct']}%</div></div>
  <div class="card"><div>Hồ sơ sạch hoàn toàn</div><div class="kpi-val">{g2['clean_records']:,} ({g2['clean_pct']}%)</div></div>
  <div class="card"><div>Còn sống / Đã mất</div><div class="kpi-val">{g1['alive']:,} / {g1['deceased']:,}</div></div>
</div>
<div class="grid-2">
  <div class="card">
    <h3>Nhóm 1: Top 10 Tỉnh/TP theo quy mô dân số & độ phủ CCCD 12 số</h3>
    <table><tr><th>Mã Tỉnh</th><th>Tên Tỉnh/TP</th><th>Công dân</th><th>Có CCCD 12 số</th><th>Tỷ lệ</th></tr>{prov_rows}</table>
  </div>
  <div class="card">
    <h3>Nhóm 2: Thống kê 11 mã cờ chất lượng dữ liệu (DQ_FLAGS)</h3>
    <table><tr><th>Mã cờ lỗi</th><th>Số hồ sơ</th></tr>{flag_rows}</table>
  </div>
</div>
<div class="card">
  <h3>Nhóm 3: Giám sát Vận hành Đồng bộ Full Load & Near Real-time</h3>
  <table><tr><th>Log ID</th><th>Loại Lô</th><th>Bắt đầu</th><th>Khóa biến động</th><th>MERGE</th><th>DELETE</th><th>Trạng thái</th><th>Thời gian</th></tr>{sync_rows}</table>
</div>
</body>
</html>"""


def provision_metabase_via_api(
    metabase_url: str,
    username: str,
    password: str,
    database_id: int = 1,
) -> Dict[str, Any]:
    """Đẩy tự động Collection và 5 biểu đồ thuộc 3 nhóm báo cáo lên Metabase REST API."""
    base = metabase_url.rstrip("/")
    login_req = urllib.request.Request(
        f"{base}/api/session",
        data=json.dumps({"username": username, "password": password}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(login_req, timeout=10) as resp:
        token = json.loads(resp.read().decode("utf-8"))["id"]

    headers = {"Content-Type": "application/json", "X-Metabase-Session": token}
    col_req = urllib.request.Request(
        f"{base}/api/collection",
        data=json.dumps({"name": "HGD C360 Analytics 2025", "color": "#509EE3"}).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(col_req, timeout=10) as resp:
        collection_id = json.loads(resp.read().decode("utf-8"))["id"]

    created_cards: List[int] = []
    for card in METABASE_CARDS_SPEC:
        payload = {
            "name": card["name"],
            "display": card["display"],
            "collection_id": collection_id,
            "dataset_query": {
                "type": "native",
                "native": {"query": card["sql"]},
                "database": database_id,
            },
            "visualization_settings": {},
        }
        c_req = urllib.request.Request(
            f"{base}/api/card",
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        with urllib.request.urlopen(c_req, timeout=10) as resp:
            created_cards.append(json.loads(resp.read().decode("utf-8"))["id"])

    return {"collection_id": collection_id, "card_ids": created_cards}


def main():
    parser = argparse.ArgumentParser(description="Khởi tạo BI Data Mart Views & Dashboard C360")
    parser.add_argument("--db-path", type=str, default=None, help="Đường dẫn file SQLite DB")
    parser.add_argument("--html-out", type=str, default=None, help="Đường dẫn xuất file HTML Dashboard")
    parser.add_argument("--push-metabase", action="store_true", help="Đẩy cấu hình biểu đồ lên Metabase Server")
    parser.add_argument(
        "--metabase-url",
        type=str,
        default=os.environ.get("METABASE_URL", "http://localhost:3000"),
        help="URL Metabase Server",
    )
    parser.add_argument("--metabase-user", type=str, default=os.environ.get("METABASE_USER", ""))
    parser.add_argument("--metabase-password", type=str, default=os.environ.get("METABASE_PASSWORD", ""))
    args = parser.parse_args()

    db_path = Path(args.db_path) if args.db_path else None
    html_out = Path(args.html_out) if args.html_out else None

    print("=" * 72)
    print(" KHỞI TẠO BI DATA MART VIEWS & TRÍCH XUẤT 3 NHÓM BÁO CÁO BI C360")
    print("=" * 72)

    snap = build_bi_datamart_and_snapshot(sqlite_path=db_path, html_out_path=html_out)
    g1 = snap["group1_demographics"]
    g2 = snap["group2_data_quality"]
    print(f" [OK] Tổng số công dân C360        : {g1['total_citizens']:,}")
    print(f" [OK] Tỷ lệ phủ CCCD 12 số         : {g1['id_coverage']['cccd_12_pct']}% ({g1['id_coverage']['has_cccd_12']:,} người)")
    print(f" [OK] Tỷ lệ hồ sơ sạch hoàn toàn   : {g2['clean_pct']}% ({g2['clean_records']:,} người)")
    print(f" [OK] Đã xuất Dashboard HTML tại   : {html_out or (PROJECT_ROOT / 'data' / 'bi_dashboard_preview.html')}")

    if args.push_metabase and args.metabase_user and args.metabase_password:
        res = provision_metabase_via_api(args.metabase_url, args.metabase_user, args.metabase_password)
        print(f" [OK] Đã tạo Metabase Collection ID={res['collection_id']} với {len(res['card_ids'])} Cards")
    print("=" * 72)


if __name__ == "__main__":
    main()
