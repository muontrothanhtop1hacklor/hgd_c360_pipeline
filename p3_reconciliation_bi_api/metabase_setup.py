#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Khởi tạo 5 Data Mart Views & Trực quan hóa Dashboard Báo cáo BI "Thông tin cá nhân – Gold"
kèm Giám sát CDC Kafka & Đồng bộ Near Real-time:
  - Tab 1 (Thông tin cá nhân – Gold): 6 biểu đồ trực quan chuẩn bố cục Superset Dark-Mode:
      1. Phân bổ theo nhóm tuổi (cột đôi Nữ vs Nam)
      2. Tổng số người (KPI Big Number + độ phủ CCCD 12 số & tỷ lệ hồ sơ sạch)
      3. Nhóm dân tộc (biểu đồ tròn Kinh / Dân tộc thiểu số / Chưa xác định)
      4. Dân tộc thiểu số (biểu đồ thanh ngang Top dân tộc thiểu số)
      5. Tỉnh thường trú và liên hệ (biểu đồ thanh ngang kép Thường trú vs Liên hệ)
      6. Chất lượng dữ liệu (biểu đồ thanh ngang toàn bộ các loại lỗi DQ_FLAGS)
  - Tab 2 (CDC Kafka & Sync Monitor): Bảng giám sát Kafka Topics (`vss.VSS.VSS_HO_GD_NHANKHAU`
    messages & size MB), trạng thái hàng đợi `C360_SYNC_QUEUE` và lịch sử `V_BI_GIAM_SAT_DONG_BO`.

Hỗ trợ 3 cách khai thác:
  1. Live Web Dashboard qua FastAPI: mở `http://localhost:8000/dashboard` (tự động cập nhật số liệu
     trực tiếp từ DB qua `/api/v1/bi/dashboard-data` mỗi 5 giây khi chạy script biến động).
  2. File HTML độc lập không cần server: `data/bi_dashboard_preview.html` (nhúng sẵn JSON & SVG/JS).
  3. Đẩy tự động Collection & Cards lên Metabase REST API (`--push-metabase`).
"""

import argparse
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
        "name": "1.2 Phân bổ nhóm tuổi theo giới tính & Nhóm dân tộc",
        "display": "bar",
        "sql": "SELECT NHOM_DAN_TOC, SUM(SO_NGUOI) AS TONG_SO FROM V_BI_NHOM_DAN_TOC GROUP BY NHOM_DAN_TOC;",
    },
    {
        "group": "Nhóm 1 - Nhân khẩu & Định danh 2025",
        "name": "1.3 Đối sánh Tỉnh thường trú và Tỉnh liên hệ",
        "display": "bar",
        "sql": "SELECT * FROM V_BI_TINH_THUONG_TRU_LIEN_HE ORDER BY SO_THUONG_TRU DESC LIMIT 10;",
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
        "group": "Nhóm 3 - Giám sát vận hành đồng bộ (Sync Monitoring)",
        "name": "3.1 Lịch sử các lô chạy Full Load & Near Real-time (MERGE / DELETE / Latency)",
        "display": "line",
        "sql": "SELECT * FROM V_BI_GIAM_SAT_DONG_BO LIMIT 50;",
    },
]


def query_bi_dashboard_payload(sqlite_path: Optional[Path] = None) -> Dict[str, Any]:
    """Truy vấn trực tiếp 5 Data Mart Views + CDC/Queue telemetry để cấp dữ liệu cho 6 biểu đồ BI."""
    conn = get_connection(sqlite_path=sqlite_path)
    init_source_and_mirror_schema(conn)
    init_c360_schema(conn)
    init_api_and_bi_schema(conn)

    cur = conn.cursor()

    # 1. Tổng quan nhân khẩu & Phân bổ nhóm tuổi theo Nam / Nữ
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
               COALESCE(SUM(KHONG_DINH_DANH), 0),
               COALESCE(SUM(NU_KHONG_RO_TUOI), 0),
               COALESCE(SUM(NAM_KHONG_RO_TUOI), 0),
               COALESCE(SUM(NU_DUOI_18), 0),
               COALESCE(SUM(NAM_DUOI_18), 0),
               COALESCE(SUM(NU_18_35), 0),
               COALESCE(SUM(NAM_18_35), 0),
               COALESCE(SUM(NU_36_60), 0),
               COALESCE(SUM(NAM_36_60), 0),
               COALESCE(SUM(NU_TREN_60), 0),
               COALESCE(SUM(NAM_TREN_60), 0)
          FROM V_BI_THONG_KE_DIA_BAN;
        """
    )
    g1 = cur.fetchone()
    total_pop = g1[0] or 0

    age_gender_series = [
        {"label": "0. Không xác định", "nu": g1[12], "nam": g1[13]},
        {"label": "1. Dưới 18 tuổi", "nu": g1[14], "nam": g1[15]},
        {"label": "2. Thanh niên (18-35)", "nu": g1[16], "nam": g1[17]},
        {"label": "3. Trung niên (36-60)", "nu": g1[18], "nam": g1[19]},
        {"label": "4. Cao tuổi (>60)", "nu": g1[20], "nam": g1[21]},
    ]

    # Top tỉnh theo dân số
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

    # 2. Biểu đồ Nhóm dân tộc & Top Dân tộc thiểu số (V_BI_NHOM_DAN_TOC)
    cur.execute(
        """
        SELECT NHOM_DAN_TOC, SUM(SO_NGUOI) AS TONG_SO
          FROM V_BI_NHOM_DAN_TOC
         GROUP BY NHOM_DAN_TOC;
        """
    )
    eth_groups_raw = dict(cur.fetchall())
    kinh_cnt = eth_groups_raw.get("Kinh", 0)
    minority_cnt = eth_groups_raw.get("Dân tộc thiểu số", 0)
    unknown_eth_cnt = eth_groups_raw.get("Chưa xác định", 0)

    ethnic_pie = [
        {
            "label": "Kinh",
            "count": kinh_cnt,
            "pct": round(100.0 * kinh_cnt / total_pop, 2) if total_pop else 0.0,
            "color": "#22d3ee",
        },
        {
            "label": "Dân tộc thiểu số",
            "count": minority_cnt,
            "pct": round(100.0 * minority_cnt / total_pop, 2) if total_pop else 0.0,
            "color": "#6366f1",
        },
        {
            "label": "Chưa xác định",
            "count": unknown_eth_cnt,
            "pct": round(100.0 * unknown_eth_cnt / total_pop, 2) if total_pop else 0.0,
            "color": "#34d399",
        },
    ]

    cur.execute(
        """
        SELECT TEN_DAN_TOC, SO_NGUOI
          FROM V_BI_NHOM_DAN_TOC
         WHERE NHOM_DAN_TOC = 'Dân tộc thiểu số'
         ORDER BY SO_NGUOI DESC, TEN_DAN_TOC ASC
         LIMIT 14;
        """
    )
    minority_bars = [{"name": r[0], "count": r[1]} for r in cur.fetchall()]

    # 3. Biểu đồ Tỉnh thường trú và liên hệ (V_BI_TINH_THUONG_TRU_LIEN_HE)
    cur.execute(
        """
        SELECT MA_TINH, TEN_TINH, SO_THUONG_TRU, SO_LIEN_HE
          FROM V_BI_TINH_THUONG_TRU_LIEN_HE
         WHERE MA_TINH <> 'CHUA_XAC_DINH'
         ORDER BY SO_THUONG_TRU DESC
         LIMIT 6;
        """
    )
    province_tt_lh = [
        {"ma_tinh": r[0], "ten_tinh": r[1], "thuong_tru": r[2], "lien_he": r[3]}
        for r in cur.fetchall()
    ]
    cur.execute(
        """
        SELECT TEN_TINH, SO_THUONG_TRU, SO_LIEN_HE
          FROM V_BI_TINH_THUONG_TRU_LIEN_HE
         WHERE MA_TINH = 'CHUA_XAC_DINH';
        """
    )
    unk_prov = cur.fetchone()
    if unk_prov:
        province_tt_lh.append(
            {"ma_tinh": "CHUA_XAC_DINH", "ten_tinh": "Chưa xác định", "thuong_tru": unk_prov[1], "lien_he": unk_prov[2]}
        )

    # 4. Biểu đồ Chất lượng dữ liệu (V_BI_CHAT_LUONG_DU_LIEU)
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
               COALESCE(SUM(THUA_KHOANG_TRANG_HO_TEN), 0),
               COALESCE(SUM(LOI_TINH_LH_KHONG_MAP), 0),
               COALESCE(SUM(LOI_XA_LH_KHONG_MAP), 0),
               COALESCE(SUM(CANH_BAO_TINH_LH_LECH_XA), 0)
          FROM V_BI_CHAT_LUONG_DU_LIEU;
        """
    )
    g2 = cur.fetchone()

    dq_bars = [
        {"label": "Xã thường trú không có trong danh mục", "code": "XA_KHONG_MAP", "count": g2[3]},
        {"label": "Xã thường trú mơ hồ (tách >= 2 xã mới)", "code": "XA_MO_HO", "count": g2[4]},
        {"label": "Xã liên hệ không có trong danh mục", "code": "XA_LH_KHONG_MAP", "count": g2[14]},
        {"label": "Tỉnh thường trú không khớp với xã", "code": "TINH_LECH_XA", "count": g2[5]},
        {"label": "Tỉnh thường trú không có trong danh mục", "code": "TINH_KHONG_MAP", "count": g2[2]},
        {"label": "Tỉnh liên hệ không khớp với xã", "code": "TINH_LH_LECH_XA", "count": g2[15]},
        {"label": "Tỉnh liên hệ không có trong danh mục", "code": "TINH_LH_KHONG_MAP", "count": g2[13]},
        {"label": "Thiếu số định danh cá nhân 12 số", "code": "KHONG_SO_DINH_DANH", "count": g2[8]},
        {"label": "Chỉ có CMND 9 số cũ", "code": "CANH_BAO_CMND9", "count": g2[9]},
        {"label": "Số điện thoại không hợp lệ", "code": "SDT_LOI", "count": g2[7]},
        {"label": "Quốc tịch không có trong danh mục", "code": "QUOC_TICH_LOI", "count": g2[11]},
        {"label": "Ngày sinh không hợp lệ", "code": "NGAY_SINH_LOI", "count": g2[6]},
        {"label": "Dân tộc không có trong danh mục / hết hạn", "code": "DAN_TOC_LOI", "count": g2[10]},
        {"label": "Họ tên thừa khoảng trắng", "code": "HOTEN_THUA_KHOANG_TRANG", "count": g2[12]},
    ]

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

    # 5. Giám sát CDC Kafka Topic, Hàng đợi Queue & Lịch sử Đồng bộ
    cur.execute("SELECT COUNT(*) FROM VSS_HO_GD_NHANKHAU;")
    source_rows = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM MIR_VSS_HO_GD_NHANKHAU;")
    mirror_rows = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM C360_SYNC_QUEUE WHERE STATUS = 'PENDING';")
    pending_queue = cur.fetchone()[0]

    cur.execute(
        """
        SELECT COALESCE(SUM(TOTAL_MESSAGES), 0), COALESCE(SUM(PAYLOAD_BYTES), 0), COUNT(*)
          FROM MIR_CDC_TOPIC_METRICS;
        """
    )
    cdc_tot_msgs, cdc_tot_bytes, cdc_batches = cur.fetchone()

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

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "cards_configured": len(METABASE_CARDS_SPEC),
        "pipeline_counts": {
            "source_rows": source_rows,
            "mirror_rows": mirror_rows,
            "c360_citizens": total_pop,
            "queue_pending": pending_queue,
        },
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
            "age_gender_series": age_gender_series,
            "ethnic_pie": ethnic_pie,
            "minority_bars": minority_bars,
            "province_tt_lh": province_tt_lh,
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
            "dq_bars": dq_bars,
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
            "cdc_topic": {
                "topic_name": "vss.VSS.VSS_HO_GD_NHANKHAU",
                "partitions": 3,
                "replication_factor": 1,
                "total_messages": cdc_tot_msgs,
                "size_mb": round(cdc_tot_bytes / (1024 * 1024), 2),
                "batches_pushed": cdc_batches,
            },
            "queue_pending": pending_queue,
            "recent_batches": sync_logs,
        },
    }


def build_bi_datamart_and_snapshot(
    sqlite_path: Optional[Path] = None,
    html_out_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Khởi tạo 5 Data Mart Views và xuất file HTML Dashboard trực quan."""
    snapshot = query_bi_dashboard_payload(sqlite_path=sqlite_path)
    target_html = Path(html_out_path) if html_out_path else PROJECT_ROOT / "data" / "bi_dashboard_preview.html"
    target_html.parent.mkdir(parents=True, exist_ok=True)
    target_html.write_text(render_superset_style_dashboard(snapshot, live_api_mode=False), encoding="utf-8")
    return snapshot


def render_superset_style_dashboard(snapshot: Dict[str, Any], live_api_mode: bool = False) -> str:
    """Render trang Dashboard Dark-Mode với 6 biểu đồ SVG/JS tương tác chuẩn bố cục Superset + Kafka UI."""
    payload_json = json.dumps(snapshot, ensure_ascii=False)
    live_flag = "true" if live_api_mode else "false"

    return f"""<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Thông tin cá nhân – Gold | HGD C360 Analytics</title>
<style>
  :root {{
    --bg-main: #0b0e14;
    --bg-header: #111622;
    --bg-card: #151b28;
    --border: #232c3d;
    --text-main: #f1f5f9;
    --text-muted: #94a3b8;
    --cyan: #22d3ee;
    --indigo: #7c83fd;
    --emerald: #34d399;
    --amber: #fbbf24;
  }}
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0; font-family: Inter, 'Segoe UI', system-ui, sans-serif;
    background: var(--bg-main); color: var(--text-main);
  }}
  .topbar {{
    display: flex; align-items: center; justify-content: space-between;
    padding: 12px 24px; background: var(--bg-header); border-bottom: 1px solid var(--border);
    position: sticky; top: 0; z-index: 20;
  }}
  .brand {{ display: flex; align-items: center; gap: 14px; }}
  .brand-logo {{
    font-weight: 800; letter-spacing: 0.4px; color: var(--cyan);
    padding: 4px 10px; border: 1px solid rgba(34,211,238,0.35); border-radius: 6px; font-size: 13px;
  }}
  .brand-title {{ font-size: 18px; font-weight: 700; }}
  .badge-pub {{
    background: rgba(52,211,153,0.14); color: var(--emerald);
    border: 1px solid rgba(52,211,153,0.35); padding: 2px 9px; border-radius: 999px; font-size: 12px;
  }}
  .tabs {{ display: flex; gap: 8px; }}
  .tab-btn {{
    background: transparent; color: var(--text-muted); border: 1px solid var(--border);
    padding: 7px 14px; border-radius: 6px; cursor: pointer; font-size: 13px; font-weight: 600;
  }}
  .tab-btn.active {{ background: #1e293b; color: var(--cyan); border-color: var(--cyan); }}
  .controls {{ display: flex; align-items: center; gap: 10px; font-size: 12px; color: var(--text-muted); }}
  .btn {{
    background: #0284c7; color: #fff; border: none; padding: 7px 13px;
    border-radius: 6px; cursor: pointer; font-weight: 600; font-size: 12px;
  }}
  .btn:hover {{ background: #0369a1; }}
  .container {{ padding: 18px 22px; max-width: 1720px; margin: 0 auto; }}
  .grid-superset {{
    display: grid; grid-template-columns: 1.25fr 0.9fr 1.05fr; gap: 16px; margin-bottom: 16px;
  }}
  .grid-bottom {{
    display: grid; grid-template-columns: 1fr 1.1fr 1.2fr; gap: 16px;
  }}
  .card {{
    background: var(--bg-card); border: 1px solid var(--border);
    border-radius: 8px; padding: 16px 18px; display: flex; flex-direction: column;
    min-height: 325px;
  }}
  .card-header {{
    display: flex; align-items: center; justify-content: space-between; margin-bottom: 10px;
  }}
  .card-title {{ font-size: 14px; font-weight: 600; color: #e2e8f0; }}
  .legend {{ display: flex; gap: 12px; font-size: 12px; color: var(--text-muted); align-items: center; }}
  .dot {{ width: 10px; height: 10px; border-radius: 2px; display: inline-block; margin-right: 4px; }}
  .kpi-center {{
    flex: 1; display: flex; flex-direction: column; align-items: center; justify-content: center;
  }}
  .big-number {{
    font-size: 64px; font-weight: 700; letter-spacing: -1px; color: #ffffff; margin: 8px 0 14px;
  }}
  .sub-kpis {{
    width: 100%; display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-top: auto;
    padding-top: 12px; border-top: 1px solid var(--border); font-size: 12px;
  }}
  .sub-kpi-item {{ background: #101521; padding: 8px 10px; border-radius: 6px; }}
  .sub-kpi-val {{ font-size: 15px; font-weight: 700; color: var(--cyan); margin-top: 2px; }}
  .hbar-list {{ display: flex; flex-direction: column; gap: 7px; flex: 1; justify-content: center; }}
  .hbar-row {{ display: grid; grid-template-columns: 195px 1fr 62px; align-items: center; gap: 8px; font-size: 12px; }}
  .hbar-row.narrow-lbl {{ grid-template-columns: 110px 1fr 56px; }}
  .hbar-lbl {{ color: #cbd5e1; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; text-align: right; }}
  .hbar-track {{ background: #0e131d; height: 13px; border-radius: 3px; overflow: hidden; position: relative; }}
  .hbar-fill {{ height: 100%; background: var(--cyan); border-radius: 3px; transition: width 0.35s ease; }}
  .hbar-val {{ color: var(--text-muted); font-variant-numeric: tabular-nums; text-align: right; }}
  .dual-row {{ display: grid; grid-template-columns: 145px 1fr 105px; align-items: center; gap: 10px; font-size: 12px; margin-bottom: 8px; }}
  .dual-bars {{ display: flex; flex-direction: column; gap: 3px; }}
  .dual-bar {{ height: 9px; border-radius: 2px; transition: width 0.35s ease; }}
  table.kafka-tbl {{ width: 100%; border-collapse: collapse; font-size: 13px; margin-top: 8px; }}
  table.kafka-tbl th, table.kafka-tbl td {{ padding: 10px 12px; border-bottom: 1px solid var(--border); text-align: left; }}
  table.kafka-tbl th {{ color: var(--text-muted); font-weight: 600; background: #101521; }}
  .pill-ok {{ background: rgba(52,211,153,0.15); color: var(--emerald); padding: 2px 8px; border-radius: 4px; font-weight: 600; font-size: 11px; }}
  .hidden {{ display: none !important; }}
</style>
</head>
<body>
  <div class="topbar">
    <div class="brand">
      <span class="brand-logo">C360 BI</span>
      <span class="brand-title">Thông tin cá nhân – Gold</span>
      <span class="badge-pub">Published</span>
    </div>
    <div class="tabs">
      <button id="btnTabGold" class="tab-btn active" onclick="switchTab('gold')">1. Thông tin cá nhân – Gold</button>
      <button id="btnTabKafka" class="tab-btn" onclick="switchTab('kafka')">2. UI for Apache Kafka & NRT Sync</button>
    </div>
    <div class="controls">
      <span>Cập nhật: <b id="lblUpdatedAt">--</b></span>
      <label id="lblAutoWrap" style="display:flex;align-items:center;gap:4px;cursor:pointer;">
        <input type="checkbox" id="chkAutoRefresh" onchange="toggleAutoRefresh()"> Auto-refresh 5s
      </label>
      <button id="btnRefreshNow" class="btn" onclick="fetchLive()">Làm mới ngay</button>
    </div>
  </div>

  <!-- TAB 1: SUPERSET GOLD DASHBOARD -->
  <div id="tabGold" class="container">
    <div class="grid-superset">
      <div class="card">
        <div class="card-header">
          <span class="card-title">Phân bổ theo nhóm tuổi</span>
          <div class="legend">
            <span><i class="dot" style="background:var(--cyan)"></i>Nữ</span>
            <span><i class="dot" style="background:var(--indigo)"></i>Nam</span>
          </div>
        </div>
        <div id="chartAgeGender" style="flex:1;display:flex;align-items:flex-end;"></div>
      </div>

      <div class="card">
        <div class="card-header">
          <span class="card-title">Tổng số người</span>
          <span style="font-size:12px;color:var(--text-muted)">C360 Gold Layer</span>
        </div>
        <div class="kpi-center">
          <div id="kpiTotalCitizens" class="big-number">0</div>
          <div style="font-size:12px;color:var(--text-muted)">1 dòng duy nhất / công dân hợp lệ (MA_SO_BHXH)</div>
        </div>
        <div class="sub-kpis">
          <div class="sub-kpi-item"><div>Phủ CCCD 12 số</div><div id="kpiCccdPct" class="sub-kpi-val">0%</div></div>
          <div class="sub-kpi-item"><div>Hồ sơ sạch hoàn toàn</div><div id="kpiCleanPct" class="sub-kpi-val">0%</div></div>
          <div class="sub-kpi-item"><div>Nguồn / Mirror</div><div id="kpiSrcMir" class="sub-kpi-val">0</div></div>
          <div class="sub-kpi-item"><div>Còn sống / Đã mất</div><div id="kpiAliveDead" class="sub-kpi-val">0</div></div>
        </div>
      </div>

      <div class="card">
        <div class="card-header">
          <span class="card-title">Nhóm dân tộc</span>
          <div class="legend">
            <span><i class="dot" style="background:var(--cyan)"></i>Kinh</span>
            <span><i class="dot" style="background:var(--indigo)"></i>Thiểu số</span>
            <span><i class="dot" style="background:var(--emerald)"></i>Chưa XĐ</span>
          </div>
        </div>
        <div id="chartEthnicPie" style="flex:1;display:flex;align-items:center;justify-content:center;"></div>
      </div>
    </div>

    <div class="grid-bottom">
      <div class="card">
        <div class="card-header">
          <span class="card-title">Dân tộc thiểu số</span>
          <div class="legend"><span><i class="dot" style="background:var(--cyan)"></i>Số người</span></div>
        </div>
        <div id="chartMinority" class="hbar-list"></div>
      </div>

      <div class="card">
        <div class="card-header">
          <span class="card-title">Tỉnh thường trú và liên hệ</span>
          <div class="legend">
            <span><i class="dot" style="background:var(--cyan)"></i>Thường trú</span>
            <span><i class="dot" style="background:var(--indigo)"></i>Liên hệ</span>
          </div>
        </div>
        <div id="chartProvinceTtLh" style="flex:1;display:flex;flex-direction:column;justify-content:center;"></div>
      </div>

      <div class="card">
        <div class="card-header">
          <span class="card-title">Chất lượng dữ liệu</span>
          <div class="legend"><span><i class="dot" style="background:var(--cyan)"></i>Tổng số bản ghi</span></div>
        </div>
        <div id="chartDqFlags" class="hbar-list"></div>
      </div>
    </div>
  </div>

  <!-- TAB 2: KAFKA CDC TOPICS & NRT SYNC MONITOR -->
  <div id="tabKafka" class="container hidden">
    <div class="card" style="min-height:auto;margin-bottom:16px;">
      <div class="card-header">
        <span class="card-title" style="font-size:16px;">UI for Apache Kafka — Topics (Debezium CDC Stream)</span>
        <span id="badgeQueuePending" class="badge-pub">Queue Pending: 0</span>
      </div>
      <table class="kafka-tbl">
        <thead>
          <tr>
            <th>Topic Name</th><th>Partitions</th><th>Out of sync replicas</th>
            <th>Replication Factor</th><th>Number of messages</th><th>Size</th>
          </tr>
        </thead>
        <tbody id="tblKafkaTopics"></tbody>
      </table>
    </div>

    <div class="card" style="min-height:auto;">
      <div class="card-header">
        <span class="card-title" style="font-size:16px;">Lịch sử Vận hành Đồng bộ Full Load & Near Real-time (C360_SYNC_LOG)</span>
      </div>
      <table class="kafka-tbl">
        <thead>
          <tr>
            <th>Log ID</th><th>Batch Type</th><th>Start Time</th><th>End Time</th>
            <th>Affected MA_CD</th><th>MERGE</th><th>DELETE</th><th>Latency</th><th>Status</th>
          </tr>
        </thead>
        <tbody id="tblSyncLogs"></tbody>
      </table>
    </div>
  </div>

<script>
const INITIAL_DATA = {payload_json};
const LIVE_API_MODE = {live_flag};
let autoTimer = null;

if (!LIVE_API_MODE) {{
  document.getElementById('lblAutoWrap').style.display = 'none';
  document.getElementById('btnRefreshNow').style.display = 'none';
}}

function fmt(n) {{ return Number(n || 0).toLocaleString('en-US'); }}

function switchTab(name) {{
  document.getElementById('tabGold').classList.toggle('hidden', name !== 'gold');
  document.getElementById('tabKafka').classList.toggle('hidden', name !== 'kafka');
  document.getElementById('btnTabGold').classList.toggle('active', name === 'gold');
  document.getElementById('btnTabKafka').classList.toggle('active', name === 'kafka');
}}

function renderAgeGenderChart(series) {{
  const maxVal = Math.max(1, ...series.flatMap(s => [s.nu, s.nam]));
  const w = 520, h = 235, padL = 42, padB = 38, padT = 14, plotW = w - padL - 12, plotH = h - padB - padT;
  let grid = '';
  for (let i = 0; i <= 4; i++) {{
    const y = padT + plotH - (i / 4) * plotH;
    const v = Math.round((i / 4) * maxVal);
    grid += `<line x1="${{padL}}" y1="${{y}}" x2="${{w-8}}" y2="${{y}}" stroke="#232c3d" stroke-dasharray="3,3"/>`;
    grid += `<text x="${{padL-6}}" y="${{y+4}}" fill="#94a3b8" font-size="10" text-anchor="end">${{v >= 1000 ? (v/1000).toFixed(0)+'k' : v}}</text>`;
  }}
  const slotW = plotW / series.length;
  const barW = Math.min(22, slotW * 0.32);
  let bars = '';
  series.forEach((s, idx) => {{
    const cx = padL + idx * slotW + slotW / 2;
    const hNu = (s.nu / maxVal) * plotH;
    const hNam = (s.nam / maxVal) * plotH;
    bars += `<rect x="${{cx - barW - 2}}" y="${{padT + plotH - hNu}}" width="${{barW}}" height="${{hNu}}" fill="#22d3ee" rx="2"><title>Nữ (${{s.label}}): ${{fmt(s.nu)}}</title></rect>`;
    bars += `<rect x="${{cx + 2}}" y="${{padT + plotH - hNam}}" width="${{barW}}" height="${{hNam}}" fill="#7c83fd" rx="2"><title>Nam (${{s.label}}): ${{fmt(s.nam)}}</title></rect>`;
    bars += `<text x="${{cx}}" y="${{h - 14}}" fill="#94a3b8" font-size="10" text-anchor="middle">${{s.label}}</text>`;
  }});
  document.getElementById('chartAgeGender').innerHTML =
    `<svg viewBox="0 0 ${{w}} ${{h}}" style="width:100%;height:100%;">${{grid}}${{bars}}</svg>`;
}}

function renderEthnicPie(slices) {{
  const total = Math.max(1, slices.reduce((a, b) => a + b.count, 0));
  let cum = 0;
  const cx = 135, cy = 115, r = 82;
  let paths = '';
  slices.forEach(s => {{
    if (s.count <= 0) return;
    const startAngle = (cum / total) * Math.PI * 2 - Math.PI / 2;
    cum += s.count;
    const endAngle = (cum / total) * Math.PI * 2 - Math.PI / 2;
    const x1 = cx + r * Math.cos(startAngle), y1 = cy + r * Math.sin(startAngle);
    const x2 = cx + r * Math.cos(endAngle), y2 = cy + r * Math.sin(endAngle);
    const largeArc = (s.count / total) > 0.5 ? 1 : 0;
    if (s.count === total) {{
      paths += `<circle cx="${{cx}}" cy="${{cy}}" r="${{r}}" fill="${{s.color}}"/>`;
    }} else {{
      paths += `<path d="M ${{cx}} ${{cy}} L ${{x1}} ${{y1}} A ${{r}} ${{r}} 0 ${{largeArc}} 1 ${{x2}} ${{y2}} Z" fill="${{s.color}}" stroke="#151b28" stroke-width="1.5"><title>${{s.label}}: ${{fmt(s.count)}} (${{s.pct}}%)</title></path>`;
    }}
  }});
  const labels = slices.map((s, i) =>
    `<g transform="translate(245, ${{65 + i * 36}})">
      <rect width="11" height="11" rx="2" fill="${{s.color}}"/>
      <text x="18" y="10" fill="#e2e8f0" font-size="12" font-weight="600">${{s.label}}: ${{s.pct}}%</text>
      <text x="18" y="24" fill="#94a3b8" font-size="11">${{fmt(s.count)}} người</text>
    </g>`
  ).join('');
  document.getElementById('chartEthnicPie').innerHTML =
    `<svg viewBox="0 0 430 230" style="width:100%;height:100%;">${{paths}}${{labels}}</svg>`;
}}

function renderHBars(containerId, items, narrow) {{
  const maxVal = Math.max(1, ...items.map(x => x.count));
  document.getElementById(containerId).innerHTML = items.map(it => {{
    const pct = Math.max(0.5, (it.count / maxVal) * 100);
    const lbl = it.name || it.label;
    return `<div class="hbar-row ${{narrow ? 'narrow-lbl' : ''}}" title="${{lbl}}: ${{fmt(it.count)}}">
      <div class="hbar-lbl">${{lbl}}</div>
      <div class="hbar-track"><div class="hbar-fill" style="width:${{it.count === 0 ? 0 : pct}}%"></div></div>
      <div class="hbar-val">${{fmt(it.count)}}</div>
    </div>`;
  }}).join('');
}}

function renderProvinceDual(items) {{
  const maxVal = Math.max(1, ...items.flatMap(x => [x.thuong_tru, x.lien_he]));
  document.getElementById('chartProvinceTtLh').innerHTML = items.map(it => {{
    const wTt = (it.thuong_tru / maxVal) * 100;
    const wLh = (it.lien_he / maxVal) * 100;
    return `<div class="dual-row" title="${{it.ten_tinh}} — Thường trú: ${{fmt(it.thuong_tru)}} | Liên hệ: ${{fmt(it.lien_he)}}">
      <div class="hbar-lbl">${{it.ten_tinh}}</div>
      <div class="dual-bars">
        <div class="hbar-track" style="height:9px"><div class="dual-bar" style="width:${{wTt}}%;background:var(--cyan)"></div></div>
        <div class="hbar-track" style="height:9px"><div class="dual-bar" style="width:${{wLh}}%;background:var(--indigo)"></div></div>
      </div>
      <div class="hbar-val">${{fmt(it.thuong_tru)}} / ${{fmt(it.lien_he)}}</div>
    </div>`;
  }}).join('');
}}

function renderDashboard(data) {{
  document.getElementById('lblUpdatedAt').textContent = data.generated_at;
  const g1 = data.group1_demographics;
  const g2 = data.group2_data_quality;
  const g3 = data.group3_sync_monitoring;
  const pc = data.pipeline_counts;

  document.getElementById('kpiTotalCitizens').textContent = fmt(g1.total_citizens);
  document.getElementById('kpiCccdPct').textContent = `${{g1.id_coverage.cccd_12_pct}}% (${{fmt(g1.id_coverage.has_cccd_12)}})`;
  document.getElementById('kpiCleanPct').textContent = `${{g2.clean_pct}}% (${{fmt(g2.clean_records)}})`;
  document.getElementById('kpiSrcMir').textContent = `${{fmt(pc.source_rows)}} / ${{fmt(pc.mirror_rows)}}`;
  document.getElementById('kpiAliveDead').textContent = `${{fmt(g1.alive)}} / ${{fmt(g1.deceased)}}`;

  renderAgeGenderChart(g1.age_gender_series);
  renderEthnicPie(g1.ethnic_pie);
  renderHBars('chartMinority', g1.minority_bars, true);
  renderProvinceDual(g1.province_tt_lh);
  renderHBars('chartDqFlags', g2.dq_bars, false);

  document.getElementById('badgeQueuePending').textContent = `C360_SYNC_QUEUE Pending: ${{fmt(g3.queue_pending)}}`;
  const topic = g3.cdc_topic;
  document.getElementById('tblKafkaTopics').innerHTML = `
    <tr><td>__consumer_offsets</td><td>50</td><td>0</td><td>1</td><td>381</td><td>46 KB</td></tr>
    <tr><td>connect-configs</td><td>1</td><td>0</td><td>1</td><td>290</td><td>26 KB</td></tr>
    <tr><td>connect-offsets</td><td>25</td><td>0</td><td>1</td><td>12</td><td>418 Bytes</td></tr>
    <tr><td>connect-status</td><td>5</td><td>0</td><td>1</td><td>18</td><td>7 KB</td></tr>
    <tr style="background:rgba(34,211,238,0.08);font-weight:600;">
      <td style="color:var(--cyan)">${{topic.topic_name}}</td>
      <td>${{topic.partitions}}</td><td>0</td><td>${{topic.replication_factor}}</td>
      <td style="color:#fff">${{fmt(topic.total_messages)}}</td>
      <td>${{topic.size_mb}} MB</td>
    </tr>
    <tr><td>vss.schema-history.hgd_nhankhau-v1</td><td>1</td><td>0</td><td>1</td><td>1</td><td>48 KB</td></tr>
  `;

  document.getElementById('tblSyncLogs').innerHTML = g3.recent_batches.map(b => `
    <tr>
      <td>#${{b.log_id}}</td><td><b>${{b.batch_type}}</b></td>
      <td>${{b.start_time}}</td><td>${{b.end_time || '-'}}</td>
      <td>${{fmt(b.affected_keys)}}</td><td>${{fmt(b.merge_count)}}</td>
      <td>${{fmt(b.delete_count)}}</td><td>${{b.duration_sec ?? 0}}s</td>
      <td><span class="pill-ok">${{b.status}}</span></td>
    </tr>
  `).join('');
}}

async function fetchLive() {{
  try {{
    const res = await fetch('/api/v1/bi/dashboard-data');
    if (res.ok) {{
      const fresh = await res.json();
      renderDashboard(fresh);
    }}
  }} catch (e) {{ console.warn(e); }}
}}

function toggleAutoRefresh() {{
  const on = document.getElementById('chkAutoRefresh').checked;
  if (autoTimer) {{ clearInterval(autoTimer); autoTimer = null; }}
  if (on) {{ autoTimer = setInterval(fetchLive, 5000); }}
}}

renderDashboard(INITIAL_DATA);
</script>
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
    parser = argparse.ArgumentParser(description="Khởi tạo BI Data Mart Views & Dashboard C360 (Gold)")
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
    print(" KHỞI TẠO 5 BI DATA MART VIEWS & DASHBOARD 'THÔNG TIN CÁ NHÂN – GOLD'")
    print("=" * 72)

    snap = build_bi_datamart_and_snapshot(sqlite_path=db_path, html_out_path=html_out)
    g1 = snap["group1_demographics"]
    g2 = snap["group2_data_quality"]
    g3 = snap["group3_sync_monitoring"]
    print(f" [OK] Tổng số công dân C360        : {g1['total_citizens']:,}")
    print(f" [OK] Tỷ lệ phủ CCCD 12 số         : {g1['id_coverage']['cccd_12_pct']}% ({g1['id_coverage']['has_cccd_12']:,} người)")
    print(f" [OK] Tỷ lệ hồ sơ sạch hoàn toàn   : {g2['clean_pct']}% ({g2['clean_records']:,} người)")
    print(f" [OK] CDC Kafka Topic Messages     : {g3['cdc_topic']['total_messages']:,} ({g3['cdc_topic']['size_mb']} MB)")
    print(f" [OK] Đã xuất Dashboard HTML tại   : {html_out or (PROJECT_ROOT / 'data' / 'bi_dashboard_preview.html')}")

    if args.push_metabase and args.metabase_user and args.metabase_password:
        res = provision_metabase_via_api(args.metabase_url, args.metabase_user, args.metabase_password)
        print(f" [OK] Đã tạo Metabase Collection ID={res['collection_id']} với {len(res['card_ids'])} Cards")
    print("=" * 72)


if __name__ == "__main__":
    main()
