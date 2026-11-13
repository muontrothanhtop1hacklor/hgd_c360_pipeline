#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BAT-C360-006: Job Đối soát Dữ liệu Tự động 3 Tầng (Nguồn <-> Mirror <-> C360).
Thực hiện:
  1. Đối soát Tầng 1 (Nguồn vs Mirror):
     - So sánh tổng số dòng 4 bảng: Dân tộc, Quốc tịch, DBHC, Nhân khẩu.
     - So sánh chi tiết phân bổ số dòng theo từng mã tỉnh (MA_TINH).
  2. Đối soát Tầng 2 (Mirror vs C360):
     - Kiểm chứng công thức vàng: COUNT(DISTINCT MA_CD) hợp lệ trên Mirror == COUNT(*) trên C360.
     - Kiểm tra mẫu 100 công dân có nhiều hộ gia đình xem C360 có lấy đúng dòng NGAY_SUA mới nhất không.
     - Quét bảo đảm không còn bản ghi rác/loại trừ (TYPEID='D' hoặc TRANG_THAI_DUYET=0) trên C360.
  3. Thống kê Chất lượng Dữ liệu (Data Quality Matrix):
     - Thống kê tỷ lệ bản ghi sạch hoàn toàn (DQ_FLAGS IS NULL).
     - Phân tích số lượng và tỷ lệ % dính từng cờ trong 11 mã cờ DQ_FLAGS.
  4. Xuất báo cáo kết quả chi tiết ra data/reconciliation_report.json.
"""

import argparse
import json
import sys
import time
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
    get_db_engine,
    init_c360_schema,
    init_source_and_mirror_schema,
)


def run_3tier_reconciliation(
    sqlite_path: Optional[Path] = None,
    out_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Thực thi toàn bộ kịch bản đối soát 3 tầng."""
    conn = get_connection(sqlite_path=sqlite_path)
    init_source_and_mirror_schema(conn)
    init_c360_schema(conn)

    cur = conn.cursor()
    t0 = time.time()
    exec_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ---------------------------------------------------------
    # 1. ĐỐI SOÁT TẦNG 1: NGUỒN (VSS_*) VS MIRROR (MIR_VSS_*)
    # ---------------------------------------------------------
    tier1_tables = {
        "DM_DANTOC": ("VSS_DM_DANTOC", "MIR_VSS_DM_DANTOC"),
        "DM_QUOC_TICH": ("VSS_DM_QUOC_TICH", "MIR_VSS_DM_QUOC_TICH"),
        "DM_DBHC": ("VSS_DM_DBHC", "MIR_VSS_DM_DBHC"),
        "HO_GD_NHANKHAU": ("VSS_HO_GD_NHANKHAU", "MIR_VSS_HO_GD_NHANKHAU"),
    }
    tier1_results = {}
    tier1_passed = True

    for label, (src_tbl, mir_tbl) in tier1_tables.items():
        cur.execute(f"SELECT COUNT(*) FROM {src_tbl}")
        src_cnt = cur.fetchone()[0]
        cur.execute(f"SELECT COUNT(*) FROM {mir_tbl}")
        mir_cnt = cur.fetchone()[0]
        diff = src_cnt - mir_cnt
        is_ok = (diff == 0)
        if not is_ok:
            tier1_passed = False
        tier1_results[label] = {
            "source_rows": src_cnt,
            "mirror_rows": mir_cnt,
            "diff": diff,
            "status": "PASSED" if is_ok else "FAILED",
        }

    # Phân bổ theo MA_TINH giữa Nguồn và Mirror
    cur.execute(
        """
        SELECT COALESCE(MA_TINH, 'UNKNOWN'), COUNT(*)
          FROM VSS_HO_GD_NHANKHAU
         GROUP BY MA_TINH
        """
    )
    src_tinh_counts = dict(cur.fetchall())

    cur.execute(
        """
        SELECT COALESCE(MA_TINH, 'UNKNOWN'), COUNT(*)
          FROM MIR_VSS_HO_GD_NHANKHAU
         GROUP BY MA_TINH
        """
    )
    mir_tinh_counts = dict(cur.fetchall())

    mismatched_provinces = {}
    all_tinh_keys = set(src_tinh_counts.keys()).union(set(mir_tinh_counts.keys()))
    for t_key in all_tinh_keys:
        s_c = src_tinh_counts.get(t_key, 0)
        m_c = mir_tinh_counts.get(t_key, 0)
        if s_c != m_c:
            mismatched_provinces[t_key] = {"source": s_c, "mirror": m_c, "diff": s_c - m_c}
            tier1_passed = False

    tier1_summary = {
        "tables": tier1_results,
        "mismatched_provinces": mismatched_provinces,
        "status": "PASSED" if tier1_passed else "FAILED",
    }

    # ---------------------------------------------------------
    # 2. ĐỐI SOÁT TẦNG 2: MIRROR (MIR_VSS_*) VS C360
    # ---------------------------------------------------------
    # Tổng số dòng trên Mirror
    cur.execute("SELECT COUNT(*) FROM MIR_VSS_HO_GD_NHANKHAU;")
    mirror_total_rows = cur.fetchone()[0]

    # Số lượng công dân hợp lệ duy nhất trên Mirror
    cur.execute(
        """
        SELECT COUNT(DISTINCT MA_CD)
          FROM MIR_VSS_HO_GD_NHANKHAU
         WHERE (TYPEID IS NULL OR TYPEID NOT IN ('D', 'GT', 'GM', 'GG', 'GK'))
           AND (TRANG_THAI_DUYET IS NOT NULL AND TRANG_THAI_DUYET NOT IN (0, 3, 4));
        """
    )
    mirror_valid_distinct_persons = cur.fetchone()[0]

    # Tổng số bản ghi trên C360
    cur.execute("SELECT COUNT(*), COUNT(DISTINCT MA_SO_BHXH) FROM C360_THONG_TIN_CA_NHAN;")
    c360_total_rows, c360_distinct_persons = cur.fetchone()

    # Kiểm tra Công thức vàng: mirror_valid_distinct_persons == c360_total_rows
    diff_t2 = mirror_valid_distinct_persons - c360_total_rows
    tier2_passed = (diff_t2 == 0 and c360_total_rows == c360_distinct_persons)

    # Kiểm tra không còn bản ghi loại trừ trên C360
    cur.execute(
        """
        SELECT COUNT(*)
          FROM C360_THONG_TIN_CA_NHAN
         WHERE MA_SO_BHXH IN (
             SELECT MA_CD
               FROM MIR_VSS_HO_GD_NHANKHAU
              WHERE TYPEID IN ('D', 'GT', 'GM', 'GG', 'GK')
                 OR TRANG_THAI_DUYET IN (0, 3, 4)
         );
        """
    )
    leaked_invalid_records = cur.fetchone()[0]
    if leaked_invalid_records > 0:
        tier2_passed = False

    # Kiểm tra mẫu tính chọn đúng dòng mới nhất của người có nhiều hộ (Sample Consistency Test)
    cur.execute(
        """
        SELECT MA_CD, COUNT(*)
          FROM MIR_VSS_HO_GD_NHANKHAU
         WHERE (TYPEID IS NULL OR TYPEID NOT IN ('D', 'GT', 'GM', 'GG', 'GK'))
           AND (TRANG_THAI_DUYET IS NOT NULL AND TRANG_THAI_DUYET NOT IN (0, 3, 4))
         GROUP BY MA_CD
        HAVING COUNT(*) > 1
         LIMIT 50;
        """
    )
    multi_household_samples = [r[0] for r in cur.fetchall()]
    sample_check_passed = True
    sample_check_errors = []

    for sample_ma_cd in multi_household_samples:
        # Lấy dòng mới nhất ở Mirror
        cur.execute(
            """
            SELECT MA_HO_GD, NGAY_SUA, ID
              FROM MIR_VSS_HO_GD_NHANKHAU
             WHERE MA_CD = ?
             ORDER BY COALESCE(NGAY_SUA, '') DESC, ID DESC
             LIMIT 1;
            """,
            (sample_ma_cd,),
        )
        mir_latest = cur.fetchone()

        cur.execute(
            """
            SELECT SOURCE_MA_HO_GD, SOURCE_MODIFY_DATE
              FROM C360_THONG_TIN_CA_NHAN
             WHERE MA_SO_BHXH = ?;
            """,
            (sample_ma_cd,),
        )
        c360_actual = cur.fetchone()

        if c360_actual and mir_latest:
            if c360_actual[0] != mir_latest[0]:
                sample_check_passed = False
                sample_check_errors.append(
                    f"MA_CD {sample_ma_cd}: Kỳ vọng hộ {mir_latest[0]} nhưng C360 lưu {c360_actual[0]}"
                )

    tier2_summary = {
        "mirror_total_rows": mirror_total_rows,
        "mirror_valid_distinct_citizens": mirror_valid_distinct_persons,
        "c360_total_citizens": c360_total_rows,
        "c360_distinct_citizens": c360_distinct_persons,
        "golden_equation_diff": diff_t2,
        "leaked_invalid_records": leaked_invalid_records,
        "multi_household_sample_test": {
            "samples_checked": len(multi_household_samples),
            "passed": sample_check_passed,
            "errors": sample_check_errors,
        },
        "status": "PASSED" if tier2_passed and sample_check_passed else "FAILED",
    }

    # ---------------------------------------------------------
    # 3. THỐNG KÊ CHẤT LƯỢNG DỮ LIỆU C360 (DATA QUALITY METRICS)
    # ---------------------------------------------------------
    cur.execute("SELECT COUNT(*) FROM C360_THONG_TIN_CA_NHAN WHERE DQ_FLAGS IS NULL;")
    clean_count = cur.fetchone()[0]

    all_flags = [
        "TINH_KHONG_MAP",
        "XA_KHONG_MAP",
        "XA_MO_HO",
        "TINH_LECH_XA",
        "NGAY_SINH_LOI",
        "SDT_LOI",
        "KHONG_SO_DINH_DANH",
        "CANH_BAO_CMND9",
        "DAN_TOC_LOI",
        "QUOC_TICH_LOI",
        "HOTEN_THUA_KHOANG_TRANG",
    ]

    flag_breakdown = {}
    for flg in all_flags:
        cur.execute(
            f"SELECT COUNT(*) FROM C360_THONG_TIN_CA_NHAN WHERE DQ_FLAGS LIKE '%{flg}%';"
        )
        cnt = cur.fetchone()[0]
        pct = round((cnt / c360_total_rows * 100), 2) if c360_total_rows > 0 else 0.0
        flag_breakdown[flg] = {"count": cnt, "percentage": pct}

    tier3_summary = {
        "total_citizens": c360_total_rows,
        "clean_citizens": clean_count,
        "clean_percentage": round((clean_count / c360_total_rows * 100), 2) if c360_total_rows > 0 else 0.0,
        "flag_breakdown": flag_breakdown,
    }

    conn.close()
    elapsed = round(time.time() - t0, 2)

    overall_status = "PASSED" if tier1_summary["status"] == "PASSED" and tier2_summary["status"] == "PASSED" else "FAILED"

    report = {
        "executed_at": exec_time,
        "elapsed_sec": elapsed,
        "overall_status": overall_status,
        "tier1_source_vs_mirror": tier1_summary,
        "tier2_mirror_vs_c360": tier2_summary,
        "tier3_data_quality": tier3_summary,
    }

    # Xuất ra file JSON
    target_out = Path(out_path) if out_path else PROJECT_ROOT / "data" / "reconciliation_report.json"
    target_out.parent.mkdir(parents=True, exist_ok=True)
    target_out.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    return report


def main():
    parser = argparse.ArgumentParser(description="Chạy Job Đối Soát Dữ Liệu 3 Tầng (Nguồn - Mirror - C360)")
    parser.add_argument("--db-path", type=str, default=None, help="Đường dẫn file SQLite DB")
    parser.add_argument("--out", type=str, default=None, help="Đường dẫn xuất file JSON báo cáo")
    args = parser.parse_args()

    db_path = Path(args.db_path) if args.db_path else None
    out_path = Path(args.out) if args.out else None

    print("=" * 76)
    print(" BẮT ĐẦU CHẠY JOB ĐỐI SOÁT DỮ LIỆU TỰ ĐỘNG 3 TẦNG (BAT-C360-006)")
    print("=" * 76)

    report = run_3tier_reconciliation(sqlite_path=db_path, out_path=out_path)

    t1 = report["tier1_source_vs_mirror"]
    print(f"\n[1] ĐỐI SOÁT TẦNG 1 (NGUỒN <-> MIRROR): {t1['status']}")
    for tbl, res in t1["tables"].items():
        print(f"  - {tbl:<18} : Nguồn={res['source_rows']:,} | Mirror={res['mirror_rows']:,} (Lệch: {res['diff']}) -> {res['status']}")

    t2 = report["tier2_mirror_vs_c360"]
    print(f"\n[2] ĐỐI SOÁT TẦNG 2 (MIRROR <-> C360): {t2['status']}")
    print(f"  - Tổng số dòng nhân khẩu Mirror          : {t2['mirror_total_rows']:,} dòng")
    print(f"  - Số công dân hợp lệ duy nhất (Mirror)   : {t2['mirror_valid_distinct_citizens']:,} công dân")
    print(f"  - Tổng số công dân trên bảng C360        : {t2['c360_total_citizens']:,} công dân")
    print(f"  - Chênh lệch Công thức vàng (Mirror-C360): {t2['golden_equation_diff']} (Mục tiêu = 0)")
    print(f"  - Bản ghi rác/loại trừ lọt vào C360      : {t2['leaked_invalid_records']} (Mục tiêu = 0)")
    print(f"  - Kiểm tra mẫu 50 công dân nhiều hộ     : {'PASSED (Khớp 100% dòng mới nhất)' if t2['multi_household_sample_test']['passed'] else 'FAILED'}")

    t3 = report["tier3_data_quality"]
    print(f"\n[3] BÁO CÁO CHẤT LƯỢNG DỮ LIỆU C360 (DATA QUALITY):")
    print(f"  - Dữ liệu sạch hoàn toàn (DQ_FLAGS IS NULL): {t3['clean_citizens']:,} / {t3['total_citizens']:,} ({t3['clean_percentage']}%)")
    print("  - Phân bổ 11 mã cờ lỗi DQ_FLAGS:")
    for flg, info in t3["flag_breakdown"].items():
        print(f"    * {flg:<24} : {info['count']:>6,} dòng ({info['percentage']:>5.2f}%)")

    print(f"\n [KẾT QUẢ TỔNG THỂ] : {report['overall_status']} (Thời gian: {report['elapsed_sec']} giây)")
    print("=" * 76)


if __name__ == "__main__":
    main()
