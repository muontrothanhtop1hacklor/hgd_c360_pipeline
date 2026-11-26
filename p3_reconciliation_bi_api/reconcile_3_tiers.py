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
     - Đối chiếu tập hợp 2 chiều bằng toán tử EXCEPT (an toàn với NULL):
       * leaked_invalid_records = |C360 EXCEPT Valid_Mirror| == 0
       * missing_valid_records  = |Valid_Mirror EXCEPT C360| == 0
     - Kiểm tra mẫu 100 công dân có >= 2 hộ gia đình: đối chiếu toàn diện hộ gia đình, họ tên,
       ngày sinh chuẩn, số định danh 12 số, tỉnh/xã thường trú, SĐT và DQ_FLAGS với dòng mới nhất.
  3. Thống kê Chất lượng Dữ liệu (Data Quality Matrix):
     - Thống kê tỷ lệ bản ghi sạch hoàn toàn (DQ_FLAGS IS NULL).
     - Phân tích số lượng và tỷ lệ % dính từng cờ (so khớp chính xác token ';FLAG;').
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
    init_api_and_bi_schema,
    init_c360_schema,
    init_source_and_mirror_schema,
)
from p2_c360_standardization.db_c360_cleaner import (
    load_standardization_lookups,
    transform_citizen_record,
)

CORE_DQ_FLAGS = [
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

AUX_ADDRESS_DQ_FLAGS = [
    "TINH_LH_KHONG_MAP",
    "XA_LH_KHONG_MAP",
    "XA_LH_MO_HO",
    "TINH_LH_LECH_XA",
    "TINH_KS_KHONG_MAP",
    "XA_KS_KHONG_MAP",
    "XA_KS_MO_HO",
    "TINH_KS_LECH_XA",
]


def run_3tier_reconciliation(
    sqlite_path: Optional[Path] = None,
    out_path: Optional[Path] = None,
    sample_size: int = 100,
) -> Dict[str, Any]:
    """Thực thi toàn bộ kịch bản đối soát 3 tầng."""
    conn = get_connection(sqlite_path=sqlite_path)
    init_source_and_mirror_schema(conn)
    init_c360_schema(conn)
    init_api_and_bi_schema(conn)

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
    tier1_results: Dict[str, Any] = {}
    tier1_passed = True

    for label, (src_tbl, mir_tbl) in tier1_tables.items():
        cur.execute(f"SELECT COUNT(*) FROM {src_tbl}")
        src_cnt = cur.fetchone()[0]
        cur.execute(f"SELECT COUNT(*) FROM {mir_tbl}")
        mir_cnt = cur.fetchone()[0]
        diff = src_cnt - mir_cnt
        is_ok = diff == 0
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
         GROUP BY COALESCE(MA_TINH, 'UNKNOWN');
        """
    )
    src_tinh_counts = dict(cur.fetchall())

    cur.execute(
        """
        SELECT COALESCE(MA_TINH, 'UNKNOWN'), COUNT(*)
          FROM MIR_VSS_HO_GD_NHANKHAU
         GROUP BY COALESCE(MA_TINH, 'UNKNOWN');
        """
    )
    mir_tinh_counts = dict(cur.fetchall())

    mismatched_provinces = {}
    all_tinh_keys = sorted(set(src_tinh_counts.keys()).union(set(mir_tinh_counts.keys())))
    for t_key in all_tinh_keys:
        s_c = src_tinh_counts.get(t_key, 0)
        m_c = mir_tinh_counts.get(t_key, 0)
        if s_c != m_c:
            mismatched_provinces[t_key] = {"source": s_c, "mirror": m_c, "diff": s_c - m_c}
            tier1_passed = False

    tier1_summary = {
        "tables": tier1_results,
        "province_groups_checked": len(all_tinh_keys),
        "mismatched_provinces": mismatched_provinces,
        "status": "PASSED" if tier1_passed else "FAILED",
    }

    # ---------------------------------------------------------
    # 2. ĐỐI SOÁT TẦNG 2: MIRROR (MIR_VSS_*) VS C360
    # ---------------------------------------------------------
    cur.execute("SELECT COUNT(*) FROM MIR_VSS_HO_GD_NHANKHAU;")
    mirror_total_rows = cur.fetchone()[0]

    cur.execute(
        """
        SELECT COUNT(DISTINCT MA_CD)
          FROM MIR_VSS_HO_GD_NHANKHAU
         WHERE MA_CD IS NOT NULL
           AND COALESCE(TYPEID, 'ZZ') NOT IN ('GT', 'GM', 'GG', 'GK')
           AND COALESCE(TRANG_THAI_DUYET, 0) NOT IN (1, 3, 4);
        """
    )
    mirror_valid_distinct_persons = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*), COUNT(DISTINCT MA_SO_BHXH) FROM C360_THONG_TIN_CA_NHAN;")
    c360_total_rows, c360_distinct_persons = cur.fetchone()

    diff_t2 = mirror_valid_distinct_persons - c360_total_rows

    # Đối chiếu tập hợp 2 chiều bằng EXCEPT (an toàn tuyệt đối khi có NULL, không bị bẫy NOT IN)
    cur.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT MA_SO_BHXH FROM C360_THONG_TIN_CA_NHAN
            EXCEPT
            SELECT DISTINCT MA_CD
              FROM MIR_VSS_HO_GD_NHANKHAU
             WHERE MA_CD IS NOT NULL
               AND COALESCE(TYPEID, 'ZZ') NOT IN ('GT', 'GM', 'GG', 'GK')
               AND COALESCE(TRANG_THAI_DUYET, 0) NOT IN (1, 3, 4)
        );
        """
    )
    leaked_invalid_records = cur.fetchone()[0]

    cur.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT DISTINCT MA_CD
              FROM MIR_VSS_HO_GD_NHANKHAU
             WHERE MA_CD IS NOT NULL
               AND COALESCE(TYPEID, 'ZZ') NOT IN ('GT', 'GM', 'GG', 'GK')
               AND COALESCE(TRANG_THAI_DUYET, 0) NOT IN (1, 3, 4)
            EXCEPT
            SELECT MA_SO_BHXH FROM C360_THONG_TIN_CA_NHAN
        );
        """
    )
    missing_valid_records = cur.fetchone()[0]

    tier2_passed = (
        diff_t2 == 0
        and c360_total_rows == c360_distinct_persons
        and leaked_invalid_records == 0
        and missing_valid_records == 0
    )

    # Kiểm tra mẫu 100 công dân có nhiều hộ gia đình: đối chiếu đầy đủ các trường chuẩn hóa
    cur.execute(
        """
        SELECT MA_CD
          FROM MIR_VSS_HO_GD_NHANKHAU
         WHERE MA_CD IS NOT NULL
           AND COALESCE(TYPEID, 'ZZ') NOT IN ('GT', 'GM', 'GG', 'GK')
           AND COALESCE(TRANG_THAI_DUYET, 0) NOT IN (1, 3, 4)
         GROUP BY MA_CD
        HAVING COUNT(*) > 1
         ORDER BY MA_CD
         LIMIT ?;
        """,
        (sample_size,),
    )
    multi_household_samples = [r[0] for r in cur.fetchall()]
    sample_check_passed = True
    sample_check_errors: List[str] = []

    cur.execute("PRAGMA table_info(MIR_VSS_HO_GD_NHANKHAU);")
    mir_col_names = [col[1] for col in cur.fetchall()]
    lookups = load_standardization_lookups(conn) if multi_household_samples else {}

    verify_fields = [
        "SOURCE_MA_HO_GD",
        "HO_TEN",
        "NGAY_SINH_CHUAN",
        "SO_DINH_DANH",
        "MA_TINH_THUONG_TRU",
        "MA_XA_THUONG_TRU",
        "SO_DIEN_THOAI",
        "DQ_FLAGS",
    ]

    for sample_ma_cd in multi_household_samples:
        cur.execute(
            """
            SELECT *
              FROM MIR_VSS_HO_GD_NHANKHAU
             WHERE MA_CD = ?
               AND COALESCE(TYPEID, 'ZZ') NOT IN ('GT', 'GM', 'GG', 'GK')
               AND COALESCE(TRANG_THAI_DUYET, 0) NOT IN (1, 3, 4)
             ORDER BY COALESCE(NGAY_SUA, '') DESC,
                      COALESCE(NGAY_TAO, '') DESC,
                      ID DESC
             LIMIT 1;
            """,
            (sample_ma_cd,),
        )
        mir_latest_row = cur.fetchone()
        cur.execute(
            f"SELECT {', '.join(verify_fields)} FROM C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH = ?;",
            (sample_ma_cd,),
        )
        c360_actual_row = cur.fetchone()

        if not mir_latest_row or not c360_actual_row:
            sample_check_passed = False
            sample_check_errors.append(f"MA_CD {sample_ma_cd}: Không tìm thấy dòng đối ứng giữa Mirror và C360")
            continue

        expected_rec = transform_citizen_record(dict(zip(mir_col_names, mir_latest_row)), lookups)
        actual_rec = dict(zip(verify_fields, c360_actual_row))

        for fld in verify_fields:
            if expected_rec.get(fld) != actual_rec.get(fld):
                sample_check_passed = False
                sample_check_errors.append(
                    f"MA_CD {sample_ma_cd} lệch cột {fld}: kỳ vọng={expected_rec.get(fld)!r}, thực tế={actual_rec.get(fld)!r}"
                )

    tier2_summary = {
        "mirror_total_rows": mirror_total_rows,
        "mirror_valid_distinct_citizens": mirror_valid_distinct_persons,
        "c360_total_citizens": c360_total_rows,
        "c360_distinct_citizens": c360_distinct_persons,
        "golden_equation_diff": diff_t2,
        "leaked_invalid_records": leaked_invalid_records,
        "missing_valid_records": missing_valid_records,
        "multi_household_sample_test": {
            "samples_checked": len(multi_household_samples),
            "fields_verified_per_sample": verify_fields,
            "passed": sample_check_passed,
            "errors": sample_check_errors,
        },
        "status": "PASSED" if (tier2_passed and sample_check_passed) else "FAILED",
    }

    # ---------------------------------------------------------
    # 3. THỐNG KÊ CHẤT LƯỢNG DỮ LIỆU C360 (DATA QUALITY METRICS)
    # ---------------------------------------------------------
    cur.execute("SELECT COUNT(*) FROM C360_THONG_TIN_CA_NHAN WHERE DQ_FLAGS IS NULL;")
    clean_count = cur.fetchone()[0]
    dirty_count = c360_total_rows - clean_count

    flag_breakdown: Dict[str, Dict[str, Any]] = {}
    for flg in CORE_DQ_FLAGS:
        cur.execute(
            "SELECT COUNT(*) FROM C360_THONG_TIN_CA_NHAN WHERE ';' || COALESCE(DQ_FLAGS, '') || ';' LIKE ?;",
            (f"%;{flg};%",),
        )
        cnt = cur.fetchone()[0]
        pct = round((cnt / c360_total_rows * 100), 2) if c360_total_rows > 0 else 0.0
        flag_breakdown[flg] = {"count": cnt, "percentage": pct}

    aux_flag_breakdown: Dict[str, Dict[str, Any]] = {}
    for flg in AUX_ADDRESS_DQ_FLAGS:
        cur.execute(
            "SELECT COUNT(*) FROM C360_THONG_TIN_CA_NHAN WHERE ';' || COALESCE(DQ_FLAGS, '') || ';' LIKE ?;",
            (f"%;{flg};%",),
        )
        cnt = cur.fetchone()[0]
        if cnt > 0:
            pct = round((cnt / c360_total_rows * 100), 2) if c360_total_rows > 0 else 0.0
            aux_flag_breakdown[flg] = {"count": cnt, "percentage": pct}

    # Top 5 tỉnh có tỷ lệ hồ sơ gắn cờ lỗi cao nhất
    cur.execute(
        """
        SELECT MA_TINH, TEN_TINH, TONG_SO_HO_SO, SO_HO_SO_CO_LOI, TY_LE_LOI_PCT
          FROM V_BI_CHAT_LUONG_DU_LIEU
         ORDER BY TY_LE_LOI_PCT DESC, SO_HO_SO_CO_LOI DESC
         LIMIT 5;
        """
    )
    top_dirty_provinces = [
        {
            "ma_tinh": r[0],
            "ten_tinh": r[1],
            "total_records": r[2],
            "dirty_records": r[3],
            "dirty_pct": r[4],
        }
        for r in cur.fetchall()
    ]

    tier3_summary = {
        "total_citizens": c360_total_rows,
        "clean_citizens": clean_count,
        "dirty_citizens": dirty_count,
        "clean_percentage": round((clean_count / c360_total_rows * 100), 2) if c360_total_rows > 0 else 0.0,
        "flag_breakdown": flag_breakdown,
        "aux_address_flag_breakdown": aux_flag_breakdown,
        "top_dirty_provinces": top_dirty_provinces,
    }

    conn.close()
    elapsed = round(time.time() - t0, 2)

    overall_status = (
        "PASSED"
        if tier1_summary["status"] == "PASSED" and tier2_summary["status"] == "PASSED"
        else "FAILED"
    )

    report = {
        "executed_at": exec_time,
        "elapsed_sec": elapsed,
        "overall_status": overall_status,
        "tier1_source_vs_mirror": tier1_summary,
        "tier2_mirror_vs_c360": tier2_summary,
        "tier3_data_quality": tier3_summary,
    }

    target_out = Path(out_path) if out_path else PROJECT_ROOT / "data" / "reconciliation_report.json"
    target_out.parent.mkdir(parents=True, exist_ok=True)
    target_out.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    return report


def main():
    parser = argparse.ArgumentParser(description="Chạy Job Đối Soát Dữ Liệu 3 Tầng (Nguồn - Mirror - C360)")
    parser.add_argument("--db-path", type=str, default=None, help="Đường dẫn file SQLite DB")
    parser.add_argument("--out", type=str, default=None, help="Đường dẫn xuất file JSON báo cáo")
    parser.add_argument("--sample-size", type=int, default=100, help="Số lượng mẫu công dân nhiều hộ cần đối chiếu")
    args = parser.parse_args()

    db_path = Path(args.db_path) if args.db_path else None
    out_path = Path(args.out) if args.out else None

    print("=" * 76)
    print(" BẮT ĐẦU CHẠY JOB ĐỐI SOÁT DỮ LIỆU TỰ ĐỘNG 3 TẦNG (BAT-C360-006)")
    print("=" * 76)

    report = run_3tier_reconciliation(sqlite_path=db_path, out_path=out_path, sample_size=args.sample_size)

    t1 = report["tier1_source_vs_mirror"]
    print(f"\n[1] ĐỐI SOÁT TẦNG 1 (NGUỒN <-> MIRROR): {t1['status']}")
    for tbl, res in t1["tables"].items():
        print(
            f"  - {tbl:<18} : Nguồn={res['source_rows']:,} | Mirror={res['mirror_rows']:,} "
            f"(Lệch: {res['diff']}) -> {res['status']}"
        )

    t2 = report["tier2_mirror_vs_c360"]
    sample_info = t2["multi_household_sample_test"]
    print(f"\n[2] ĐỐI SOÁT TẦNG 2 (MIRROR <-> C360): {t2['status']}")
    print(f"  - Tổng số dòng nhân khẩu Mirror          : {t2['mirror_total_rows']:,} dòng")
    print(f"  - Số công dân hợp lệ duy nhất (Mirror)   : {t2['mirror_valid_distinct_citizens']:,} công dân")
    print(f"  - Tổng số công dân trên bảng C360        : {t2['c360_total_citizens']:,} công dân")
    print(f"  - Chênh lệch Công thức vàng (Mirror-C360): {t2['golden_equation_diff']} (Mục tiêu = 0)")
    print(f"  - Bản ghi rác/loại trừ lọt vào C360      : {t2['leaked_invalid_records']} (Mục tiêu = 0)")
    print(f"  - Công dân hợp lệ bị thiếu trên C360     : {t2['missing_valid_records']} (Mục tiêu = 0)")
    print(
        f"  - Kiểm tra mẫu {sample_info['samples_checked']} công dân nhiều hộ    : "
        f"{'PASSED (Khớp 100% toàn bộ trường chuẩn hóa)' if sample_info['passed'] else 'FAILED'}"
    )

    t3 = report["tier3_data_quality"]
    print("\n[3] BÁO CÁO CHẤT LƯỢNG DỮ LIỆU C360 (DATA QUALITY):")
    print(
        f"  - Dữ liệu sạch hoàn toàn (DQ_FLAGS IS NULL): "
        f"{t3['clean_citizens']:,} / {t3['total_citizens']:,} ({t3['clean_percentage']}%)"
    )
    print("  - Phân bổ 11 mã cờ lỗi chính DQ_FLAGS:")
    for flg, info in t3["flag_breakdown"].items():
        print(f"    * {flg:<24} : {info['count']:>6,} dòng ({info['percentage']:>5.2f}%)")

    print(f"\n [KẾT QUẢ TỔNG THỂ] : {report['overall_status']} (Thời gian: {report['elapsed_sec']} giây)")
    print("=" * 76)


if __name__ == "__main__":
    main()
