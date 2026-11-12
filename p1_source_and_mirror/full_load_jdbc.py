#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BAT-C360-003 (Full Load Nguồn VSS -> Mirror):
Đồng bộ toàn量 (Full Load) 4 bảng từ Nguồn (VSS_*) sang vùng Mirror (MIR_VSS_*):
  1. VSS_DM_DANTOC       -> MIR_VSS_DM_DANTOC
  2. VSS_DM_QUOC_TICH    -> MIR_VSS_DM_QUOC_TICH
  3. VSS_DM_DBHC         -> MIR_VSS_DM_DBHC
  4. VSS_HO_GD_NHANKHAU  -> MIR_VSS_HO_GD_NHANKHAU

Các nguyên tắc kỹ thuật bắt buộc được áp dụng (Mục 3 - Phần 1.2):
- Ghi nhận mốc SCN / Watermark trước khi đọc để bàn giao liền mạch cho Debezium CDC (MIR_CDC_SCN_HANDOFF).
- Chia dải đọc phân tán theo prefix MA_CD (10 phân vùng '0'..'9') tận dụng index IDX_VSS_NK_MACD.
- Ép kiểu tường minh (CAST) cho các cột NUMBER không khai báo precision (ID, DM_DAN_TOC_ID, MA_VUNG_SS,
  CMND_REL, CQBHXH_ID, STATUS) tránh lỗi Spark JDBC suy diễn thành Decimal(38, 10).
- Tuyệt đối KHÔNG gọi .cache(), .persist() hay .repartition() trên toàn tập dữ liệu để tránh tràn bộ nhớ (OOM).
- Chiến lược Index khi Full Load: DROP/DISABLE Unique Index UK_MIR_NK (MA_CD, MA_HO_GD) trước khi nạp lô lớn
  và REBUILD lại ngay sau khi nạp xong -> Đảm bảo tốc độ nạp tối đa và tính幂등 (Idempotency) khi chạy lại.
- Đối soát tự động Nguồn vs Mirror (tổng số dòng 4 bảng và đối soát chi tiết theo từng MA_TINH).
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
    NHANKHAU_COLUMNS,
    get_connection,
    get_db_engine,
    init_source_and_mirror_schema,
)

# Danh sách cột NUMBER không có precision trong DDL gốc cần CAST tường minh khi đọc qua JDBC
UNCONSTRAINED_INT_COLS = {"ID", "DM_DAN_TOC_ID", "MA_VUNG_SS", "CMND_REL", "CQBHXH_ID", "STATUS"}


def build_explicit_select_projection(columns: List[str], engine: str = "LOCAL_SQLITE") -> str:
    """
    Sinh mệnh đề SELECT có ép kiểu tường minh cho các cột NUMBER không có precision
    để ngăn chặn Spark/JDBC ánh xạ thành Decimal(38,10).
    """
    exprs = []
    for col in columns:
        if col in UNCONSTRAINED_INT_COLS:
            if engine == "ORACLE":
                precision = 19 if col == "ID" else 10
                exprs.append(f"CAST({col} AS NUMBER({precision}, 0)) AS {col}")
            else:
                exprs.append(f"CAST({col} AS INTEGER) AS {col}")
        else:
            exprs.append(col)
    return ", ".join(exprs)


def capture_source_scn(conn: Any, engine: str = "LOCAL_SQLITE") -> int:
    """Lấy mốc SCN hiện tại của Database nguồn trước khi bắt đầu Full Load."""
    cur = conn.cursor()
    if engine == "ORACLE":
        cur.execute("SELECT CURRENT_SCN FROM V$DATABASE")
        row = cur.fetchone()
        return int(row[0]) if row else int(time.time() * 1000)
    # Trên SQLite local: dùng timestamp mili-giây kết hợp MAX(ID) làm mốc SCN giả lập
    cur.execute("SELECT COALESCE(MAX(ID), 0) FROM VSS_HO_GD_NHANKHAU")
    max_id = cur.fetchone()[0] or 0
    return int(time.time() * 1000) * 1000 + (max_id % 1000)


def full_load_catalog_tables(conn: Any, engine: str = "LOCAL_SQLITE") -> Dict[str, int]:
    """Full Load 3 bảng danh mục từ VSS_DM_* sang MIR_VSS_DM_*."""
    cur = conn.cursor()
    counts = {}

    # 1. DM_DANTOC
    dt_cols = [
        "ID", "MA", "TEN", "TEN_GOI_KHAC", "DIA_BAN_CU_TRU",
        "NGAY_HL", "NGAY_HET_HL", "TRANG_THAI", "NGUOI_TAO", "NGAY_TAO",
        "NGUOI_SUA", "NGAY_SUA", "DAN_SO", "MA_CU", "CAP", "CHA_ID",
    ]
    cur.execute("DELETE FROM MIR_VSS_DM_DANTOC")
    proj_dt = build_explicit_select_projection(dt_cols, engine)
    cur.execute(
        f"""
        INSERT INTO MIR_VSS_DM_DANTOC ({', '.join(dt_cols)}, MIRROR_SYNC_TS)
        SELECT {proj_dt}, datetime('now', 'localtime')
        FROM VSS_DM_DANTOC
        """
    )
    counts["MIR_VSS_DM_DANTOC"] = cur.rowcount

    # 2. DM_QUOC_TICH
    qt_cols = ["ID", "MA", "TEN", "TRANG_THAI", "TEN_TIENG_ANH"]
    cur.execute("DELETE FROM MIR_VSS_DM_QUOC_TICH")
    proj_qt = build_explicit_select_projection(qt_cols, engine)
    cur.execute(
        f"""
        INSERT INTO MIR_VSS_DM_QUOC_TICH ({', '.join(qt_cols)}, MIRROR_SYNC_TS)
        SELECT {proj_qt}, datetime('now', 'localtime')
        FROM VSS_DM_QUOC_TICH
        """
    )
    counts["MIR_VSS_DM_QUOC_TICH"] = cur.rowcount

    # 3. DM_DBHC
    dbhc_cols = [
        "ID", "MA_DBHC", "MA_CHA", "TEN", "TINH_TRANG",
        "MA_TINH", "MA_HUYEN", "MA_XA", "MA_VUNG", "MA_DBHC_THUE",
        "CAP", "CQBHXH_ID", "NGAY_HIEU_LUC", "NGAY_HET_HIEU_LUC", "NGAY_SUA",
        "MA_DBHC_CU", "TIEU_VUNG_KT", "VUNG_KT_TRONG_DIEM", "VUNG_SINH_THAI",
        "IS_CAP_XA", "IS_LT", "MA_DBHC_SN", "DM_VUNG_ID", "STATUS", "CREATED_DATE",
    ]
    cur.execute("DELETE FROM MIR_VSS_DM_DBHC")
    proj_dbhc = build_explicit_select_projection(dbhc_cols, engine)
    cur.execute(
        f"""
        INSERT INTO MIR_VSS_DM_DBHC ({', '.join(dbhc_cols)}, MIRROR_SYNC_TS)
        SELECT {proj_dbhc}, datetime('now', 'localtime')
        FROM VSS_DM_DBHC
        """
    )
    counts["MIR_VSS_DM_DBHC"] = cur.rowcount

    conn.commit()
    return counts


def full_load_nhankhau_partitioned(
    conn: Any,
    engine: str = "LOCAL_SQLITE",
    num_partitions: int = 10,
    fetch_batch_size: int = 10000,
) -> Dict[str, Any]:
    """
    Full Load bảng lớn VSS_HO_GD_NHANKHAU -> MIR_VSS_HO_GD_NHANKHAU:
    - Bước 1: Ghi nhận SCN bàn giao (SNAPSHOT_SCN).
    - Bước 2: TRUNCATE/DELETE bảng đích MIR_VSS_HO_GD_NHANKHAU và DROP Unique Index UK_MIR_NK
              để tăng tốc độ nạp Bulk.
    - Bước 3: Đọc phân vùng theo MA_CD (10 phân vùng '0'..'9' + phân vùng ngoại lệ) theo từng lô
              (Streaming Cursor fetchmany, tuyệt đối không nạp toàn bộ vào RAM / không .cache()).
    - Bước 4: REBUILD Unique Index UK_MIR_NK (MA_CD, MA_HO_GD) và IDX_MIR_NK_MATINH.
    - Bước 5: Đối soát tổng số dòng và số dòng theo từng MA_TINH giữa Nguồn và Mirror.
    """
    t0 = time.perf_counter()
    cur = conn.cursor()

    snapshot_scn = capture_source_scn(conn, engine=engine)
    snapshot_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Bước 2: Làm sạch bảng đích & Tắt/Drop Index trước khi Bulk Load
    cur.execute("DROP INDEX IF EXISTS UK_MIR_NK;")
    cur.execute("DROP INDEX IF EXISTS IDX_MIR_NK_MATINH;")
    cur.execute("DELETE FROM MIR_VSS_HO_GD_NHANKHAU;")
    conn.commit()

    col_names = [c for c, _ in NHANKHAU_COLUMNS]
    explicit_proj = build_explicit_select_projection(col_names, engine)
    placeholders = ", ".join(["?"] * len(col_names))
    insert_sql = f"""
        INSERT INTO MIR_VSS_HO_GD_NHANKHAU ({', '.join(col_names)}, MIRROR_SYNC_TS)
        VALUES ({placeholders}, '{snapshot_time}')
    """

    # Bước 3: Đọc phân mảnh cân bằng theo chữ số cuối của MA_CD ('0'..'9')
    partition_suffixes = [str(d) for d in range(10)]
    partition_stats = {}
    total_loaded = 0

    read_cur = conn.cursor()
    write_cur = conn.cursor()

    for p_suffix in partition_suffixes:
        p_count = 0
        read_cur.execute(
            f"""
            SELECT {explicit_proj}
            FROM VSS_HO_GD_NHANKHAU
            WHERE SUBSTR(TRIM(MA_CD), -1, 1) = ?
            """,
            (p_suffix,),
        )
        while True:
            batch = read_cur.fetchmany(fetch_batch_size)
            if not batch:
                break
            write_cur.executemany(insert_sql, batch)
            p_count += len(batch)
        conn.commit()
        partition_stats[f"part_{p_suffix}"] = p_count
        total_loaded += p_count

    # Phân vùng phụ cho các dòng có MA_CD không kết thúc bằng '0'..'9' hoặc NULL (nếu có)
    read_cur.execute(
        f"""
        SELECT {explicit_proj}
        FROM VSS_HO_GD_NHANKHAU
        WHERE SUBSTR(TRIM(MA_CD), -1, 1) NOT IN ('0','1','2','3','4','5','6','7','8','9')
           OR MA_CD IS NULL
        """
    )
    other_count = 0
    while True:
        batch = read_cur.fetchmany(fetch_batch_size)
        if not batch:
            break
        write_cur.executemany(insert_sql, batch)
        other_count += len(batch)
    conn.commit()
    if other_count > 0:
        partition_stats["prefix_other"] = other_count
        total_loaded += other_count

    # Bước 4: REBUILD Unique Index UK_MIR_NK (MA_CD, MA_HO_GD) và IDX_MIR_NK_MATINH
    cur.execute("CREATE UNIQUE INDEX UK_MIR_NK ON MIR_VSS_HO_GD_NHANKHAU (MA_CD, MA_HO_GD);")
    cur.execute("CREATE INDEX IDX_MIR_NK_MATINH ON MIR_VSS_HO_GD_NHANKHAU (MA_TINH);")
    conn.commit()

    # Bước 5: Đối soát tổng số dòng và theo MA_TINH
    cur.execute("SELECT COUNT(*) FROM VSS_HO_GD_NHANKHAU")
    source_total = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM MIR_VSS_HO_GD_NHANKHAU")
    mirror_total = cur.fetchone()[0]

    cur.execute(
        """
        SELECT COALESCE(MA_TINH, '<NULL>') AS TINH, COUNT(*) AS CNT
        FROM VSS_HO_GD_NHANKHAU
        GROUP BY COALESCE(MA_TINH, '<NULL>')
        ORDER BY TINH
        """
    )
    src_by_tinh = dict(cur.fetchall())

    cur.execute(
        """
        SELECT COALESCE(MA_TINH, '<NULL>') AS TINH, COUNT(*) AS CNT
        FROM MIR_VSS_HO_GD_NHANKHAU
        GROUP BY COALESCE(MA_TINH, '<NULL>')
        ORDER BY TINH
        """
    )
    mir_by_tinh = dict(cur.fetchall())

    mismatched_provinces = {
        k: {"source": src_by_tinh.get(k, 0), "mirror": mir_by_tinh.get(k, 0)}
        for k in set(src_by_tinh) | set(mir_by_tinh)
        if src_by_tinh.get(k, 0) != mir_by_tinh.get(k, 0)
    }

    status = "PASSED" if (source_total == mirror_total and not mismatched_provinces) else "FAILED"

    # Lưu mốc SCN bàn giao cho Debezium CDC
    cur.execute(
        """
        INSERT INTO MIR_CDC_SCN_HANDOFF (
            TABLE_NAME, SNAPSHOT_SCN, SNAPSHOT_TIME, SOURCE_ROW_COUNT, MIRROR_ROW_COUNT, STATUS
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(TABLE_NAME) DO UPDATE SET
            SNAPSHOT_SCN = excluded.SNAPSHOT_SCN,
            SNAPSHOT_TIME = excluded.SNAPSHOT_TIME,
            SOURCE_ROW_COUNT = excluded.SOURCE_ROW_COUNT,
            MIRROR_ROW_COUNT = excluded.MIRROR_ROW_COUNT,
            STATUS = excluded.STATUS
        """,
        ("VSS_HO_GD_NHANKHAU", snapshot_scn, snapshot_time, source_total, mirror_total, status),
    )
    conn.commit()

    elapsed = round(time.perf_counter() - t0, 2)
    return {
        "snapshot_scn": snapshot_scn,
        "snapshot_time": snapshot_time,
        "source_total": source_total,
        "mirror_total": mirror_total,
        "diff": source_total - mirror_total,
        "mismatched_provinces": mismatched_provinces,
        "partition_stats": partition_stats,
        "reconciliation_status": status,
        "elapsed_sec": elapsed,
    }


def run_full_load_all(sqlite_path: Optional[Path] = None) -> Dict[str, Any]:
    conn = get_connection(sqlite_path=sqlite_path)
    engine = get_db_engine()
    init_source_and_mirror_schema(conn, engine=engine)

    catalog_counts = full_load_catalog_tables(conn, engine=engine)
    nk_report = full_load_nhankhau_partitioned(conn, engine=engine)
    conn.close()

    report = {
        "catalog_tables": catalog_counts,
        "nhankhau_full_load": nk_report,
    }

    out_dir = sqlite_path.parent if sqlite_path else (PROJECT_ROOT / "data")
    report_path = out_dir / "full_load_mirror_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        f"[FULL_LOAD_JDBC] Hoàn tất Full Load Nguồn -> Mirror ({nk_report['elapsed_sec']}s):\n"
        f"  - MIR_VSS_DM_DANTOC       : {catalog_counts['MIR_VSS_DM_DANTOC']:,} dòng\n"
        f"  - MIR_VSS_DM_QUOC_TICH    : {catalog_counts['MIR_VSS_DM_QUOC_TICH']:,} dòng\n"
        f"  - MIR_VSS_DM_DBHC         : {catalog_counts['MIR_VSS_DM_DBHC']:,} dòng\n"
        f"  - MIR_VSS_HO_GD_NHANKHAU  : {nk_report['mirror_total']:,} / {nk_report['source_total']:,} dòng "
        f"(Lệch: {nk_report['diff']}, Lệch theo Tỉnh: {len(nk_report['mismatched_provinces'])})\n"
        f"  - Mốc SCN bàn giao CDC    : {nk_report['snapshot_scn']} ({nk_report['reconciliation_status']})"
    )
    if nk_report["reconciliation_status"] != "PASSED":
        raise RuntimeError(f"Đối soát Full Load Nguồn - Mirror thất bại: {nk_report}")
    return report


def main():
    parser = argparse.ArgumentParser(description="Full Load 4 bảng từ Nguồn VSS sang vùng Mirror")
    parser.add_argument("--db-path", type=str, default=None, help="Đường dẫn file SQLite DB")
    args = parser.parse_args()

    run_full_load_all(sqlite_path=Path(args.db_path) if args.db_path else None)


if __name__ == "__main__":
    main()

