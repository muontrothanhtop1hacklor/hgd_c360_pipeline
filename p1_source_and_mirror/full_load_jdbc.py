#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BAT-C360-003 (Full Load Nguồn VSS -> Mirror):
Đồng bộ toàn lượng (Full Load) 4 bảng từ Nguồn (VSS_*) sang vùng Mirror (MIR_VSS_*):
  1. VSS_DM_DANTOC       -> MIR_VSS_DM_DANTOC
  2. VSS_DM_QUOC_TICH    -> MIR_VSS_DM_QUOC_TICH
  3. VSS_DM_DBHC         -> MIR_VSS_DM_DBHC
  4. VSS_HO_GD_NHANKHAU  -> MIR_VSS_HO_GD_NHANKHAU

Các nguyên tắc kỹ thuật bắt buộc được áp dụng (Mục 3 - Phần 1.2):
- Ghi nhận mốc SCN / Watermark trước khi đọc để bàn giao liền mạch cho Debezium CDC (MIR_CDC_SCN_HANDOFF).
- Phân mảnh đọc song song theo dải khóa SARGable trên cột MA_CD (`WHERE MA_CD >= ? AND MA_CD < ?`)
  tận dụng trực tiếp INDEX RANGE SCAN trên IDX_VSS_NK_MACD, không bọc hàm quanh MA_CD gây Full Table Scan.
- Hỗ trợ chạy phân tán qua PySpark JDBC (`spark.read.jdbc(..., predicates=range_predicates)`) và đa luồng DB-API
  (`ThreadPoolExecutor`) theo chế độ streaming batch, tuyệt đối KHÔNG gọi `.cache()`, `.persist()`, `.repartition()`.
- Ép kiểu tường minh (CAST) cho đúng 11 cột NUMBER không khai báo precision ở DDL gốc:
  + Bảng NHANKHAU: NGAY_SINH, NGAY_KS, NGAY_CAP_CMTND, NGAY_CAP_HC, DEAD_DATE,
                   NGAY_BATDAU, NGAY_HETHAN, NGAY_PHATSINH, NGAY_SINH_BK, LOCK_DATA
  + Bảng DBHC    : IS_CAP_XA
- Chuẩn hóa MA_HO_GD rỗng/NULL -> 'HO_MAC_DINH' ngay lúc đọc để tương thích Debezium JDBC Sink MERGE.
- Lưu Checkpoint từng partition vào MIR_FULL_LOAD_CHECKPOINT để chạy tiếp (Resume) khi lỗi giữa chừng.
- Chiến lược Index: Chưa đánh Unique Index UK_MIR_NK trước Full Load; chỉ REBUILD sau khi toàn bộ phân vùng hoàn tất.
- Đối soát tự động Nguồn vs Mirror (tổng số dòng 4 bảng và đối soát chi tiết theo từng MA_TINH).
"""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from p1_source_and_mirror.db_env import (
    NHANKHAU_COLUMNS,
    ensure_mirror_indexes,
    get_connection,
    get_db_engine,
    init_source_and_mirror_schema,
)

# Đúng 11 cột NUMBER không khai báo precision/scale trong DDL gốc cần CAST tường minh khi đọc JDBC
UNCONSTRAINED_NUMBER_COLS_NHANKHAU = {
    "NGAY_SINH",
    "NGAY_KS",
    "NGAY_CAP_CMTND",
    "NGAY_CAP_HC",
    "DEAD_DATE",
    "NGAY_BATDAU",
    "NGAY_HETHAN",
    "NGAY_PHATSINH",
    "NGAY_SINH_BK",
    "LOCK_DATA",
}
UNCONSTRAINED_NUMBER_COLS_DBHC = {"IS_CAP_XA"}
UNCONSTRAINED_INT_COLS = UNCONSTRAINED_NUMBER_COLS_NHANKHAU | UNCONSTRAINED_NUMBER_COLS_DBHC


def build_explicit_select_projection(columns: List[str], engine: str = "LOCAL_SQLITE") -> str:
    """
    Sinh mệnh đề SELECT:
    - Ép kiểu tường minh CAST cho các cột NUMBER không khai báo precision để tránh Decimal(38,10).
    - Chuẩn hóa MA_HO_GD NULL/rỗng thành 'HO_MAC_DINH' đồng nhất với JDBC Sink trên Mirror.
    """
    exprs = []
    for col in columns:
        if col == "MA_HO_GD":
            if engine == "ORACLE":
                exprs.append("NVL(TRIM(MA_HO_GD), 'HO_MAC_DINH') AS MA_HO_GD")
            else:
                exprs.append("COALESCE(NULLIF(TRIM(MA_HO_GD), ''), 'HO_MAC_DINH') AS MA_HO_GD")
        elif col in UNCONSTRAINED_INT_COLS:
            if engine == "ORACLE":
                exprs.append(f"CAST({col} AS NUMBER(18, 0)) AS {col}")
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
    cur.execute("SELECT COALESCE(MAX(ID), 0) FROM VSS_HO_GD_NHANKHAU")
    max_id = cur.fetchone()[0] or 0
    return int(time.time() * 1000) * 1000 + (max_id % 1000)


def full_load_catalog_tables(conn: Any, engine: str = "LOCAL_SQLITE") -> Dict[str, int]:
    """Full Load 3 bảng danh mục từ VSS_DM_* sang MIR_VSS_DM_*."""
    cur = conn.cursor()
    counts = {}
    ts_expr = "SYSTIMESTAMP" if engine == "ORACLE" else "datetime('now', 'localtime')"

    # 1. DM_DANTOC
    dt_cols = [
        "ID", "CAN_CU", "MA", "TEN", "TEN_GOI_KHAC", "DIA_BAN_CU_TRU",
        "NGAY_HL", "NGAY_HET_HL", "TRANG_THAI", "NGUOI_TAO", "NGAY_TAO",
        "NGUOI_SUA", "NGAY_SUA", "DAN_SO", "MA_CU", "CAP", "CHA_ID",
    ]
    cur.execute("DELETE FROM MIR_VSS_DM_DANTOC")
    proj_dt = build_explicit_select_projection(dt_cols, engine)
    cur.execute(
        f"""
        INSERT INTO MIR_VSS_DM_DANTOC ({', '.join(dt_cols)}, MIRROR_SYNC_TS)
        SELECT {proj_dt}, {ts_expr}
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
        SELECT {proj_qt}, {ts_expr}
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
        SELECT {proj_dbhc}, {ts_expr}
        FROM VSS_DM_DBHC
        """
    )
    counts["MIR_VSS_DM_DBHC"] = cur.rowcount

    conn.commit()
    return counts


def compute_macd_range_partitions(
    conn: Any,
    num_partitions: int = 10,
    engine: str = "LOCAL_SQLITE",
) -> List[Tuple[str, Optional[str], Optional[str]]]:
    """
    Chia bảng VSS_HO_GD_NHANKHAU thành N phân vùng dải khóa liên tiếp [lower, upper)
    dựa trên Covering Index IDX_VSS_NK_MACD.
    Mỗi phân vùng truy vấn bằng điều kiện SARGable (`MA_CD >= ? AND MA_CD < ?`),
    tuyệt đối không bọc hàm quanh MA_CD.
    """
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM VSS_HO_GD_NHANKHAU WHERE MA_CD IS NOT NULL")
    total_rows = cur.fetchone()[0] or 0

    if total_rows == 0 or num_partitions <= 1:
        return [("part_0", None, None)]

    split_points: List[str] = []
    for idx in range(1, num_partitions):
        offset = (total_rows * idx) // num_partitions
        if engine == "ORACLE":
            cur.execute(
                """
                SELECT MA_CD
                  FROM VSS_HO_GD_NHANKHAU
                 WHERE MA_CD IS NOT NULL
                 ORDER BY MA_CD
                OFFSET :off ROWS FETCH NEXT 1 ROWS ONLY
                """,
                {"off": offset},
            )
        else:
            cur.execute(
                """
                SELECT MA_CD
                  FROM VSS_HO_GD_NHANKHAU
                 WHERE MA_CD IS NOT NULL
                 ORDER BY MA_CD
                 LIMIT 1 OFFSET ?
                """,
                (offset,),
            )
        row = cur.fetchone()
        if row and row[0] is not None:
            val = str(row[0])
            if not split_points or val > split_points[-1]:
                split_points.append(val)

    if not split_points:
        return [("part_0", None, None)]

    partitions: List[Tuple[str, Optional[str], Optional[str]]] = []
    prev_bound: Optional[str] = None
    for idx, bound in enumerate(split_points):
        partitions.append((f"part_{idx}", prev_bound, bound))
        prev_bound = bound
    partitions.append((f"part_{len(split_points)}", prev_bound, None))
    return partitions


def build_range_where_clause(lower_bound: Optional[str], upper_bound: Optional[str]) -> Tuple[str, Tuple[Any, ...]]:
    """Tạo mệnh đề WHERE SARGable theo cặp [lower_bound, upper_bound) trên cột MA_CD."""
    if lower_bound is None and upper_bound is None:
        return ("WHERE MA_CD IS NOT NULL", ())
    if lower_bound is None and upper_bound is not None:
        return ("WHERE MA_CD < ?", (upper_bound,))
    if lower_bound is not None and upper_bound is not None:
        return ("WHERE MA_CD >= ? AND MA_CD < ?", (lower_bound, upper_bound))
    return ("WHERE MA_CD >= ?", (lower_bound,))


def build_spark_range_predicates(partitions: List[Tuple[str, Optional[str], Optional[str]]]) -> List[str]:
    """Sinh danh sách predicates cho spark.read.jdbc(predicates=...) tận dụng Index Range Scan."""
    predicates: List[str] = []
    for _, lower, upper in partitions:
        if lower is None and upper is None:
            predicates.append("MA_CD IS NOT NULL")
        elif lower is None and upper is not None:
            safe_u = upper.replace("'", "''")
            predicates.append(f"MA_CD < '{safe_u}'")
        elif lower is not None and upper is not None:
            safe_l = lower.replace("'", "''")
            safe_u = upper.replace("'", "''")
            predicates.append(f"MA_CD >= '{safe_l}' AND MA_CD < '{safe_u}'")
        else:
            safe_l = lower.replace("'", "''")  # type: ignore[union-attr]
            predicates.append(f"MA_CD >= '{safe_l}'")
    return predicates


def run_spark_jdbc_full_load(
    jdbc_url: str,
    db_properties: Dict[str, str],
    partitions: List[Tuple[str, Optional[str], Optional[str]]],
    explicit_projection: str,
    source_table: str = "VSS.VSS_HO_GD_NHANKHAU",
    target_table: str = "MIR_VSS_HO_GD_NHANKHAU",
    batch_size: int = 10000,
    spark: Any = None,
) -> Dict[str, Any]:
    """
    Thực thi Full Load phân tán qua PySpark JDBC bằng mảng `predicates` Index Range Scan.
    Tuyệt đối KHÔNG gọi `.cache()`, `.persist()`, hay `.repartition()` để tránh tràn RAM/Disk (OOM).
    """
    own_spark = False
    if spark is None:
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.appName("HGD_FullLoad_VSS_To_Mirror")
            .config("spark.sql.shuffle.partitions", str(max(len(partitions), 4)))
            .getOrCreate()
        )
        own_spark = True

    try:
        predicates = build_spark_range_predicates(partitions)
        subquery = f"(SELECT {explicit_projection} FROM {source_table}) src_view"
        df = spark.read.jdbc(
            url=jdbc_url,
            table=subquery,
            predicates=predicates,
            properties=db_properties,
        )
        # Ghi streaming trực tiếp theo từng partition đọc được, không cache/repartition
        df.write.mode("append").option("batchsize", str(batch_size)).jdbc(
            url=jdbc_url,
            table=target_table,
            properties=db_properties,
        )
        return {
            "mode": "SPARK_JDBC",
            "predicate_count": len(predicates),
            "predicates": predicates,
        }
    finally:
        if own_spark:
            spark.stop()


def _upsert_checkpoint(
    cur: Any,
    part_id: str,
    lower_bound: Optional[str],
    upper_bound: Optional[str],
    rows_loaded: int,
    status: str,
) -> None:
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cur.execute(
        """
        INSERT INTO MIR_FULL_LOAD_CHECKPOINT (
            TABLE_NAME, PARTITION_ID, LOWER_BOUND, UPPER_BOUND, ROWS_LOADED, STATUS, UPDATED_AT
        ) VALUES ('VSS_HO_GD_NHANKHAU', ?, ?, ?, ?, ?, ?)
        ON CONFLICT(TABLE_NAME, PARTITION_ID) DO UPDATE SET
            LOWER_BOUND = excluded.LOWER_BOUND,
            UPPER_BOUND = excluded.UPPER_BOUND,
            ROWS_LOADED = excluded.ROWS_LOADED,
            STATUS = excluded.STATUS,
            UPDATED_AT = excluded.UPDATED_AT
        """,
        (part_id, lower_bound, upper_bound, rows_loaded, status, now_str),
    )


def _load_single_range_partition(
    part_id: str,
    lower_bound: Optional[str],
    upper_bound: Optional[str],
    explicit_proj: str,
    insert_sql: str,
    fetch_batch_size: int,
    sqlite_db_file: Optional[str],
    shared_conn: Any,
    write_lock: threading.Lock,
    cleanup_partial: bool = False,
) -> Tuple[str, int]:
    """Đọc 1 phân vùng Index Range Scan theo lô streaming (fetchmany) và ghi sang Mirror."""
    where_clause, params = build_range_where_clause(lower_bound, upper_bound)

    if sqlite_db_file:
        read_conn = get_connection(sqlite_path=Path(sqlite_db_file))
    else:
        read_conn = shared_conn

    p_count = 0
    try:
        read_cur = read_conn.cursor()
        if cleanup_partial:
            # Khi resume phân vùng lỗi dở dang, dọn sạch dải dữ liệu của đúng phân vùng đó trước khi nạp lại
            with write_lock:
                w_conn = get_connection(sqlite_path=Path(sqlite_db_file)) if sqlite_db_file else shared_conn
                try:
                    w_cur = w_conn.cursor()
                    w_cur.execute(f"DELETE FROM MIR_VSS_HO_GD_NHANKHAU {where_clause}", params)
                    _upsert_checkpoint(w_cur, part_id, lower_bound, upper_bound, 0, "IN_PROGRESS")
                    w_conn.commit()
                finally:
                    if sqlite_db_file:
                        w_conn.close()

        read_cur.execute(
            f"""
            SELECT {explicit_proj}
              FROM VSS_HO_GD_NHANKHAU
              {where_clause}
            """,
            params,
        )

        while True:
            batch = read_cur.fetchmany(fetch_batch_size)
            if not batch:
                break
            with write_lock:
                w_conn = get_connection(sqlite_path=Path(sqlite_db_file)) if sqlite_db_file else shared_conn
                try:
                    w_cur = w_conn.cursor()
                    w_cur.executemany(insert_sql, batch)
                    w_conn.commit()
                finally:
                    if sqlite_db_file:
                        w_conn.close()
            p_count += len(batch)

        with write_lock:
            w_conn = get_connection(sqlite_path=Path(sqlite_db_file)) if sqlite_db_file else shared_conn
            try:
                w_cur = w_conn.cursor()
                _upsert_checkpoint(w_cur, part_id, lower_bound, upper_bound, p_count, "COMPLETED")
                w_conn.commit()
            finally:
                if sqlite_db_file:
                    w_conn.close()
    except Exception:
        with write_lock:
            w_conn = get_connection(sqlite_path=Path(sqlite_db_file)) if sqlite_db_file else shared_conn
            try:
                w_cur = w_conn.cursor()
                _upsert_checkpoint(w_cur, part_id, lower_bound, upper_bound, p_count, "FAILED")
                w_conn.commit()
            finally:
                if sqlite_db_file:
                    w_conn.close()
        raise
    finally:
        if sqlite_db_file:
            read_conn.close()

    return (part_id, p_count)


def full_load_nhankhau_partitioned(
    conn: Any,
    engine: str = "LOCAL_SQLITE",
    num_partitions: int = 10,
    fetch_batch_size: int = 10000,
    parallel_workers: int = 4,
    resume: bool = False,
    use_spark: bool = False,
    spark_jdbc_url: Optional[str] = None,
    spark_db_props: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Full Load bảng lớn VSS_HO_GD_NHANKHAU -> MIR_VSS_HO_GD_NHANKHAU:
    - Bước 1: Ghi nhận SCN bàn giao (SNAPSHOT_SCN) trước khi đọc.
    - Bước 2: Nếu không phải resume -> dọn bảng đích & DROP Index trước khi Bulk Load.
              Nếu resume=True -> đọc các phân vùng chưa COMPLETED từ MIR_FULL_LOAD_CHECKPOINT.
    - Bước 3: Tính dải phân vùng Index Range Scan trên MA_CD, chạy song song (PySpark JDBC hoặc ThreadPoolExecutor)
              với streaming cursor fetchmany, tuyệt đối không .cache().
    - Bước 4: REBUILD Unique Index UK_MIR_NK (MA_CD, MA_HO_GD), IDX_MIR_NK_MOD và IDX_MIR_NK_MATINH.
    - Bước 5: Đối soát tổng số dòng và số dòng theo từng MA_TINH giữa Nguồn và Mirror.
    """
    t0 = time.perf_counter()
    cur = conn.cursor()

    snapshot_scn = capture_source_scn(conn, engine=engine)
    snapshot_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if resume:
        cur.execute(
            "SELECT SNAPSHOT_SCN, SNAPSHOT_TIME FROM MIR_CDC_SCN_HANDOFF WHERE TABLE_NAME = 'VSS_HO_GD_NHANKHAU'"
        )
        prev_scn_row = cur.fetchone()
        if prev_scn_row:
            snapshot_scn = int(prev_scn_row[0])
            snapshot_time = str(prev_scn_row[1])

    col_names = [c for c, _ in NHANKHAU_COLUMNS]
    explicit_proj = build_explicit_select_projection(col_names, engine)
    placeholders = ", ".join(["?"] * len(col_names))
    insert_sql = f"""
        INSERT INTO MIR_VSS_HO_GD_NHANKHAU ({', '.join(col_names)}, MIRROR_SYNC_TS)
        VALUES ({placeholders}, '{snapshot_time}')
    """

    sqlite_db_file: Optional[str] = None
    if engine != "ORACLE":
        db_list = cur.execute("PRAGMA database_list").fetchall()
        for _, db_name, file_path in db_list:
            if db_name == "main" and file_path:
                sqlite_db_file = file_path
                break

    partitions_to_run: List[Tuple[str, Optional[str], Optional[str], bool]] = []
    partition_stats: Dict[str, int] = {}

    if resume:
        cur.execute(
            """
            SELECT PARTITION_ID, LOWER_BOUND, UPPER_BOUND, ROWS_LOADED, STATUS
              FROM MIR_FULL_LOAD_CHECKPOINT
             WHERE TABLE_NAME = 'VSS_HO_GD_NHANKHAU'
             ORDER BY PARTITION_ID
            """
        )
        cp_rows = cur.fetchall()
        if cp_rows:
            cur.execute("DROP INDEX IF EXISTS UK_MIR_NK;")
            cur.execute("DROP INDEX IF EXISTS IDX_MIR_NK_MOD;")
            cur.execute("DROP INDEX IF EXISTS IDX_MIR_NK_MATINH;")
            conn.commit()
            for part_id, lb, ub, rows_loaded, cp_status in cp_rows:
                if cp_status == "COMPLETED":
                    partition_stats[part_id] = rows_loaded
                else:
                    partitions_to_run.append((part_id, lb, ub, True))
        else:
            resume = False

    if not resume:
        # Chạy mới từ đầu: Lưu mốc SCN trước khi đọc, DROP Index & dọn bảng đích
        cur.execute(
            """
            INSERT INTO MIR_CDC_SCN_HANDOFF (
                TABLE_NAME, SNAPSHOT_SCN, SNAPSHOT_TIME, SOURCE_ROW_COUNT, MIRROR_ROW_COUNT, STATUS
            ) VALUES (?, ?, ?, 0, 0, 'IN_PROGRESS')
            ON CONFLICT(TABLE_NAME) DO UPDATE SET
                SNAPSHOT_SCN = excluded.SNAPSHOT_SCN,
                SNAPSHOT_TIME = excluded.SNAPSHOT_TIME,
                SOURCE_ROW_COUNT = 0,
                MIRROR_ROW_COUNT = 0,
                STATUS = 'IN_PROGRESS'
            """,
            ("VSS_HO_GD_NHANKHAU", snapshot_scn, snapshot_time),
        )
        cur.execute("DROP INDEX IF EXISTS UK_MIR_NK;")
        cur.execute("DROP INDEX IF EXISTS IDX_MIR_NK_MOD;")
        cur.execute("DROP INDEX IF EXISTS IDX_MIR_NK_MATINH;")
        cur.execute("DELETE FROM MIR_VSS_HO_GD_NHANKHAU;")
        cur.execute("DELETE FROM MIR_FULL_LOAD_CHECKPOINT WHERE TABLE_NAME = 'VSS_HO_GD_NHANKHAU';")

        partitions = compute_macd_range_partitions(conn, num_partitions=num_partitions, engine=engine)
        for part_id, lb, ub in partitions:
            _upsert_checkpoint(cur, part_id, lb, ub, 0, "PENDING")
            partitions_to_run.append((part_id, lb, ub, False))
        conn.commit()

    if use_spark and spark_jdbc_url and spark_db_props:
        spark_parts = [(p[0], p[1], p[2]) for p in partitions_to_run]
        if spark_parts:
            run_spark_jdbc_full_load(
                jdbc_url=spark_jdbc_url,
                db_properties=spark_db_props,
                partitions=spark_parts,
                explicit_projection=explicit_proj,
                batch_size=fetch_batch_size,
            )
            for part_id, lb, ub, _ in spark_parts:
                where_clause, params = build_range_where_clause(lb, ub)
                cur.execute(f"SELECT COUNT(*) FROM MIR_VSS_HO_GD_NHANKHAU {where_clause}", params)
                cnt = cur.fetchone()[0]
                partition_stats[part_id] = cnt
                _upsert_checkpoint(cur, part_id, lb, ub, cnt, "COMPLETED")
            conn.commit()
    else:
        write_lock = threading.Lock()
        if sqlite_db_file and len(partitions_to_run) > 1 and parallel_workers > 1:
            workers = min(parallel_workers, len(partitions_to_run))
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = [
                    executor.submit(
                        _load_single_range_partition,
                        part_id,
                        lb,
                        ub,
                        explicit_proj,
                        insert_sql,
                        fetch_batch_size,
                        sqlite_db_file,
                        conn,
                        write_lock,
                        cleanup_partial,
                    )
                    for part_id, lb, ub, cleanup_partial in partitions_to_run
                ]
                for fut in as_completed(futures):
                    p_id, p_cnt = fut.result()
                    partition_stats[p_id] = p_cnt
        else:
            for part_id, lb, ub, cleanup_partial in partitions_to_run:
                p_id, p_cnt = _load_single_range_partition(
                    part_id=part_id,
                    lower_bound=lb,
                    upper_bound=ub,
                    explicit_proj=explicit_proj,
                    insert_sql=insert_sql,
                    fetch_batch_size=fetch_batch_size,
                    sqlite_db_file=None,
                    shared_conn=conn,
                    write_lock=write_lock,
                    cleanup_partial=cleanup_partial,
                )
                partition_stats[p_id] = p_cnt

    # Bổ sung nạp các dòng MA_CD IS NULL (nếu có) chỉ khi chạy lần đầu hoặc còn sót
    if not resume:
        read_null_cur = conn.cursor()
        write_null_cur = conn.cursor()
        read_null_cur.execute(
            f"SELECT {explicit_proj} FROM VSS_HO_GD_NHANKHAU WHERE MA_CD IS NULL"
        )
        null_count = 0
        while True:
            batch = read_null_cur.fetchmany(fetch_batch_size)
            if not batch:
                break
            write_null_cur.executemany(insert_sql, batch)
            null_count += len(batch)
        if null_count > 0:
            partition_stats["part_null"] = null_count
        conn.commit()

    # Bước 4: REBUILD Unique Index UK_MIR_NK (MA_CD, MA_HO_GD), IDX_MIR_NK_MOD và IDX_MIR_NK_MATINH
    ensure_mirror_indexes(conn, engine=engine)

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

    # Sắp xếp partition_stats theo chỉ số
    sorted_parts = dict(
        sorted(partition_stats.items(), key=lambda kv: (0, int(kv[0].split("_")[1])) if kv[0].startswith("part_") and kv[0].split("_")[1].isdigit() else (1, 0))
    )

    elapsed = round(time.perf_counter() - t0, 2)
    return {
        "snapshot_scn": snapshot_scn,
        "snapshot_time": snapshot_time,
        "source_total": source_total,
        "mirror_total": mirror_total,
        "diff": source_total - mirror_total,
        "mismatched_provinces": mismatched_provinces,
        "partition_stats": sorted_parts,
        "resume_mode": resume,
        "execution_mode": "SPARK_JDBC" if use_spark else ("MULTI_THREAD_RANGE_SCAN" if parallel_workers > 1 else "SINGLE_THREAD_RANGE_SCAN"),
        "reconciliation_status": status,
        "elapsed_sec": elapsed,
    }


def run_full_load_all(
    sqlite_path: Optional[Path] = None,
    num_partitions: int = 10,
    parallel_workers: int = 4,
    resume: bool = False,
    use_spark: Optional[bool] = None,
    spark_jdbc_url: Optional[str] = None,
    spark_db_props: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    conn = get_connection(sqlite_path=sqlite_path)
    engine = get_db_engine()
    init_source_and_mirror_schema(conn, engine=engine)

    if use_spark is None:
        use_spark = os.environ.get("USE_SPARK_JDBC", "0") == "1"

    catalog_counts = full_load_catalog_tables(conn, engine=engine)
    nk_report = full_load_nhankhau_partitioned(
        conn,
        engine=engine,
        num_partitions=num_partitions,
        parallel_workers=parallel_workers,
        resume=resume,
        use_spark=use_spark,
        spark_jdbc_url=spark_jdbc_url,
        spark_db_props=spark_db_props,
    )
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
        f"[FULL_LOAD_JDBC] Hoàn tất Full Load Nguồn -> Mirror ({nk_report['elapsed_sec']}s, "
        f"Mode={nk_report['execution_mode']}, Resume={resume}):\n"
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
    parser.add_argument("--partitions", type=int, default=10, help="Số dải partition Index Range Scan")
    parser.add_argument("--workers", type=int, default=4, help="Số luồng đọc song song (DB-API mode)")
    parser.add_argument("--resume", action="store_true", help="Chạy tiếp các partition chưa COMPLETED từ checkpoint")
    parser.add_argument("--use-spark", action="store_true", help="Thực thi phân tán qua PySpark JDBC")
    parser.add_argument("--jdbc-url", type=str, default=None, help="JDBC URL khi chạy --use-spark")
    args = parser.parse_args()

    run_full_load_all(
        sqlite_path=Path(args.db_path) if args.db_path else None,
        num_partitions=args.partitions,
        parallel_workers=args.workers,
        resume=args.resume,
        use_spark=args.use_spark,
        spark_jdbc_url=args.jdbc_url,
    )


if __name__ == "__main__":
    main()
