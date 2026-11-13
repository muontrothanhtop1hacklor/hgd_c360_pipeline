#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BAT-C360-005A: Job Full Load nạp toàn bộ dữ liệu Chuẩn hóa Công dân (C360).
Thực hiện:
  1. Đặt cờ PAUSE_NRT = 1 trong C360_SYNC_CONTROL để tạm dừng luồng Near Real-time.
  2. Lọc nghiệp vụ: Loại bỏ TYPEID in ('GT','GM','GG','GK') và TRANG_THAI_DUYET in (1,3,4).
  3. Khử trùng theo MA_CD: Chọn 1 dòng duy nhất có MODIFY_DATE -> CREATED_DATE -> ID mới nhất.
  4. Áp dụng chuẩn hóa toàn diện từ db_c360_cleaner.py (Single Source of Truth).
  5. Xóa và nạp mới C360_THONG_TIN_CA_NHAN theo từng batch (batch_size = 5.000).
  6. Xóa các sự kiện trong C360_SYNC_QUEUE phát sinh trước mốc snapshot.
  7. Mở lại cờ PAUSE_NRT = 0 và ghi log C360_SYNC_LOG.
"""

import argparse
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
from p2_c360_standardization.build_map_dbhc import build_and_refresh_dbhc_maps
from p2_c360_standardization.db_c360_cleaner import (
    load_standardization_lookups,
    transform_citizen_record,
)


def run_c360_full_load(
    sqlite_path: Optional[Path] = None,
    batch_size: int = 5000,
) -> Dict[str, Any]:
    """Thực thi toàn bộ luồng Batch Full Load sang C360_THONG_TIN_CA_NHAN."""
    conn = get_connection(sqlite_path=sqlite_path)
    init_source_and_mirror_schema(conn)
    init_c360_schema(conn)

    cur = conn.cursor()
    start_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    t0 = time.time()
    batch_id = int(time.time())

    # 1. Đặt cờ tạm dừng luồng Near Real-time
    cur.execute("UPDATE C360_SYNC_CONTROL SET PAUSE_NRT = 1 WHERE PROCESS_NAME = 'C360_SYNC';")
    conn.commit()

    try:
        # 2. Đảm bảo 2 bảng Map DBHC đã sẵn sàng
        cur.execute("SELECT COUNT(*) FROM C360_MAP_DBHC_TINH;")
        if cur.fetchone()[0] == 0:
            build_and_refresh_dbhc_maps(sqlite_path=sqlite_path)

        lookups = load_standardization_lookups(conn)

        # 3. Chụp mốc snapshot thời gian
        snapshot_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 4. Truy vấn lọc nghiệp vụ & khử trùng MA_CD từ Mirror
        # Lọc: TYPEID not in ('GT','GM','GG','GK') và TRANG_THAI_DUYET not in (1,3,4)
        # Khử trùng: ROW_NUMBER() over (PARTITION BY MA_CD ORDER BY MODIFY_DATE DESC, CREATED_DATE DESC, ID DESC) = 1
        query = """
        WITH filtered AS (
            SELECT *
              FROM MIR_VSS_HO_GD_NHANKHAU
             WHERE (TYPEID IS NULL OR TYPEID NOT IN ('D', 'GT', 'GM', 'GG', 'GK'))
               AND (TRANG_THAI_DUYET IS NOT NULL AND TRANG_THAI_DUYET NOT IN (0, 3, 4))
        ),
        ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY MA_CD
                       ORDER BY COALESCE(NGAY_SUA, '') DESC,
                                COALESCE(NGAY_TAO, '') DESC,
                                ID DESC
                   ) AS rn
              FROM filtered
        )
        SELECT * FROM ranked WHERE rn = 1;
        """
        cur.execute(query)
        col_names = [d[0] for d in cur.description]
        raw_rows = cur.fetchall()

        total_valid_citizens = len(raw_rows)

        # 5. Xóa dữ liệu cũ trên bảng C360
        cur.execute("DELETE FROM C360_THONG_TIN_CA_NHAN;")

        # 6. Chuẩn hóa và chèn theo từng batch
        insert_sql = """
        INSERT INTO C360_THONG_TIN_CA_NHAN (
            MA_SO_BHXH, MA_SO_BHXH_CU, SO_DINH_DANH, HO_TEN, HO_TEN_KHONG_DAU,
            GIOI_TINH, NGAY_SINH_CHUAN, NGAY_SINH_GOC, SO_CCCD, SO_CMTND,
            SO_DIEN_THOAI, EMAIL, MA_TINH_THUONG_TRU, TEN_TINH_THUONG_TRU,
            MA_XA_THUONG_TRU, TEN_XA_THUONG_TRU, DIA_CHI_THUONG_TRU,
            MA_TINH_LIEN_HE, MA_XA_LIEN_HE, DIA_CHI_LIEN_HE,
            MA_TINH_KHAI_SINH, MA_XA_KHAI_SINH, DIA_CHI_KHAI_SINH,
            DM_DAN_TOC_ID, MA_DAN_TOC, TEN_DAN_TOC, QUOC_TICH_RAW,
            MA_QUOC_TICH, TEN_QUOC_TICH, IS_ALIVE, DEAD_DATE,
            SOURCE_MA_HO_GD, SOURCE_MODIFY_DATE, SOURCE_CREATED_DATE,
            DQ_FLAGS, SYNC_BATCH_ID
        ) VALUES (
            :MA_SO_BHXH, :MA_SO_BHXH_CU, :SO_DINH_DANH, :HO_TEN, :HO_TEN_KHONG_DAU,
            :GIOI_TINH, :NGAY_SINH_CHUAN, :NGAY_SINH_GOC, :SO_CCCD, :SO_CMTND,
            :SO_DIEN_THOAI, :EMAIL, :MA_TINH_THUONG_TRU, :TEN_TINH_THUONG_TRU,
            :MA_XA_THUONG_TRU, :TEN_XA_THUONG_TRU, :DIA_CHI_THUONG_TRU,
            :MA_TINH_LIEN_HE, :MA_XA_LIEN_HE, :DIA_CHI_LIEN_HE,
            :MA_TINH_KHAI_SINH, :MA_XA_KHAI_SINH, :DIA_CHI_KHAI_SINH,
            :DM_DAN_TOC_ID, :MA_DAN_TOC, :TEN_DAN_TOC, :QUOC_TICH_RAW,
            :MA_QUOC_TICH, :TEN_QUOC_TICH, :IS_ALIVE, :DEAD_DATE,
            :SOURCE_MA_HO_GD, :SOURCE_MODIFY_DATE, :SOURCE_CREATED_DATE,
            :DQ_FLAGS, :SYNC_BATCH_ID
        );
        """

        buffer: List[Dict[str, Any]] = []
        for row in raw_rows:
            raw_dict = dict(zip(col_names, row))
            c360_record = transform_citizen_record(raw_dict, lookups, sync_batch_id=batch_id)
            buffer.append(c360_record)

            if len(buffer) >= batch_size:
                cur.executemany(insert_sql, buffer)
                buffer.clear()

        if buffer:
            cur.executemany(insert_sql, buffer)
            buffer.clear()

        # 7. Xóa các sự kiện Queue đã phát sinh trước mốc snapshot
        cur.execute(
            "DELETE FROM C360_SYNC_QUEUE WHERE ENQUEUE_TIME <= ?;",
            (snapshot_time,),
        )

        end_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 8. Ghi log hoàn thành vào C360_SYNC_LOG
        cur.execute(
            """
            INSERT INTO C360_SYNC_LOG (
                BATCH_TYPE, START_TIME, END_TIME, AFFECTED_KEYS, MERGE_COUNT, DELETE_COUNT, STATUS, ERROR_MSG
            ) VALUES ('FULL_LOAD', ?, ?, ?, ?, 0, 'SUCCESS', NULL);
            """,
            (start_time_str, end_time_str, total_valid_citizens, total_valid_citizens),
        )

        # Cập nhật C360_SYNC_CONTROL
        cur.execute(
            """
            UPDATE C360_SYNC_CONTROL
               SET LAST_FULL_LOAD_TIME = ?,
                   LAST_NRT_STATUS = 'READY'
             WHERE PROCESS_NAME = 'C360_SYNC';
            """,
            (snapshot_time,),
        )

        conn.commit()

    except Exception as e:
        conn.rollback()
        # Ghi log lỗi
        try:
            err_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cur.execute(
                """
                INSERT INTO C360_SYNC_LOG (
                    BATCH_TYPE, START_TIME, END_TIME, AFFECTED_KEYS, MERGE_COUNT, DELETE_COUNT, STATUS, ERROR_MSG
                ) VALUES ('FULL_LOAD', ?, ?, 0, 0, 0, 'FAILED', ?);
                """,
                (start_time_str, err_end, str(e)),
            )
            conn.commit()
        except Exception:
            pass
        raise e
    finally:
        # BẮT BUỘC nhả cờ PAUSE_NRT = 0 trong mọi tình huống để không làm treo NRT
        cur.execute("UPDATE C360_SYNC_CONTROL SET PAUSE_NRT = 0 WHERE PROCESS_NAME = 'C360_SYNC';")
        conn.commit()
        conn.close()

    elapsed = round(time.time() - t0, 2)
    return {
        "status": "SUCCESS",
        "total_citizens_loaded": total_valid_citizens,
        "batch_id": batch_id,
        "snapshot_time": snapshot_time,
        "elapsed_sec": elapsed,
    }


def main():
    parser = argparse.ArgumentParser(description="Chạy Job Full Load nạp toàn bộ C360")
    parser.add_argument("--db-path", type=str, default=None, help="Đường dẫn file SQLite DB")
    parser.add_argument("--batch-size", type=int, default=5000, help="Kích thước batch insert")
    args = parser.parse_args()

    db_path = Path(args.db_path) if args.db_path else None
    print("=" * 70)
    print(" BẮT ĐẦU CHẠY JOB FULL LOAD CHUẨN HÓA C360 (BAT-C360-005A)")
    print("=" * 70)

    result = run_c360_full_load(sqlite_path=db_path, batch_size=args.batch_size)
    print(f" [OK] Trạng thái nạp           : {result['status']}")
    print(f" [OK] Số công dân hợp lệ C360 : {result['total_citizens_loaded']:,} công dân")
    print(f" [OK] Snapshot thời gian       : {result['snapshot_time']}")
    print(f" [OK] Thời gian thực hiện      : {result['elapsed_sec']} giây")
    print("=" * 70)


if __name__ == "__main__":
    main()
