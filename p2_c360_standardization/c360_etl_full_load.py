#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BAT-C360-005A: Job Full Load nạp toàn bộ dữ liệu Chuẩn hóa Công dân (C360).
Thực hiện:
  1. Đặt cờ PAUSE_NRT = 1 trong C360_SYNC_CONTROL và chờ Worker NRT đang chạy (nếu có) kết thúc.
  2. Chụp mốc snapshot (snapshot_time, snap_max_qid) trước khi đọc Mirror để không mất sự kiện CDC mới.
  3. Lọc nghiệp vụ: Loại bỏ TYPEID in ('GT','GM','GG','GK') và TRANG_THAI_DUYET in (1,3,4).
  4. Khử trùng theo MA_CD: Chọn 1 dòng duy nhất có NGAY_SUA -> NGAY_TAO -> ID mới nhất.
  5. Đọc streaming theo từng lô (fetchmany, không fetchall để tránh OOM) và áp dụng db_c360_cleaner.py.
  6. Xóa và nạp mới C360_THONG_TIN_CA_NHAN trong cùng Transaction, sau đó xóa Queue <= snap_max_qid.
  7. Mở lại cờ PAUSE_NRT = 0 (trong khối finally) và ghi log C360_SYNC_LOG.
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
    nrt_wait_timeout_sec: float = 10.0,
) -> Dict[str, Any]:
    """Thực thi toàn bộ luồng Batch Full Load sang C360_THONG_TIN_CA_NHAN."""
    conn = get_connection(sqlite_path=sqlite_path)
    init_source_and_mirror_schema(conn)
    init_c360_schema(conn)

    cur = conn.cursor()
    start_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    t0 = time.time()
    batch_id = int(time.time())

    # 1. Đặt cờ tạm dừng luồng Near Real-time và chờ batch NRT đang chạy (nếu có) hoàn tất
    cur.execute("UPDATE C360_SYNC_CONTROL SET PAUSE_NRT = 1 WHERE PROCESS_NAME = 'C360_SYNC';")
    conn.commit()

    wait_start = time.time()
    while time.time() - wait_start < nrt_wait_timeout_sec:
        cur.execute("SELECT LAST_NRT_STATUS FROM C360_SYNC_CONTROL WHERE PROCESS_NAME = 'C360_SYNC';")
        status_row = cur.fetchone()
        if not status_row or status_row[0] != "RUNNING":
            break
        time.sleep(0.2)

    try:
        # 2. Đảm bảo 2 bảng Map DBHC đã sẵn sàng
        cur.execute("SELECT COUNT(*) FROM C360_MAP_DBHC_TINH;")
        if cur.fetchone()[0] == 0:
            build_and_refresh_dbhc_maps(sqlite_path=sqlite_path)

        lookups = load_standardization_lookups(conn)

        # 3. Chụp mốc snapshot (thời điểm + QUEUE_ID lớn nhất hiện tại) trước khi đọc Mirror
        snapshot_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cur.execute("SELECT COALESCE(MAX(QUEUE_ID), 0) FROM C360_SYNC_QUEUE;")
        snap_max_qid = int(cur.fetchone()[0] or 0)

        # 4. Truy vấn lọc nghiệp vụ & khử trùng MA_CD từ Mirror (streaming qua read_cur riêng)
        query = """
        WITH filtered AS (
            SELECT *
              FROM MIR_VSS_HO_GD_NHANKHAU
             WHERE COALESCE(TYPEID, 'ZZ') NOT IN ('GT', 'GM', 'GG', 'GK')
               AND COALESCE(TRANG_THAI_DUYET, 0) NOT IN (1, 3, 4)
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
        read_cur = conn.cursor()
        read_cur.execute(query)
        col_names = [d[0] for d in read_cur.description]

        # 5. Xóa dữ liệu cũ trên bảng C360 (cùng transaction với bước insert)
        write_cur = conn.cursor()
        write_cur.execute("DELETE FROM C360_THONG_TIN_CA_NHAN;")

        # 6. Đọc streaming theo từng lô (fetchmany) -> chuẩn hóa -> chèn theo batch
        insert_sql = """
        INSERT INTO C360_THONG_TIN_CA_NHAN (
            MA_SO_BHXH, MA_SO_BHXH_CU, SO_DINH_DANH, HO_TEN, HO_TEN_KHONG_DAU,
            GIOI_TINH, NGAY_SINH_CHUAN, NAM_SINH, NGAY_SINH_GOC, SO_CCCD, SO_CMTND,
            SO_DIEN_THOAI, SO_DIEN_THOAI_GOC, EMAIL,
            MA_TINH_THUONG_TRU, TEN_TINH_THUONG_TRU, MA_XA_THUONG_TRU, TEN_XA_THUONG_TRU,
            DIA_CHI_THUONG_TRU, MA_TINH_GOC, MA_XA_GOC,
            MA_TINH_LIEN_HE, TEN_TINH_LIEN_HE, MA_XA_LIEN_HE, TEN_XA_LIEN_HE,
            DIA_CHI_LIEN_HE, MA_TINH_LH_GOC, MA_XA_LH_GOC,
            MA_TINH_KHAI_SINH, TEN_TINH_KHAI_SINH, MA_XA_KHAI_SINH, TEN_XA_KHAI_SINH,
            DIA_CHI_KHAI_SINH, MA_TINH_KS_GOC, MA_XA_KS_GOC,
            DM_DAN_TOC_ID, MA_DAN_TOC, TEN_DAN_TOC, QUOC_TICH_RAW,
            MA_QUOC_TICH, TEN_QUOC_TICH, IS_ALIVE, DEAD_DATE,
            SOURCE_MA_HO_GD, SOURCE_MODIFY_DATE, SOURCE_CREATED_DATE,
            DQ_FLAGS, SYNC_BATCH_ID
        ) VALUES (
            :MA_SO_BHXH, :MA_SO_BHXH_CU, :SO_DINH_DANH, :HO_TEN, :HO_TEN_KHONG_DAU,
            :GIOI_TINH, :NGAY_SINH_CHUAN, :NAM_SINH, :NGAY_SINH_GOC, :SO_CCCD, :SO_CMTND,
            :SO_DIEN_THOAI, :SO_DIEN_THOAI_GOC, :EMAIL,
            :MA_TINH_THUONG_TRU, :TEN_TINH_THUONG_TRU, :MA_XA_THUONG_TRU, :TEN_XA_THUONG_TRU,
            :DIA_CHI_THUONG_TRU, :MA_TINH_GOC, :MA_XA_GOC,
            :MA_TINH_LIEN_HE, :TEN_TINH_LIEN_HE, :MA_XA_LIEN_HE, :TEN_XA_LIEN_HE,
            :DIA_CHI_LIEN_HE, :MA_TINH_LH_GOC, :MA_XA_LH_GOC,
            :MA_TINH_KHAI_SINH, :TEN_TINH_KHAI_SINH, :MA_XA_KHAI_SINH, :TEN_XA_KHAI_SINH,
            :DIA_CHI_KHAI_SINH, :MA_TINH_KS_GOC, :MA_XA_KS_GOC,
            :DM_DAN_TOC_ID, :MA_DAN_TOC, :TEN_DAN_TOC, :QUOC_TICH_RAW,
            :MA_QUOC_TICH, :TEN_QUOC_TICH, :IS_ALIVE, :DEAD_DATE,
            :SOURCE_MA_HO_GD, :SOURCE_MODIFY_DATE, :SOURCE_CREATED_DATE,
            :DQ_FLAGS, :SYNC_BATCH_ID
        );
        """

        total_valid_citizens = 0
        while True:
            chunk = read_cur.fetchmany(batch_size)
            if not chunk:
                break
            buffer: List[Dict[str, Any]] = [
                transform_citizen_record(dict(zip(col_names, row)), lookups, sync_batch_id=batch_id)
                for row in chunk
            ]
            write_cur.executemany(insert_sql, buffer)
            total_valid_citizens += len(buffer)

        # 7. Chỉ xóa các sự kiện Queue đã tồn tại trước mốc snapshot (QUEUE_ID <= snap_max_qid)
        if snap_max_qid > 0:
            write_cur.execute(
                "DELETE FROM C360_SYNC_QUEUE WHERE QUEUE_ID <= ? AND ENQUEUE_TIME <= ?;",
                (snap_max_qid, snapshot_time),
            )

        end_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 8. Ghi log hoàn thành vào C360_SYNC_LOG
        write_cur.execute(
            """
            INSERT INTO C360_SYNC_LOG (
                BATCH_TYPE, START_TIME, END_TIME, AFFECTED_KEYS, MERGE_COUNT, DELETE_COUNT, STATUS, ERROR_MSG
            ) VALUES ('FULL_LOAD', ?, ?, ?, ?, 0, 'SUCCESS', NULL);
            """,
            (start_time_str, end_time_str, total_valid_citizens, total_valid_citizens),
        )

        # Cập nhật C360_SYNC_CONTROL
        write_cur.execute(
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
        "snap_max_qid": snap_max_qid,
        "elapsed_sec": elapsed,
    }


def main():
    parser = argparse.ArgumentParser(description="Chạy Job Full Load nạp toàn bộ C360")
    parser.add_argument("--db-path", type=str, default=None, help="Đường dẫn file SQLite DB")
    parser.add_argument("--batch-size", type=int, default=5000, help="Kích thước batch streaming/insert")
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
