#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BAT-C360-005B: Worker Đồng bộ Near Real-time (Mirror -> C360) qua Queue.
Lập lịch chạy định kỳ mỗi phút:
  1. Kiểm tra cờ PAUSE_NRT trong C360_SYNC_CONTROL (nếu Full Load đang chạy thì bỏ qua).
  2. Đánh dấu LAST_NRT_STATUS = 'RUNNING' để phối hợp khóa với Full Load.
  3. Lấy chính xác tối đa `max_batch_size` bản ghi (QUEUE_ID, MA_CD) ở trạng thái 'PENDING'
     (và đủ tuổi `min_age_seconds` nếu cấu hình chống out-of-order commit).
  4. Khử trùng lấy danh sách DISTINCT MA_CD từ đúng các dòng vừa bắt.
  5. Với mỗi MA_CD:
     - Nếu còn dòng hợp lệ trên Mirror: MERGE (UPSERT) vào C360_THONG_TIN_CA_NHAN.
     - Nếu KHÔNG còn dòng hợp lệ trên Mirror (bị xóa ở nguồn, đổi MA_CD, hoặc chuyển
       sang diện giảm/chưa duyệt): DELETE khỏi C360_THONG_TIN_CA_NHAN.
  6. CHỈ xóa đúng các QUEUE_ID đã được bắt trong lô này (WHERE QUEUE_ID IN (...)),
     tuyệt đối không xóa theo khoảng <= v_cut để tránh mất sự kiện khi lô lớn hoặc commit trễ.
  7. Ghi nhận kết quả vào C360_SYNC_LOG và cập nhật C360_SYNC_CONTROL.
"""

import argparse
import sys
import time
from datetime import datetime, timedelta
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
from p2_c360_standardization.db_c360_cleaner import (
    load_standardization_lookups,
    transform_citizen_record,
)


def _delete_queue_ids(cur: Any, queue_ids: List[int], chunk_size: int = 500) -> int:
    """Xóa chính xác danh sách QUEUE_ID đã xử lý theo từng chunk (tránh vượt giới hạn tham số SQL)."""
    total_deleted = 0
    for i in range(0, len(queue_ids), chunk_size):
        chunk = queue_ids[i : i + chunk_size]
        placeholders = ",".join("?" for _ in chunk)
        cur.execute(f"DELETE FROM C360_SYNC_QUEUE WHERE QUEUE_ID IN ({placeholders});", chunk)
        total_deleted += cur.rowcount if cur.rowcount > 0 else 0
    return total_deleted


def run_c360_nrt_batch(
    sqlite_path: Optional[Path] = None,
    max_batch_size: int = 5000,
    min_age_seconds: int = 0,
) -> Dict[str, Any]:
    """Xử lý 1 lô hàng đợi Near Real-time từ C360_SYNC_QUEUE."""
    conn = get_connection(sqlite_path=sqlite_path)
    init_source_and_mirror_schema(conn)
    init_c360_schema(conn)

    cur = conn.cursor()
    now_dt = datetime.now()
    start_time_str = now_dt.strftime("%Y-%m-%d %H:%M:%S")
    t0 = time.time()
    batch_id = int(time.time())

    # 1. Kiểm tra Concurrency Control (PAUSE_NRT)
    cur.execute("SELECT PAUSE_NRT FROM C360_SYNC_CONTROL WHERE PROCESS_NAME = 'C360_SYNC';")
    row = cur.fetchone()
    if row and row[0] == 1:
        conn.close()
        return {
            "status": "SKIPPED",
            "reason": "PAUSED_BY_FULL_LOAD",
            "processed_keys": 0,
            "deleted_queue_rows": 0,
        }

    # 2. Lấy chính xác danh sách (QUEUE_ID, MA_CD) cần xử lý trong lô này
    if min_age_seconds > 0:
        cutoff_time_str = (now_dt - timedelta(seconds=min_age_seconds)).strftime("%Y-%m-%d %H:%M:%S")
        cur.execute(
            """
            SELECT QUEUE_ID, MA_CD
              FROM C360_SYNC_QUEUE
             WHERE STATUS = 'PENDING'
               AND ENQUEUE_TIME <= ?
             ORDER BY QUEUE_ID ASC
             LIMIT ?;
            """,
            (cutoff_time_str, max_batch_size),
        )
    else:
        cur.execute(
            """
            SELECT QUEUE_ID, MA_CD
              FROM C360_SYNC_QUEUE
             WHERE STATUS = 'PENDING'
             ORDER BY QUEUE_ID ASC
             LIMIT ?;
            """,
            (max_batch_size,),
        )

    pending_rows = cur.fetchall()
    if not pending_rows:
        conn.close()
        return {
            "status": "EMPTY",
            "message": "Không có sự kiện mới trong hàng đợi",
            "processed_keys": 0,
            "deleted_queue_rows": 0,
        }

    captured_queue_ids = [r[0] for r in pending_rows]
    v_cut = captured_queue_ids[-1]
    ma_cd_list = list(dict.fromkeys(r[1] for r in pending_rows))

    # Đánh dấu RUNNING trong C360_SYNC_CONTROL để Full Load (nếu khởi chạy) biết mà đợi
    cur.execute(
        "UPDATE C360_SYNC_CONTROL SET LAST_NRT_STATUS = 'RUNNING' WHERE PROCESS_NAME = 'C360_SYNC';"
    )
    conn.commit()

    lookups = load_standardization_lookups(conn)

    # 3. Chuẩn bị các câu lệnh MERGE và DELETE
    select_latest_sql = """
    SELECT *
      FROM MIR_VSS_HO_GD_NHANKHAU
     WHERE MA_CD = ?
       AND COALESCE(TYPEID, 'ZZ') NOT IN ('GT', 'GM', 'GG', 'GK')
       AND COALESCE(TRANG_THAI_DUYET, 0) NOT IN (1, 3, 4)
     ORDER BY COALESCE(NGAY_SUA, '') DESC,
              COALESCE(NGAY_TAO, '') DESC,
              ID DESC
     LIMIT 1;
    """

    upsert_c360_sql = """
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
        DQ_FLAGS, SYNC_DATE, SYNC_BATCH_ID
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
        :DQ_FLAGS, datetime('now', 'localtime'), :SYNC_BATCH_ID
    )
    ON CONFLICT(MA_SO_BHXH) DO UPDATE SET
        MA_SO_BHXH_CU = excluded.MA_SO_BHXH_CU,
        SO_DINH_DANH = excluded.SO_DINH_DANH,
        HO_TEN = excluded.HO_TEN,
        HO_TEN_KHONG_DAU = excluded.HO_TEN_KHONG_DAU,
        GIOI_TINH = excluded.GIOI_TINH,
        NGAY_SINH_CHUAN = excluded.NGAY_SINH_CHUAN,
        NAM_SINH = excluded.NAM_SINH,
        NGAY_SINH_GOC = excluded.NGAY_SINH_GOC,
        SO_CCCD = excluded.SO_CCCD,
        SO_CMTND = excluded.SO_CMTND,
        SO_DIEN_THOAI = excluded.SO_DIEN_THOAI,
        SO_DIEN_THOAI_GOC = excluded.SO_DIEN_THOAI_GOC,
        EMAIL = excluded.EMAIL,
        MA_TINH_THUONG_TRU = excluded.MA_TINH_THUONG_TRU,
        TEN_TINH_THUONG_TRU = excluded.TEN_TINH_THUONG_TRU,
        MA_XA_THUONG_TRU = excluded.MA_XA_THUONG_TRU,
        TEN_XA_THUONG_TRU = excluded.TEN_XA_THUONG_TRU,
        DIA_CHI_THUONG_TRU = excluded.DIA_CHI_THUONG_TRU,
        MA_TINH_GOC = excluded.MA_TINH_GOC,
        MA_XA_GOC = excluded.MA_XA_GOC,
        MA_TINH_LIEN_HE = excluded.MA_TINH_LIEN_HE,
        TEN_TINH_LIEN_HE = excluded.TEN_TINH_LIEN_HE,
        MA_XA_LIEN_HE = excluded.MA_XA_LIEN_HE,
        TEN_XA_LIEN_HE = excluded.TEN_XA_LIEN_HE,
        DIA_CHI_LIEN_HE = excluded.DIA_CHI_LIEN_HE,
        MA_TINH_LH_GOC = excluded.MA_TINH_LH_GOC,
        MA_XA_LH_GOC = excluded.MA_XA_LH_GOC,
        MA_TINH_KHAI_SINH = excluded.MA_TINH_KHAI_SINH,
        TEN_TINH_KHAI_SINH = excluded.TEN_TINH_KHAI_SINH,
        MA_XA_KHAI_SINH = excluded.MA_XA_KHAI_SINH,
        TEN_XA_KHAI_SINH = excluded.TEN_XA_KHAI_SINH,
        DIA_CHI_KHAI_SINH = excluded.DIA_CHI_KHAI_SINH,
        MA_TINH_KS_GOC = excluded.MA_TINH_KS_GOC,
        MA_XA_KS_GOC = excluded.MA_XA_KS_GOC,
        DM_DAN_TOC_ID = excluded.DM_DAN_TOC_ID,
        MA_DAN_TOC = excluded.MA_DAN_TOC,
        TEN_DAN_TOC = excluded.TEN_DAN_TOC,
        QUOC_TICH_RAW = excluded.QUOC_TICH_RAW,
        MA_QUOC_TICH = excluded.MA_QUOC_TICH,
        TEN_QUOC_TICH = excluded.TEN_QUOC_TICH,
        IS_ALIVE = excluded.IS_ALIVE,
        DEAD_DATE = excluded.DEAD_DATE,
        SOURCE_MA_HO_GD = excluded.SOURCE_MA_HO_GD,
        SOURCE_MODIFY_DATE = excluded.SOURCE_MODIFY_DATE,
        SOURCE_CREATED_DATE = excluded.SOURCE_CREATED_DATE,
        DQ_FLAGS = excluded.DQ_FLAGS,
        SYNC_DATE = datetime('now', 'localtime'),
        SYNC_BATCH_ID = excluded.SYNC_BATCH_ID;
    """

    delete_c360_sql = "DELETE FROM C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH = ?;"

    merge_count = 0
    delete_count = 0
    deleted_queue_rows = 0

    try:
        cur.execute("PRAGMA table_info(MIR_VSS_HO_GD_NHANKHAU);")
        col_names = [col[1] for col in cur.fetchall()]

        for ma_cd in ma_cd_list:
            cur.execute(select_latest_sql, (ma_cd,))
            latest_row = cur.fetchone()

            if latest_row:
                raw_dict = dict(zip(col_names, latest_row))
                record = transform_citizen_record(raw_dict, lookups, sync_batch_id=batch_id)
                cur.execute(upsert_c360_sql, record)
                merge_count += 1
            else:
                cur.execute(delete_c360_sql, (ma_cd,))
                if cur.rowcount > 0:
                    delete_count += cur.rowcount

        # 4. CHỈ xóa đúng các QUEUE_ID đã được bắt và xử lý trong lô này
        deleted_queue_rows = _delete_queue_ids(cur, captured_queue_ids)

        end_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 5. Ghi log hoàn thành vào C360_SYNC_LOG
        cur.execute(
            """
            INSERT INTO C360_SYNC_LOG (
                BATCH_TYPE, START_TIME, END_TIME, AFFECTED_KEYS, MERGE_COUNT, DELETE_COUNT, STATUS, ERROR_MSG
            ) VALUES ('NEAR_REALTIME', ?, ?, ?, ?, ?, 'SUCCESS', NULL);
            """,
            (start_time_str, end_time_str, len(ma_cd_list), merge_count, delete_count),
        )

        # Cập nhật C360_SYNC_CONTROL
        cur.execute(
            """
            UPDATE C360_SYNC_CONTROL
               SET LAST_NRT_RUN = ?,
                   LAST_NRT_STATUS = 'SUCCESS'
             WHERE PROCESS_NAME = 'C360_SYNC';
            """,
            (end_time_str,),
        )

        conn.commit()

    except Exception as e:
        conn.rollback()
        err_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            cur.execute(
                """
                INSERT INTO C360_SYNC_LOG (
                    BATCH_TYPE, START_TIME, END_TIME, AFFECTED_KEYS, MERGE_COUNT, DELETE_COUNT, STATUS, ERROR_MSG
                ) VALUES ('NEAR_REALTIME', ?, ?, ?, ?, ?, 'FAILED', ?);
                """,
                (start_time_str, err_end, len(ma_cd_list), merge_count, delete_count, str(e)),
            )
            cur.execute(
                """
                UPDATE C360_SYNC_CONTROL
                   SET LAST_NRT_RUN = ?,
                       LAST_NRT_STATUS = 'FAILED'
                 WHERE PROCESS_NAME = 'C360_SYNC';
                """,
                (err_end,),
            )
            conn.commit()
        except Exception:
            pass
        raise e
    finally:
        conn.close()

    elapsed = round(time.time() - t0, 2)
    return {
        "status": "SUCCESS",
        "v_cut": v_cut,
        "processed_keys": len(ma_cd_list),
        "deleted_queue_rows": deleted_queue_rows,
        "merge_count": merge_count,
        "delete_count": delete_count,
        "elapsed_sec": elapsed,
    }


def main():
    parser = argparse.ArgumentParser(description="Chạy 1 batch xử lý Near Real-time C360")
    parser.add_argument("--db-path", type=str, default=None, help="Đường dẫn file SQLite DB")
    parser.add_argument("--batch-size", type=int, default=5000, help="Số dòng Queue tối đa xử lý 1 lần")
    parser.add_argument(
        "--min-age-seconds",
        type=int,
        default=0,
        help="Độ trễ tối thiểu (giây) của sự kiện Queue để chống out-of-order commit (mặc định: 0)",
    )
    args = parser.parse_args()

    db_path = Path(args.db_path) if args.db_path else None
    print("=" * 70)
    print(" BẮT ĐẦU CHẠY WORKER NEAR REAL-TIME C360 (BAT-C360-005B)")
    print("=" * 70)

    result = run_c360_nrt_batch(
        sqlite_path=db_path,
        max_batch_size=args.batch_size,
        min_age_seconds=args.min_age_seconds,
    )
    print(f" [OK] Trạng thái xử lý : {result['status']}")
    print(f" [OK] Số khóa xử lý    : {result.get('processed_keys', 0)} khóa MA_CD")
    print(f" [OK] Số dòng MERGE    : {result.get('merge_count', 0)} dòng")
    print(f" [OK] Số dòng DELETE   : {result.get('delete_count', 0)} dòng")
    print(f" [OK] Số dòng Queue xóa: {result.get('deleted_queue_rows', 0)} dòng")
    print(f" [OK] Thời gian xử lý  : {result.get('elapsed_sec', 0)} giây")
    print("=" * 70)


if __name__ == "__main__":
    main()
