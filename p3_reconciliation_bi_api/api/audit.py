#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Async Batch Audit Logger:
Ghi nhật ký tra cứu API bất đồng bộ vào bảng C360_API_AUDIT_LOG,
đảm bảo thời gian phản hồi của API đạt tốc độ tối đa (< 20ms).
"""

import asyncio
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from p1_source_and_mirror.db_env import get_connection

audit_queue: asyncio.Queue = asyncio.Queue()
_worker_task: Optional[asyncio.Task] = None


async def enqueue_audit_log(
    client_id: Optional[str],
    endpoint: str,
    method: str,
    query_param: Optional[str],
    response_status: int,
    exec_time_ms: float,
    matched_by: Optional[str] = None,
    ip_address: Optional[str] = None,
) -> None:
    """Đẩy bản ghi audit vào hàng đợi bộ nhớ."""
    log_entry = (
        client_id,
        endpoint,
        method,
        query_param,
        response_status,
        round(exec_time_ms, 2),
        matched_by,
        ip_address,
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    await audit_queue.put(log_entry)


async def audit_worker_loop() -> None:
    """Worker chạy nền gom lô và lưu vào C360_API_AUDIT_LOG."""
    while True:
        batch: List[tuple] = []
        try:
            # Chờ bản ghi đầu tiên
            first_item = await audit_queue.get()
            batch.append(first_item)
            audit_queue.task_done()

            # Gom thêm các bản ghi đang chờ trong queue (tối đa 100 bản ghi)
            while not audit_queue.empty() and len(batch) < 100:
                item = audit_queue.get_nowait()
                batch.append(item)
                audit_queue.task_done()

            if batch:
                conn = get_connection()
                cur = conn.cursor()
                cur.executemany(
                    """
                    INSERT INTO C360_API_AUDIT_LOG (
                        CLIENT_ID, ENDPOINT, METHOD, QUERY_PARAM,
                        RESPONSE_STATUS, EXEC_TIME_MS, MATCHED_BY, IP_ADDRESS, CREATED_TIME
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    batch,
                )
                conn.commit()
                conn.close()

        except asyncio.CancelledError:
            # Xả nốt queue trước khi tắt
            while not audit_queue.empty():
                item = audit_queue.get_nowait()
                batch.append(item)
                audit_queue.task_done()
            if batch:
                try:
                    conn = get_connection()
                    cur = conn.cursor()
                    cur.executemany(
                        """
                        INSERT INTO C360_API_AUDIT_LOG (
                            CLIENT_ID, ENDPOINT, METHOD, QUERY_PARAM,
                            RESPONSE_STATUS, EXEC_TIME_MS, MATCHED_BY, IP_ADDRESS, CREATED_TIME
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """,
                        batch,
                    )
                    conn.commit()
                    conn.close()
                except Exception:
                    pass
            break
        except Exception as e:
            # Không làm gián đoạn ứng dụng chính khi có lỗi audit
            sys.stderr.write(f"[WARN] Lỗi ghi Audit Log: {e}\n")
            await asyncio.sleep(1)


def start_audit_worker() -> None:
    global _worker_task
    if _worker_task is None or _worker_task.done():
        _worker_task = asyncio.create_task(audit_worker_loop())


async def stop_audit_worker() -> None:
    global _worker_task
    if _worker_task and not _worker_task.done():
        _worker_task.cancel()
        try:
            await _worker_task
        except asyncio.CancelledError:
            pass
