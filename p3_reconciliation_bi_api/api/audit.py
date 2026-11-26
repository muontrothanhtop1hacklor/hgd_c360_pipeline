#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Async Batch Audit Logger:
Ghi nhật ký tra cứu API bất đồng bộ theo lô (mỗi AUDIT_BATCH_MAX_SIZE bản ghi hoặc mỗi
AUDIT_BATCH_FLUSH_INTERVAL_SEC giây) vào bảng C360_API_AUDIT_LOG, đảm bảo không chặn
luồng phản hồi HTTP (< 20ms) và xả sạch hàng đợi khi đóng ứng dụng.
"""

import asyncio
import os
import sys
import threading
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Deque, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from p1_source_and_mirror.db_env import get_connection

_buffer_lock = threading.Lock()
_pending_buffer: Deque[tuple] = deque()
_wakeup_event: Optional[asyncio.Event] = None
_worker_loop: Optional[asyncio.AbstractEventLoop] = None
_worker_task: Optional[asyncio.Task] = None


def _get_batch_max_size() -> int:
    try:
        return max(1, int(os.environ.get("AUDIT_BATCH_MAX_SIZE", "200")))
    except ValueError:
        return 200


def _get_flush_interval_sec() -> float:
    try:
        return max(0.1, float(os.environ.get("AUDIT_BATCH_FLUSH_INTERVAL_SEC", "3.0")))
    except ValueError:
        return 3.0


def _write_batch_to_db(batch: List[tuple]) -> int:
    if not batch:
        return 0
    conn = get_connection()
    try:
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
        return len(batch)
    finally:
        conn.close()


def flush_audit_logs_sync() -> int:
    """Xả đồng bộ toàn bộ các bản ghi đang chờ trong hàng đợi bộ nhớ xuống DB."""
    batch: List[tuple] = []
    with _buffer_lock:
        while _pending_buffer:
            batch.append(_pending_buffer.popleft())
    if not batch:
        return 0
    try:
        return _write_batch_to_db(batch)
    except Exception as e:
        sys.stderr.write(f"[WARN] Lỗi xả Audit Log xuống DB: {e}\n")
        return 0


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
    """Đẩy bản ghi audit vào hàng đợi bộ nhớ (O(1), không chờ I/O DB)."""
    log_entry = (
        client_id,
        endpoint,
        method,
        (query_param[:950] if query_param and len(query_param) > 950 else query_param),
        response_status,
        round(exec_time_ms, 2),
        matched_by,
        ip_address,
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    max_size = _get_batch_max_size()
    with _buffer_lock:
        _pending_buffer.append(log_entry)
        should_wake = len(_pending_buffer) >= max_size

    if should_wake and _wakeup_event is not None and _worker_loop is not None:
        try:
            _worker_loop.call_soon_threadsafe(_wakeup_event.set)
        except RuntimeError:
            pass


async def audit_worker_loop(wakeup_event: asyncio.Event) -> None:
    """Worker chạy nền gom lô theo ngưỡng kích thước hoặc chu kỳ thời gian."""
    while True:
        try:
            flush_interval = _get_flush_interval_sec()
            max_size = _get_batch_max_size()
            try:
                await asyncio.wait_for(wakeup_event.wait(), timeout=flush_interval)
            except asyncio.TimeoutError:
                pass
            wakeup_event.clear()

            batch: List[tuple] = []
            with _buffer_lock:
                while _pending_buffer and len(batch) < max_size:
                    batch.append(_pending_buffer.popleft())

            if batch:
                await asyncio.to_thread(_write_batch_to_db, batch)

        except asyncio.CancelledError:
            flush_audit_logs_sync()
            break
        except Exception as e:
            sys.stderr.write(f"[WARN] Lỗi ghi Async Audit Log: {e}\n")
            await asyncio.sleep(0.5)


def start_audit_worker() -> None:
    global _worker_task, _wakeup_event, _worker_loop
    if _worker_task is None or _worker_task.done():
        _worker_loop = asyncio.get_running_loop()
        _wakeup_event = asyncio.Event()
        _worker_task = asyncio.create_task(audit_worker_loop(_wakeup_event))


async def stop_audit_worker() -> None:
    global _worker_task, _wakeup_event, _worker_loop
    if _worker_task and not _worker_task.done():
        _worker_task.cancel()
        try:
            await _worker_task
        except asyncio.CancelledError:
            pass
    flush_audit_logs_sync()
    _worker_task = None
    _wakeup_event = None
    _worker_loop = None
