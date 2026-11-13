#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Router chứa 4 Dịch vụ REST APIs Tra cứu C360 (FastAPI).
Lưu ý định tuyến: Các static path (/tra-cuu-lo, /tim-kiem, /thay-doi) được khai báo TRƯỚC
path parameter (/{ma}) để tránh xung đột routing.
"""

import base64
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from p1_source_and_mirror.db_env import get_connection
from p2_c360_standardization.db_c360_cleaner import remove_vietnamese_accents
from p3_reconciliation_bi_api.api.audit import enqueue_audit_log
from p3_reconciliation_bi_api.api.auth import AuthenticatedClient, require_scope
from p3_reconciliation_bi_api.api.schemas import (
    BatchQueryRequest,
    BatchQueryResponse,
    BatchQueryResultItem,
    ChangeFeedResponse,
    CitizenResponse,
    ConflictCandidateResponse,
    SearchCitizensResponse,
    SingleCitizenLookupResponse,
)

router = APIRouter(prefix="/api/v1/ca-nhan", tags=["Tra cứu C360"])


def row_to_citizen(row: tuple, col_names: List[str]) -> CitizenResponse:
    d = dict(zip(col_names, row))
    flags_str = d.get("DQ_FLAGS")
    flags_list = [f.strip() for f in flags_str.split(";") if f.strip()] if flags_str else []

    return CitizenResponse(
        id=d.get("ID"),
        ma_so_bhxh=str(d.get("MA_SO_BHXH") or ""),
        ma_so_bhxh_cu=d.get("MA_SO_BHXH_CU"),
        so_dinh_danh=d.get("SO_DINH_DANH"),
        ho_ten=str(d.get("HO_TEN") or ""),
        ho_ten_khong_dau=str(d.get("HO_TEN_KHONG_DAU") or ""),
        gioi_tinh=d.get("GIOI_TINH"),
        ngay_sinh_chuan=d.get("NGAY_SINH_CHUAN"),
        ngay_sinh_goc=d.get("NGAY_SINH_GOC"),
        so_cccd=d.get("SO_CCCD"),
        so_cmtnd=d.get("SO_CMTND"),
        so_dien_thoai=d.get("SO_DIEN_THOAI"),
        email=d.get("EMAIL"),
        ma_tinh_thuong_tru=d.get("MA_TINH_THUONG_TRU"),
        ten_tinh_thuong_tru=d.get("TEN_TINH_THUONG_TRU"),
        ma_xa_thuong_tru=d.get("MA_XA_THUONG_TRU"),
        ten_xa_thuong_tru=d.get("TEN_XA_THUONG_TRU"),
        dia_chi_thuong_tru=d.get("DIA_CHI_THUONG_TRU"),
        ma_tinh_lien_he=d.get("MA_TINH_LIEN_HE"),
        ma_xa_lien_he=d.get("MA_XA_LIEN_HE"),
        dia_chi_lien_he=d.get("DIA_CHI_LIEN_HE"),
        ma_tinh_khai_sinh=d.get("MA_TINH_KHAI_SINH"),
        ma_xa_khai_sinh=d.get("MA_XA_KHAI_SINH"),
        dia_chi_khai_sinh=d.get("DIA_CHI_KHAI_SINH"),
        ma_dan_toc=d.get("MA_DAN_TOC"),
        ten_dan_toc=d.get("TEN_DAN_TOC"),
        ma_quoc_tich=d.get("MA_QUOC_TICH"),
        ten_quoc_tich=d.get("TEN_QUOC_TICH"),
        is_alive=d.get("IS_ALIVE", 1),
        dead_date=d.get("DEAD_DATE"),
        source_ma_ho_gd=d.get("SOURCE_MA_HO_GD"),
        source_modify_date=d.get("SOURCE_MODIFY_DATE"),
        dq_flags=flags_str,
        dq_flags_list=flags_list,
        sync_date=d.get("SYNC_DATE"),
    )


# -----------------------------------------------------------------------------
# API 2: POST /api/v1/ca-nhan/tra-cuu-lo
# -----------------------------------------------------------------------------
@router.post(
    "/tra-cuu-lo",
    response_model=BatchQueryResponse,
    summary="Tra cứu công dân theo lô (Tối đa 1.000 mã/lần)",
)
async def batch_lookup_citizens(
    payload: BatchQueryRequest,
    request: Request,
    client: AuthenticatedClient = Depends(require_scope("read:batch")),
):
    t0 = time.time()
    ma_list = [m.strip() for m in payload.ma_list if m.strip()]
    if not ma_list:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"code": "MSG-E003", "message": "Danh sách ma_list không được rỗng"},
        )

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(C360_THONG_TIN_CA_NHAN);")
    col_names = [col[1] for col in cur.fetchall()]

    placeholders = ",".join(["?"] * len(ma_list))

    cur.execute(f"SELECT * FROM C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH IN ({placeholders});", ma_list)
    by_ma_bhxh = {r[col_names.index("MA_SO_BHXH")]: r for r in cur.fetchall()}

    cur.execute(f"SELECT * FROM C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH_CU IN ({placeholders});", ma_list)
    by_ma_cu: Dict[str, List[tuple]] = {}
    for r in cur.fetchall():
        key = r[col_names.index("MA_SO_BHXH_CU")]
        by_ma_cu.setdefault(key, []).append(r)

    cur.execute(f"SELECT * FROM C360_THONG_TIN_CA_NHAN WHERE SO_DINH_DANH IN ({placeholders});", ma_list)
    by_cccd: Dict[str, List[tuple]] = {}
    for r in cur.fetchall():
        key = r[col_names.index("SO_DINH_DANH")]
        by_cccd.setdefault(key, []).append(r)

    conn.close()

    results: List[BatchQueryResultItem] = []
    found_cnt = 0
    not_found_cnt = 0
    conflict_cnt = 0

    for query_key in ma_list:
        # Tầng 1
        if query_key in by_ma_bhxh:
            results.append(
                BatchQueryResultItem(
                    query_key=query_key,
                    status="FOUND",
                    matched_by="MA_SO_BHXH",
                    data=row_to_citizen(by_ma_bhxh[query_key], col_names),
                )
            )
            found_cnt += 1
            continue

        # Tầng 2
        cu_matches = by_ma_cu.get(query_key, [])
        if len(cu_matches) == 1:
            results.append(
                BatchQueryResultItem(
                    query_key=query_key,
                    status="FOUND",
                    matched_by="MA_SO_BHXH_CU",
                    data=row_to_citizen(cu_matches[0], col_names),
                )
            )
            found_cnt += 1
            continue
        elif len(cu_matches) > 1:
            results.append(
                BatchQueryResultItem(
                    query_key=query_key,
                    status="CONFLICT",
                    matched_by="MA_SO_BHXH_CU",
                    candidates=[row_to_citizen(r, col_names) for r in cu_matches],
                )
            )
            conflict_cnt += 1
            continue

        # Tầng 3
        cccd_matches = by_cccd.get(query_key, [])
        if len(cccd_matches) == 1:
            results.append(
                BatchQueryResultItem(
                    query_key=query_key,
                    status="FOUND",
                    matched_by="SO_DINH_DANH",
                    data=row_to_citizen(cccd_matches[0], col_names),
                )
            )
            found_cnt += 1
            continue
        elif len(cccd_matches) > 1:
            results.append(
                BatchQueryResultItem(
                    query_key=query_key,
                    status="CONFLICT",
                    matched_by="SO_DINH_DANH",
                    candidates=[row_to_citizen(r, col_names) for r in cccd_matches],
                )
            )
            conflict_cnt += 1
            continue

        # Không tìm thấy
        results.append(BatchQueryResultItem(query_key=query_key, status="NOT_FOUND"))
        not_found_cnt += 1

    exec_ms = (time.time() - t0) * 1000
    await enqueue_audit_log(
        client.client_id, request.url.path, request.method, f"batch_size={len(ma_list)}", 200, exec_ms, "BATCH", request.client.host if request.client else None
    )

    return BatchQueryResponse(
        total_requested=len(ma_list),
        found_count=found_cnt,
        not_found_count=not_found_cnt,
        conflict_count=conflict_cnt,
        results=results,
    )


# -----------------------------------------------------------------------------
# API 3: GET /api/v1/ca-nhan/tim-kiem
# -----------------------------------------------------------------------------
@router.get(
    "/tim-kiem",
    response_model=SearchCitizensResponse,
    summary="Tìm kiếm theo Họ tên không dấu + Ngày sinh linh hoạt",
)
async def search_citizens(
    ho_ten: str = Query(..., min_length=2, description="Họ và tên (có dấu hoặc không dấu)"),
    ngay_sinh: Optional[str] = Query(None, description="Ngày sinh YYYY, YYYYMM, hoặc YYYYMMDD"),
    ma_tinh: Optional[str] = Query(None, description="Mã tỉnh 3 số lọc kết quả"),
    limit: int = Query(50, ge=1, le=200, description="Số lượng kết quả tối đa"),
    request: Request = None,
    client: AuthenticatedClient = Depends(require_scope("read:search")),
):
    t0 = time.time()
    ho_ten_kd = remove_vietnamese_accents(ho_ten)
    dob_digits = "".join(c for c in (ngay_sinh or "") if c.isdigit()) or None

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(C360_THONG_TIN_CA_NHAN);")
    col_names = [col[1] for col in cur.fetchall()]

    query = """
    SELECT *
      FROM C360_THONG_TIN_CA_NHAN
     WHERE HO_TEN_KHONG_DAU = ?
       AND (? IS NULL OR (
           NGAY_SINH_CHUAN IS NOT NULL AND (
               NGAY_SINH_CHUAN LIKE ? || '%'
               OR ? LIKE NGAY_SINH_CHUAN || '%'
           )
       ))
       AND (? IS NULL OR MA_TINH_THUONG_TRU = ?)
     ORDER BY MA_SO_BHXH ASC
     LIMIT ?;
    """
    cur.execute(query, (ho_ten_kd, dob_digits, dob_digits, dob_digits, ma_tinh, ma_tinh, limit))
    rows = cur.fetchall()
    conn.close()

    citizens = [row_to_citizen(r, col_names) for r in rows]
    exec_ms = (time.time() - t0) * 1000
    if request:
        await enqueue_audit_log(
            client.client_id, request.url.path, request.method, f"name={ho_ten_kd}&dob={dob_digits}", 200, exec_ms, "SEARCH", request.client.host if request.client else None
        )

    return SearchCitizensResponse(
        query_name=ho_ten,
        query_dob=ngay_sinh,
        total_matched=len(citizens),
        data=citizens,
    )


# -----------------------------------------------------------------------------
# API 4: GET /api/v1/ca-nhan/thay-doi
# -----------------------------------------------------------------------------
@router.get(
    "/thay-doi",
    response_model=ChangeFeedResponse,
    summary="Lấy danh sách bản ghi thay đổi theo Cursor (Độ trễ an toàn 120s)",
)
async def get_change_feed(
    cursor: Optional[str] = Query(None, description="Con trỏ phân trang Base64(sync_date|id)"),
    limit: int = Query(100, ge=1, le=1000, description="Số lượng bản ghi tối đa"),
    request: Request = None,
    client: AuthenticatedClient = Depends(require_scope("read:changes")),
):
    t0 = time.time()
    # Tính safe watermark lag: CURRENT_TIME - 120s
    safe_watermark_dt = datetime.now() - timedelta(seconds=120)
    safe_watermark_str = safe_watermark_dt.strftime("%Y-%m-%d %H:%M:%S")

    last_sync_date = None
    last_id = None

    if cursor:
        try:
            decoded = base64.b64decode(cursor.encode("utf-8")).decode("utf-8")
            parts = decoded.split("|")
            last_sync_date = parts[0]
            last_id = int(parts[1])
        except Exception:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"code": "MSG-E003", "message": "Định dạng cursor không hợp lệ"},
            )

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(C360_THONG_TIN_CA_NHAN);")
    col_names = [col[1] for col in cur.fetchall()]

    query = """
    SELECT *
      FROM C360_THONG_TIN_CA_NHAN
     WHERE SYNC_DATE <= ?
       AND (
           ? IS NULL
           OR SYNC_DATE > ?
           OR (SYNC_DATE = ? AND ID > ?)
       )
     ORDER BY SYNC_DATE ASC, ID ASC
     LIMIT ?;
    """
    cur.execute(query, (safe_watermark_str, last_sync_date, last_sync_date, last_sync_date, last_id, limit + 1))
    rows = cur.fetchall()
    conn.close()

    has_more = len(rows) > limit
    actual_rows = rows[:limit]
    citizens = [row_to_citizen(r, col_names) for r in actual_rows]

    next_cursor = None
    if actual_rows:
        last_item = actual_rows[-1]
        last_date_val = str(last_item[col_names.index("SYNC_DATE")])
        last_id_val = last_item[col_names.index("ID")]
        token = f"{last_date_val}|{last_id_val}"
        next_cursor = base64.b64encode(token.encode("utf-8")).decode("utf-8")

    exec_ms = (time.time() - t0) * 1000
    if request:
        await enqueue_audit_log(
            client.client_id, request.url.path, request.method, f"cursor={cursor}&limit={limit}", 200, exec_ms, "FEED", request.client.host if request.client else None
        )

    return ChangeFeedResponse(
        safe_watermark=safe_watermark_str,
        next_cursor=next_cursor,
        has_more=has_more,
        data=citizens,
    )


# -----------------------------------------------------------------------------
# API 1: GET /api/v1/ca-nhan/{ma} (Đặt sau cùng để tránh nuốt các static paths)
# -----------------------------------------------------------------------------
@router.get(
    "/{ma}",
    response_model=SingleCitizenLookupResponse,
    summary="Tra cứu cá nhân theo mã (Dò cascade 3 tầng)",
)
async def get_citizen_by_code(
    ma: str,
    request: Request,
    client: AuthenticatedClient = Depends(require_scope("read:single")),
):
    t0 = time.time()
    clean_ma = ma.strip()

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(C360_THONG_TIN_CA_NHAN);")
    col_names = [col[1] for col in cur.fetchall()]

    # Tầng 1: Tra cứu theo MA_SO_BHXH hiện hành
    cur.execute("SELECT * FROM C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH = ?;", (clean_ma,))
    rows = cur.fetchall()
    if rows:
        conn.close()
        exec_ms = (time.time() - t0) * 1000
        await enqueue_audit_log(
            client.client_id, request.url.path, request.method, clean_ma, 200, exec_ms, "MA_SO_BHXH", request.client.host if request.client else None
        )
        return SingleCitizenLookupResponse(
            matched_by="MA_SO_BHXH",
            data=row_to_citizen(rows[0], col_names),
        )

    # Tầng 2: Tra cứu theo MA_SO_BHXH_CU
    cur.execute("SELECT * FROM C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH_CU = ?;", (clean_ma,))
    rows = cur.fetchall()
    if len(rows) == 1:
        conn.close()
        exec_ms = (time.time() - t0) * 1000
        await enqueue_audit_log(
            client.client_id, request.url.path, request.method, clean_ma, 200, exec_ms, "MA_SO_BHXH_CU", request.client.host if request.client else None
        )
        return SingleCitizenLookupResponse(
            matched_by="MA_SO_BHXH_CU",
            data=row_to_citizen(rows[0], col_names),
        )
    elif len(rows) > 1:
        conn.close()
        exec_ms = (time.time() - t0) * 1000
        await enqueue_audit_log(
            client.client_id, request.url.path, request.method, clean_ma, 409, exec_ms, "CONFLICT_MA_CU", request.client.host if request.client else None
        )
        candidates = [row_to_citizen(r, col_names) for r in rows]
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": "MSG-E004",
                "message": f"Mã cũ '{clean_ma}' khớp với {len(rows)} công dân khác nhau trên hệ thống",
                "matched_tier": "MA_SO_BHXH_CU",
                "candidate_count": len(rows),
                "candidates": [c.model_dump() for c in candidates],
            },
        )

    # Tầng 3: Tra cứu theo SO_DINH_DANH (12 số)
    cur.execute("SELECT * FROM C360_THONG_TIN_CA_NHAN WHERE SO_DINH_DANH = ?;", (clean_ma,))
    rows = cur.fetchall()
    if len(rows) == 1:
        conn.close()
        exec_ms = (time.time() - t0) * 1000
        await enqueue_audit_log(
            client.client_id, request.url.path, request.method, clean_ma, 200, exec_ms, "SO_DINH_DANH", request.client.host if request.client else None
        )
        return SingleCitizenLookupResponse(
            matched_by="SO_DINH_DANH",
            data=row_to_citizen(rows[0], col_names),
        )
    elif len(rows) > 1:
        conn.close()
        exec_ms = (time.time() - t0) * 1000
        await enqueue_audit_log(
            client.client_id, request.url.path, request.method, clean_ma, 409, exec_ms, "CONFLICT_CCCD", request.client.host if request.client else None
        )
        candidates = [row_to_citizen(r, col_names) for r in rows]
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": "MSG-E004",
                "message": f"Số định danh '{clean_ma}' khớp với {len(rows)} công dân khác nhau trên hệ thống",
                "matched_tier": "SO_DINH_DANH",
                "candidate_count": len(rows),
                "candidates": [c.model_dump() for c in candidates],
            },
        )

    conn.close()
    exec_ms = (time.time() - t0) * 1000
    await enqueue_audit_log(
        client.client_id, request.url.path, request.method, clean_ma, 404, exec_ms, "NONE", request.client.host if request.client else None
    )
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail={"code": "MSG-I001", "message": f"Không tìm thấy công dân nào khớp với mã '{clean_ma}'"},
    )
