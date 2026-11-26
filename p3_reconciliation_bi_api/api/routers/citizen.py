#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Router chứa 4 Dịch vụ REST APIs Tra cứu C360 (FastAPI):
  - API 1: GET  /api/v1/ca-nhan/{ma}          (Cascade 3 tầng: MA_SO_BHXH -> MA_SO_BHXH_CU -> SO_DINH_DANH; 409 Conflict)
  - API 2: POST /api/v1/ca-nhan/tra-cuu-lo    (Tối đa 1.000 mã/lần, chia chunk IN <= 500 an toàn cho Oracle & SQLite)
  - API 3: GET  /api/v1/ca-nhan/tim-kiem      (Họ tên không dấu + Ngày sinh linh hoạt 2 chiều YYYY / YYYYMM / YYYYMMDD)
  - API 4: GET  /api/v1/ca-nhan/thay-doi      (Cursor keyset Base64(sync_date|id) + Độ trễ an toàn NRT_SAFE_LAG_SECONDS=120s)

Lưu ý định tuyến: Các static path (/tra-cuu-lo, /tim-kiem, /thay-doi) được khai báo TRƯỚC
path parameter (/{ma}) để không bị /{ma} nuốt route.
"""

import base64
import os
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from p1_source_and_mirror.db_env import get_connection
from p2_c360_standardization.build_map_dbhc import normalize_tinh_code
from p2_c360_standardization.db_c360_cleaner import clean_hoten, is_valid_calendar_date
from p3_reconciliation_bi_api.api.audit import enqueue_audit_log
from p3_reconciliation_bi_api.api.auth import AuthenticatedClient, require_scope
from p3_reconciliation_bi_api.api.schemas import (
    BatchQueryRequest,
    BatchQueryResponse,
    BatchQueryResultItem,
    ChangeFeedResponse,
    CitizenResponse,
    SearchCitizensResponse,
    SingleCitizenLookupResponse,
)

router = APIRouter(prefix="/api/v1/ca-nhan", tags=["Tra cứu C360"])

CURSOR_TS_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")


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
        nam_sinh=d.get("NAM_SINH"),
        ngay_sinh_goc=d.get("NGAY_SINH_GOC"),
        so_cccd=d.get("SO_CCCD"),
        so_cmtnd=d.get("SO_CMTND"),
        so_dien_thoai=d.get("SO_DIEN_THOAI"),
        so_dien_thoai_goc=d.get("SO_DIEN_THOAI_GOC"),
        email=d.get("EMAIL"),
        ma_tinh_thuong_tru=d.get("MA_TINH_THUONG_TRU"),
        ten_tinh_thuong_tru=d.get("TEN_TINH_THUONG_TRU"),
        ma_xa_thuong_tru=d.get("MA_XA_THUONG_TRU"),
        ten_xa_thuong_tru=d.get("TEN_XA_THUONG_TRU"),
        dia_chi_thuong_tru=d.get("DIA_CHI_THUONG_TRU"),
        ma_tinh_goc=d.get("MA_TINH_GOC"),
        ma_xa_goc=d.get("MA_XA_GOC"),
        ma_tinh_lien_he=d.get("MA_TINH_LIEN_HE"),
        ten_tinh_lien_he=d.get("TEN_TINH_LIEN_HE"),
        ma_xa_lien_he=d.get("MA_XA_LIEN_HE"),
        ten_xa_lien_he=d.get("TEN_XA_LIEN_HE"),
        dia_chi_lien_he=d.get("DIA_CHI_LIEN_HE"),
        ma_tinh_khai_sinh=d.get("MA_TINH_KHAI_SINH"),
        ten_tinh_khai_sinh=d.get("TEN_TINH_KHAI_SINH"),
        ma_xa_khai_sinh=d.get("MA_XA_KHAI_SINH"),
        ten_xa_khai_sinh=d.get("TEN_XA_KHAI_SINH"),
        dia_chi_khai_sinh=d.get("DIA_CHI_KHAI_SINH"),
        ma_dan_toc=d.get("MA_DAN_TOC"),
        ten_dan_toc=d.get("TEN_DAN_TOC"),
        ma_quoc_tich=d.get("MA_QUOC_TICH"),
        ten_quoc_tich=d.get("TEN_QUOC_TICH"),
        is_alive=d.get("IS_ALIVE", 1) if d.get("IS_ALIVE") is not None else 1,
        dead_date=d.get("DEAD_DATE"),
        source_ma_ho_gd=d.get("SOURCE_MA_HO_GD"),
        source_modify_date=d.get("SOURCE_MODIFY_DATE"),
        dq_flags=flags_str,
        dq_flags_list=flags_list,
        sync_date=d.get("SYNC_DATE"),
    )


def _validate_search_dob(ngay_sinh: Optional[str]) -> Optional[str]:
    """
    Kiểm tra chặt chẽ tham số ngày sinh tìm kiếm (CHK-12):
    Chấp nhận YYYY (4 số), YYYYMM (6 số), hoặc YYYYMMDD (8 số) hợp lệ theo lịch.
    Hỗ trợ cả người dùng nhập YYYY-MM-DD hoặc YYYY-MM.
    """
    if ngay_sinh is None:
        return None
    raw = ngay_sinh.strip()
    if not raw:
        return None

    normalized = raw.replace("-", "")
    if not re.match(r"^(?:\d{4}|\d{6}|\d{8})$", normalized):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "code": "MSG-E003",
                "message": "Tham số ngay_sinh không hợp lệ (chỉ chấp nhận YYYY, YYYYMM hoặc YYYYMMDD)",
            },
        )

    year = int(normalized[:4])
    if year < 1850 or year > 2026:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"code": "MSG-E003", "message": f"Năm sinh '{year}' nằm ngoài khoảng hợp lệ [1850..2026]"},
        )

    if len(normalized) >= 6:
        month = int(normalized[4:6])
        if month < 1 or month > 12:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"code": "MSG-E003", "message": f"Tháng sinh '{month:02d}' không hợp lệ"},
            )

    if len(normalized) == 8:
        month = int(normalized[4:6])
        day = int(normalized[6:8])
        if not is_valid_calendar_date(year, month, day):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"code": "MSG-E003", "message": f"Ngày sinh '{raw}' không tồn tại theo lịch"},
            )

    return normalized


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
    ma_list = [m.strip() for m in payload.ma_list if m and m.strip()]
    if not ma_list or len(ma_list) > 1000:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "code": "MSG-E003",
                "message": "Danh sách ma_list phải chứa từ 1 đến tối đa 1.000 mã hợp lệ",
            },
        )

    unique_keys = list(dict.fromkeys(ma_list))

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(C360_THONG_TIN_CA_NHAN);")
        col_names = [col[1] for col in cur.fetchall()]
        idx_bhxh = col_names.index("MA_SO_BHXH")
        idx_ma_cu = col_names.index("MA_SO_BHXH_CU")
        idx_cccd = col_names.index("SO_DINH_DANH")

        by_ma_bhxh: Dict[str, tuple] = {}
        by_ma_cu: Dict[str, List[tuple]] = {}
        by_cccd: Dict[str, List[tuple]] = {}

        # Chia chunk <= 500 phần tử để an toàn tuyệt đối cho cả giới hạn IN (1000) của Oracle và biến bind SQLite
        chunk_size = 500
        for i in range(0, len(unique_keys), chunk_size):
            chunk = unique_keys[i : i + chunk_size]
            placeholders = ",".join(["?"] * len(chunk))

            cur.execute(f"SELECT * FROM C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH IN ({placeholders});", chunk)
            for r in cur.fetchall():
                by_ma_bhxh[r[idx_bhxh]] = r

            cur.execute(f"SELECT * FROM C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH_CU IN ({placeholders});", chunk)
            for r in cur.fetchall():
                by_ma_cu.setdefault(r[idx_ma_cu], []).append(r)

            cur.execute(f"SELECT * FROM C360_THONG_TIN_CA_NHAN WHERE SO_DINH_DANH IN ({placeholders});", chunk)
            for r in cur.fetchall():
                by_cccd.setdefault(r[idx_cccd], []).append(r)
    finally:
        conn.close()

    results: List[BatchQueryResultItem] = []
    found_cnt = 0
    not_found_cnt = 0
    conflict_cnt = 0

    for query_key in ma_list:
        # Tầng 1: MA_SO_BHXH hiện hành
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

        # Tầng 2: MA_SO_BHXH_CU
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

        # Tầng 3: SO_DINH_DANH 12 số
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

        results.append(BatchQueryResultItem(query_key=query_key, status="NOT_FOUND"))
        not_found_cnt += 1

    exec_ms = (time.time() - t0) * 1000
    await enqueue_audit_log(
        client.client_id,
        request.url.path,
        request.method,
        f"batch_size={len(ma_list)}&unique={len(unique_keys)}",
        200,
        exec_ms,
        "BATCH",
        request.client.host if request.client else None,
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
    summary="Tìm kiếm theo Họ tên không dấu + Ngày sinh linh hoạt 2 chiều",
)
async def search_citizens(
    ho_ten: str = Query(..., min_length=2, max_length=250, description="Họ và tên (có dấu hoặc không dấu)"),
    ngay_sinh: Optional[str] = Query(None, description="Ngày sinh YYYY, YYYYMM, hoặc YYYYMMDD"),
    ma_tinh: Optional[str] = Query(None, description="Mã tỉnh thường trú lọc kết quả (vd: 01 hoặc 001)"),
    limit: int = Query(50, ge=1, le=200, description="Số lượng kết quả tối đa"),
    request: Request = None,
    client: AuthenticatedClient = Depends(require_scope("read:search")),
):
    t0 = time.time()
    _, ho_ten_kd, _ = clean_hoten(ho_ten)
    if len(ho_ten_kd) < 2:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"code": "MSG-E003", "message": "Tham số ho_ten phải có ít nhất 2 ký tự hợp lệ"},
        )

    dob_digits = _validate_search_dob(ngay_sinh)
    norm_tinh = normalize_tinh_code(ma_tinh) if (ma_tinh and ma_tinh.strip()) else None

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(C360_THONG_TIN_CA_NHAN);")
        col_names = [col[1] for col in cur.fetchall()]

        # Tận dụng Composite Index IDX_C360_TIM_KIEM (HO_TEN_KHONG_DAU, NGAY_SINH_CHUAN)
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
        cur.execute(query, (ho_ten_kd, dob_digits, dob_digits, dob_digits, norm_tinh, norm_tinh, limit))
        rows = cur.fetchall()
    finally:
        conn.close()

    citizens = [row_to_citizen(r, col_names) for r in rows]
    exec_ms = (time.time() - t0) * 1000
    if request:
        await enqueue_audit_log(
            client.client_id,
            request.url.path,
            request.method,
            f"name={ho_ten_kd}&dob={dob_digits}&tinh={norm_tinh}",
            200,
            exec_ms,
            "SEARCH",
            request.client.host if request.client else None,
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
    try:
        lag_seconds = max(0, int(os.environ.get("NRT_SAFE_LAG_SECONDS", "120")))
    except ValueError:
        lag_seconds = 120

    safe_watermark_dt = datetime.now() - timedelta(seconds=lag_seconds)
    safe_watermark_str = safe_watermark_dt.strftime("%Y-%m-%d %H:%M:%S")

    last_sync_date: Optional[str] = None
    last_id: Optional[int] = None

    if cursor is not None and cursor.strip() != "":
        try:
            decoded = base64.b64decode(cursor.strip().encode("utf-8"), validate=True).decode("utf-8")
            parts = decoded.split("|")
            if len(parts) != 2 or not CURSOR_TS_PATTERN.match(parts[0]):
                raise ValueError("Invalid cursor structure")
            last_sync_date = parts[0]
            last_id = int(parts[1])
            if last_id < 0:
                raise ValueError("Negative cursor ID")
        except Exception:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"code": "MSG-E003", "message": "Định dạng cursor phân trang không hợp lệ"},
            )

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(C360_THONG_TIN_CA_NHAN);")
        col_names = [col[1] for col in cur.fetchall()]

        # Tận dụng Composite Index IDX_C360_THAY_DOI (SYNC_DATE, ID)
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
        cur.execute(
            query,
            (safe_watermark_str, last_sync_date, last_sync_date, last_sync_date, last_id, limit + 1),
        )
        rows = cur.fetchall()
    finally:
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
            client.client_id,
            request.url.path,
            request.method,
            f"cursor={cursor}&limit={limit}",
            200,
            exec_ms,
            "FEED",
            request.client.host if request.client else None,
        )

    return ChangeFeedResponse(
        safe_watermark=safe_watermark_str,
        next_cursor=next_cursor,
        has_more=has_more,
        data=citizens,
    )


# -----------------------------------------------------------------------------
# API 1: GET /api/v1/ca-nhan/{ma} (Đặt sau cùng để không che khuất static paths)
# -----------------------------------------------------------------------------
@router.get(
    "/{ma}",
    response_model=SingleCitizenLookupResponse,
    summary="Tra cứu cá nhân theo mã (Dò cascade 3 tầng: MA_SO_BHXH -> MA_SO_BHXH_CU -> SO_DINH_DANH)",
)
async def get_citizen_by_code(
    ma: str,
    request: Request,
    client: AuthenticatedClient = Depends(require_scope("read:single")),
):
    t0 = time.time()
    clean_ma = ma.strip()
    if not clean_ma:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"code": "MSG-E003", "message": "Mã tra cứu cá nhân không được để trống"},
        )

    client_ip = request.client.host if request.client else None
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(C360_THONG_TIN_CA_NHAN);")
        col_names = [col[1] for col in cur.fetchall()]

        # Tầng 1: Tra cứu theo MA_SO_BHXH hiện hành (Unique Index Scan)
        cur.execute("SELECT * FROM C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH = ?;", (clean_ma,))
        rows = cur.fetchall()
        if rows:
            exec_ms = (time.time() - t0) * 1000
            await enqueue_audit_log(
                client.client_id, request.url.path, request.method, clean_ma, 200, exec_ms, "MA_SO_BHXH", client_ip
            )
            return SingleCitizenLookupResponse(
                matched_by="MA_SO_BHXH",
                data=row_to_citizen(rows[0], col_names),
            )

        # Tầng 2: Tra cứu theo MA_SO_BHXH_CU (IDX_C360_MA_CU)
        cur.execute("SELECT * FROM C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH_CU = ?;", (clean_ma,))
        rows = cur.fetchall()
        if len(rows) == 1:
            exec_ms = (time.time() - t0) * 1000
            await enqueue_audit_log(
                client.client_id, request.url.path, request.method, clean_ma, 200, exec_ms, "MA_SO_BHXH_CU", client_ip
            )
            return SingleCitizenLookupResponse(
                matched_by="MA_SO_BHXH_CU",
                data=row_to_citizen(rows[0], col_names),
            )
        elif len(rows) > 1:
            exec_ms = (time.time() - t0) * 1000
            await enqueue_audit_log(
                client.client_id, request.url.path, request.method, clean_ma, 409, exec_ms, "CONFLICT_MA_CU", client_ip
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

        # Tầng 3: Tra cứu theo SO_DINH_DANH 12 số (IDX_C360_SO_DINH_DANH)
        cur.execute("SELECT * FROM C360_THONG_TIN_CA_NHAN WHERE SO_DINH_DANH = ?;", (clean_ma,))
        rows = cur.fetchall()
        if len(rows) == 1:
            exec_ms = (time.time() - t0) * 1000
            await enqueue_audit_log(
                client.client_id, request.url.path, request.method, clean_ma, 200, exec_ms, "SO_DINH_DANH", client_ip
            )
            return SingleCitizenLookupResponse(
                matched_by="SO_DINH_DANH",
                data=row_to_citizen(rows[0], col_names),
            )
        elif len(rows) > 1:
            exec_ms = (time.time() - t0) * 1000
            await enqueue_audit_log(
                client.client_id, request.url.path, request.method, clean_ma, 409, exec_ms, "CONFLICT_CCCD", client_ip
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
    finally:
        conn.close()

    exec_ms = (time.time() - t0) * 1000
    await enqueue_audit_log(
        client.client_id, request.url.path, request.method, clean_ma, 404, exec_ms, "NONE", client_ip
    )
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail={"code": "MSG-I001", "message": f"Không tìm thấy công dân nào khớp với mã '{clean_ma}'"},
    )
