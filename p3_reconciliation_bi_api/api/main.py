#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FastAPI Application Entry Point: Dịch vụ REST APIs Tra cứu C360 & Live BI Dashboard (BAT-C360-007).
Chuẩn hóa mã lỗi đầu vào theo Detail Design (CHK-12 -> HTTP 400 MSG-E003).
"""

import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from p1_source_and_mirror.db_env import (
    get_connection,
    init_api_and_bi_schema,
    init_c360_schema,
    init_source_and_mirror_schema,
)
from p3_reconciliation_bi_api.api.audit import enqueue_audit_log, start_audit_worker, stop_audit_worker
from p3_reconciliation_bi_api.api.routers.citizen import router as citizen_router
from p3_reconciliation_bi_api.metabase_setup import (
    query_bi_dashboard_payload,
    render_superset_style_dashboard,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    conn = get_connection()
    try:
        init_source_and_mirror_schema(conn)
        init_c360_schema(conn)
        init_api_and_bi_schema(conn)
    finally:
        conn.close()

    start_audit_worker()
    yield
    await stop_audit_worker()


app = FastAPI(
    title="HGD C360 Citizen Lookup REST APIs & Live BI",
    description="Hệ thống 4 Dịch vụ REST APIs Tra cứu Công dân (Customer 360) & Live BI Dashboard",
    version="1.1.0",
    lifespan=lifespan,
)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Chuẩn hóa mọi lỗi tham số Pydantic (vd: ma_list rỗng hoặc > 1000 mã) về HTTP 400 MSG-E003."""
    await enqueue_audit_log(
        None,
        request.url.path,
        request.method,
        "validation_error",
        status.HTTP_400_BAD_REQUEST,
        0.1,
        "VALIDATION_ERROR",
        request.client.host if request.client else None,
    )
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={
            "detail": {
                "code": "MSG-E003",
                "message": "Tham số đầu vào không hợp lệ",
                "errors": [
                    {"loc": [str(x) for x in err.get("loc", [])], "msg": err.get("msg", "")}
                    for err in exc.errors()
                ],
            }
        },
    )


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(citizen_router)


@app.get(
    "/dashboard",
    response_class=HTMLResponse,
    tags=["Live BI Dashboard"],
    summary="Giao diện Dashboard trực quan 'Thông tin cá nhân – Gold' & Giám sát CDC Kafka",
)
async def live_bi_dashboard():
    snapshot = query_bi_dashboard_payload()
    return HTMLResponse(content=render_superset_style_dashboard(snapshot, live_api_mode=True))


@app.get(
    "/api/v1/bi/dashboard-data",
    tags=["Live BI Dashboard"],
    summary="API cấp dữ liệu thời gian thực cho 6 biểu đồ BI & bảng giám sát CDC/NRT",
)
async def get_live_bi_dashboard_data():
    return query_bi_dashboard_payload()


@app.get("/api/health", summary="Kiểm tra sức khỏe dịch vụ API")
async def health_check():
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM C360_THONG_TIN_CA_NHAN;")
        cnt = cur.fetchone()[0]
    finally:
        conn.close()
    return {
        "status": "HEALTHY",
        "service": "C360_REST_API",
        "total_c360_records": cnt,
    }


@app.get("/", summary="Thông tin hệ thống")
async def root():
    return {
        "name": "HGD C360 Pipeline REST API & Live BI",
        "dashboard_url": "/dashboard",
        "docs_url": "/docs",
        "redoc_url": "/redoc",
        "endpoints": [
            "GET  /dashboard",
            "GET  /api/v1/bi/dashboard-data",
            "GET  /api/v1/ca-nhan/{ma}",
            "POST /api/v1/ca-nhan/tra-cuu-lo",
            "GET  /api/v1/ca-nhan/tim-kiem",
            "GET  /api/v1/ca-nhan/thay-doi",
        ],
    }


if __name__ == "__main__":
    import uvicorn

    host = os.environ.get("API_HOST", "0.0.0.0")
    port = int(os.environ.get("API_PORT", "8000"))
    uvicorn.run("p3_reconciliation_bi_api.api.main:app", host=host, port=port, reload=True)
