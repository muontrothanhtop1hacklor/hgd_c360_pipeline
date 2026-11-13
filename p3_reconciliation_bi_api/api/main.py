#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FastAPI Application Entry Point: Dịch vụ REST APIs Tra cứu C360 (BAT-C360-007).
"""

import sys
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from p1_source_and_mirror.db_env import get_connection, init_c360_schema, init_source_and_mirror_schema
from p3_reconciliation_bi_api.api.audit import start_audit_worker, stop_audit_worker
from p3_reconciliation_bi_api.api.routers.citizen import router as citizen_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Khởi tạo DB schema nếu chưa có
    conn = get_connection()
    init_source_and_mirror_schema(conn)
    init_c360_schema(conn)
    conn.close()

    # Khởi chạy luồng Async Audit Worker
    start_audit_worker()
    yield
    # Dừng luồng Async Audit Worker
    await stop_audit_worker()


app = FastAPI(
    title="HGD C360 Citizen Lookup REST APIs",
    description="Hệ thống 4 Dịch vụ REST APIs Tra cứu Dữ liệu Chuẩn hóa Công dân (Customer 360) theo 3 tầng ưu tiên",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(citizen_router)


@app.get("/api/health", summary="Kiểm tra sức khỏe dịch vụ API")
async def health_check():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM C360_THONG_TIN_CA_NHAN;")
    cnt = cur.fetchone()[0]
    conn.close()
    return {
        "status": "HEALTHY",
        "service": "C360_REST_API",
        "total_c360_records": cnt,
    }


@app.get("/", summary="Thông tin hệ thống")
async def root():
    return {
        "name": "HGD C360 Pipeline REST API",
        "docs_url": "/docs",
        "redoc_url": "/redoc",
        "endpoints": [
            "GET  /api/v1/ca-nhan/{ma}",
            "POST /api/v1/ca-nhan/tra-cuu-lo",
            "GET  /api/v1/ca-nhan/tim-kiem",
            "GET  /api/v1/ca-nhan/thay-doi",
        ],
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("p3_reconciliation_bi_api.api.main:app", host="0.0.0.0", port=8000, reload=True)
