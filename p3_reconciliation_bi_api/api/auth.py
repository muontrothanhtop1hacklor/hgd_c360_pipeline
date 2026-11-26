#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Middleware & Dependency Xác thực X-API-Key (SHA-256) và Phân quyền Scopes.
DB tuyệt đối không lưu plain-text API key, chỉ đối chiếu qua mã băm SHA-256.
Mọi truy cập bị từ chối (401 / 403) đều được ghi nhận bất đồng bộ vào C360_API_AUDIT_LOG.
"""

import hashlib
import sys
import time
from pathlib import Path
from typing import Optional, Set

from fastapi import Depends, HTTPException, Request, Security, status
from fastapi.security import APIKeyHeader

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from p1_source_and_mirror.db_env import get_connection
from p3_reconciliation_bi_api.api.audit import enqueue_audit_log

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)

SCOPE_ALIASES = {
    "read:changes": {"read:changes", "read:cdc"},
    "read:cdc": {"read:changes", "read:cdc"},
}


class AuthenticatedClient:
    def __init__(self, client_id: str, client_name: str, scopes: Set[str]):
        self.client_id = client_id
        self.client_name = client_name
        self.scopes = scopes

    def has_scope(self, required_scope: str) -> bool:
        if "admin" in self.scopes or required_scope in self.scopes:
            return True
        accepted = SCOPE_ALIASES.get(required_scope)
        if accepted and (self.scopes & accepted):
            return True
        return False


async def get_current_client(
    request: Request,
    x_api_key: Optional[str] = Security(API_KEY_HEADER),
) -> AuthenticatedClient:
    """Xác thực client dựa trên mã băm SHA-256 của header X-API-Key."""
    t0 = time.time()
    client_ip = request.client.host if request.client else None

    if not x_api_key or not x_api_key.strip():
        await enqueue_audit_log(
            None,
            request.url.path,
            request.method,
            "missing_api_key",
            status.HTTP_401_UNAUTHORIZED,
            (time.time() - t0) * 1000,
            "AUTH_FAILED",
            client_ip,
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "MSG-E001", "message": "Thiếu header X-API-Key xác thực"},
        )

    clean_key = x_api_key.strip()
    key_hash = hashlib.sha256(clean_key.encode("utf-8")).hexdigest()

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT CLIENT_ID, CLIENT_NAME, SCOPES, IS_ACTIVE
              FROM C360_API_CLIENTS
             WHERE API_KEY_HASH = ?;
            """,
            (key_hash,),
        )
        row = cur.fetchone()
    finally:
        conn.close()

    if not row or row[3] != 1:
        await enqueue_audit_log(
            row[0] if row else None,
            request.url.path,
            request.method,
            f"key_prefix={clean_key[:4]}***",
            status.HTTP_401_UNAUTHORIZED,
            (time.time() - t0) * 1000,
            "AUTH_FAILED",
            client_ip,
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "MSG-E001", "message": "X-API-Key không hợp lệ hoặc đã bị khóa"},
        )

    scopes_set = {s.strip() for s in str(row[2] or "").split(",") if s.strip()}
    return AuthenticatedClient(client_id=row[0], client_name=row[1], scopes=scopes_set)


def require_scope(required_scope: str):
    """Factory kiểm tra quyền scope cụ thể cho từng endpoint."""

    async def scope_checker(
        request: Request,
        client: AuthenticatedClient = Depends(get_current_client),
    ) -> AuthenticatedClient:
        if not client.has_scope(required_scope):
            client_ip = request.client.host if request.client else None
            await enqueue_audit_log(
                client.client_id,
                request.url.path,
                request.method,
                f"required_scope={required_scope}",
                status.HTTP_403_FORBIDDEN,
                0.1,
                "FORBIDDEN_SCOPE",
                client_ip,
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={
                    "code": "MSG-E002",
                    "message": f"Client '{client.client_id}' không có quyền truy cập scope '{required_scope}'",
                },
            )
        return client

    return scope_checker
