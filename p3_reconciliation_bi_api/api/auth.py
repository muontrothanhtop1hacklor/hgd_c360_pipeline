#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Middleware & Dependency Xác thực X-API-Key (SHA-256) và Phân quyền Scopes.
"""

import hashlib
import sys
from pathlib import Path
from typing import Optional, Set

from fastapi import Depends, Header, HTTPException, Security, status
from fastapi.security import APIKeyHeader

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from p1_source_and_mirror.db_env import get_connection

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)


class AuthenticatedClient:
    def __init__(self, client_id: str, client_name: str, scopes: Set[str]):
        self.client_id = client_id
        self.client_name = client_name
        self.scopes = scopes

    def has_scope(self, required_scope: str) -> bool:
        return required_scope in self.scopes or "admin" in self.scopes


def get_current_client(
    x_api_key: Optional[str] = Security(API_KEY_HEADER),
) -> AuthenticatedClient:
    """Xác thực client dựa trên mã băm SHA-256 của header X-API-Key."""
    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "MSG-E001", "message": "Thiếu header X-API-Key xác thực"},
        )

    key_hash = hashlib.sha256(x_api_key.strip().encode("utf-8")).hexdigest()

    conn = get_connection()
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
    conn.close()

    if not row or row[3] != 1:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "MSG-E001", "message": "X-API-Key không hợp lệ hoặc đã bị khóa"},
        )

    scopes_set = {s.strip() for s in row[2].split(",") if s.strip()}
    return AuthenticatedClient(client_id=row[0], client_name=row[1], scopes=scopes_set)


def require_scope(required_scope: str):
    """Factory kiểm tra quyền scope cụ thể cho endpoint."""
    def scope_checker(client: AuthenticatedClient = Depends(get_current_client)) -> AuthenticatedClient:
        if not client.has_scope(required_scope):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail={
                    "code": "MSG-E002",
                    "message": f"Client không có quyền truy cập scope yêu cầu: '{required_scope}'",
                },
            )
        return client

    return scope_checker
