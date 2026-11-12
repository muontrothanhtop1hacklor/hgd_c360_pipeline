#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script đăng ký và quản lý cấu hình Debezium Oracle Source Connector & JDBC Sink Connector
kèm cơ chế bàn giao mốc SCN (SNAPSHOT_SCN) từ bước Full Load sang CDC.
"""

import argparse
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, Optional

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from p1_source_and_mirror.db_env import get_connection


def get_handoff_scn(sqlite_path: Optional[Path] = None) -> Optional[int]:
    """Đọc mốc SNAPSHOT_SCN đã được ghi nhận trước khi chạy Full Load."""
    conn = get_connection(sqlite_path=sqlite_path)
    cur = conn.cursor()
    cur.execute(
        "SELECT SNAPSHOT_SCN FROM MIR_CDC_SCN_HANDOFF WHERE TABLE_NAME = 'VSS_HO_GD_NHANKHAU' AND STATUS = 'PASSED'"
    )
    row = cur.fetchone()
    conn.close()
    return int(row[0]) if row else None


def build_connectors_payload(scn: Optional[int] = None) -> Dict[str, Dict[str, Any]]:
    base_dir = Path(__file__).resolve().parent
    src_cfg = json.loads((base_dir / "oracle-source-connector.json").read_text(encoding="utf-8"))
    sink_cfg = json.loads((base_dir / "jdbc-sink-connector.json").read_text(encoding="utf-8"))

    if scn is not None:
        # Gắn chính xác mốc SCN từ bước Full Load để CDC đọc nối tiếp không trùng/sót
        src_cfg["config"]["log.mining.scn.start"] = str(scn)

    return {"source": src_cfg, "sink": sink_cfg}


def post_connector(connect_url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        f"{connect_url.rstrip('/')}/connectors",
        data=data,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main():
    parser = argparse.ArgumentParser(description="Đăng ký Debezium Source & JDBC Sink Connector với SCN Handoff")
    parser.add_argument("--connect-url", type=str, default="http://localhost:8083", help="Kafka Connect REST URL")
    parser.add_argument("--db-path", type=str, default=None, help="Đường dẫn SQLite DB để đọc mốc SCN")
    parser.add_argument("--dry-run", action="store_true", help="Chỉ kiểm tra và in cấu hình JSON sau khi gắn SCN")
    args = parser.parse_args()

    scn = get_handoff_scn(Path(args.db_path) if args.db_path else None)
    payloads = build_connectors_payload(scn=scn)

    if args.dry_run:
        print(f"[DEBEZIUM_CONFIG] Dry-run thành công với SNAPSHOT_SCN = {scn}:")
        print(json.dumps(payloads, ensure_ascii=False, indent=2))
        return

    try:
        res_src = post_connector(args.connect_url, payloads["source"])
        res_sink = post_connector(args.connect_url, payloads["sink"])
        print(f"[DEBEZIUM_CONFIG] Đã đăng ký thành công Source & Sink Connectors: {res_src['name']}, {res_sink['name']}")
    except urllib.error.URLError as e:
        print(
            f"[DEBEZIUM_CONFIG] Kafka Connect chưa chạy tại {args.connect_url} ({e}). "
            f"Đã sẵn sàng cấu hình với SNAPSHOT_SCN={scn}."
        )


if __name__ == "__main__":
    main()

