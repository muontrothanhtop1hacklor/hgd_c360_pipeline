#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script kiểm tra nhanh toàn bộ trạng thái Tuần 1 (Thiết kế, Danh mục, Nguồn VSS, Mirror & CDC Handoff).
"""

import json
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from p1_source_and_mirror.db_env import get_connection


def main():
    print("=" * 78)
    print(" BÁO CÁO KIỂM TRA TRẠNG THÁI TUẦN 1 (HGD -> MIRROR -> C360 PIPELINE)")
    print("=" * 78)

    # 1. Kiểm tra bộ tài liệu Thiết kế
    design_files = [
        PROJECT_ROOT / "design" / "ThietKeCoBan_BasicDesign_HGD_C360.md",
        PROJECT_ROOT / "design" / "ThietKeCoBan_BasicDesign_HGD_C360.xlsx",
        PROJECT_ROOT / "design" / "ThietKeChiTiet_DetailDesign_HGD_C360.md",
        PROJECT_ROOT / "design" / "ThietKeChiTiet_DetailDesign_HGD_C360.xlsx",
    ]
    print("\n[1] TÀI LIỆU THIẾT KẾ (design/):")
    for f in design_files:
        status = f"OK ({f.stat().st_size:,} bytes)" if f.exists() else "MISSING"
        print(f"  - {f.name:<45} : {status}")

    # 2. Kiểm tra Database Nguồn & Mirror
    conn = get_connection()
    cur = conn.cursor()

    tables = [
        "VSS_DM_DANTOC",
        "VSS_DM_QUOC_TICH",
        "VSS_DM_DBHC",
        "VSS_HO_GD_NHANKHAU",
        "MIR_VSS_DM_DANTOC",
        "MIR_VSS_DM_QUOC_TICH",
        "MIR_VSS_DM_DBHC",
        "MIR_VSS_HO_GD_NHANKHAU",
    ]
    print("\n[2] SỐ LƯỢNG BẢN GHI CÁC BẢNG NGUỒN (VSS_*) & MIRROR (MIR_VSS_*):")
    for tbl in tables:
        cur.execute(f"SELECT COUNT(*) FROM {tbl}")
        cnt = cur.fetchone()[0]
        print(f"  - {tbl:<28} : {cnt:>10,} dòng")

    # 3. Kiểm tra Unique Index & SCN Handoff
    cur.execute("PRAGMA index_list('MIR_VSS_HO_GD_NHANKHAU')")
    indexes = {row[1]: row[2] for row in cur.fetchall()}
    uk_status = "ACTIVE (UNIQUE)" if indexes.get("UK_MIR_NK") == 1 else "MISSING"

    cur.execute(
        "SELECT SNAPSHOT_SCN, SNAPSHOT_TIME, SOURCE_ROW_COUNT, MIRROR_ROW_COUNT, STATUS FROM MIR_CDC_SCN_HANDOFF"
    )
    scn_row = cur.fetchone()
    print("\n[3] CHỈ MỤC MIRROR & MỐC SCN BÀN GIAO DEBEZIUM CDC:")
    print(f"  - Unique Index UK_MIR_NK (MA_CD, MA_HO_GD) : {uk_status}")
    if scn_row:
        print(
            f"  - Mốc SCN bàn giao (SNAPSHOT_SCN)          : {scn_row[0]} "
            f"(Time: {scn_row[1]}, Nguồn: {scn_row[2]:,}, Mirror: {scn_row[3]:,}, Status: {scn_row[4]})"
        )

    conn.close()

    # 4. Kiểm tra Ground Truth JSON
    fl_path = PROJECT_ROOT / "data" / "full_load_mirror_report.json"
    if fl_path.exists():
        fl_data = json.loads(fl_path.read_text(encoding="utf-8"))
        parts = fl_data.get("nhankhau_full_load", {}).get("partition_stats", {})
        print("\n[4] PHÂN BỔ ĐỌC SONG SONG THEO PHÂN VÙNG MA_CD (10 PARTITIONS):")
        for k, v in parts.items():
            print(f"  - {k:<12} : {v:>8,} dòng")

    print("=" * 78)


if __name__ == "__main__":
    main()

