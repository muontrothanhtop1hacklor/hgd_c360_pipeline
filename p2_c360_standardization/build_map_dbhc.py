#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BAT-C360-004: Tiền xử lý & Xây dựng 2 Bảng Map Địa bàn hành chính (TỈNH & XÃ).
Sau sáp nhập 2025, cấp Huyện được bãi bỏ:
  - 36 Tỉnh/Thành phố mới (và các mã tỉnh cũ sáp nhập) -> C360_MAP_DBHC_TINH
  - 3.322 Xã/Phường mới (và các mã xã sáp nhập / mơ hồ) -> C360_MAP_DBHC_XA

Nguyên tắc kỹ thuật bắt buộc:
  - Thực thi DELETE + INSERT trong cùng 1 Transaction (tuyệt đối KHÔNG dùng TRUNCATE)
    để các luồng đọc đồng thời (Near Real-time) không đọc phải bảng rỗng.
  - Phân tầng ưu tiên map: HIEN_HANH (Ưu tiên 1) > SAP_NHAP (Ưu tiên 2) > MO_HO (Ưu tiên 3).
  - Tự động sinh đầy đủ các biến thể mã tra cứu (chuỗi 2-3 số, đuôi TTT, mất số 0 ở đầu).
"""

import argparse
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from p1_source_and_mirror.db_env import (
    get_connection,
    get_db_engine,
    init_c360_schema,
    init_source_and_mirror_schema,
)


def normalize_tinh_code(code_str: str) -> str:
    """Chuẩn hóa mã tỉnh về 3 chữ số (ví dụ: '01' -> '001', '33TTT' -> '033')."""
    cleaned = code_str.strip().upper().replace("TTT", "")
    if cleaned.isdigit():
        return cleaned.zfill(3)
    return cleaned


def normalize_xa_code(code_str: str) -> str:
    """Chuẩn hóa mã xã về 5 chữ số (ví dụ: '4' -> '00004', '13' -> '00013')."""
    cleaned = code_str.strip()
    if cleaned.isdigit():
        return cleaned.zfill(5)
    return cleaned


def generate_tinh_lookup_variants(token: str) -> Set[str]:
    """Sinh các biến thể tra cứu cho mã tỉnh: '01', '1', '001', '01TTT'."""
    variants = set()
    cleaned = token.strip().upper().replace("TTT", "")
    if not cleaned:
        return variants

    variants.add(cleaned)
    variants.add(cleaned + "TTT")

    if cleaned.isdigit():
        num_val = int(cleaned)
        variants.add(str(num_val))
        variants.add(f"{num_val:02d}")
        variants.add(f"{num_val:03d}")
        variants.add(f"{num_val:02d}TTT")

    return variants


def generate_xa_lookup_variants(token: str) -> Set[str]:
    """Sinh các biến thể tra cứu cho mã xã: '00004', '4'."""
    variants = set()
    cleaned = token.strip()
    if not cleaned:
        return variants

    variants.add(cleaned)
    if cleaned.isdigit():
        num_val = int(cleaned)
        variants.add(str(num_val))
        variants.add(f"{num_val:05d}")

    return variants


def build_and_refresh_dbhc_maps(sqlite_path: Optional[Path] = None) -> Dict[str, Any]:
    """Xây dựng và làm mới dữ liệu 2 bảng Map C360_MAP_DBHC_TINH và C360_MAP_DBHC_XA."""
    conn = get_connection(sqlite_path=sqlite_path)
    init_source_and_mirror_schema(conn)
    init_c360_schema(conn)

    cur = conn.cursor()
    start_time = time.time()

    # Xác định bảng đọc danh mục: ưu tiên MIR_VSS_DM_DBHC nếu có dữ liệu, nếu không fallback VSS_DM_DBHC
    table_name = "MIR_VSS_DM_DBHC"
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    if cur.fetchone()[0] == 0:
        table_name = "VSS_DM_DBHC"

    # 1. Đọc danh sách cấp Tỉnh đang hiệu lực
    # TINH_TRANG = '1' và IS_CAP_XA = 0 (hoặc IS_CAP_XA IS NULL)
    cur.execute(
        f"""
        SELECT MA_DBHC, TEN, MA_DBHC_SN
          FROM {table_name}
         WHERE TINH_TRANG = '1'
           AND (IS_CAP_XA = 0 OR IS_CAP_XA IS NULL)
           AND (NGAY_HET_HIEU_LUC IS NULL OR NGAY_HET_HIEU_LUC >= '2026-01-01')
        """
    )
    tinh_rows = cur.fetchall()

    # Bảng tạm lưu map tỉnh: lookup_key -> (ma_tinh_moi, ten_tinh_moi, loai_map, prio)
    # prio: 1 (HIEN_HANH) < 2 (SAP_NHAP)
    tinh_map_dict: Dict[str, Tuple[str, str, str, int]] = {}
    province_id_to_name: Dict[str, str] = {}

    for row in tinh_rows:
        raw_code = str(row[0] or "").strip()
        tinh_name = str(row[1] or "").strip()
        sn_codes = str(row[2] or "").strip()

        ma_tinh_3so = normalize_tinh_code(raw_code)
        province_id_to_name[ma_tinh_3so] = tinh_name

        # Ưu tiên 1: Mã hiện hành
        for variant in generate_tinh_lookup_variants(raw_code):
            tinh_map_dict[variant] = (ma_tinh_3so, tinh_name, "HIEN_HANH", 1)

        # Ưu tiên 2: Các mã tỉnh cũ trong MA_DBHC_SN (ngăn cách bởi ';')
        if sn_codes:
            for sn_token in sn_codes.split(";"):
                sn_token = sn_token.strip()
                if not sn_token:
                    continue
                for variant in generate_tinh_lookup_variants(sn_token):
                    # Chỉ gán nếu chưa có hoặc có priority thấp hơn
                    if variant not in tinh_map_dict or tinh_map_dict[variant][3] > 2:
                        tinh_map_dict[variant] = (ma_tinh_3so, tinh_name, "SAP_NHAP", 2)

    # 2. Đọc danh sách cấp Xã đang hiệu lực
    cur.execute(
        f"""
        SELECT MA_DBHC, TEN, MA_CHA, MA_DBHC_SN
          FROM {table_name}
         WHERE TINH_TRANG = '1'
           AND IS_CAP_XA = 1
           AND (NGAY_HET_HIEU_LUC IS NULL OR NGAY_HET_HIEU_LUC >= '2026-01-01')
        """
    )
    xa_rows = cur.fetchall()

    # Xử lý cấp Xã:
    # 2.1. Ưu tiên 1: Các xã hiện hành
    xa_active_dict: Dict[str, Tuple[str, str, str, str]] = {}  # 5so -> (ma_xa_moi, ten_xa, ma_tinh_moi, ten_tinh)
    # Gom nhóm mã xã cũ trong MA_DBHC_SN: old_token_5so -> list of target_wards (ma_xa, ten_xa, ma_tinh, ten_tinh)
    sn_groups = defaultdict(list)

    for row in xa_rows:
        raw_ma_xa = str(row[0] or "").strip()
        ten_xa = str(row[1] or "").strip()
        ma_cha_tinh = str(row[2] or "").strip()
        sn_codes = str(row[3] or "").strip()

        ma_xa_5so = normalize_xa_code(raw_ma_xa)
        ma_tinh_3so = normalize_tinh_code(ma_cha_tinh)
        ten_tinh = province_id_to_name.get(ma_tinh_3so, f"Tỉnh {ma_tinh_3so}")

        target_info = (ma_xa_5so, ten_xa, ma_tinh_3so, ten_tinh)
        xa_active_dict[ma_xa_5so] = target_info

        if sn_codes:
            for token in sn_codes.split(";"):
                token = token.strip()
                if token:
                    token_5so = normalize_xa_code(token)
                    sn_groups[token_5so].append(target_info)

    # Bảng map xã tổng hợp: lookup_key -> (ma_xa_moi, ten_xa_moi, ma_tinh_moi, ten_tinh_moi, loai_map)
    xa_map_dict: Dict[str, Tuple[Optional[str], Optional[str], Optional[str], Optional[str], str]] = {}

    # Chèn Ưu tiên 1: Xã hiện hành
    for ma_5so, info in xa_active_dict.items():
        for variant in generate_xa_lookup_variants(ma_5so):
            xa_map_dict[variant] = (info[0], info[1], info[2], info[3], "HIEN_HANH")

    # Chèn Ưu tiên 2 & 3: Xã sáp nhập & Xã mơ hồ
    for old_token_5so, targets in sn_groups.items():
        # Nếu đã có trong xã hiện hành, giữ ưu tiên 1
        if old_token_5so in xa_active_dict:
            continue

        variants = generate_xa_lookup_variants(old_token_5so)

        # Loại bỏ các target trùng lặp (nếu có)
        unique_targets = list({t[0]: t for t in targets}.values())

        if len(unique_targets) == 1:
            # Ưu tiên 2: Sáp nhập 1 - 1
            tgt = unique_targets[0]
            for variant in variants:
                if variant not in xa_map_dict:
                    xa_map_dict[variant] = (tgt[0], tgt[1], tgt[2], tgt[3], "SAP_NHAP")
        else:
            # Ưu tiên 3: Xã mơ hồ (bị tách sang >= 2 xã mới)
            # Kiểm tra xem các xã mới có cùng thuộc 1 tỉnh không
            distinct_provinces = {t[2] for t in unique_targets}
            if len(distinct_provinces) == 1:
                # Cùng 1 tỉnh: giữ Tỉnh, Xã = NULL
                single_tinh_id = list(distinct_provinces)[0]
                single_tinh_name = unique_targets[0][3]
                for variant in variants:
                    if variant not in xa_map_dict:
                        xa_map_dict[variant] = (None, None, single_tinh_id, single_tinh_name, "MO_HO")
            else:
                # Khác tỉnh: cả Tỉnh và Xã đều = NULL
                for variant in variants:
                    if variant not in xa_map_dict:
                        xa_map_dict[variant] = (None, None, None, None, "MO_HO")

    # 3. Ghi dữ liệu vào DB trong 1 TRANSACTION DUY NHẤT (DELETE + INSERT, cấm TRUNCATE)
    try:
        cur.execute("DELETE FROM C360_MAP_DBHC_TINH;")
        cur.execute("DELETE FROM C360_MAP_DBHC_XA;")

        # Chèn Tỉnh
        tinh_insert_data = [
            (lookup, val[0], val[1], val[2])
            for lookup, val in tinh_map_dict.items()
        ]
        cur.executemany(
            """
            INSERT INTO C360_MAP_DBHC_TINH (MA_TINH_LOOKUP, MA_TINH_MOI, TEN_TINH_MOI, LOAI_MAP)
            VALUES (?, ?, ?, ?);
            """,
            tinh_insert_data,
        )

        # Chèn Xã
        xa_insert_data = [
            (lookup, val[0], val[1], val[2], val[3], val[4])
            for lookup, val in xa_map_dict.items()
        ]
        cur.executemany(
            """
            INSERT INTO C360_MAP_DBHC_XA (MA_XA_LOOKUP, MA_XA_MOI, TEN_XA_MOI, MA_TINH_MOI, TEN_TINH_MOI, LOAI_MAP)
            VALUES (?, ?, ?, ?, ?, ?);
            """,
            xa_insert_data,
        )

        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        raise RuntimeError(f"Lỗi khi làm mới 2 bảng Map DBHC: {e}") from e

    conn.close()
    elapsed = round(time.time() - start_time, 2)

    summary = {
        "status": "SUCCESS",
        "active_provinces": len(tinh_rows),
        "total_tinh_lookup_keys": len(tinh_insert_data),
        "active_wards": len(xa_rows),
        "total_xa_lookup_keys": len(xa_insert_data),
        "elapsed_sec": elapsed,
    }
    return summary


def main():
    parser = argparse.ArgumentParser(description="Xây dựng 2 Bảng Map DBHC C360 (Tỉnh & Xã)")
    parser.add_argument("--db-path", type=str, default=None, help="Đường dẫn file SQLite DB")
    args = parser.parse_args()

    db_path = Path(args.db_path) if args.db_path else None
    print("=" * 70)
    print(" BẮT ĐẦU XÂY DỰNG & LÀM MỚI 2 BẢNG MAP ĐỊA BÀN HÀNH CHÍNH (C360_MAP_DBHC_*)")
    print("=" * 70)

    summary = build_and_refresh_dbhc_maps(sqlite_path=db_path)
    print(f" [OK] Số đơn vị Tỉnh đang hiệu lực      : {summary['active_provinces']} tỉnh")
    print(f" [OK] Tổng khóa tra cứu Tỉnh (biến thể) : {summary['total_tinh_lookup_keys']:,} khóa")
    print(f" [OK] Số đơn vị Xã đang hiệu lực        : {summary['active_wards']:,} xã/phường")
    print(f" [OK] Tổng khóa tra cứu Xã (biến thể)   : {summary['total_xa_lookup_keys']:,} khóa")
    print(f" [OK] Thời gian thực hiện (Transaction) : {summary['elapsed_sec']} giây")
    print("=" * 70)


if __name__ == "__main__":
    main()
