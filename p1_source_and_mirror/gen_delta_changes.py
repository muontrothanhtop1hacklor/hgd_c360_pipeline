#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BAT-C360-002 (Script 2 - Change / Delta Generator):
Sinh dữ liệu biến động trực tiếp trên bảng nguồn VSS.VSS_HO_GD_NHANKHAU để kiểm thử
luồng CDC (Debezium -> Kafka -> Mirror) và luồng Near Real-time Sync (Queue -> C360 -> BI & API).

Hỗ trợ cả 2 chế độ:
  - Chế độ kiểm thử chuẩn (mặc định `--insert-count 5`): Chạy nhanh < 1 giây cho Unit/Integration Test.
  - Chế độ bơm lô lớn để Demo Before/After trên BI (`--insert-count 50000` hoặc `--insert-count 100000`):
    Sinh streaming theo từng batch 5.000 dòng (không tràn RAM), phân bổ thực tế theo 36 tỉnh,
    dân tộc Kinh/thiểu số, lỗi chất lượng dữ liệu DQ_FLAGS và tỷ lệ lọc nghiệp vụ (~4%),
    ghi nhận lưu lượng vào MIR_CDC_TOPIC_METRICS để trực quan hóa luồng Kafka CDC.

Bao phủ đầy đủ 4 nhóm thao tác bắt buộc theo mục 3 - Phần 1.1:
1. INSERT:
   - 1a: Thêm lô công dân hoàn toàn mới (MA_CD mới, hỗ trợ từ 5 đến 100.000+ dòng).
   - 1b: Thêm dòng hộ gia đình mới (MA_HO_GD mới, NGAY_SUA mới hơn) cho MA_CD đã tồn tại.
2. UPDATE thông thường:
   - 2a: Sửa dữ liệu bẩn sang sạch -> gỡ cờ DQ_FLAGS.
   - 2b: Đổi TRANG_THAI_DUYET sang (1, 3, 4) hoặc TYPEID sang ('GG', 'GT') -> loại khỏi C360.
   - 2c: Đổi TINH_TRANG_SONG từ 1 -> 0 (kèm NGAY_CHET) -> IS_ALIVE = 0.
3. UPDATE đổi mã định danh MA_CD (Cấp lại mã số BHXH):
   - Đổi MA_CD từ MA_CD_A sang MA_CD_B, lưu MA_CD_A vào MA_CD_OLD -> xóa sạch mã cũ trên C360.
4. DELETE:
   - 4a: Xóa dòng hộ mới nhất của công dân có 2 hộ -> C360 tự động lùi về hộ cũ còn lại.
   - 4b: Xóa toàn bộ dòng của công dân chỉ có 1 hộ -> C360 xóa hẳn công dân đó.
"""

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from p1_source_and_mirror.db_env import (
    NHANKHAU_COLUMNS,
    get_connection,
    get_db_engine,
    init_c360_schema,
    init_source_and_mirror_schema,
)
from p1_source_and_mirror.gen_seed_nhankhau import (
    DEM_NAM,
    DEM_NU,
    HO_LIST,
    TEN_NAM,
    TEN_NU,
    fetch_reference_pools,
    make_empty_row,
    remove_accents,
)

CDC_TOPIC_NAME = "vss.VSS.VSS_HO_GD_NHANKHAU"
APPROX_CDC_MSG_BYTES = 2870  # ~2.8 KB JSON Debezium envelope (before + after + source metadata)


def _get_mirror_upsert_sql(col_names: List[str]) -> str:
    col_list_str = ", ".join(col_names)
    placeholders = ", ".join(["?"] * len(col_names))
    update_set_str = ", ".join([f"{c} = excluded.{c}" for c in col_names if c not in ("MA_CD", "MA_HO_GD")])
    return f"""
        INSERT INTO MIR_VSS_HO_GD_NHANKHAU ({col_list_str}, MIRROR_SYNC_TS)
        VALUES ({placeholders}, datetime('now', 'localtime'))
        ON CONFLICT(MA_CD, MA_HO_GD) DO UPDATE SET
            {update_set_str},
            MIRROR_SYNC_TS = datetime('now', 'localtime')
    """


def _norm_mirror_tuple(row_dict: Dict[str, Any], col_names: List[str]) -> Tuple[Any, ...]:
    ho_gd = row_dict.get("MA_HO_GD")
    norm_ho = str(ho_gd).strip() if (ho_gd is not None and str(ho_gd).strip()) else "HO_MAC_DINH"
    return tuple((norm_ho if c == "MA_HO_GD" else row_dict.get(c)) for c in col_names)


def sync_cdc_events_to_mirror(conn: Any, cdc_events: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Mô phỏng chính xác cơ chế Debezium Source + Kafka JDBC Sink (insert.mode=upsert,
    pk.mode=record_key, pk.fields=MA_CD,MA_HO_GD, delete.enabled=true).
    """
    cur = conn.cursor()
    col_names = [c for c, _ in NHANKHAU_COLUMNS]
    upsert_sql = _get_mirror_upsert_sql(col_names)

    upserted = 0
    deleted = 0

    for ev in cdc_events:
        op = ev["op"]
        if op == "DELETE":
            ho_gd = ev["before"].get("MA_HO_GD")
            norm_ho_gd = str(ho_gd).strip() if (ho_gd is not None and str(ho_gd).strip()) else "HO_MAC_DINH"
            cur.execute(
                "DELETE FROM MIR_VSS_HO_GD_NHANKHAU WHERE MA_CD = ? AND MA_HO_GD = ?",
                (ev["before"]["MA_CD"], norm_ho_gd),
            )
            deleted += cur.rowcount if cur.rowcount > 0 else 0
        elif op == "UPDATE_PK":
            ho_gd = ev["before"].get("MA_HO_GD")
            norm_ho_gd = str(ho_gd).strip() if (ho_gd is not None and str(ho_gd).strip()) else "HO_MAC_DINH"
            cur.execute(
                "DELETE FROM MIR_VSS_HO_GD_NHANKHAU WHERE MA_CD = ? AND MA_HO_GD = ?",
                (ev["before"]["MA_CD"], norm_ho_gd),
            )
            deleted += cur.rowcount if cur.rowcount > 0 else 0
            cur.execute(upsert_sql, _norm_mirror_tuple(ev["after"], col_names))
            upserted += 1
        elif op in ("INSERT", "UPDATE"):
            cur.execute(upsert_sql, _norm_mirror_tuple(ev["after"], col_names))
            upserted += 1

    conn.commit()
    return {"mirror_upserted": upserted, "mirror_deleted": deleted}


def fetch_row_as_dict(cur: Any, row_id: int) -> Optional[Dict[str, Any]]:
    col_names = [c for c, _ in NHANKHAU_COLUMNS]
    cur.execute(f"SELECT {', '.join(col_names)} FROM VSS_HO_GD_NHANKHAU WHERE ID = ?", (row_id,))
    r = cur.fetchone()
    if not r:
        return None
    return dict(zip(col_names, r))


def _build_bulk_citizen_row(
    seq_idx: int,
    row_id: int,
    pools: Dict[str, Any],
    now_ts: str,
) -> Dict[str, Any]:
    """Sinh 1 dòng công dân mới theo phân phối thực tế (dân tộc, tỉnh trọng điểm, lỗi DQ & lọc nghiệp vụ)."""
    active_wards = pools["active_wards"]
    merged_wards = pools["merged_wards_1to1"]
    valid_dantoc = pools["valid_dantoc"]

    new_ma_cd = f"99{seq_idx:08d}"
    gioi_tinh = 1 if (seq_idx % 2 == 1) else 2
    ho = HO_LIST[seq_idx % len(HO_LIST)]
    dem = DEM_NAM[seq_idx % len(DEM_NAM)] if gioi_tinh == 1 else DEM_NU[seq_idx % len(DEM_NU)]
    ten = TEN_NAM[seq_idx % len(TEN_NAM)] if gioi_tinh == 1 else TEN_NU[seq_idx % len(TEN_NU)]

    # 5 dòng đầu tiên (9900000001..9900000005) luôn giữ hợp lệ sạch để tương thích 100% với Unit Test
    is_protected_seed = seq_idx <= 5

    ho_ten = f"  {ho}   {dem}   {ten}  " if (not is_protected_seed and seq_idx % 20 == 0) else f"{ho} {dem} {ten}"
    ho_ten_kd = remove_accents(f"{ho} {dem} {ten}")

    is_typeid_g = (not is_protected_seed) and (seq_idx % 50 == 1)
    is_unapproved = (not is_protected_seed) and (seq_idx % 50 == 2)

    ward_new, tinh_new, ward_name = active_wards[seq_idx % len(active_wards)]
    ward_lh, tinh_lh, ward_lh_name = active_wards[(seq_idx + 11) % len(active_wards)]

    ma_tinh = tinh_new
    ma_xa = ward_new
    ma_tinh_lh = tinh_lh
    ma_xa_lh = ward_lh

    if not is_protected_seed and not is_typeid_g and not is_unapproved:
        ab = seq_idx % 100
        if ab in (15, 16, 17, 18) and merged_wards:
            old_xa, _, new_tinh = merged_wards[seq_idx % len(merged_wards)]
            ma_tinh, ma_xa = new_tinh, old_xa
        elif ab == 23:
            ma_tinh, ma_xa = None, "00999"
        elif ab in (26, 27):
            ma_tinh = "01" if tinh_new != "01" else "79"
        elif ab in (30, 31):
            ma_tinh, ma_xa = "99", "99999"
        elif ab == 32:
            ma_tinh_lh, ma_xa_lh = "99", "99999"
        elif ab == 33:
            ma_tinh_lh = "01" if tinh_lh != "01" else "79"

    birth_year = 1955 + (seq_idx % 68)
    birth_month = (seq_idx % 12) + 1
    birth_day = (seq_idx % 28) + 1
    ngay_sinh: Optional[int] = int(f"{birth_year:04d}{birth_month:02d}{birth_day:02d}")
    if not is_protected_seed and (seq_idx % 100 == 40):
        ngay_sinh = 19950230

    cccd_val: Optional[str] = f"0{tinh_new[:2].zfill(2)}{gioi_tinh - 1}{str(birth_year)[-2:]}{(800000 + seq_idx) % 1000000:06d}"
    so_cmtnd: Optional[str] = None
    if not is_protected_seed and (seq_idx % 100 in (43, 44)):
        cccd_val = None
        so_cmtnd = f"{200000000 + (seq_idx % 700000000):09d}"
    elif not is_protected_seed and (seq_idx % 100 == 45):
        cccd_val = "000000000000"

    phone_val = f"09{(10000000 + seq_idx) % 90000000:08d}"
    if not is_protected_seed and (seq_idx % 100 == 49):
        phone_val = "0000000000"

    quoc_tich = "VN"
    if not is_protected_seed and (seq_idx % 100 == 55):
        quoc_tich = "ZZ_INVALID"

    dm_dan_toc_id = 171 if (seq_idx % 6 != 0) else valid_dantoc[seq_idx % len(valid_dantoc)]
    if not is_protected_seed and (seq_idx % 100 == 56):
        dm_dan_toc_id = 248

    r = make_empty_row()
    r["ID"] = row_id
    r["MA_CD"] = new_ma_cd
    r["MA_HO_GD"] = f"HGD_DLT_{seq_idx:07d}"
    r["HO_TEN"] = ho_ten
    r["HO_TEN_KD"] = ho_ten_kd
    r["TEN_KD"] = remove_accents(ten)
    r["SO_SO_BHXH"] = new_ma_cd
    r["GIOI_TINH"] = gioi_tinh
    r["NGAY_SINH"] = ngay_sinh
    r["QUOC_TICH"] = quoc_tich
    r["DM_DAN_TOC_ID"] = dm_dan_toc_id
    r["SO_CCCD"] = cccd_val
    r["SO_CMTND"] = so_cmtnd
    r["PHONE_NUMBER"] = phone_val
    r["MA_TINH"] = ma_tinh
    r["MA_XA"] = ma_xa
    r["DIA_CHI"] = f"Số {seq_idx % 300 + 1}, {ward_name}"
    r["MA_TINH_HK"] = ma_tinh
    r["MA_XA_HK"] = ma_xa
    r["DIA_CHI_HK"] = r["DIA_CHI"]
    r["MA_TINH_LH"] = ma_tinh_lh
    r["MA_XA_LH"] = ma_xa_lh
    r["DIA_CHI_LH"] = f"LH {ward_lh_name}"
    r["MA_TINH_KS"] = tinh_new
    r["MA_XA_KS"] = ward_new
    r["DIA_CHI_KS"] = f"KS {ward_name}"
    r["TRANG_THAI"] = 1
    r["TINH_TRANG_SONG"] = 1
    r["TYPEID"] = "GT" if is_typeid_g else "TM"
    r["TRANG_THAI_DUYET"] = 1 if is_unapproved else 2
    r["NGAY_TAO"] = now_ts
    r["NGAY_SUA"] = now_ts
    r["NGUOI_TAO"] = "DELTA_GEN"
    r["NGUOI_SUA"] = "DELTA_GEN"
    return r


def apply_delta_changes(
    conn: Any,
    sync_to_mirror: bool = True,
    out_dir: Optional[Path] = None,
    bulk_insert_count: int = 5,
    batch_size: int = 5000,
) -> Dict[str, Any]:
    t0 = time.perf_counter()
    init_source_and_mirror_schema(conn)
    init_c360_schema(conn)

    cur = conn.cursor()
    col_names = [c for c, _ in NHANKHAU_COLUMNS]
    placeholders = ", ".join(["?"] * len(col_names))
    insert_sql = f"INSERT INTO VSS_HO_GD_NHANKHAU ({', '.join(col_names)}) VALUES ({placeholders})"
    mirror_upsert_sql = _get_mirror_upsert_sql(col_names)

    now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    pools = fetch_reference_pools(conn)

    # Đảm bảo tính idempotent nếu chạy lại dải mã '99%'
    max_new_ma_cd = f"99{bulk_insert_count:08d}"
    cur.execute(
        "DELETE FROM VSS_HO_GD_NHANKHAU WHERE MA_CD >= '9900000001' AND MA_CD <= ?;",
        (max_new_ma_cd,),
    )
    conn.commit()

    cur.execute("SELECT COALESCE(MAX(ID), 0) FROM VSS_HO_GD_NHANKHAU")
    next_id = (cur.fetchone()[0] or 0) + 1

    cdc_events: List[Dict[str, Any]] = []
    manifest: Dict[str, Any] = {
        "executed_at": now_ts,
        "bulk_insert_requested": bulk_insert_count,
        "inserted_new_citizens_count": 0,
        "inserted_new_citizens": [],  # Giữ tối đa 20 mẫu đầu trong JSON để tránh phình file khi bơm 100k dòng
        "inserted_new_household_for_existing": [],
        "updated_dirty_to_clean": [],
        "updated_status_to_filtered_out": [],
        "updated_status_to_deceased": [],
        "updated_ma_cd_reissued": [],
        "deleted_latest_household_fallback": [],
        "deleted_single_household_citizen": [],
    }

    # -------------------------------------------------------------------------
    # 1a. INSERT lô công dân mới (Streaming theo batch_size để chống OOM khi bơm 50k-100k dòng)
    # -------------------------------------------------------------------------
    bulk_mirror_upserted = 0
    src_batch: List[Tuple[Any, ...]] = []
    mir_batch: List[Tuple[Any, ...]] = []

    for i in range(1, bulk_insert_count + 1):
        row = _build_bulk_citizen_row(i, next_id, pools, now_ts)
        next_id += 1
        src_batch.append(tuple(row[c] for c in col_names))
        if sync_to_mirror:
            mir_batch.append(_norm_mirror_tuple(row, col_names))

        if len(manifest["inserted_new_citizens"]) < 20:
            manifest["inserted_new_citizens"].append(
                {"MA_CD": row["MA_CD"], "MA_HO_GD": row["MA_HO_GD"], "ID": row["ID"]}
            )

        if len(src_batch) >= batch_size:
            cur.executemany(insert_sql, src_batch)
            if sync_to_mirror and mir_batch:
                cur.executemany(mirror_upsert_sql, mir_batch)
                bulk_mirror_upserted += len(mir_batch)
                mir_batch.clear()
            conn.commit()
            src_batch.clear()

    if src_batch:
        cur.executemany(insert_sql, src_batch)
        if sync_to_mirror and mir_batch:
            cur.executemany(mirror_upsert_sql, mir_batch)
            bulk_mirror_upserted += len(mir_batch)
            mir_batch.clear()
        conn.commit()
        src_batch.clear()

    manifest["inserted_new_citizens_count"] = bulk_insert_count

    # -------------------------------------------------------------------------
    # 1b. INSERT hộ gia đình mới cho công dân đã tồn tại (chọn 3 công dân đang có 1 hộ hợp lệ)
    # -------------------------------------------------------------------------
    cur.execute(
        """
        SELECT MA_CD, MAX(ID)
        FROM VSS_HO_GD_NHANKHAU
        WHERE COALESCE(TYPEID, 'ZZ') NOT IN ('GT', 'GM', 'GG', 'GK')
          AND COALESCE(TRANG_THAI_DUYET, 0) NOT IN (1, 3, 4)
          AND MA_CD NOT LIKE '99%' AND MA_CD NOT LIKE '88%'
        GROUP BY MA_CD
        HAVING COUNT(*) = 1
        ORDER BY MA_CD
        LIMIT 3
        """
    )
    existing_single = cur.fetchall()
    for ma_cd, old_id in existing_single:
        base_row = fetch_row_as_dict(cur, old_id)
        if not base_row:
            continue
        new_row = dict(base_row)
        new_row["ID"] = next_id
        next_id += 1
        new_row["MA_HO_GD"] = f"HGD_DELTA_NEW_{ma_cd}"
        new_row["MA_TINH"] = "79"
        new_row["MA_XA"] = "26734"
        new_row["DIA_CHI"] = f"Hộ mới chuyển vào TP.HCM của {ma_cd}"
        new_row["MA_TINH_HK"] = "79"
        new_row["MA_XA_HK"] = "26734"
        new_row["DIA_CHI_HK"] = new_row["DIA_CHI"]
        new_row["NGAY_TAO"] = now_ts
        new_row["NGAY_SUA"] = now_ts

        cur.execute("DELETE FROM VSS_HO_GD_NHANKHAU WHERE MA_CD = ? AND MA_HO_GD = ?", (ma_cd, new_row["MA_HO_GD"]))
        cur.execute(insert_sql, tuple(new_row[c] for c in col_names))
        cdc_events.append({"op": "INSERT", "after": new_row})
        manifest["inserted_new_household_for_existing"].append(
            {
                "MA_CD": ma_cd,
                "OLD_MA_HO_GD": base_row["MA_HO_GD"],
                "NEW_MA_HO_GD": new_row["MA_HO_GD"],
                "NEW_ID": new_row["ID"],
                "EXPECTED_MA_TINH_THUONG_TRU": "79",
                "EXPECTED_MA_XA_THUONG_TRU": "26734",
            }
        )

    # -------------------------------------------------------------------------
    # 2a. UPDATE dữ liệu bẩn -> sạch (chọn 5 dòng có lỗi địa chỉ/ngày sinh/CCCD)
    # -------------------------------------------------------------------------
    cur.execute(
        """
        SELECT ID, MA_CD, MA_HO_GD
        FROM VSS_HO_GD_NHANKHAU
        WHERE COALESCE(TYPEID, 'ZZ') NOT IN ('GT', 'GM', 'GG', 'GK')
          AND COALESCE(TRANG_THAI_DUYET, 0) NOT IN (1, 3, 4)
          AND MA_CD NOT LIKE '99%'
          AND (MA_TINH = '99' OR NGAY_SINH IN (19950230, 17500101, '19950230', '17500101') OR SO_CCCD = '000000000000')
        ORDER BY ID
        LIMIT 5
        """
    )
    dirty_targets = cur.fetchall()
    for row_id, ma_cd, ma_ho_gd in dirty_targets:
        cur.execute(
            """
            UPDATE VSS_HO_GD_NHANKHAU
            SET MA_TINH = '01',
                MA_XA = '00004',
                MA_TINH_HK = '01',
                MA_XA_HK = '00004',
                DIA_CHI = 'Đã chuẩn hóa địa chỉ sạch Ba Đình, Hà Nội',
                MA_TINH_LH = '01',
                MA_XA_LH = '00004',
                MA_TINH_KS = '01',
                MA_XA_KS = '00004',
                NGAY_SINH = 19920615,
                SO_CCCD = '001092008888',
                PHONE_NUMBER = '0912888999',
                QUOC_TICH = 'VN',
                DM_DAN_TOC_ID = 171,
                NGAY_SUA = ?
            WHERE ID = ?
            """,
            (now_ts, row_id),
        )
        updated_row = fetch_row_as_dict(cur, row_id)
        if updated_row:
            cdc_events.append({"op": "UPDATE", "after": updated_row})
            manifest["updated_dirty_to_clean"].append(
                {"ID": row_id, "MA_CD": ma_cd, "MA_HO_GD": ma_ho_gd, "EXPECTED_DQ_FLAGS": None}
            )

    # -------------------------------------------------------------------------
    # 2b. UPDATE đổi TRANG_THAI_DUYET sang (1, 3, 4) hoặc TYPEID sang ('GG', 'GT')
    # -------------------------------------------------------------------------
    cur.execute(
        """
        SELECT MIN(ID), MA_CD, MIN(MA_HO_GD)
        FROM VSS_HO_GD_NHANKHAU
        WHERE COALESCE(TYPEID, 'ZZ') NOT IN ('GT', 'GM', 'GG', 'GK')
          AND COALESCE(TRANG_THAI_DUYET, 0) NOT IN (1, 3, 4)
          AND MA_CD NOT LIKE '99%' AND MA_CD NOT LIKE '88%'
          AND MA_CD > '0000000020'
        GROUP BY MA_CD
        HAVING COUNT(*) = 1
        ORDER BY MA_CD
        LIMIT 4
        """
    )
    filter_targets = cur.fetchall()
    for idx, (row_id, ma_cd, ma_ho_gd) in enumerate(filter_targets):
        if idx % 2 == 0:
            new_tt_duyet = [4, 3, 1][(idx // 2) % 3]
            cur.execute(
                "UPDATE VSS_HO_GD_NHANKHAU SET TRANG_THAI_DUYET = ?, NGAY_SUA = ? WHERE ID = ?",
                (new_tt_duyet, now_ts, row_id),
            )
            reason = f"TRANG_THAI_DUYET={new_tt_duyet}"
        else:
            new_typeid = "GG" if (idx // 2) % 2 == 0 else "GT"
            cur.execute(
                "UPDATE VSS_HO_GD_NHANKHAU SET TYPEID = ?, NGAY_SUA = ? WHERE ID = ?",
                (new_typeid, now_ts, row_id),
            )
            reason = f"TYPEID='{new_typeid}'"
        updated_row = fetch_row_as_dict(cur, row_id)
        if updated_row:
            cdc_events.append({"op": "UPDATE", "after": updated_row})
            manifest["updated_status_to_filtered_out"].append(
                {"ID": row_id, "MA_CD": ma_cd, "MA_HO_GD": ma_ho_gd, "REASON": reason}
            )

    # -------------------------------------------------------------------------
    # 2c. UPDATE đổi TINH_TRANG_SONG từ 1 -> 0 (vẫn giữ trên C360 nhưng IS_ALIVE = 0)
    # -------------------------------------------------------------------------
    cur.execute(
        """
        SELECT MIN(ID), MA_CD, MIN(MA_HO_GD)
        FROM VSS_HO_GD_NHANKHAU
        WHERE COALESCE(TYPEID, 'ZZ') NOT IN ('GT', 'GM', 'GG', 'GK')
          AND COALESCE(TRANG_THAI_DUYET, 0) NOT IN (1, 3, 4)
          AND TINH_TRANG_SONG = 1 AND TRANG_THAI = 1
          AND MA_CD NOT LIKE '99%' AND MA_CD NOT LIKE '88%'
          AND MA_CD > '0000000050'
        GROUP BY MA_CD
        HAVING COUNT(*) = 1
        ORDER BY MA_CD
        LIMIT 2
        """
    )
    deceased_targets = cur.fetchall()
    for row_id, ma_cd, ma_ho_gd in deceased_targets:
        cur.execute(
            """
            UPDATE VSS_HO_GD_NHANKHAU
            SET TINH_TRANG_SONG = 0, NGAY_CHET = '20261101', NGAY_SUA = ?
            WHERE ID = ?
            """,
            (now_ts, row_id),
        )
        updated_row = fetch_row_as_dict(cur, row_id)
        if updated_row:
            cdc_events.append({"op": "UPDATE", "after": updated_row})
            manifest["updated_status_to_deceased"].append(
                {"ID": row_id, "MA_CD": ma_cd, "MA_HO_GD": ma_ho_gd, "EXPECTED_TRANG_THAI_HIEU_LUC": 0}
            )

    # -------------------------------------------------------------------------
    # 3. UPDATE đổi mã định danh MA_CD từ MA_CD_A sang MA_CD_B, lưu MA_CD_A vào MA_CD_OLD
    # -------------------------------------------------------------------------
    cur.execute(
        """
        SELECT MIN(ID), MA_CD, MIN(MA_HO_GD)
        FROM VSS_HO_GD_NHANKHAU
        WHERE COALESCE(TYPEID, 'ZZ') NOT IN ('GT', 'GM', 'GG', 'GK')
          AND COALESCE(TRANG_THAI_DUYET, 0) NOT IN (1, 3, 4)
          AND MA_CD NOT LIKE '99%' AND MA_CD NOT LIKE '88%'
          AND MA_CD > '0000000080'
        GROUP BY MA_CD
        HAVING COUNT(*) = 1
        ORDER BY MA_CD
        LIMIT 3
        """
    )
    reissue_targets = cur.fetchall()
    for idx, (row_id, old_ma_cd, ma_ho_gd) in enumerate(reissue_targets, start=1):
        new_ma_cd = f"8800000{idx:03d}"
        cur.execute("DELETE FROM VSS_HO_GD_NHANKHAU WHERE MA_CD = ?", (new_ma_cd,))
        before_row = fetch_row_as_dict(cur, row_id)
        cur.execute(
            """
            UPDATE VSS_HO_GD_NHANKHAU
            SET MA_CD = ?,
                MA_CD_OLD = ?,
                SO_SO_BHXH = ?,
                NGAY_SUA = ?
            WHERE ID = ?
            """,
            (new_ma_cd, old_ma_cd, new_ma_cd, now_ts, row_id),
        )
        after_row = fetch_row_as_dict(cur, row_id)
        if before_row and after_row:
            cdc_events.append({"op": "UPDATE_PK", "before": before_row, "after": after_row})
            manifest["updated_ma_cd_reissued"].append(
                {
                    "ID": row_id,
                    "MA_CD_OLD": old_ma_cd,
                    "MA_CD_NEW": new_ma_cd,
                    "MA_HO_GD": ma_ho_gd,
                }
            )

    # -------------------------------------------------------------------------
    # 4a. DELETE 1 dòng hộ gia đình mới nhất của công dân có 2 hộ gia đình -> lùi về hộ cũ
    # -------------------------------------------------------------------------
    cur.execute(
        """
        SELECT MA_CD, MAX(ID) AS LATEST_ID, MIN(ID) AS OLDER_ID
        FROM VSS_HO_GD_NHANKHAU
        WHERE COALESCE(TYPEID, 'ZZ') NOT IN ('GT', 'GM', 'GG', 'GK')
          AND COALESCE(TRANG_THAI_DUYET, 0) NOT IN (1, 3, 4)
          AND MA_CD NOT LIKE '99%' AND MA_CD NOT LIKE '88%'
        GROUP BY MA_CD
        HAVING COUNT(*) = 2
        ORDER BY MA_CD
        LIMIT 3
        """
    )
    multi_targets = cur.fetchall()
    for ma_cd, latest_id, older_id in multi_targets:
        latest_row = fetch_row_as_dict(cur, latest_id)
        older_row = fetch_row_as_dict(cur, older_id)
        if latest_row and older_row:
            cur.execute("DELETE FROM VSS_HO_GD_NHANKHAU WHERE ID = ?", (latest_id,))
            cdc_events.append({"op": "DELETE", "before": latest_row})
            manifest["deleted_latest_household_fallback"].append(
                {
                    "MA_CD": ma_cd,
                    "DELETED_ID": latest_id,
                    "DELETED_MA_HO_GD": latest_row["MA_HO_GD"],
                    "FALLBACK_ID": older_id,
                    "FALLBACK_MA_HO_GD": older_row["MA_HO_GD"],
                    "FALLBACK_MA_TINH": older_row["MA_TINH"],
                    "FALLBACK_MA_XA": older_row["MA_XA"],
                }
            )

    # -------------------------------------------------------------------------
    # 4b. DELETE toàn bộ dòng của công dân chỉ có 1 hộ gia đình -> xóa hẳn khỏi C360
    # -------------------------------------------------------------------------
    cur.execute(
        """
        SELECT MIN(ID), MA_CD, MIN(MA_HO_GD)
        FROM VSS_HO_GD_NHANKHAU
        WHERE COALESCE(TYPEID, 'ZZ') NOT IN ('GT', 'GM', 'GG', 'GK')
          AND COALESCE(TRANG_THAI_DUYET, 0) NOT IN (1, 3, 4)
          AND MA_CD NOT LIKE '99%' AND MA_CD NOT LIKE '88%'
          AND MA_CD > '0000000120'
        GROUP BY MA_CD
        HAVING COUNT(*) = 1
        ORDER BY MA_CD
        LIMIT 3
        """
    )
    delete_full_targets = cur.fetchall()
    for row_id, ma_cd, ma_ho_gd in delete_full_targets:
        before_row = fetch_row_as_dict(cur, row_id)
        if before_row:
            cur.execute("DELETE FROM VSS_HO_GD_NHANKHAU WHERE ID = ?", (row_id,))
            cdc_events.append({"op": "DELETE", "before": before_row})
            manifest["deleted_single_household_citizen"].append(
                {"ID": row_id, "MA_CD": ma_cd, "MA_HO_GD": ma_ho_gd}
            )

    conn.commit()

    op_insert = bulk_insert_count + len(manifest["inserted_new_household_for_existing"])
    op_update = (
        len(manifest["updated_dirty_to_clean"])
        + len(manifest["updated_status_to_filtered_out"])
        + len(manifest["updated_status_to_deceased"])
        + len(manifest["updated_ma_cd_reissued"])
    )
    op_delete = (
        len(manifest["deleted_latest_household_fallback"])
        + len(manifest["deleted_single_household_citizen"])
    )
    total_cdc_msgs = op_insert + op_update + op_delete + len(manifest["updated_ma_cd_reissued"])
    payload_bytes = total_cdc_msgs * APPROX_CDC_MSG_BYTES

    total_mirror_upserted = bulk_mirror_upserted
    total_mirror_deleted = 0
    if sync_to_mirror:
        cdc_stats = sync_cdc_events_to_mirror(conn, cdc_events)
        total_mirror_upserted += cdc_stats["mirror_upserted"]
        total_mirror_deleted += cdc_stats["mirror_deleted"]
        manifest["cdc_mirror_sync"] = {
            "mirror_upserted": total_mirror_upserted,
            "mirror_deleted": total_mirror_deleted,
        }

    # Ghi nhận thông số lưu lượng Topic CDC vào MIR_CDC_TOPIC_METRICS
    cur.execute(
        """
        INSERT INTO MIR_CDC_TOPIC_METRICS (
            TOPIC_NAME, PARTITIONS, OP_INSERT, OP_UPDATE, OP_DELETE,
            TOTAL_MESSAGES, PAYLOAD_BYTES, MIRROR_UPSERTED, MIRROR_DELETED
        ) VALUES (?, 3, ?, ?, ?, ?, ?, ?, ?);
        """,
        (
            CDC_TOPIC_NAME,
            op_insert,
            op_update,
            op_delete,
            total_cdc_msgs,
            payload_bytes,
            total_mirror_upserted,
            total_mirror_deleted,
        ),
    )
    conn.commit()

    elapsed_sec = round(time.perf_counter() - t0, 2)
    manifest["cdc_topic_metrics"] = {
        "topic": CDC_TOPIC_NAME,
        "partitions": 3,
        "messages": total_cdc_msgs,
        "size_mb": round(payload_bytes / (1024 * 1024), 2),
        "elapsed_sec": elapsed_sec,
    }

    target_dir = Path(out_dir) if out_dir else (PROJECT_ROOT / "data")
    gt_path = target_dir / "delta_ground_truth.json"
    gt_path.parent.mkdir(parents=True, exist_ok=True)
    gt_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        f"[GEN_DELTA_CHANGES] Đã thực thi thành công {total_cdc_msgs:,} thông điệp CDC ({elapsed_sec}s):\n"
        f"  - Topic Kafka CDC                         : {CDC_TOPIC_NAME} ({total_cdc_msgs:,} msgs ~ {manifest['cdc_topic_metrics']['size_mb']} MB)\n"
        f"  - INSERT công dân mới                     : {bulk_insert_count:,}\n"
        f"  - INSERT hộ mới cho công dân đã có        : {len(manifest['inserted_new_household_for_existing'])}\n"
        f"  - UPDATE dữ liệu bẩn -> sạch              : {len(manifest['updated_dirty_to_clean'])}\n"
        f"  - UPDATE trạng thái -> loại khỏi C360     : {len(manifest['updated_status_to_filtered_out'])}\n"
        f"  - UPDATE sang đã mất (TINH_TRANG_SONG=0)  : {len(manifest['updated_status_to_deceased'])}\n"
        f"  - UPDATE đổi MA_CD (lưu MA_CD_OLD)        : {len(manifest['updated_ma_cd_reissued'])}\n"
        f"  - DELETE hộ mới nhất (fallback hộ cũ)     : {len(manifest['deleted_latest_household_fallback'])}\n"
        f"  - DELETE toàn bộ công dân                 : {len(manifest['deleted_single_household_citizen'])}\n"
        f"  -> Chi tiết Ground Truth lưu tại: {gt_path}"
    )
    return manifest


def main():
    parser = argparse.ArgumentParser(description="Script 2: Sinh dữ liệu biến động (Delta/CDC) trên VSS_HO_GD_NHANKHAU")
    parser.add_argument("--db-path", type=str, default=None, help="Đường dẫn file SQLite DB")
    parser.add_argument(
        "--insert-count",
        type=int,
        default=5,
        help="Số lượng công dân mới cần bơm thêm (vd: 5 cho test nhanh, 50000 hoặc 100000 cho Demo BI)",
    )
    parser.add_argument(
        "--no-sync-mirror",
        action="store_true",
        help="Chỉ ghi biến động vào bảng Nguồn VSS_HO_GD_NHANKHAU, không tự động đẩy CDC sang Mirror",
    )
    parser.add_argument(
        "--run-nrt",
        action="store_true",
        help="Tự động kích hoạt luôn Worker Near Real-time sau khi đẩy CDC sang Mirror",
    )
    args = parser.parse_args()

    db_path = Path(args.db_path) if args.db_path else None
    conn = get_connection(sqlite_path=db_path)
    init_source_and_mirror_schema(conn, engine=get_db_engine())
    apply_delta_changes(
        conn,
        sync_to_mirror=not args.no_sync_mirror,
        bulk_insert_count=max(1, args.insert_count),
    )
    conn.close()

    if args.run_nrt and not args.no_sync_mirror:
        from p2_c360_standardization.c360_nrt_worker import run_c360_nrt_batch

        while True:
            res = run_c360_nrt_batch(sqlite_path=db_path, max_batch_size=10000)
            if res["status"] in ("EMPTY", "SKIPPED"):
                break
            print(
                f"[NRT_SYNC] Đã xử lý lô Queue: keys={res['processed_keys']:,}, "
                f"merge={res['merge_count']:,}, delete={res['delete_count']:,} ({res['elapsed_sec']}s)"
            )


if __name__ == "__main__":
    main()
