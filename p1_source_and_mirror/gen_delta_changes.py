#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BAT-C360-002 (Script 2 - Change / Delta Generator):
Sinh dữ liệu biến động trực tiếp trên bảng nguồn VSS.VSS_HO_GD_NHANKHAU để kiểm thử
luồng CDC (Debezium -> Mirror) và luồng Near Real-time Sync (Queue -> C360).

Bao phủ đầy đủ 4 nhóm thao tác bắt buộc theo mục 3 - Phần 1.1:
1. INSERT:
   - 1a: Thêm công dân hoàn toàn mới (MA_CD mới).
   - 1b: Thêm dòng hộ gia đình mới (MA_HO_GD mới, NGAY_SUA mới hơn) cho MA_CD đã tồn tại.
2. UPDATE thông thường:
   - 2a: Sửa dữ liệu bẩn (địa chỉ 99/99999, ngày sinh sai, CCCD sai, SĐT sai) sang sạch -> gỡ cờ DQ_FLAGS.
   - 2b: Đổi TRANG_THAI_DUYET từ 1 -> 0 hoặc TYPEID -> 'D' -> loại khỏi C360.
   - 2c: Đổi TINH_TRANG_SONG từ 1 -> 0 (kèm NGAY_CHET) -> TRANG_THAI_HIEU_LUC về 0.
3. UPDATE đổi mã định danh MA_CD (Cấp lại mã số BHXH):
   - Đổi MA_CD từ MA_CD_A sang MA_CD_B, lưu MA_CD_A vào cột MA_CD_OLD, cập nhật NGAY_SUA
     -> Xóa/tính lại MA_CD_A cũ (không để mồ côi) và tạo/cập nhật MA_CD_B mới.
4. DELETE:
   - 4a: Xóa dòng hộ gia đình mới nhất của công dân có 2 hộ gia đình -> C360 tự động lùi về dòng hộ cũ còn lại.
   - 4b: Xóa toàn bộ dòng của công dân chỉ có 1 hộ gia đình -> C360 xóa hẳn công dân đó.
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

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
    init_source_and_mirror_schema,
)


def sync_cdc_events_to_mirror(conn: Any, cdc_events: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Mô phỏng chính xác cơ chế Debezium Source + Kafka JDBC Sink (insert.mode=upsert,
    pk.mode=record_key, pk.fields=MA_CD,MA_HO_GD, delete.enabled=true) khi chạy ở chế độ Local
    hoặc khi cần đẩy ngay lô CDC sang bảng Mirror.
    """
    cur = conn.cursor()
    col_names = [c for c, _ in NHANKHAU_COLUMNS]
    col_list_str = ", ".join(col_names)
    placeholders = ", ".join(["?"] * len(col_names))
    update_set_str = ", ".join([f"{c} = excluded.{c}" for c in col_names if c not in ("MA_CD", "MA_HO_GD")])

    upsert_sql = f"""
        INSERT INTO MIR_VSS_HO_GD_NHANKHAU ({col_list_str}, MIRROR_SYNC_TS)
        VALUES ({placeholders}, datetime('now', 'localtime'))
        ON CONFLICT(MA_CD, MA_HO_GD) DO UPDATE SET
            {update_set_str},
            MIRROR_SYNC_TS = datetime('now', 'localtime')
    """

    upserted = 0
    deleted = 0

    for ev in cdc_events:
        op = ev["op"]
        if op == "DELETE":
            cur.execute(
                "DELETE FROM MIR_VSS_HO_GD_NHANKHAU WHERE MA_CD = ? AND MA_HO_GD = ?",
                (ev["before"]["MA_CD"], ev["before"]["MA_HO_GD"]),
            )
            deleted += cur.rowcount
        elif op == "UPDATE_PK":
            # Debezium phát 1 bản ghi DELETE cho khóa cũ (MA_CD_A, MA_HO_GD) + 1 bản ghi UPSERT cho khóa mới (MA_CD_B, MA_HO_GD)
            cur.execute(
                "DELETE FROM MIR_VSS_HO_GD_NHANKHAU WHERE MA_CD = ? AND MA_HO_GD = ?",
                (ev["before"]["MA_CD"], ev["before"]["MA_HO_GD"]),
            )
            deleted += cur.rowcount
            row_vals = tuple(ev["after"][c] for c in col_names)
            cur.execute(upsert_sql, row_vals)
            upserted += 1
        elif op in ("INSERT", "UPDATE"):
            row_vals = tuple(ev["after"][c] for c in col_names)
            cur.execute(upsert_sql, row_vals)
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


def apply_delta_changes(
    conn: Any,
    sync_to_mirror: bool = True,
    out_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    cur = conn.cursor()
    col_names = [c for c, _ in NHANKHAU_COLUMNS]
    placeholders = ", ".join(["?"] * len(col_names))
    insert_sql = f"INSERT INTO VSS_HO_GD_NHANKHAU ({', '.join(col_names)}) VALUES ({placeholders})"

    cur.execute("SELECT COALESCE(MAX(ID), 0) FROM VSS_HO_GD_NHANKHAU")
    next_id = (cur.fetchone()[0] or 0) + 1
    now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cdc_events: List[Dict[str, Any]] = []
    manifest: Dict[str, Any] = {
        "executed_at": now_ts,
        "inserted_new_citizens": [],
        "inserted_new_household_for_existing": [],
        "updated_dirty_to_clean": [],
        "updated_status_to_filtered_out": [],
        "updated_status_to_deceased": [],
        "updated_ma_cd_reissued": [],
        "deleted_latest_household_fallback": [],
        "deleted_single_household_citizen": [],
    }

    # -------------------------------------------------------------------------
    # 1a. INSERT công dân hoàn toàn mới (5 công dân mới)
    # -------------------------------------------------------------------------
    for i in range(1, 6):
        new_ma_cd = f"9900000{i:03d}"
        # Đảm bảo idempotent nếu chạy lại
        cur.execute("DELETE FROM VSS_HO_GD_NHANKHAU WHERE MA_CD = ?", (new_ma_cd,))
        row = {c: None for c in col_names}
        row["ID"] = next_id
        next_id += 1
        row["MA_CD"] = new_ma_cd
        row["MA_HO_GD"] = f"HGD_NEW_{i:04d}"
        row["HO_TEN"] = f"NGUYỄN MỚI THÊM {i}"
        row["HO_TEN_KD"] = f"NGUYEN MOI THEM {i}"
        row["TEN_KD"] = f"THEM{i}"
        row["SO_SO_BHXH"] = new_ma_cd
        row["GIOI_TINH"] = 1 if i % 2 == 1 else 2
        row["NGAY_SINH"] = "19950820"
        row["QUOC_TICH"] = "VN"
        row["DM_DAN_TOC_ID"] = 171
        row["SO_CCCD"] = f"001095990{i:03d}"
        row["PHONE_NUMBER"] = f"0988777{i:03d}"
        row["MA_TINH"] = "01"
        row["MA_XA"] = "00004"
        row["DIA_CHI"] = f"Số {i} Ba Đình, Hà Nội"
        row["MA_TINH_HK"] = "01"
        row["MA_XA_HK"] = "00004"
        row["DIA_CHI_HK"] = row["DIA_CHI"]
        row["MA_TINH_LH"] = "01"
        row["MA_XA_LH"] = "00004"
        row["DIA_CHI_LH"] = row["DIA_CHI"]
        row["MA_TINH_KS"] = "01"
        row["MA_XA_KS"] = "00004"
        row["DIA_CHI_KS"] = row["DIA_CHI"]
        row["TRANG_THAI"] = 1
        row["TINH_TRANG_SONG"] = 1
        row["TRANG_THAI_DUYET"] = 1
        row["TYPEID"] = "1"
        row["NGAY_TAO"] = "2026-11-10 09:00:00"
        row["NGAY_SUA"] = "2026-11-10 09:00:00"
        row["NGUOI_TAO"] = "DELTA_GEN"
        row["NGUOI_SUA"] = "DELTA_GEN"

        cur.execute(insert_sql, tuple(row[c] for c in col_names))
        cdc_events.append({"op": "INSERT", "after": row})
        manifest["inserted_new_citizens"].append(
            {"MA_CD": new_ma_cd, "MA_HO_GD": row["MA_HO_GD"], "ID": row["ID"]}
        )

    # -------------------------------------------------------------------------
    # 1b. INSERT hộ gia đình mới cho công dân đã tồn tại (chọn 3 công dân đang có 1 hộ hợp lệ)
    # -------------------------------------------------------------------------
    cur.execute(
        """
        SELECT MA_CD, MAX(ID)
        FROM VSS_HO_GD_NHANKHAU
        WHERE COALESCE(TYPEID, '') != 'D' AND TRANG_THAI_DUYET = 1
          AND MA_CD NOT LIKE '99%'
        GROUP BY MA_CD
        HAVING COUNT(*) = 1
        ORDER BY MA_CD
        LIMIT 3
        """
    )
    existing_single = cur.fetchall()
    for idx, (ma_cd, old_id) in enumerate(existing_single, start=1):
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
        new_row["NGAY_TAO"] = "2026-11-10 09:30:00"
        new_row["NGAY_SUA"] = "2026-11-10 09:30:00"

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
    # 2a. UPDATE dữ liệu bẩn -> sạch (chọn các dòng có MA_TINH = '99' hoặc NGAY_SINH sai)
    # -------------------------------------------------------------------------
    cur.execute(
        """
        SELECT ID, MA_CD, MA_HO_GD
        FROM VSS_HO_GD_NHANKHAU
        WHERE COALESCE(TYPEID, '') != 'D' AND TRANG_THAI_DUYET = 1
          AND (MA_TINH = '99' OR NGAY_SINH = 'INVALID_DATE' OR SO_CCCD = '000000000000')
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
                NGAY_SINH = '19920615',
                SO_CCCD = '001092008888',
                PHONE_NUMBER = '0912888999',
                QUOC_TICH = 'VN',
                DM_DAN_TOC_ID = 171,
                NGAY_SUA = '2026-11-10 10:00:00'
            WHERE ID = ?
            """,
            (row_id,),
        )
        updated_row = fetch_row_as_dict(cur, row_id)
        if updated_row:
            cdc_events.append({"op": "UPDATE", "after": updated_row})
            manifest["updated_dirty_to_clean"].append(
                {"ID": row_id, "MA_CD": ma_cd, "MA_HO_GD": ma_ho_gd, "EXPECTED_DQ_FLAGS": None}
            )

    # -------------------------------------------------------------------------
    # 2b. UPDATE đổi TRANG_THAI_DUYET từ 1 -> 0 hoặc TYPEID -> 'D' (chọn 4 công dân có 1 hộ)
    # -------------------------------------------------------------------------
    cur.execute(
        """
        SELECT MIN(ID), MA_CD, MIN(MA_HO_GD)
        FROM VSS_HO_GD_NHANKHAU
        WHERE COALESCE(TYPEID, '') != 'D' AND TRANG_THAI_DUYET = 1
          AND MA_CD NOT LIKE '99%'
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
            cur.execute(
                "UPDATE VSS_HO_GD_NHANKHAU SET TRANG_THAI_DUYET = 0, NGAY_SUA = '2026-11-10 10:05:00' WHERE ID = ?",
                (row_id,),
            )
            reason = "TRANG_THAI_DUYET=0"
        else:
            cur.execute(
                "UPDATE VSS_HO_GD_NHANKHAU SET TYPEID = 'D', NGAY_SUA = '2026-11-10 10:05:00' WHERE ID = ?",
                (row_id,),
            )
            reason = "TYPEID='D'"
        updated_row = fetch_row_as_dict(cur, row_id)
        if updated_row:
            cdc_events.append({"op": "UPDATE", "after": updated_row})
            manifest["updated_status_to_filtered_out"].append(
                {"ID": row_id, "MA_CD": ma_cd, "MA_HO_GD": ma_ho_gd, "REASON": reason}
            )

    # -------------------------------------------------------------------------
    # 2c. UPDATE đổi TINH_TRANG_SONG từ 1 -> 0 (vẫn giữ trên C360 nhưng TRANG_THAI_HIEU_LUC = 0)
    # -------------------------------------------------------------------------
    cur.execute(
        """
        SELECT MIN(ID), MA_CD, MIN(MA_HO_GD)
        FROM VSS_HO_GD_NHANKHAU
        WHERE COALESCE(TYPEID, '') != 'D' AND TRANG_THAI_DUYET = 1
          AND TINH_TRANG_SONG = 1 AND TRANG_THAI = 1
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
            SET TINH_TRANG_SONG = 0, NGAY_CHET = '20261101', NGAY_SUA = '2026-11-10 10:10:00'
            WHERE ID = ?
            """,
            (row_id,),
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
        WHERE COALESCE(TYPEID, '') != 'D' AND TRANG_THAI_DUYET = 1
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
                NGAY_SUA = '2026-11-10 10:20:00'
            WHERE ID = ?
            """,
            (new_ma_cd, old_ma_cd, new_ma_cd, row_id),
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
        WHERE COALESCE(TYPEID, '') != 'D' AND TRANG_THAI_DUYET = 1
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
        WHERE COALESCE(TYPEID, '') != 'D' AND TRANG_THAI_DUYET = 1
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

    if sync_to_mirror:
        cdc_stats = sync_cdc_events_to_mirror(conn, cdc_events)
        manifest["cdc_mirror_sync"] = cdc_stats

    target_dir = Path(out_dir) if out_dir else (PROJECT_ROOT / "data")
    gt_path = target_dir / "delta_ground_truth.json"
    gt_path.parent.mkdir(parents=True, exist_ok=True)
    gt_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        f"[GEN_DELTA_CHANGES] Đã thực thi thành công {len(cdc_events)} biến động trên VSS_HO_GD_NHANKHAU:\n"
        f"  - INSERT công dân mới                     : {len(manifest['inserted_new_citizens'])}\n"
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
        "--no-sync-mirror",
        action="store_true",
        help="Chỉ ghi biến động vào bảng Nguồn VSS_HO_GD_NHANKHAU, không tự động đẩy CDC sang Mirror",
    )
    args = parser.parse_args()

    conn = get_connection(sqlite_path=Path(args.db_path) if args.db_path else None)
    init_source_and_mirror_schema(conn, engine=get_db_engine())
    apply_delta_changes(conn, sync_to_mirror=not args.no_sync_mirror)
    conn.close()


if __name__ == "__main__":
    main()
