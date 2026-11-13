#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BAT-C360-001 (Step 1): Nạp dữ liệu 3 bảng Danh mục từ bộ file CSV thực tế vào hệ thống Nguồn (VSS_DM_*).
- DM_DAN_TOC_202611091513.csv    -> VSS_DM_DANTOC (59 dòng)
- DM_QUOC_TICH_202611091512.csv  -> VSS_DM_QUOC_TICH (243 dòng)
- DM_DBHC_202611091512.csv       -> VSS_DM_DBHC (15.325 dòng)

Lưu ý từ kết quả khảo sát dữ liệu thực tế (Sheet QA của BD/DD):
- Trong 3.322 xã đang hiệu lực của DM_DBHC_202611091512.csv, hiện có 0 mã xã cũ trong MA_DBHC_SN bị trùng lặp.
- Khi bật cờ --inject-moho (mặc định True), script bổ sung có kiểm soát 2 mã xã cũ bị tách đôi:
  + '00999': gắn vào MA_DBHC_SN của 2 phường thuộc cùng TP. Hà Nội (MA_CHA = '01': '00004' và '00007')
    -> Kiểm thử kịch bản Ưu tiên 3 (MO_HO) cùng Tỉnh: Xã gán NULL, Tỉnh vẫn suy ra được '01'.
  + '00998': gắn vào MA_DBHC_SN của 1 phường thuộc Hà Nội ('00004', MA_CHA = '01') và 1 phường thuộc Hưng Yên ('11956', MA_CHA = '33')
    -> Kiểm thử kịch bản Ưu tiên 3 (MO_HO) khác Tỉnh: Xã gán NULL và không suy ngược được Tỉnh.
"""

import argparse
import csv
import sys
from pathlib import Path
from typing import Any, Optional

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from p1_source_and_mirror.db_env import get_connection, get_db_engine, init_source_and_mirror_schema


def to_int(val: Optional[str]) -> Optional[int]:
    if val is None:
        return None
    s = val.strip()
    if not s:
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def to_str(val: Optional[str]) -> Optional[str]:
    if val is None:
        return None
    s = val.strip()
    return s if s != "" else None


def load_dm_dantoc(conn: Any, csv_path: Path) -> int:
    cur = conn.cursor()
    cur.execute("DELETE FROM VSS_DM_DANTOC")
    rows = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(
                (
                    to_int(r.get("ID")),
                    to_str(r.get("MA")),
                    to_str(r.get("TEN")) or "Chưa rõ",
                    to_str(r.get("TEN_GOI_KHAC")),
                    to_str(r.get("DIA_BAN_CU_TRU")),
                    to_str(r.get("NGAY_HL")),
                    to_str(r.get("NGAY_HET_HL")),
                    to_int(r.get("TRANG_THAI")),
                    to_int(r.get("NGUOI_TAO")),
                    to_str(r.get("NGAY_TAO")),
                    to_int(r.get("NGUOI_SUA")),
                    to_str(r.get("NGAY_SUA")),
                    to_int(r.get("DAN_SO")),
                    to_str(r.get("MA_CU")),
                    to_int(r.get("CAP")),
                    to_int(r.get("CHA_ID")),
                )
            )

    cur.executemany(
        """
        INSERT INTO VSS_DM_DANTOC (
            ID, MA, TEN, TEN_GOI_KHAC, DIA_BAN_CU_TRU,
            NGAY_HL, NGAY_HET_HL, TRANG_THAI, NGUOI_TAO, NGAY_TAO,
            NGUOI_SUA, NGAY_SUA, DAN_SO, MA_CU, CAP, CHA_ID
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    return len(rows)


def load_dm_quoc_tich(conn: Any, csv_path: Path) -> int:
    cur = conn.cursor()
    cur.execute("DELETE FROM VSS_DM_QUOC_TICH")
    rows = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(
                (
                    to_int(r.get("ID")),
                    to_str(r.get("MA")) or "UNK",
                    to_str(r.get("TEN")) or "Chưa rõ",
                    to_int(r.get("TRANG_THAI")) if to_int(r.get("TRANG_THAI")) is not None else 1,
                    to_str(r.get("TEN_TIENG_ANH")),
                )
            )

    cur.executemany(
        """
        INSERT INTO VSS_DM_QUOC_TICH (
            ID, MA, TEN, TRANG_THAI, TEN_TIENG_ANH
        ) VALUES (?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    return len(rows)


def load_dm_dbhc(conn: Any, csv_path: Path, inject_moho: bool = True) -> int:
    cur = conn.cursor()
    cur.execute("DELETE FROM VSS_DM_DBHC")
    rows = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            ma_dbhc = to_str(r.get("MA_DBHC"))
            ma_cha = to_str(r.get("MA_CHA"))
            is_cap_xa = to_int(r.get("IS_CAP_XA"))
            tinh_trang = to_str(r.get("TINH_TRANG"))
            ngay_het_hl = to_str(r.get("NGAY_HET_HIEU_LUC"))
            ma_dbhc_sn = to_str(r.get("MA_DBHC_SN"))

            # Bổ sung có kiểm soát 2 mã xã cũ bị tách đôi ('00999' cùng tỉnh '01', '00998' khác tỉnh '01' & '33')
            if inject_moho and tinh_trang == "1" and is_cap_xa == 1 and ngay_het_hl and ngay_het_hl.startswith("2099-12-31"):
                if ma_dbhc == "00004" and ma_cha == "01":
                    ma_dbhc_sn = f"{ma_dbhc_sn};00999;00998" if ma_dbhc_sn else "00999;00998"
                elif ma_dbhc == "00008" and ma_cha == "01":
                    ma_dbhc_sn = f"{ma_dbhc_sn};00999" if ma_dbhc_sn else "00999"
                elif ma_dbhc == "11953" and ma_cha == "33":
                    ma_dbhc_sn = f"{ma_dbhc_sn};00998" if ma_dbhc_sn else "00998"

            rows.append(
                (
                    to_int(r.get("ID")),
                    ma_dbhc,
                    ma_cha,
                    to_str(r.get("TEN")),
                    tinh_trang,
                    to_str(r.get("MA_TINH")),
                    to_str(r.get("MA_HUYEN")),
                    to_str(r.get("MA_XA")),
                    to_str(r.get("MA_VUNG")),
                    to_str(r.get("MA_DBHC_THUE")),
                    to_str(r.get("CAP")),
                    to_int(r.get("CQBHXH_ID")),
                    to_str(r.get("NGAY_HIEU_LUC")),
                    ngay_het_hl,
                    to_str(r.get("NGAY_SUA")),
                    to_str(r.get("MA_DBHC_CU")),
                    to_str(r.get("TIEU_VUNG_KT")),
                    to_str(r.get("VUNG_KT_TRONG_DIEM")),
                    to_str(r.get("VUNG_SINH_THAI")),
                    is_cap_xa,
                    to_int(r.get("IS_LT")),
                    ma_dbhc_sn,
                    to_int(r.get("DM_VUNG_ID")),
                    to_int(r.get("STATUS")) if to_int(r.get("STATUS")) is not None else 1,
                    to_str(r.get("CREATED_DATE")) or to_str(r.get("NGAY_HIEU_LUC")),
                )
            )

    cur.executemany(
        """
        INSERT INTO VSS_DM_DBHC (
            ID, MA_DBHC, MA_CHA, TEN, TINH_TRANG,
            MA_TINH, MA_HUYEN, MA_XA, MA_VUNG, MA_DBHC_THUE,
            CAP, CQBHXH_ID, NGAY_HIEU_LUC, NGAY_HET_HIEU_LUC, NGAY_SUA,
            MA_DBHC_CU, TIEU_VUNG_KT, VUNG_KT_TRONG_DIEM, VUNG_SINH_THAI,
            IS_CAP_XA, IS_LT, MA_DBHC_SN, DM_VUNG_ID, STATUS, CREATED_DATE
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    return len(rows)


def run_load_danhmuc(
    danhmuc_dir: Optional[Path] = None,
    sqlite_path: Optional[Path] = None,
    inject_moho: bool = True,
) -> dict:
    dm_dir = Path(danhmuc_dir) if danhmuc_dir else PROJECT_ROOT / "data" / "danhmuc"
    conn = get_connection(sqlite_path=sqlite_path)
    engine = get_db_engine()
    init_source_and_mirror_schema(conn, engine=engine)

    dt_csv = next(dm_dir.glob("DM_DAN_TOC*.csv"))
    qt_csv = next(dm_dir.glob("DM_QUOC_TICH*.csv"))
    dbhc_csv = next(dm_dir.glob("DM_DBHC*.csv"))

    n_dt = load_dm_dantoc(conn, dt_csv)
    n_qt = load_dm_quoc_tich(conn, qt_csv)
    n_dbhc = load_dm_dbhc(conn, dbhc_csv, inject_moho=inject_moho)

    # Thống kê nhanh dữ liệu hiệu lực
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*) FROM VSS_DM_DBHC
        WHERE TINH_TRANG = '1' AND IS_CAP_XA = 0 AND IS_LT = 1
          AND NGAY_HET_HIEU_LUC LIKE '2099-12-31%'
        """
    )
    active_provinces = cur.fetchone()[0]

    cur.execute(
        """
        SELECT COUNT(*) FROM VSS_DM_DBHC
        WHERE TINH_TRANG = '1' AND IS_CAP_XA = 1
          AND NGAY_HET_HIEU_LUC LIKE '2099-12-31%'
        """
    )
    active_wards = cur.fetchone()[0]

    conn.close()

    summary = {
        "VSS_DM_DANTOC": n_dt,
        "VSS_DM_QUOC_TICH": n_qt,
        "VSS_DM_DBHC": n_dbhc,
        "active_provinces_2025": active_provinces,
        "active_wards_2025": active_wards,
        "inject_moho": inject_moho,
    }
    print(
        f"[LOAD_DANHMUC] Hoàn tất nạp 3 bảng Danh mục nguồn:\n"
        f"  - VSS_DM_DANTOC     : {n_dt:,} dòng\n"
        f"  - VSS_DM_QUOC_TICH  : {n_qt:,} dòng\n"
        f"  - VSS_DM_DBHC       : {n_dbhc:,} dòng "
        f"(Tỉnh hiệu lực 2025: {active_provinces}, Xã hiệu lực 2025: {active_wards:,}, Inject MO_HO: {inject_moho})"
    )
    return summary


def main():
    parser = argparse.ArgumentParser(description="Nạp 3 bảng Danh mục CSV vào VSS_DM_*")
    parser.add_argument("--danhmuc-dir", type=str, default=None, help="Thư mục chứa 3 file CSV danh mục")
    parser.add_argument("--db-path", type=str, default=None, help="Đường dẫn file SQLite DB")
    parser.add_argument(
        "--no-inject-moho",
        action="store_true",
        help="Tắt tiêm 2 mã xã cũ bị tách đôi ('00999', '00998')",
    )
    args = parser.parse_args()

    run_load_danhmuc(
        danhmuc_dir=Path(args.danhmuc_dir) if args.danhmuc_dir else None,
        sqlite_path=Path(args.db_path) if args.db_path else None,
        inject_moho=not args.no_inject_moho,
    )


if __name__ == "__main__":
    main()
