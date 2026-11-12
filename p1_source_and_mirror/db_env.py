#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Module quản lý kết nối Database (Dual-Engine: Oracle DB & Local SQLite)
cho dự án HGD -> Mirror -> C360 -> BI & API.
- Khi DB_ENGINE=ORACLE: Kết nối trực tiếp tới Oracle 19c/21c qua thư viện python-oracledb.
- Khi DB_ENGINE=LOCAL_SQLITE (mặc định khi máy chủ chưa bật container Oracle):
  Sử dụng SQLite WAL mode tốc độ cao tại data/hgd_c360_local.db, mô phỏng đầy đủ
  2 vùng bảng Nguồn (VSS_*) và Mirror (MIR_VSS_*), bao gồm cả Unique Index UK_MIR_NK (MA_CD, MA_HO_GD).
"""

import os
import sqlite3
import sys
from pathlib import Path
from typing import Any, Optional

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SQLITE_PATH = PROJECT_ROOT / "data" / "hgd_c360_local.db"


NHANKHAU_COLUMNS = [
    ("ID", "INTEGER PRIMARY KEY"),
    ("MA_HO_GD", "TEXT"),
    ("MA_CD", "TEXT"),
    ("HO_TEN", "TEXT"),
    ("SO_SO_BHXH", "TEXT"),
    ("GIOI_TINH", "INTEGER"),
    ("NGAY_SINH", "TEXT"),
    ("QUOC_TICH", "TEXT"),
    ("DM_DAN_TOC_ID", "INTEGER"),
    ("SO_CMTND", "TEXT"),
    ("DIA_CHI", "TEXT"),
    ("MA_TINH_KS", "TEXT"),
    ("MA_HUYEN_KS", "TEXT"),
    ("MA_XA_KS", "TEXT"),
    ("MA_TINH_LH", "TEXT"),
    ("MA_HUYEN_LH", "TEXT"),
    ("MA_XA_LH", "TEXT"),
    ("SO_DIEN_THOAI", "TEXT"),
    ("EMAIL", "TEXT"),
    ("MA_VUNG_SS", "INTEGER"),
    ("MQH_CHU_HO", "TEXT"),
    ("TRANG_THAI", "INTEGER"),
    ("MA_HUYEN", "TEXT"),
    ("MA_XA", "TEXT"),
    ("MA_THON", "TEXT"),
    ("GHI_CHU", "TEXT"),
    ("NGUOI_TAO", "TEXT"),
    ("NGAY_TAO", "TEXT"),
    ("NGUOI_SUA", "TEXT"),
    ("NGAY_SUA", "TEXT"),
    ("SO_THE_BHYT", "TEXT"),
    ("MA_DVI", "TEXT"),
    ("MA_DT", "TEXT"),
    ("MUC_TIEN_DONG", "REAL"),
    ("PHUONG_THUC_DONG", "TEXT"),
    ("MA_KCB_TINH", "TEXT"),
    ("MA_KCB_BV", "TEXT"),
    ("TU_NGAY", "TEXT"),
    ("SO_THANG", "INTEGER"),
    ("TY_LE_NSNN", "REAL"),
    ("TY_LE_NSDP", "REAL"),
    ("TY_LE_HTRO_KHAC", "REAL"),
    ("TO_CHUC_HTRO", "TEXT"),
    ("CMND_REL", "INTEGER"),
    ("NOI_CAP_GKS", "TEXT"),
    ("TINH_TRANG_SONG", "INTEGER"),
    ("NGAY_CHET", "TEXT"),
    ("MA_TINH", "TEXT"),
    ("NGAY_SINH_DT", "TEXT"),
    ("MA_CQBHXH", "TEXT"),
    ("MA_XA_HK", "TEXT"),
    ("MA_HUYEN_HK", "TEXT"),
    ("MA_TINH_HK", "TEXT"),
    ("DIA_CHI_HK", "TEXT"),
    ("MA_THON_HK", "TEXT"),
    ("MA_THON_LH", "TEXT"),
    ("DIA_CHI_KS", "TEXT"),
    ("DIA_CHI_LH", "TEXT"),
    ("IS_SYN", "INTEGER"),
    ("TRANG_THAI_DUYET", "INTEGER"),
    ("NGAY_DUYET", "TEXT"),
    ("MA_DVI_IMP", "TEXT"),
    ("MA_PB", "TEXT"),
    ("TYPEID", "TEXT"),
    ("IMPORT_ID", "INTEGER"),
    ("MA_HUYEN_CU", "TEXT"),
    ("MA_HUYEN_HK_CU", "TEXT"),
    ("MA_HUYEN_KS_CU", "TEXT"),
    ("MA_HUYEN_LH_CU", "TEXT"),
    ("MA_THON_CU", "TEXT"),
    ("MA_THON_HK_CU", "TEXT"),
    ("MA_THON_LH_CU", "TEXT"),
    ("MA_TINH_CU", "TEXT"),
    ("MA_TINH_HK_CU", "TEXT"),
    ("MA_TINH_KS_CU", "TEXT"),
    ("MA_TINH_LH_CU", "TEXT"),
    ("MA_XA_CU", "TEXT"),
    ("MA_XA_HK_CU", "TEXT"),
    ("MA_XA_KS_CU", "TEXT"),
    ("MA_XA_LH_CU", "TEXT"),
    ("NOI_CAP_CMND", "TEXT"),
    ("NGAY_CAP_CMND", "TEXT"),
    ("MA_CQBHXH_CAP", "TEXT"),
    ("HO_TEN_KD", "TEXT"),
    ("IS_SEND", "INTEGER"),
    ("IS_UPDATE_CSKCB", "INTEGER"),
    ("IS_UPDATE_CHU_HO", "INTEGER"),
    ("NGUON_PS", "INTEGER"),
    ("SO_CCCD", "TEXT"),
    ("TEN_KD", "TEXT"),
    ("SYNC_STATUS", "INTEGER"),
    ("SYNC_MSG", "TEXT"),
    ("PB_VSSID", "TEXT"),
    ("SO_NAM_LT", "INTEGER"),
    ("NGAY_5NAM_LT", "TEXT"),
    ("LOAI_GT", "TEXT"),
    ("MA_CD_OLD", "TEXT"),
    ("SYNC_TIME", "TEXT"),
    ("PHONE_NUMBER", "TEXT"),
    ("XA_PA04", "TEXT"),
    ("XA_PA04_CHECK", "INTEGER"),
    ("MA_DT_GOC", "TEXT"),
    ("TU_NGAY_GOC", "TEXT"),
    ("DEN_NGAY_GOC", "TEXT"),
    ("MA_THE_CU", "TEXT"),
    ("DEN_NGAY", "TEXT"),
    ("NGAY_CAP_THE", "TEXT"),
    ("MA_KV", "TEXT"),
    ("MA_TV_HGD", "TEXT"),
    ("CREATE_TYPE", "INTEGER"),
    ("USER_TYPE", "INTEGER"),
]


def get_db_engine(override_engine: Optional[str] = None) -> str:
    if override_engine:
        return override_engine.upper()
    return os.environ.get("DB_ENGINE", "LOCAL_SQLITE").upper()


def get_connection(
    sqlite_path: Optional[Path] = None,
    override_engine: Optional[str] = None,
) -> Any:
    """Trả về kết nối DB (SQLite hoặc Oracle) tùy theo cấu hình."""
    engine = get_db_engine(override_engine)
    if engine == "ORACLE":
        import oracledb

        dsn = os.environ.get("ORACLE_DSN", "localhost:1521/FREEPDB1")
        user = os.environ.get("ORACLE_USER", "VSS")
        password = os.environ.get("ORACLE_PASSWORD", "vss123")
        return oracledb.connect(user=user, password=password, dsn=dsn)

    db_file = Path(sqlite_path) if sqlite_path else Path(os.environ.get("SQLITE_DB_PATH", DEFAULT_SQLITE_PATH))
    db_file.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_file), timeout=60.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA cache_size=-64000;")
    return conn


def init_source_and_mirror_schema(conn: Any, engine: str = "LOCAL_SQLITE") -> None:
    """Khởi tạo đầy đủ 4 bảng Nguồn (VSS_*) và 4 bảng Mirror (MIR_VSS_*)."""
    if engine == "ORACLE":
        return

    cur = conn.cursor()

    # 1. Bảng Danh mục Nguồn & Mirror: DM_DANTOC
    for prefix in ("VSS_", "MIR_VSS_"):
        extra_col = ", MIRROR_SYNC_TS TEXT DEFAULT (datetime('now', 'localtime'))" if prefix == "MIR_VSS_" else ""
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {prefix}DM_DANTOC (
                ID INTEGER PRIMARY KEY,
                MA TEXT,
                TEN TEXT NOT NULL,
                TEN_GOI_KHAC TEXT,
                DIA_BAN_CU_TRU TEXT,
                NGAY_HL TEXT,
                NGAY_HET_HL TEXT,
                TRANG_THAI INTEGER,
                NGUOI_TAO INTEGER,
                NGAY_TAO TEXT,
                NGUOI_SUA INTEGER,
                NGAY_SUA TEXT,
                DAN_SO INTEGER,
                MA_CU TEXT,
                CAP INTEGER,
                CHA_ID INTEGER
                {extra_col}
            );
            """
        )

        # 2. Bảng Danh mục Nguồn & Mirror: DM_QUOC_TICH
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {prefix}DM_QUOC_TICH (
                ID INTEGER PRIMARY KEY,
                MA TEXT NOT NULL,
                TEN TEXT NOT NULL,
                TRANG_THAI INTEGER NOT NULL,
                TEN_TIENG_ANH TEXT
                {extra_col}
            );
            """
        )

        # 3. Bảng Danh mục Nguồn & Mirror: DM_DBHC
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {prefix}DM_DBHC (
                ID INTEGER PRIMARY KEY,
                MA_DBHC TEXT,
                MA_CHA TEXT,
                TEN TEXT,
                TINH_TRANG TEXT,
                MA_TINH TEXT,
                MA_HUYEN TEXT,
                MA_XA TEXT,
                MA_VUNG TEXT,
                MA_DBHC_THUE TEXT,
                CAP TEXT,
                CQBHXH_ID INTEGER,
                NGAY_HIEU_LUC TEXT,
                NGAY_HET_HIEU_LUC TEXT,
                NGAY_SUA TEXT,
                MA_DBHC_CU TEXT,
                TIEU_VUNG_KT TEXT,
                VUNG_KT_TRONG_DIEM TEXT,
                VUNG_SINH_THAI TEXT,
                IS_CAP_XA INTEGER,
                IS_LT INTEGER,
                MA_DBHC_SN TEXT,
                DM_VUNG_ID INTEGER,
                STATUS INTEGER,
                CREATED_DATE TEXT
                {extra_col}
            );
            """
        )

    # 4. Bảng Nhân khẩu Nguồn (VSS_HO_GD_NHANKHAU) & Mirror (MIR_VSS_HO_GD_NHANKHAU)
    cols_ddl = ",\n                ".join(f"{col} {dtype}" for col, dtype in NHANKHAU_COLUMNS)
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS VSS_HO_GD_NHANKHAU (
            {cols_ddl},
            CONSTRAINT UK_VSS_NK_MACD_MAHOGD UNIQUE (MA_CD, MA_HO_GD)
        );
        """
    )

    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS MIR_VSS_HO_GD_NHANKHAU (
            {cols_ddl},
            MIRROR_SYNC_TS TEXT DEFAULT (datetime('now', 'localtime'))
        );
        """
    )

    # Indexes trên Nguồn & Mirror
    cur.execute("CREATE INDEX IF NOT EXISTS IDX_VSS_NK_MACD ON VSS_HO_GD_NHANKHAU (MA_CD);")
    cur.execute("CREATE INDEX IF NOT EXISTS IDX_VSS_NK_MATINH ON VSS_HO_GD_NHANKHAU (MA_TINH);")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS UK_MIR_NK ON MIR_VSS_HO_GD_NHANKHAU (MA_CD, MA_HO_GD);")
    cur.execute("CREATE INDEX IF NOT EXISTS IDX_MIR_NK_MATINH ON MIR_VSS_HO_GD_NHANKHAU (MA_TINH);")

    # 5. Bảng lưu trạng thái SCN / Watermark bàn giao giữa Full Load và CDC
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS MIR_CDC_SCN_HANDOFF (
            TABLE_NAME TEXT PRIMARY KEY,
            SNAPSHOT_SCN INTEGER NOT NULL,
            SNAPSHOT_TIME TEXT NOT NULL,
            SOURCE_ROW_COUNT INTEGER NOT NULL,
            MIRROR_ROW_COUNT INTEGER NOT NULL,
            STATUS TEXT NOT NULL
        );
        """
    )

    conn.commit()
