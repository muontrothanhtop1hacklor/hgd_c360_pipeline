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
    ("NGAY_SINH", "INTEGER"),
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
    conn.execute("PRAGMA busy_timeout=60000;")
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
                CAN_CU TEXT,
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
        # Bổ sung cột CAN_CU nếu DB cũ chưa có
        existing_dt_cols = {r[1] for r in cur.execute(f"PRAGMA table_info({prefix}DM_DANTOC)").fetchall()}
        if "CAN_CU" not in existing_dt_cols:
            cur.execute(f"ALTER TABLE {prefix}DM_DANTOC ADD COLUMN CAN_CU TEXT;")

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

    # Indexes trên Nguồn (Mirror chưa đánh Index lúc tạo bảng rỗng để tối ưu tốc độ Full Load;
    # Unique Index UK_MIR_NK chỉ được tạo sau bước Full Load qua ensure_mirror_indexes)
    cur.execute("CREATE INDEX IF NOT EXISTS IDX_VSS_NK_MACD ON VSS_HO_GD_NHANKHAU (MA_CD);")
    cur.execute("CREATE INDEX IF NOT EXISTS IDX_VSS_NK_MATINH ON VSS_HO_GD_NHANKHAU (MA_TINH);")

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

    # 6. Bảng lưu Checkpoint theo từng partition khi chạy Full Load (hỗ trợ Resume khi lỗi)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS MIR_FULL_LOAD_CHECKPOINT (
            TABLE_NAME TEXT NOT NULL,
            PARTITION_ID TEXT NOT NULL,
            LOWER_BOUND TEXT,
            UPPER_BOUND TEXT,
            ROWS_LOADED INTEGER DEFAULT 0,
            STATUS TEXT NOT NULL,
            UPDATED_AT TEXT NOT NULL,
            PRIMARY KEY (TABLE_NAME, PARTITION_ID)
        );
        """
    )

    conn.commit()


def ensure_mirror_indexes(conn: Any, engine: str = "LOCAL_SQLITE") -> None:
    """Tạo Unique Index UK_MIR_NK và các Index phụ trợ trên Mirror sau khi Full Load xong."""
    if engine == "ORACLE":
        return
    cur = conn.cursor()
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS UK_MIR_NK ON MIR_VSS_HO_GD_NHANKHAU (MA_CD, MA_HO_GD);")
    cur.execute(
        "CREATE INDEX IF NOT EXISTS IDX_MIR_NK_MOD "
        "ON MIR_VSS_HO_GD_NHANKHAU (MA_CD, NGAY_SUA DESC, NGAY_TAO DESC, ID DESC);"
    )
    cur.execute("CREATE INDEX IF NOT EXISTS IDX_MIR_NK_MATINH ON MIR_VSS_HO_GD_NHANKHAU (MA_TINH);")
    conn.commit()


def init_c360_schema(conn: Any, engine: str = "LOCAL_SQLITE") -> None:
    """Khởi tạo đầy đủ View danh mục, 2 bảng Map DBHC, bảng C360, Queue, Control, Log & Triggers (Tuần 2)."""
    if engine == "ORACLE":
        return

    cur = conn.cursor()

    # 0. View danh mục Địa bàn hành chính đang hiệu lực (sau sáp nhập 2025)
    cur.execute(
        """
        CREATE VIEW IF NOT EXISTS V_MIR_DM_DBHC_HIEU_LUC AS
        SELECT ID, MA_DBHC, MA_CHA, TEN, TINH_TRANG, MA_TINH, MA_HUYEN, MA_XA,
               CAP, NGAY_HIEU_LUC, NGAY_HET_HIEU_LUC, MA_DBHC_CU, IS_CAP_XA, IS_LT, MA_DBHC_SN
          FROM MIR_VSS_DM_DBHC
         WHERE TINH_TRANG = '1'
           AND (NGAY_HET_HIEU_LUC IS NULL OR NGAY_HET_HIEU_LUC >= '2026-01-01');
        """
    )

    # 1. Bảng Map Tỉnh
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS C360_MAP_DBHC_TINH (
            MA_TINH_LOOKUP TEXT PRIMARY KEY,
            MA_TINH_MOI TEXT NOT NULL,
            TEN_TINH_MOI TEXT NOT NULL,
            LOAI_MAP TEXT NOT NULL
        );
        """
    )

    # 2. Bảng Map Xã
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS C360_MAP_DBHC_XA (
            MA_XA_LOOKUP TEXT PRIMARY KEY,
            MA_XA_MOI TEXT,
            TEN_XA_MOI TEXT,
            MA_TINH_MOI TEXT,
            TEN_TINH_MOI TEXT,
            LOAI_MAP TEXT NOT NULL
        );
        """
    )

    # 3. Bảng Chuẩn hóa C360 (giữ đầy đủ các cột *_GOC để đối soát)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS C360_THONG_TIN_CA_NHAN (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            MA_SO_BHXH TEXT UNIQUE NOT NULL,
            MA_SO_BHXH_CU TEXT,
            SO_DINH_DANH TEXT,
            HO_TEN TEXT NOT NULL,
            HO_TEN_KHONG_DAU TEXT NOT NULL,
            GIOI_TINH INTEGER,
            NGAY_SINH_CHUAN TEXT,
            NAM_SINH INTEGER,
            NGAY_SINH_GOC TEXT,
            SO_CCCD TEXT,
            SO_CMTND TEXT,
            SO_DIEN_THOAI TEXT,
            SO_DIEN_THOAI_GOC TEXT,
            EMAIL TEXT,
            MA_TINH_THUONG_TRU TEXT,
            TEN_TINH_THUONG_TRU TEXT,
            MA_XA_THUONG_TRU TEXT,
            TEN_XA_THUONG_TRU TEXT,
            DIA_CHI_THUONG_TRU TEXT,
            MA_TINH_GOC TEXT,
            MA_XA_GOC TEXT,
            MA_TINH_LIEN_HE TEXT,
            TEN_TINH_LIEN_HE TEXT,
            MA_XA_LIEN_HE TEXT,
            TEN_XA_LIEN_HE TEXT,
            DIA_CHI_LIEN_HE TEXT,
            MA_TINH_LH_GOC TEXT,
            MA_XA_LH_GOC TEXT,
            MA_TINH_KHAI_SINH TEXT,
            TEN_TINH_KHAI_SINH TEXT,
            MA_XA_KHAI_SINH TEXT,
            TEN_XA_KHAI_SINH TEXT,
            DIA_CHI_KHAI_SINH TEXT,
            MA_TINH_KS_GOC TEXT,
            MA_XA_KS_GOC TEXT,
            DM_DAN_TOC_ID INTEGER,
            MA_DAN_TOC TEXT,
            TEN_DAN_TOC TEXT,
            QUOC_TICH_RAW TEXT,
            MA_QUOC_TICH TEXT,
            TEN_QUOC_TICH TEXT,
            IS_ALIVE INTEGER DEFAULT 1,
            DEAD_DATE TEXT,
            SOURCE_MA_HO_GD TEXT,
            SOURCE_MODIFY_DATE TEXT,
            SOURCE_CREATED_DATE TEXT,
            DQ_FLAGS TEXT,
            SYNC_DATE TEXT DEFAULT (datetime('now', 'localtime')),
            SYNC_BATCH_ID INTEGER
        );
        """
    )

    # Migration an toàn nếu DB cũ đã tạo bảng C360_THONG_TIN_CA_NHAN thiếu cột mới
    existing_c360_cols = {r[1] for r in cur.execute("PRAGMA table_info(C360_THONG_TIN_CA_NHAN)").fetchall()}
    extra_c360_cols = [
        ("NAM_SINH", "INTEGER"),
        ("SO_DIEN_THOAI_GOC", "TEXT"),
        ("MA_TINH_GOC", "TEXT"),
        ("MA_XA_GOC", "TEXT"),
        ("TEN_TINH_LIEN_HE", "TEXT"),
        ("TEN_XA_LIEN_HE", "TEXT"),
        ("MA_TINH_LH_GOC", "TEXT"),
        ("MA_XA_LH_GOC", "TEXT"),
        ("TEN_TINH_KHAI_SINH", "TEXT"),
        ("TEN_XA_KHAI_SINH", "TEXT"),
        ("MA_TINH_KS_GOC", "TEXT"),
        ("MA_XA_KS_GOC", "TEXT"),
    ]
    for col_name, col_type in extra_c360_cols:
        if col_name not in existing_c360_cols:
            cur.execute(f"ALTER TABLE C360_THONG_TIN_CA_NHAN ADD COLUMN {col_name} {col_type};")

    # Chỉ mục C360
    cur.execute("CREATE INDEX IF NOT EXISTS IDX_C360_MA_CU ON C360_THONG_TIN_CA_NHAN (MA_SO_BHXH_CU);")
    cur.execute("CREATE INDEX IF NOT EXISTS IDX_C360_SO_DINH_DANH ON C360_THONG_TIN_CA_NHAN (SO_DINH_DANH);")
    cur.execute("CREATE INDEX IF NOT EXISTS IDX_C360_TIM_KIEM ON C360_THONG_TIN_CA_NHAN (HO_TEN_KHONG_DAU, NGAY_SINH_CHUAN);")
    cur.execute("CREATE INDEX IF NOT EXISTS IDX_C360_THAY_DOI ON C360_THONG_TIN_CA_NHAN (SYNC_DATE, ID);")
    cur.execute("CREATE INDEX IF NOT EXISTS IDX_C360_TINH ON C360_THONG_TIN_CA_NHAN (MA_TINH_THUONG_TRU);")

    # 4. Bảng Hàng đợi Queue
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS C360_SYNC_QUEUE (
            QUEUE_ID INTEGER PRIMARY KEY AUTOINCREMENT,
            MA_CD TEXT NOT NULL,
            ACTION TEXT DEFAULT 'UPSERT',
            ENQUEUE_TIME TEXT DEFAULT (datetime('now', 'localtime')),
            STATUS TEXT DEFAULT 'PENDING'
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS IDX_QUEUE_STATUS ON C360_SYNC_QUEUE (STATUS, ENQUEUE_TIME);")

    # 5. Bảng Điều khiển Đồng bộ Sync Control
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS C360_SYNC_CONTROL (
            PROCESS_NAME TEXT PRIMARY KEY,
            PAUSE_NRT INTEGER DEFAULT 0,
            LAST_FULL_LOAD_TIME TEXT,
            LAST_NRT_RUN TEXT,
            LAST_NRT_STATUS TEXT DEFAULT 'READY'
        );
        """
    )
    cur.execute(
        """
        INSERT OR IGNORE INTO C360_SYNC_CONTROL (PROCESS_NAME, PAUSE_NRT, LAST_NRT_STATUS)
        VALUES ('C360_SYNC', 0, 'READY');
        """
    )

    # 6. Bảng Nhật ký Đồng bộ Sync Log
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS C360_SYNC_LOG (
            LOG_ID INTEGER PRIMARY KEY AUTOINCREMENT,
            BATCH_TYPE TEXT NOT NULL,
            START_TIME TEXT NOT NULL,
            END_TIME TEXT,
            AFFECTED_KEYS INTEGER DEFAULT 0,
            MERGE_COUNT INTEGER DEFAULT 0,
            DELETE_COUNT INTEGER DEFAULT 0,
            STATUS TEXT NOT NULL,
            ERROR_MSG TEXT
        );
        """
    )

    # 7. Trigger trên bảng Mirror để đẩy sự kiện vào Queue (Near Real-time CDC -> C360)
    cur.execute(
        """
        CREATE TRIGGER IF NOT EXISTS TRG_MIR_NK_QUEUE_INS
        AFTER INSERT ON MIR_VSS_HO_GD_NHANKHAU
        FOR EACH ROW
        BEGIN
            INSERT INTO C360_SYNC_QUEUE (MA_CD, ACTION) VALUES (NEW.MA_CD, 'INSERT');
        END;
        """
    )

    cur.execute(
        """
        CREATE TRIGGER IF NOT EXISTS TRG_MIR_NK_QUEUE_DEL
        AFTER DELETE ON MIR_VSS_HO_GD_NHANKHAU
        FOR EACH ROW
        BEGIN
            INSERT INTO C360_SYNC_QUEUE (MA_CD, ACTION) VALUES (OLD.MA_CD, 'DELETE');
        END;
        """
    )

    cur.execute(
        """
        CREATE TRIGGER IF NOT EXISTS TRG_MIR_NK_QUEUE_UPD
        AFTER UPDATE ON MIR_VSS_HO_GD_NHANKHAU
        FOR EACH ROW
        BEGIN
            INSERT INTO C360_SYNC_QUEUE (MA_CD, ACTION) VALUES (NEW.MA_CD, 'UPDATE');
            INSERT INTO C360_SYNC_QUEUE (MA_CD, ACTION)
            SELECT OLD.MA_CD, 'UPDATE_OLD'
            WHERE OLD.MA_CD <> NEW.MA_CD;
        END;
        """
    )

    ensure_mirror_indexes(conn, engine=engine)
    conn.commit()

