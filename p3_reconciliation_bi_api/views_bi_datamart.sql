-- ==============================================================================
-- DDL DATA MART VIEWS PHỤC VỤ DASHBOARD BÁO CÁO BI (METABASE / SUPERSET / POWERBI)
-- Phân hệ: p3_reconciliation_bi_api (Tuần 3)
-- Lưu ý kỹ thuật:
--   - So khớp chính xác token cờ lỗi bằng ';' || COALESCE(DQ_FLAGS, '') || ';' LIKE '%;<FLAG>;%'
--     để không bao giờ bị trùng chuỗi con (ví dụ TINH_KHONG_MAP vs TINH_LH_KHONG_MAP).
--   - Tính độ tuổi linh hoạt theo năm hiện tại dựa trên cột NAM_SINH đã chuẩn hóa.
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- VIEW 1: THỐNG KÊ NHÂN KHẨU & ĐỊNH DANH THEO ĐỊA BÀN MỚI 2025
-- ------------------------------------------------------------------------------
DROP VIEW IF EXISTS V_BI_THONG_KE_DIA_BAN;
CREATE VIEW V_BI_THONG_KE_DIA_BAN AS
SELECT
    COALESCE(MA_TINH_THUONG_TRU, 'CHUA_XAC_DINH') AS MA_TINH,
    COALESCE(TEN_TINH_THUONG_TRU, 'Chưa xác định') AS TEN_TINH,
    COALESCE(MA_XA_THUONG_TRU, 'CHUA_XAC_DINH') AS MA_XA,
    COALESCE(TEN_XA_THUONG_TRU, 'Chưa xác định') AS TEN_XA,
    COUNT(*) AS TONG_SO_CONG_DAN,
    SUM(CASE WHEN GIOI_TINH = 1 THEN 1 ELSE 0 END) AS SO_NAM,
    SUM(CASE WHEN GIOI_TINH = 2 THEN 1 ELSE 0 END) AS SO_NU,
    SUM(CASE WHEN IS_ALIVE = 1 THEN 1 ELSE 0 END) AS CON_SONG,
    SUM(CASE WHEN IS_ALIVE = 0 THEN 1 ELSE 0 END) AS DA_MAT,
    -- Phân bổ độ tuổi theo năm hiện tại (dựa trên cột NAM_SINH chuẩn hóa)
    SUM(CASE
        WHEN NAM_SINH IS NOT NULL
         AND (CAST(STRFTIME('%Y', 'now', 'localtime') AS INT) - NAM_SINH) < 18
        THEN 1 ELSE 0
    END) AS DUOI_18_TUOI,
    SUM(CASE
        WHEN NAM_SINH IS NOT NULL
         AND (CAST(STRFTIME('%Y', 'now', 'localtime') AS INT) - NAM_SINH) BETWEEN 18 AND 35
        THEN 1 ELSE 0
    END) AS TU_18_DEN_35,
    SUM(CASE
        WHEN NAM_SINH IS NOT NULL
         AND (CAST(STRFTIME('%Y', 'now', 'localtime') AS INT) - NAM_SINH) BETWEEN 36 AND 60
        THEN 1 ELSE 0
    END) AS TU_36_DEN_60,
    SUM(CASE
        WHEN NAM_SINH IS NOT NULL
         AND (CAST(STRFTIME('%Y', 'now', 'localtime') AS INT) - NAM_SINH) > 60
        THEN 1 ELSE 0
    END) AS TREN_60_TUOI,
    SUM(CASE WHEN NAM_SINH IS NULL THEN 1 ELSE 0 END) AS KHONG_RO_TUOI,
    -- Phân loại mức độ phủ Số định danh / CCCD 12 số
    SUM(CASE WHEN SO_DINH_DANH IS NOT NULL THEN 1 ELSE 0 END) AS CO_CCCD_12_SO,
    SUM(CASE WHEN ';' || COALESCE(DQ_FLAGS, '') || ';' LIKE '%;CANH_BAO_CMND9;%' THEN 1 ELSE 0 END) AS CHI_CO_CMND_9_SO,
    SUM(CASE
        WHEN SO_DINH_DANH IS NULL
         AND ';' || COALESCE(DQ_FLAGS, '') || ';' NOT LIKE '%;CANH_BAO_CMND9;%'
        THEN 1 ELSE 0
    END) AS KHONG_DINH_DANH,
    ROUND(100.0 * SUM(CASE WHEN SO_DINH_DANH IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS TY_LE_PHU_CCCD_PCT
FROM C360_THONG_TIN_CA_NHAN
GROUP BY
    COALESCE(MA_TINH_THUONG_TRU, 'CHUA_XAC_DINH'),
    COALESCE(TEN_TINH_THUONG_TRU, 'Chưa xác định'),
    COALESCE(MA_XA_THUONG_TRU, 'CHUA_XAC_DINH'),
    COALESCE(TEN_XA_THUONG_TRU, 'Chưa xác định');

-- ------------------------------------------------------------------------------
-- VIEW 2: BÁO CÁO CHẤT LƯỢNG DỮ LIỆU DATA QUALITY (THEO TỈNH & MÃ CỜ DQ_FLAGS)
-- ------------------------------------------------------------------------------
DROP VIEW IF EXISTS V_BI_CHAT_LUONG_DU_LIEU;
CREATE VIEW V_BI_CHAT_LUONG_DU_LIEU AS
SELECT
    COALESCE(MA_TINH_THUONG_TRU, 'CHUA_XAC_DINH') AS MA_TINH,
    COALESCE(TEN_TINH_THUONG_TRU, 'Chưa xác định') AS TEN_TINH,
    COUNT(*) AS TONG_SO_HO_SO,
    SUM(CASE WHEN DQ_FLAGS IS NULL THEN 1 ELSE 0 END) AS SO_HO_SO_SACH,
    SUM(CASE WHEN DQ_FLAGS IS NOT NULL THEN 1 ELSE 0 END) AS SO_HO_SO_CO_LOI,
    ROUND(100.0 * SUM(CASE WHEN DQ_FLAGS IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS TY_LE_SACH_PCT,
    ROUND(100.0 * SUM(CASE WHEN DQ_FLAGS IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS TY_LE_LOI_PCT,
    -- 11 Mã cờ lỗi chính (so khớp chính xác từng token giữa dấu ';')
    SUM(CASE WHEN ';' || COALESCE(DQ_FLAGS, '') || ';' LIKE '%;TINH_KHONG_MAP;%' THEN 1 ELSE 0 END) AS LOI_TINH_KHONG_MAP,
    SUM(CASE WHEN ';' || COALESCE(DQ_FLAGS, '') || ';' LIKE '%;XA_KHONG_MAP;%' THEN 1 ELSE 0 END) AS LOI_XA_KHONG_MAP,
    SUM(CASE WHEN ';' || COALESCE(DQ_FLAGS, '') || ';' LIKE '%;XA_MO_HO;%' THEN 1 ELSE 0 END) AS LOI_XA_MO_HO,
    SUM(CASE WHEN ';' || COALESCE(DQ_FLAGS, '') || ';' LIKE '%;TINH_LECH_XA;%' THEN 1 ELSE 0 END) AS CANH_BAO_TINH_LECH_XA,
    SUM(CASE WHEN ';' || COALESCE(DQ_FLAGS, '') || ';' LIKE '%;NGAY_SINH_LOI;%' THEN 1 ELSE 0 END) AS LOI_NGAY_SINH,
    SUM(CASE WHEN ';' || COALESCE(DQ_FLAGS, '') || ';' LIKE '%;SDT_LOI;%' THEN 1 ELSE 0 END) AS LOI_SO_DIEN_THOAI,
    SUM(CASE WHEN ';' || COALESCE(DQ_FLAGS, '') || ';' LIKE '%;KHONG_SO_DINH_DANH;%' THEN 1 ELSE 0 END) AS THIEU_SO_DINH_DANH,
    SUM(CASE WHEN ';' || COALESCE(DQ_FLAGS, '') || ';' LIKE '%;CANH_BAO_CMND9;%' THEN 1 ELSE 0 END) AS CANH_BAO_CMND_9_SO,
    SUM(CASE WHEN ';' || COALESCE(DQ_FLAGS, '') || ';' LIKE '%;DAN_TOC_LOI;%' THEN 1 ELSE 0 END) AS LOI_DAN_TOC,
    SUM(CASE WHEN ';' || COALESCE(DQ_FLAGS, '') || ';' LIKE '%;QUOC_TICH_LOI;%' THEN 1 ELSE 0 END) AS LOI_QUOC_TICH,
    SUM(CASE WHEN ';' || COALESCE(DQ_FLAGS, '') || ';' LIKE '%;HOTEN_THUA_KHOANG_TRANG;%' THEN 1 ELSE 0 END) AS THUA_KHOANG_TRANG_HO_TEN,
    -- Nhóm cờ lỗi địa chỉ phụ (Liên hệ _LH & Khai sinh _KS - CHK-05)
    SUM(CASE
        WHEN DQ_FLAGS LIKE '%_LH_%' OR DQ_FLAGS LIKE '%_KS_%'
        THEN 1 ELSE 0
    END) AS LOI_DIA_CHI_LH_KS
FROM C360_THONG_TIN_CA_NHAN
GROUP BY
    COALESCE(MA_TINH_THUONG_TRU, 'CHUA_XAC_DINH'),
    COALESCE(TEN_TINH_THUONG_TRU, 'Chưa xác định');

-- ------------------------------------------------------------------------------
-- VIEW 3: GIÁM SÁT VẬN HÀNH ĐỒNG BỘ NEAR REAL-TIME (SYNC MONITORING)
-- ------------------------------------------------------------------------------
DROP VIEW IF EXISTS V_BI_GIAM_SAT_DONG_BO;
CREATE VIEW V_BI_GIAM_SAT_DONG_BO AS
SELECT
    LOG_ID,
    BATCH_TYPE,
    START_TIME,
    END_TIME,
    AFFECTED_KEYS AS SO_KHOA_BIEN_DONG,
    MERGE_COUNT AS SO_DONG_MERGE,
    DELETE_COUNT AS SO_DONG_DELETE,
    STATUS,
    ERROR_MSG,
    CASE
        WHEN END_TIME IS NOT NULL AND START_TIME IS NOT NULL
        THEN ROUND((JULIANDAY(END_TIME) - JULIANDAY(START_TIME)) * 86400, 2)
        ELSE NULL
    END AS THOI_GIAN_XU_LY_GIAY
FROM C360_SYNC_LOG
ORDER BY LOG_ID DESC;
