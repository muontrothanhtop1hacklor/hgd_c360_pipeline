-- ==============================================================================
-- DDL DATA MART VIEWS PHỤC VỤ DASHBOARD BÁO CÁO BI (SUPERSET / METABASE / LIVE UI)
-- Phân hệ: p3_reconciliation_bi_api (Tuần 3)
-- Bao phủ đầy đủ 5 Views cho 3 nhóm báo cáo & toàn bộ 6 biểu đồ "Thông tin cá nhân - Gold":
--   1. V_BI_THONG_KE_DIA_BAN          : Dân số, độ tuổi theo giới tính (Nam/Nữ), độ phủ CCCD 12 số
--   2. V_BI_CHAT_LUONG_DU_LIEU        : Chi tiết 11 cờ DQ_FLAGS Thường trú + cờ Liên hệ (_LH)
--   3. V_BI_GIAM_SAT_DONG_BO          : Lịch sử các lô chạy Full Load & Near Real-time
--   4. V_BI_NHOM_DAN_TOC              : Tỷ trọng Kinh / Dân tộc thiểu số / Chưa xác định & Top dân tộc
--   5. V_BI_TINH_THUONG_TRU_LIEN_HE   : Đối sánh phân bổ Tỉnh thường trú vs Tỉnh liên hệ
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
    -- Tổng hợp nhóm tuổi
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
    -- Phân tách Nam / Nữ theo từng nhóm tuổi (phục vụ biểu đồ cột đôi giống Superset)
    SUM(CASE WHEN NAM_SINH IS NULL AND GIOI_TINH = 1 THEN 1 ELSE 0 END) AS NAM_KHONG_RO_TUOI,
    SUM(CASE WHEN NAM_SINH IS NULL AND GIOI_TINH = 2 THEN 1 ELSE 0 END) AS NU_KHONG_RO_TUOI,
    SUM(CASE WHEN NAM_SINH IS NOT NULL AND (CAST(STRFTIME('%Y', 'now', 'localtime') AS INT) - NAM_SINH) < 18 AND GIOI_TINH = 1 THEN 1 ELSE 0 END) AS NAM_DUOI_18,
    SUM(CASE WHEN NAM_SINH IS NOT NULL AND (CAST(STRFTIME('%Y', 'now', 'localtime') AS INT) - NAM_SINH) < 18 AND GIOI_TINH = 2 THEN 1 ELSE 0 END) AS NU_DUOI_18,
    SUM(CASE WHEN NAM_SINH IS NOT NULL AND (CAST(STRFTIME('%Y', 'now', 'localtime') AS INT) - NAM_SINH) BETWEEN 18 AND 35 AND GIOI_TINH = 1 THEN 1 ELSE 0 END) AS NAM_18_35,
    SUM(CASE WHEN NAM_SINH IS NOT NULL AND (CAST(STRFTIME('%Y', 'now', 'localtime') AS INT) - NAM_SINH) BETWEEN 18 AND 35 AND GIOI_TINH = 2 THEN 1 ELSE 0 END) AS NU_18_35,
    SUM(CASE WHEN NAM_SINH IS NOT NULL AND (CAST(STRFTIME('%Y', 'now', 'localtime') AS INT) - NAM_SINH) BETWEEN 36 AND 60 AND GIOI_TINH = 1 THEN 1 ELSE 0 END) AS NAM_36_60,
    SUM(CASE WHEN NAM_SINH IS NOT NULL AND (CAST(STRFTIME('%Y', 'now', 'localtime') AS INT) - NAM_SINH) BETWEEN 36 AND 60 AND GIOI_TINH = 2 THEN 1 ELSE 0 END) AS NU_36_60,
    SUM(CASE WHEN NAM_SINH IS NOT NULL AND (CAST(STRFTIME('%Y', 'now', 'localtime') AS INT) - NAM_SINH) > 60 AND GIOI_TINH = 1 THEN 1 ELSE 0 END) AS NAM_TREN_60,
    SUM(CASE WHEN NAM_SINH IS NOT NULL AND (CAST(STRFTIME('%Y', 'now', 'localtime') AS INT) - NAM_SINH) > 60 AND GIOI_TINH = 2 THEN 1 ELSE 0 END) AS NU_TREN_60,
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
    -- Cờ lỗi địa chỉ liên hệ (_LH) phục vụ biểu đồ Chất lượng dữ liệu chi tiết
    SUM(CASE WHEN ';' || COALESCE(DQ_FLAGS, '') || ';' LIKE '%;TINH_LH_KHONG_MAP;%' THEN 1 ELSE 0 END) AS LOI_TINH_LH_KHONG_MAP,
    SUM(CASE WHEN ';' || COALESCE(DQ_FLAGS, '') || ';' LIKE '%;XA_LH_KHONG_MAP;%' THEN 1 ELSE 0 END) AS LOI_XA_LH_KHONG_MAP,
    SUM(CASE WHEN ';' || COALESCE(DQ_FLAGS, '') || ';' LIKE '%;TINH_LH_LECH_XA;%' THEN 1 ELSE 0 END) AS CANH_BAO_TINH_LH_LECH_XA,
    SUM(CASE WHEN DQ_FLAGS LIKE '%_LH_%' OR DQ_FLAGS LIKE '%_KS_%' THEN 1 ELSE 0 END) AS LOI_DIA_CHI_LH_KS
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

-- ------------------------------------------------------------------------------
-- VIEW 4: PHÂN BỔ NHÓM DÂN TỘC & CHI TIẾT DÂN TỘC THIỂU SỐ
-- ------------------------------------------------------------------------------
DROP VIEW IF EXISTS V_BI_NHOM_DAN_TOC;
CREATE VIEW V_BI_NHOM_DAN_TOC AS
SELECT
    CASE
        WHEN TEN_DAN_TOC IS NULL THEN 'Chưa xác định'
        WHEN DM_DAN_TOC_ID = 171 OR UPPER(TEN_DAN_TOC) = 'KINH' THEN 'Kinh'
        ELSE 'Dân tộc thiểu số'
    END AS NHOM_DAN_TOC,
    COALESCE(TEN_DAN_TOC, 'Chưa xác định') AS TEN_DAN_TOC,
    COUNT(*) AS SO_NGUOI
FROM C360_THONG_TIN_CA_NHAN
GROUP BY
    CASE
        WHEN TEN_DAN_TOC IS NULL THEN 'Chưa xác định'
        WHEN DM_DAN_TOC_ID = 171 OR UPPER(TEN_DAN_TOC) = 'KINH' THEN 'Kinh'
        ELSE 'Dân tộc thiểu số'
    END,
    COALESCE(TEN_DAN_TOC, 'Chưa xác định');

-- ------------------------------------------------------------------------------
-- VIEW 5: ĐỐI SÁNH PHÂN BỔ TỈNH THƯỜNG TRÚ VÀ TỈNH LIÊN HỆ
-- ------------------------------------------------------------------------------
DROP VIEW IF EXISTS V_BI_TINH_THUONG_TRU_LIEN_HE;
CREATE VIEW V_BI_TINH_THUONG_TRU_LIEN_HE AS
WITH tt AS (
    SELECT COALESCE(MA_TINH_THUONG_TRU, 'CHUA_XAC_DINH') AS MA_TINH,
           COALESCE(TEN_TINH_THUONG_TRU, 'Chưa xác định') AS TEN_TINH,
           COUNT(*) AS SO_THUONG_TRU
      FROM C360_THONG_TIN_CA_NHAN
     GROUP BY COALESCE(MA_TINH_THUONG_TRU, 'CHUA_XAC_DINH'),
              COALESCE(TEN_TINH_THUONG_TRU, 'Chưa xác định')
),
lh AS (
    SELECT COALESCE(MA_TINH_LIEN_HE, 'CHUA_XAC_DINH') AS MA_TINH,
           COALESCE(TEN_TINH_LIEN_HE, 'Chưa xác định') AS TEN_TINH,
           COUNT(*) AS SO_LIEN_HE
      FROM C360_THONG_TIN_CA_NHAN
     GROUP BY COALESCE(MA_TINH_LIEN_HE, 'CHUA_XAC_DINH'),
              COALESCE(TEN_TINH_LIEN_HE, 'Chưa xác định')
),
all_keys AS (
    SELECT MA_TINH, TEN_TINH FROM tt
    UNION
    SELECT MA_TINH, TEN_TINH FROM lh
)
SELECT k.MA_TINH,
       k.TEN_TINH,
       COALESCE(tt.SO_THUONG_TRU, 0) AS SO_THUONG_TRU,
       COALESCE(lh.SO_LIEN_HE, 0) AS SO_LIEN_HE
  FROM all_keys k
  LEFT JOIN tt ON k.MA_TINH = tt.MA_TINH
  LEFT JOIN lh ON k.MA_TINH = lh.MA_TINH;
