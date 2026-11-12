-- =============================================================================
-- DDL THIẾT KẾ CÁC BẢNG VÙNG MIRROR TRÊN DB ĐÍCH (SCHEMA C360 / MIRROR)
--
-- GIẢI TRÌNH THIẾT KẾ VẬT LÝ (THEO YÊU CẦU PHẦN 1 - MỤC 2):
-- 1. Tại sao bảng MIR_HO_GD_NHANKHAU được thiết kế là BẢNG PHẲNG (Unpartitioned)
--    thay vì giữ PARTITION BY RANGE (MA_TINH) như bảng nguồn?
--    - Trên thực tế, cột MA_TINH ở nguồn chứa nhiều định dạng lộn xộn ('01TTT',
--      '001', '01', '99TTT', NULL) và thường xuyên được UPDATE khi cán bộ sửa
--      địa chỉ lỗi hoặc khi công dân chuyển tỉnh. Nếu chia Partition theo MA_TINH
--      ở Mirror, mỗi lệnh UPDATE MA_TINH từ CDC Sink sẽ buộc DB đích phải xóa
--      bản ghi ở partition cũ và chèn sang partition mới (Row Movement), làm
--      đổi ROWID và gây chậm/contention cho индекс + Trigger Queue.
--    - Truy vấn đọc từ Mirror sang C360 ở chế độ Near Real-time luôn tra cứu
--      theo danh sách MA_CD (qua Index IDX_MIR_NK_MOD), không lọc theo MA_TINH.
--
-- 2. Chiến lược đánh Unique Index UK_MIR_NK (MA_CD, MA_HO_GD):
--    - KHÔNG tạo Unique Index trước khi chạy Full Load vì việc kiểm tra trùng
--      và cập nhật cây B-Tree cho hàng triệu/chục triệu dòng sẽ làm chậm tốc độ
--      ghi Batch/JDBC gấp 5-10 lần và sinh lượng lớn Redo/Undo.
--    - Quy trình chuẩn:
--      (a) Trước Full Load: DROP INDEX UK_MIR_NK và DISABLE Trigger Queue.
--      (b) Trong Full Load: Nạp song song tốc độ cao (APPEND / Batch Insert),
--          chuẩn hóa NVL(TRIM(MA_HO_GD), 'HO_MAC_DINH') để khóa kép không bị NULL.
--      (c) Ngay sau khi Full Load xong (trước khi bật Debezium Sink):
--          Chạy CREATE UNIQUE INDEX UK_MIR_NK ON MIR_HO_GD_NHANKHAU (MA_CD, MA_HO_GD)
--          NOLOGGING PARALLEL 4; và ENABLE Trigger Queue.
-- =============================================================================

-- 1. Bảng Mirror Nhân khẩu: MIR_HO_GD_NHANKHAU (Bảng phẳng)
CREATE TABLE MIR_HO_GD_NHANKHAU (
    MA_CD                    VARCHAR2(30 BYTE) NOT NULL,
    MA_CN                    VARCHAR2(30 BYTE),
    MA_HO_GD                 VARCHAR2(30 BYTE) DEFAULT 'HO_MAC_DINH' NOT NULL,
    HO_TEN                   VARCHAR2(250 BYTE),
    HO_TEN_KHAC              VARCHAR2(250 BYTE),
    GIOI_TINH                CHAR(1 BYTE),
    NGAY_SINH                NUMBER(18,0),
    NOI_SINH                 VARCHAR2(250 BYTE),
    NGAY_KS                  NUMBER(18,0),
    SO_GIAY_KS               VARCHAR2(50 BYTE),
    NOI_KS                   VARCHAR2(250 BYTE),
    QH_CHU_HO                VARCHAR2(2 BYTE),
    SO_CMTND                 VARCHAR2(50 BYTE),
    NGAY_CAP_CMTND           NUMBER(18,0),
    NOI_CAP_CMTND            VARCHAR2(250 BYTE),
    SO_HC                    VARCHAR2(50 BYTE),
    NGAY_CAP_HC              NUMBER(18,0),
    NOI_CAP_HC               VARCHAR2(250 BYTE),
    DIA_CHI                  VARCHAR2(500 BYTE),
    DC_LIEN_LAC              VARCHAR2(250 BYTE),
    EMAIL_ACC                VARCHAR2(50 BYTE),
    PHONE_NUMBER             VARCHAR2(50 BYTE),
    MA_TINH                  VARCHAR2(10 BYTE),
    MA_HUYEN                 VARCHAR2(10 BYTE),
    MA_XA                    VARCHAR2(10 BYTE),
    NGHE_NGHIEP              VARCHAR2(250 BYTE),
    QUOC_TICH                VARCHAR2(10 BYTE),
    IS_ALIVE                 CHAR(1 BYTE),
    DEAD_DATE                NUMBER(18,0),
    IS_BH                    CHAR(1 BYTE),
    LOAI_BH                  VARCHAR2(30 BYTE),
    MA_BH                    VARCHAR2(30 BYTE),
    CREATED_DATE             DATE,
    MODIFY_DATE              DATE,
    MA_NV                    VARCHAR2(100 CHAR),
    GHI_CHU                  VARCHAR2(500 BYTE),
    MA_THE                   VARCHAR2(2000 BYTE),
    MA_SO                    VARCHAR2(2000 BYTE),
    ISSYNCH                  CHAR(1 BYTE),
    NGAY_BATDAU              NUMBER(18,0),
    NGAY_HETHAN              NUMBER(18,0),
    MA_THON                  VARCHAR2(10 BYTE),
    ISCHU_HO                 CHAR(1 BYTE),
    TEN_DOITUONG_BH          VARCHAR2(2000 BYTE),
    TEN_TINH                 VARCHAR2(250 BYTE),
    TEN_HUYEN                VARCHAR2(250 BYTE),
    TEN_XA                   VARCHAR2(250 BYTE),
    TEN_THON                 VARCHAR2(250 BYTE),
    TEN_QH_CHUHO             VARCHAR2(250 BYTE),
    MA_DOITUONG_BH           VARCHAR2(10 BYTE),
    STATUS                   CHAR(1 BYTE),
    CARD_ID                  VARCHAR2(2000 BYTE),
    TYPEID                   VARCHAR2(2 BYTE),
    NGAY_PHATSINH            NUMBER(18,0),
    NGUOI_KE_KHAI            VARCHAR2(200 BYTE),
    TRAN_STATUS              VARCHAR2(5 BYTE),
    TRANG_THAI               CHAR(1 BYTE),
    MA_TINH_KS               VARCHAR2(5 BYTE),
    MA_HUYEN_KS              VARCHAR2(5 BYTE),
    MA_XA_KS                 VARCHAR2(5 BYTE),
    TEN_TINH_KS              VARCHAR2(200 BYTE),
    TEN_HUYEN_KS             VARCHAR2(200 BYTE),
    TEN_XA_KS                VARCHAR2(200 BYTE),
    ID                       VARCHAR2(40 BYTE),
    MA_OFFLINE               NVARCHAR2(100),
    NGAY_SINH_BK             NUMBER(18,0),
    MA_HUYEN_IMPORT          VARCHAR2(20 BYTE),
    ID_CA                    VARCHAR2(20 BYTE),
    NGOAI_TINH               VARCHAR2(10 BYTE),
    MA_NV_TAO                VARCHAR2(100 CHAR),
    MA_TINH_THE              VARCHAR2(10 BYTE),
    USER_NGOAI_TINH          NVARCHAR2(30),
    MA_THE_MOI               VARCHAR2(50 CHAR),
    CARD_ID_MOI              VARCHAR2(50 BYTE),
    SYNC_TST                 NUMBER(1),
    MA_CD_OLD                VARCHAR2(30 BYTE),
    HO_TEN_UNSIGNED          VARCHAR2(250 BYTE),
    USER_ID                  NUMBER(10),
    USER_ID_TINH             NUMBER(10),
    USER_ID_TW               NUMBER(10),
    TRANG_THAI_DUYET         NUMBER(2),
    TYPE_PM                  NUMBER(1),
    USER_UPDATE              NUMBER(10),
    LY_DO_PHE_DUYET          VARCHAR2(1000 CHAR),
    APPROVED_DATE            DATE,
    TEN_CHA_ME               VARCHAR2(250 BYTE),
    KEY_SL                   VARCHAR2(250 CHAR),
    DM_DAN_TOC_ID            NUMBER(19),
    NGAY_TAO_HC              DATE,
    USER_HC_ID               NUMBER(10),
    USER_HC                  VARCHAR2(100 BYTE),
    USER_DUYET_HC_ID         NUMBER(10),
    USER_DUYET_HC            VARCHAR2(100 BYTE),
    NGAY_DUYET_HC            DATE,
    LY_DO_HC                 VARCHAR2(1000 BYTE),
    USER_DMS                 VARCHAR2(100 CHAR),
    USER_DMS_ID              NUMBER(19),
    NGAY_DE_NGHI_DMS         DATE,
    USER_DUYET_DMS_ID        NUMBER(19),
    USER_DUYET_DMS           VARCHAR2(100 CHAR),
    NGAY_DUYET_DMS           DATE,
    LY_DO_DMS                VARCHAR2(1000 CHAR),
    LOCK_DATA                NUMBER(10,0),
    MA_TINH_LH               VARCHAR2(10 BYTE),
    MA_HUYEN_LH              VARCHAR2(10 BYTE),
    MA_XA_LH                 VARCHAR2(10 BYTE),
    XAC_THUC_BCA             NUMBER(2),
    DANG_TG                  VARCHAR2(20 BYTE),
    SO_CCCD                  VARCHAR2(50 BYTE),
    TRANG_THAI_QUET          NUMBER(2),
    TRANG_THAI_XAC_THUC_BCA  NUMBER(2)
);

-- Câu lệnh đánh Index (được gọi tự động ở cuối Job Full Load sau khi nạp xong dữ liệu):
CREATE UNIQUE INDEX UK_MIR_NK ON MIR_HO_GD_NHANKHAU (MA_CD, MA_HO_GD);
CREATE INDEX IDX_MIR_NK_MOD ON MIR_HO_GD_NHANKHAU (MA_CD, MODIFY_DATE DESC, CREATED_DATE DESC, ID DESC);


-- 2. Bảng Mirror Danh mục Dân tộc: MIR_DM_DANTOC
CREATE TABLE MIR_DM_DANTOC (
    ID              NUMBER(19)          NOT NULL,
    CAN_CU          VARCHAR2(255 CHAR)  NOT NULL,
    MA              VARCHAR2(2 CHAR)    NOT NULL,
    NGAY_HL         DATE                NOT NULL,
    NGAY_HET_HL     DATE,
    TEN             VARCHAR2(100 CHAR)  NOT NULL,
    CONSTRAINT PK_MIR_DM_DANTOC PRIMARY KEY (ID)
);


-- 3. Bảng Mirror Danh mục Quốc tịch: MIR_DM_QUOC_TICH
CREATE TABLE MIR_DM_QUOC_TICH (
    ID              NUMBER(19)          NOT NULL,
    MA              VARCHAR2(2 CHAR)    NOT NULL,
    TEN             VARCHAR2(100 CHAR)  NOT NULL,
    CONSTRAINT PK_MIR_DM_QUOC_TICH PRIMARY KEY (ID)
);


-- 4. Bảng Mirror Danh mục Địa bàn hành chính: MIR_DM_DBHC
CREATE TABLE MIR_DM_DBHC (
    MA_DBHC             VARCHAR2(5 BYTE),
    MA_CHA              VARCHAR2(5 BYTE),
    TEN                 VARCHAR2(200 BYTE),
    TINH_TRANG          VARCHAR2(1 BYTE),
    STATUS              CHAR(1 BYTE),
    CREATED_DATE        DATE,
    ID                  NUMBER(20),
    DM_VUNG_ID          NUMBER(10),
    MA_DBHC_SN          VARCHAR2(200 BYTE),
    IS_LT               NUMBER(1),
    IS_CAP_XA           NUMBER,
    NGAY_HIEU_LUC       TIMESTAMP(6),
    NGAY_HET_HIEU_LUC   TIMESTAMP(6)
);

CREATE INDEX IDX_MIR_DBHC_ACTIVE ON MIR_DM_DBHC (TINH_TRANG, IS_CAP_XA, MA_DBHC);

