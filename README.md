# XÂY DỰNG PIPELINE DỮ LIỆU HỘ GIA ĐÌNH - NHÂN KHẨU (HGD -> MIRROR -> C360 -> BI & API)

Dự án xây dựng **Data Pipeline hoàn chỉnh từ đầu đến cuối (End-to-End)** trong 4 tuần cho bài toán thực tế:  
**Hệ thống nguồn Hộ gia đình – Nhân khẩu (`VSS.VSS_HO_GD_NHANKHAU` & 3 bảng danh mục) $\longrightarrow$ Vùng đệm Mirror $\longrightarrow$ Chuẩn hóa thông tin công dân (`Customer 360 - C360`) $\longrightarrow$ Đối soát dữ liệu 3 tầng, Báo cáo BI & Dịch vụ 4 REST APIs**.

---

## 1. Lộ Trình Triển Khai 4 Tuần & Điều Hướng Tài Liệu

Bảng tổng hợp tiến độ và liên kết trực tiếp đến tài liệu chi tiết của từng tuần:

| Tuần | Tên Phân Hệ & Phạm Vi Nghiệp Vụ | Thư Mục | Trạng Thái | Tài Liệu Chi Tiết |
| :---: | :--- | :---: | :---: | :--- |
| **-** | **Bộ Tài Liệu Thiết Kế (Basic & Detail Design)** | `design/` | **HOÀN THÀNH 100%** | [Tài liệu Thiết kế Hệ thống](design/README.md) |
| **Tuần 1** | **Khảo sát, Thiết kế, Sinh dữ liệu giả lập & Đồng bộ Nguồn $\rightarrow$ Mirror** | `p1_source_and_mirror/` | **HOÀN THÀNH 100%** | [README Chi Tiết Tuần 1](p1_source_and_mirror/README.md) |
| **Tuần 2** | **Chuẩn hóa dữ liệu công dân (C360) & Đồng bộ Near Real-time qua Queue** | `p2_c360_standardization/` | **HOÀN THÀNH 100%** | [README Chi Tiết Tuần 2](p2_c360_standardization/README.md) |
| **Tuần 3** | **Job Đối soát dữ liệu 3 tầng, Dashboard BI & Dịch vụ 4 REST APIs C360** | `p3_reconciliation_bi_api/` | **HOÀN THÀNH 100%** | [README Chi Tiết Tuần 3](p3_reconciliation_bi_api/README.md) |
| **Tuần 4** | **Lập lịch Apache Airflow, Kịch bản Kiểm thử Toàn trình E2E & Nghiệm thu** | `p4_airflow_and_e2e_test/` | **HOÀN THÀNH 100%** | [README Chi Tiết Tuần 4](p4_airflow_and_e2e_test/README.md) |

---

## 2. Kiến Trúc Luồng Dữ Liệu Toàn Cảnh (End-to-End Architecture)

```
+-----------------------------------------------------------------------------------------------------------------+
|                               KIẾN TRÚC TỔNG THỂ DATA PIPELINE HGD -> C360 -> BI & API                          |
+-----------------------------------------------------------------------------------------------------------------+
|                                                                                                                 |
|  [TẦNG 1: NGUỒN VSS]                                                                                            |
|   ├── VSS_DM_DANTOC (59 dòng)                                                                                   |
|   ├── VSS_DM_QUOC_TICH (243 dòng)                                                                               |
|   ├── VSS_DM_DBHC (15.325 dòng)                                                                                 |
|   └── VSS_HO_GD_NHANKHAU (52.500 dòng giả lập: sạch + bẩn + biên)                                               |
|        │                                                                                                        |
|        ├── (Batch Full Load JDBC - Phân mảnh 10 Partitions theo MA_CD) ----------------+                         |
|        │                                                                               |                        |
|        └── (CDC Near Real-time: Debezium LogMiner -> Kafka -> JDBC Sink) -------------+                         |
|                                                                                        |                        |
|                                                                                        v                        |
|  [TẦNG 2: VÙNG MIRROR]                                                                                          |
|   ├── MIR_VSS_DM_* & MIR_VSS_HO_GD_NHANKHAU                                                                     |
|   ├── Unique Index UK_MIR_NK (MA_CD, MA_HO_GD)                                                                  |
|   └── MIR_CDC_SCN_HANDOFF (Bàn giao mốc SCN không trùng lặp / không mất mát)                                    |
|        │                                                                                                        |
|        ├── (Cách 1: Batch Full Load C360) ---------------------------------------------+                        |
|        │    - Khử trùng theo MA_CD (ưu tiên dòng mới nhất theo MODIFY_DATE -> ID)      |                        |
|        │    - Lọc TYPEID ('GT','GM','GG','GK') & TRANG_THAI_DUYET (1, 3, 4)            |                        |
|        │    - Tra cứu 2 Bảng Map DBHC sau sáp nhập 2025 (36 Tỉnh & 3.322 Xã)           |                        |
|        │    - Làm sạch Họ tên, Ngày sinh, CCCD 12 số, SĐT, Dân tộc, Quốc tịch          |                        |
|        │    - Gán chuỗi 11 cờ chất lượng dữ liệu DQ_FLAGS                              v                        |
|        │                                                                +------------------------------+        |
|        └── (Cách 2: Trigger nhẹ trên Mirror -> Queue)                  |  [TẦNG 3: BẢNG CHUẨN HÓA]    |        |
|             │                                                           |  C360_THONG_TIN_CA_NHAN      |        |
|             ├── [C360_SYNC_QUEUE] (Lưu khóa MA_CD biến động)            |  (1 dòng duy nhất / công dân)|        |
|             └── [Near Real-time Worker] (Đọc lô mỗi phút qua v_cut) --->+------------------------------+        |
|                                                                                        │                        |
|                        +---------------------------------------------------------------+                        |
|                        │                               │                               │                        |
|                        v                               v                               v                        |
|  [TẦNG 4: ĐẦU RA KHAI THÁC]                                                                                     |
|   1. ĐỐI SOÁT 3 TẦNG                 2. DASHBOARD BI (METABASE)       3. DỊCH VỤ 4 REST APIS (FASTAPI)          |
|   - reconcile_3_tiers.py             - Thống kê Địa bàn 2025          - GET  /api/v1/ca-nhan/{ma} (Cascade)     |
|   - Nguồn <-> Mirror                 - Báo cáo Chất lượng DQ_FLAGS    - POST /api/v1/ca-nhan/tra-cuu-lo         |
|   - COUNT(DISTINCT MA_CD)            - Giám sát Đồng bộ NRT           - GET  /api/v1/ca-nhan/tim-kiem           |
|     == COUNT(*) C360 (Khớp 100%)                                      - GET  /api/v1/ca-nhan/thay-doi (Cursor)  |
|                                                                       - X-API-Key SHA-256 & Async Audit Log     |
|                                                                                                                 |
|  [ĐIỀU PHỐI TỔNG THỂ: APACHE AIRFLOW DAGS]                                                                      |
|   ├── dag_01_initial_setup_full_load                                                                            |
|   ├── dag_02_cdc_near_realtime_monitor                                                                          |
|   └── dag_03_daily_reconciliation_dq                                                                            |
+-----------------------------------------------------------------------------------------------------------------+
```

---

## 3. Cấu Trúc Thư Mục Dự Án

```text
hgd_c360_pipeline/
├── README.md                           # Trang chủ dự án, kiến trúc tổng thể và điều hướng
├── .env.example                        # File mẫu cấu hình kết nối Oracle, SQLite, Kafka, API
├── docker-compose.yml                  # Hạ tầng Container chia theo Profile (db, cdc, bi, all)
├── docs/                               # Yêu cầu đề bài gốc 4 tuần
│   └── requirements/
│       └── Yeucau_DataPipeline_HGD_C360_4Tuan.md
├── design/                             # Bộ tài liệu Thiết kế cơ bản (15 sheets) & Chi tiết (11 sheets)
│   ├── README.md                       # Giới thiệu bộ tài liệu thiết kế & Sheet QA khảo sát thực tế
│   ├── ThietKeCoBan_BasicDesign_HGD_C360.md
│   ├── ThietKeCoBan_BasicDesign_HGD_C360.xlsx
│   ├── ThietKeChiTiet_DetailDesign_HGD_C360.md
│   ├── ThietKeChiTiet_DetailDesign_HGD_C360.xlsx
│   └── generate_excel_designs.py       # Script tự động đồng bộ Markdown -> Excel
├── data/                               # Dữ liệu danh mục CSV và các file báo cáo Ground Truth
│   ├── danhmuc/                        # 3 file CSV danh mục thực tế
│   │   ├── DM_DAN_TOC_202611091513.csv      # 59 dòng (Dân tộc Kinh ID=171)
│   │   ├── DM_DBHC_202611091512.csv         # 15.325 dòng (36 tỉnh, 3.322 xã hiệu lực 2025)
│   │   └── DM_QUOC_TICH_202611091512.csv    # 243 dòng (Việt Nam ID=233)
│   ├── seed_ground_truth.json          # Thống kê chi tiết bộ sinh dữ liệu nguồn ban đầu
│   ├── full_load_mirror_report.json    # Báo cáo đối soát Full Load Nguồn vs Mirror
│   ├── delta_ground_truth.json         # Danh sách chi tiết các biến động giả lập CDC
│   └── hgd_c360_local.db               # SQLite Database engine phục vụ kiểm thử cục bộ
├── p1_source_and_mirror/               # TUẦN 1: Nguồn, Sinh dữ liệu (Seed & Delta), Full Load & CDC
│   ├── README.md                       # Báo cáo & Hướng dẫn chi tiết Tuần 1 (Đã hoàn thành 100%)
│   ├── ddl_source_vss.sql              # DDL 4 bảng nguồn VSS
│   ├── ddl_mirror.sql                  # DDL 4 bảng Mirror & bảng SCN Handoff
│   ├── load_danhmuc.py                 # Nạp 3 bảng danh mục từ CSV vào nguồn
│   ├── gen_seed_nhankhau.py            # Script 1: Sinh 50k công dân bao phủ dữ liệu sạch/bẩn/biên
│   ├── gen_delta_changes.py            # Script 2: Giả lập biến động nguồn (INSERT/UPDATE/DELETE)
│   ├── full_load_jdbc.py               # Job Full Load JDBC đọc song song 10 partitions theo MA_CD
│   ├── check_week1_status.py           # Script kiểm tra nhanh tình trạng và đối soát Tuần 1
│   ├── db_env.py                       # Cơ chế chuyển đổi linh hoạt Oracle <-> SQLite
│   └── debezium/                       # Cấu hình Debezium Source, JDBC Sink & SCN Handoff
├── p2_c360_standardization/            # TUẦN 2: Bảng Map DBHC, Chuẩn hóa C360 & Queue Near Real-time (HOÀN THÀNH 100%)
│   ├── README.md                       # Báo cáo & Hướng dẫn chi tiết Tuần 2
│   ├── ddl_c360_schema.sql             # DDL tạo bảng Map, bảng C360, Queue, Control & Log
│   ├── build_map_dbhc.py               # Xây dựng 2 bảng Map DBHC (36 tỉnh, 3.322 xã) trong 1 Transaction
│   ├── db_c360_cleaner.py              # Single Source of Truth: Làm sạch Họ tên, Ngày sinh, CCCD, SĐT, 11 cờ DQ_FLAGS
│   ├── c360_etl_full_load.py           # Job Full Load nạp 48.000 công dân sạch sang C360_THONG_TIN_CA_NHAN
│   └── c360_nrt_worker.py              # Near Real-time Worker xử lý Queue (MERGE / DELETE qua v_cut)
├── p3_reconciliation_bi_api/           # TUẦN 3: Job Đối soát 3 tầng, Dashboard BI & 4 REST APIs (HOÀN THÀNH 100%)
│   ├── README.md                       # Báo cáo & Hướng dẫn chi tiết Tuần 3
│   ├── reconcile_3_tiers.py            # Job Đối soát dữ liệu 3 tầng (Nguồn <-> Mirror <-> C360, Công thức Vàng diff = 0)
│   ├── views_bi_datamart.sql           # 3 Data Mart Views: Địa bàn, Chất lượng DQ_FLAGS, Giám sát Đồng bộ NRT
│   └── api/                            # Dịch vụ FastAPI 4 REST Endpoints (X-API-Key SHA-256, Async Audit Log)
│       ├── main.py                     # Entrypoint FastAPI với Lifespan quản lý Async Audit Worker
│       ├── auth.py                     # Middleware xác thực X-API-Key và phân quyền scope
│       ├── schemas.py                  # Pydantic schemas cho 4 dịch vụ tra cứu & thay đổi
│       ├── audit.py                    # Worker ghi log bất đồng bộ vào C360_API_AUDIT_LOG
│       └── routers/citizen.py          # 4 Endpoints: /{ma}, /tra-cuu-lo, /tim-kiem, /thay-doi
└── p4_airflow_and_e2e_test/            # TUẦN 4: Airflow DAGs & Bộ Kiểm thử Toàn trình E2E (HOÀN THÀNH 100%)
    ├── README.md                       # Báo cáo & Hướng dẫn chi tiết Tuần 4
    ├── dags/                           # Bộ 3 DAGs điều phối Apache Airflow
    │   ├── dag_01_initial_setup_full_load.py       # DAG nạp danh mục, sinh seed, full load Mirror & C360
    │   ├── dag_02_cdc_near_realtime_monitor.py    # DAG giám sát Debezium & kích hoạt NRT Worker mỗi phút
    │   └── dag_03_daily_reconciliation_dq.py      # DAG chạy đối soát 3 tầng & snapshot chất lượng hàng ngày
    └── tests/                          # Test Suite tự động bằng Pytest (9/9 PASSED)
        ├── test_week1_p1.py            # Unit & Integration test Tuần 1 (2 passed)
        ├── test_week2_p2.py            # Unit & Integration test Tuần 2: Map DBHC, Cleaners, Full Load & NRT (3 passed)
        ├── test_week3_api_and_recon.py # Test Đối soát 3 tầng & 4 REST API endpoints (3 passed)
        └── test_week4_e2e.py           # Kịch bản kiểm thử toàn trình E2E 8 bước khép kín (1 passed)
```

---

## 4. Chiến Lược Môi Trường & Tối Ưu Máy Trạm 16GB RAM

Hệ thống được thiết kế theo nguyên tắc **Dual-Engine** thông minh:
1. **Engine 1 – Oracle Production / Azure for Students:**
   - DDL chuẩn Oracle cho schema `VSS`, `MIRROR`, `C360` với Partitioning và Indexes chuẩn mực.
   - Thích hợp khi bật Docker Compose hoặc triển khai trên máy ảo VM Cloud.
2. **Engine 2 – Local Lightweight Engine (SQLite & In-Memory):**
   - Hoạt động mượt mà ngay trên máy trạm cá nhân với mức tiêu thụ RAM **< 300MB**.
   - Hỗ trợ đầy đủ 100% cú pháp SQL, Transaction, Unique Index và logic chuẩn hóa để chạy demo và chạy kiểm thử tự động với Pytest ngay cả khi máy tính đang bận.

---

## 5. Hướng Dẫn Chạy Nhanh & Nghiệm Thu (Quick Start)

### 5.1. Cài đặt môi trường
Yêu cầu Python $\ge 3.10$:
```powershell
pip install pytest openpyxl fastapi pydantic uvicorn httpx
```

### 5.2. Chạy toàn bộ Test Suite nghiệm thu 4 tuần (9/9 Passed)
```powershell
pytest p4_airflow_and_e2e_test/tests/ -v
```

### 5.3. Khởi chạy Dịch vụ REST API C360 (FastAPI)
```powershell
uvicorn p3_reconciliation_bi_api.api.main:app --host 0.0.0.0 --port 8000 --reload
# Xem tài liệu API tương tác tại: http://localhost:8000/docs
```

### 5.4. Chạy Job Đối Soát Dữ Liệu 3 Tầng
```powershell
python p3_reconciliation_bi_api/reconcile_3_tiers.py
```
