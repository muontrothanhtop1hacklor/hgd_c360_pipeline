# TUẦN 4: LẬP LỊCH APACHE AIRFLOW, KIỂM THỬ TOÀN TRÌNH E2E & NGHIỆM THU

> **Trạng thái:** `KẾ HOẠCH & ĐẶC TẢ THỰC HIỆN CHI TIẾT (Sẵn Sàng Triển Khai Tiếp Nối Tuần 3)`  
> **Thư mục quản lý:** `p4_airflow_and_e2e_test/`  
> **Tài liệu thiết kế liên kết:** [`design/ThietKeChiTiet_DetailDesign_HGD_C360.md`](../design/ThietKeChiTiet_DetailDesign_HGD_C360.md) (Mục 6: Kế hoạch Kiểm thử Toàn trình & Nghiệm thu)

---

## 1. Mục tiêu và Phạm vi Tuần 4

Tuần 4 là giai đoạn đóng gói, tự động hóa điều phối toàn bộ data pipeline bằng **Apache Airflow** và thực thi kịch bản kiểm thử toàn trình (**End-to-End Test Suite**) để nghiệm thu dự án:
1. **Lập Lịch & Điều Phối Bằng Apache Airflow:**
   - Xây dựng các DAGs tự động hóa: thiết lập lịch chạy, cơ chế thử lại (retry), thông báo lỗi và quản lý phụ thuộc giữa các tác vụ.
   - Tách biệt rõ ràng giữa luồng chạy theo lô (Batch / Full Load), luồng giám sát luồng dữ liệu thời gian thực (CDC & Near Real-time Monitoring) và luồng đối soát định kỳ hàng ngày (Daily Reconciliation).
2. **Kịch Bản Kiểm Thử Toàn Trình Tự Động (E2E Test Automation):**
   - Mô phỏng chu trình sống hoàn chỉnh của dữ liệu: Khởi tạo nguồn $\rightarrow$ Full Load $\rightarrow$ Bật CDC $\rightarrow$ Kích hoạt biến động Delta $\rightarrow$ Đồng bộ tự động sang Mirror, C360 $\rightarrow$ Khai thác qua BI & REST API $\rightarrow$ Đối soát dữ liệu sau biến động.
   - Đo lường thời gian đáp ứng (End-to-End Latency) từ lúc biến động xảy ra ở Nguồn đến khi phản ánh lên API tra cứu C360 (< 60 giây).
3. **Nghiệm Thu Toàn Diện Theo 5 Tiêu Chí Đề Bài:**
   - Hoàn tất hồ sơ thiết kế, kiểm tra tính lũy đẳng (idempotency), bảo toàn số liệu 100% và bảo vệ tài nguyên phần cứng (RAM < 300MB trên Local Engine).

---

## 2. Kiến trúc Điều Phối Airflow & Chu Trình Kiểm Thử E2E

```
+---------------------------------------------------------------------------------------------------+
|                           TUẦN 4: AIRFLOW ORCHESTRATION & E2E AUTOMATION                          |
+---------------------------------------------------------------------------------------------------+
|                                                                                                   |
|  [AIRFLOW DAGS: p4_.../dags/]                                                                     |
|                                                                                                   |
|  1. DAG: dag_01_initial_setup_full_load                                                           |
|     [Load Danh Mục] --> [Gen Seed Data] --> [Full Load Mirror] --> [Build Map] --> [Full Load C360]  |
|                                                                                                   |
|  2. DAG: dag_02_cdc_near_realtime_monitor (Lặp mỗi 1 phút / Sensor)                               |
|     [Kiểm tra Debezium Connector] --> [Sensor: C360_SYNC_QUEUE] --> [Kích hoạt NRT Worker]        |
|                                                                                                   |
|  3. DAG: dag_03_daily_reconciliation_dq (Chạy lúc 01:00 AM hàng ngày)                             |
|     [Đối soát Nguồn <-> Mirror] --> [Đối soát Mirror <-> C360] --> [Xuất Báo cáo BI Snapshot]     |
|                                                                                                   |
+---------------------------------------------------------------------------------------------------+
|  [KỊCH BẢN KIỂM THỬ TOÀN TRÌNH E2E: p4_.../tests/test_week4_e2e.py]                               |
|                                                                                                   |
|   (B1: Initial Seed) ---> (B2: Full Load) ---> (B3: Đối soát Lần 1: Khớp 100%)                    |
|                                                        │                                          |
|                                                        v                                          |
|   (B6: Gọi 4 REST APIs) <--- (B5: NRT Worker) <--- (B4: Gen Delta Changes: INSERT/UPDATE/DELETE)  |
|            │                                                                                      |
|            v                                                                                      |
|   (B7: Đối soát Lần 2: Khớp 100% Sau Biến Động) ---> (B8: Xuất Biên bản Nghiệm thu)               |
+---------------------------------------------------------------------------------------------------+
```

---

## 3. Danh Sách Apache Airflow DAGs (`p4_airflow_and_e2e_test/dags/`)

### 3.1. DAG 1: `dag_01_initial_setup_full_load.py`
- **Mục đích:** Khởi tạo toàn bộ dữ liệu ban đầu từ nguồn đến C360.
- **Lịch chạy:** Chạy thủ công (Manual / On-demand).
- **Chuỗi tác vụ (Task Pipeline):**
  1. `task_load_danhmuc`: Nạp 3 bảng danh mục từ CSV vào `VSS_DM_*`.
  2. `task_gen_seed`: Sinh 50.000 công dân mẫu vào `VSS_HO_GD_NHANKHAU`.
  3. `task_full_load_mirror`: Nạp song song sang `MIR_VSS_*` và ghi nhận mốc `SNAPSHOT_SCN`.
  4. `task_build_map_dbhc`: Khởi tạo 2 bảng tra cứu phẳng `C360_MAP_DBHC_TINH` và `_XA`.
  5. `task_full_load_c360`: Tải toàn bộ dữ liệu vào `C360_THONG_TIN_CA_NHAN`.
  6. `task_initial_reconciliation`: Kiểm tra đối soát Tầng 1 và Tầng 2, đảm bảo không lệch trước khi mở hệ thống.

### 3.2. DAG 2: `dag_02_cdc_near_realtime_monitor.py`
- **Mục đích:** Giám sát Debezium Connectors và kích hoạt xử lý hàng đợi Near Real-time.
- **Lịch chạy:** `@continuous` hoặc lập lịch mỗi phút `* * * * *` (`max_active_runs = 1`, `catchup = False`).
- **Chuỗi tác vụ:**
  1. `task_check_connectors`: Gọi Kafka Connect API kiểm tra trạng thái `RUNNING` của Source/Sink connector.
  2. `task_run_nrt_worker`: Thực thi `c360_nrt_worker.py` để xử lý các lô `MA_CD` trong `C360_SYNC_QUEUE`.

### 3.3. DAG 3: `dag_03_daily_reconciliation_dq.py`
- **Mục đích:** Báo cáo đối soát 3 tầng định kỳ và đánh giá sức khỏe chất lượng dữ liệu.
- **Lịch chạy:** Hàng ngày vào lúc 01:00 AM (`0 1 * * *`).
- **Chuỗi tác vụ:**
  1. `task_reconcile_all_tiers`: Chạy `reconcile_3_tiers.py` so sánh Nguồn - Mirror - C360.
  2. `task_generate_dq_metrics`: Tổng hợp thống kê 11 mã cờ `DQ_FLAGS` đẩy vào bảng Data Mart BI.
  3. `task_alert_if_mismatch`: Phát cảnh báo nếu phát hiện bất kỳ sự sai lệch số lượng công dân nào.

---

## 4. Kịch Bản Kiểm Thử Toàn Trình End-to-End (E2E Test Plan)

Kịch bản kiểm thử E2E tự động hóa hoàn toàn trong `pytest p4_airflow_and_e2e_test/tests/test_week4_e2e.py` bao gồm 8 bước khép kín:

| Bước | Hành động kiểm thử | Kết quả mong đợi |
| :---: | :--- | :--- |
| **B1** | Sinh Initial Seed 50.000 công dân | `VSS_HO_GD_NHANKHAU` đạt 52.500 dòng (có 2.500 người nhiều hộ) |
| **B2** | Chạy Full Load Mirror & C360 | Mirror có 52.500 dòng, C360 có đúng **48.000** công dân hợp lệ |
| **B3** | Chạy đối soát lần 1 (Trước biến động) | Độ lệch Nguồn $\leftrightarrow$ Mirror = 0; Mirror $\leftrightarrow$ C360 khớp 100% |
| **B4** | Chạy Script Delta (`gen_delta_changes.py`) | Tạo đủ các biến động `INSERT`, `UPDATE`, đổi `MA_CD`, `DELETE` |
| **B5** | Kích hoạt Near Real-time Worker | Các biến động trong Queue được xử lý trong < 1 phút, không nghẽn |
| **B6** | Kiểm chứng qua 4 REST APIs | - API 1: Tìm thấy người mới; người đổi mã tìm được qua mã mới & mã cũ.<br>- API 2: Tra cứu lô 1.000 mã thành công.<br>- API 3: Tìm thấy người vừa cập nhật qua họ tên không dấu.<br>- API 4: Concursor trả đúng danh sách các bản ghi thay đổi mới. |
| **B7** | Kiểm chứng qua Báo cáo BI | Metabase hiển thị số liệu đồng bộ mới, tỷ lệ cờ lỗi cập nhật chính xác |
| **B8** | Chạy đối soát lần 2 (Sau biến động) | `COUNT(DISTINCT MA_CD)` hợp lệ trên Mirror **vẫn khớp 100%** với `COUNT(*)` C360 |

---

## 5. Danh Mục Kiểm Tra Nghiệm Thu (Acceptance Checklist)

- [x] **Tiêu chí 1: Tài liệu Thiết kế:** Có đủ Basic Design (15 sheets) và Detail Design (11 sheets) khớp 100% với code, kèm Sheet QA giải trình thực tế danh mục.
- [x] **Tiêu chí 2: Cấu trúc & Bảo mật:** Cấu hình môi trường tách bạch trong `.env`, không hardcode mật khẩu hay API key nhạy cảm, hỗ trợ Dual-Engine (Oracle / Local SQLite).
- [x] **Tiêu chí 3: Bộ dữ liệu Kiểm thử:** Bộ sinh Seed và Delta bao phủ 100% dữ liệu sạch, bẩn, biên (11 cờ `DQ_FLAGS`).
- [ ] **Tiêu chí 4: Demo Biến động Thời gian thực:** Chạy script thay đổi dữ liệu ở nguồn và chứng minh dữ liệu phản ánh lên C360, BI và API trong vòng 1-2 phút.
- [ ] **Tiêu chí 5: Đối soát Toàn vẹn Số liệu:** Công thức `COUNT(DISTINCT MA_CD) == COUNT(*)` luôn thỏa mãn 100% cả trước và sau khi phát sinh biến động.

---

## 6. Danh Sách File Dự Kiến & Hiện Có Trong Tuần 4

```text
p4_airflow_and_e2e_test/
├── README.md                           # Tài liệu chi tiết tuần 4 (file hiện tại)
├── dags/
│   ├── dag_01_initial_setup_full_load.py     # DAG nạp dữ liệu ban đầu
│   ├── dag_02_cdc_near_realtime_monitor.py   # DAG giám sát CDC & kích hoạt NRT Worker
│   └── dag_03_daily_reconciliation_dq.py     # DAG đối soát 3 tầng định kỳ
└── tests/
    ├── test_week1_p1.py                # Test Suite Tuần 1 (PASSED 100%)
    ├── test_week2_p2.py                # Test Suite Tuần 2 (Chuẩn hóa & NRT)
    ├── test_week3_p3.py                # Test Suite Tuần 3 (Đối soát & 4 APIs)
    └── test_week4_e2e.py               # Test Suite Toàn trình E2E khép kín
```

