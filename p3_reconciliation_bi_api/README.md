# TUẦN 3: ĐỐI SOÁT DỮ LIỆU 3 TẦNG, DASHBOARD BI & DỊCH VỤ REST API C360

> **Trạng thái:** `KẾ HOẠCH & ĐẶC TẢ THỰC HIỆN CHI TIẾT (Sẵn Sàng Triển Khai Tiếp Nối Tuần 2)`  
> **Thư mục quản lý:** `p3_reconciliation_bi_api/`  
> **Tài liệu thiết kế liên kết:** [`design/ThietKeChiTiet_DetailDesign_HGD_C360.md`](../design/ThietKeChiTiet_DetailDesign_HGD_C360.md) (Mục 3: Đặc tả 4 REST APIs & Mục 5: Kịch bản Đối soát 3 Tầng)

---

## 1. Mục tiêu và Phạm vi Tuần 3

Sau khi dữ liệu đã được chuẩn hóa liên tục sang bảng `C360_THONG_TIN_CA_NHAN` ở Tuần 2, Tuần 3 tập trung xây dựng toàn bộ hệ thống khai thác dữ liệu đầu ra và cơ chế giám sát toàn vẹn số liệu:
1. **Job Đối soát Tự động 3 Tầng (Data Reconciliation):**
   - Đối soát đối chiếu liên tầng: **Nguồn (`VSS`) $\leftrightarrow$ Mirror (`MIR_VSS`) $\leftrightarrow$ Bảng Chuẩn hóa (`C360`)**.
   - Chứng minh công thức đối soát vàng: `COUNT(DISTINCT MA_CD)` thỏa điều kiện lọc trên Mirror **luôn khớp 100%** với `COUNT(*)` trên C360.
   - Thống kê tỷ lệ và số lượng bản ghi theo 11 mã cờ lỗi trong `DQ_FLAGS`.
2. **Dashboard Báo cáo Trực quan BI (Metabase / PowerBI):**
   - Thiết kế các View tổng hợp (Data Mart) tối ưu cho BI.
   - Triển khai 3 nhóm báo cáo: Thống kê Nhân khẩu/Định danh 2025, Báo cáo Chất lượng Dữ liệu (Data Quality) và Giám sát Vận hành Đồng bộ Near Real-time (Sync Monitoring).
3. **Dịch vụ 4 REST API Tra cứu C360 Hiệu năng cao (FastAPI):**
   - Xây dựng 4 endpoint tra cứu phục vụ các hệ thống nghiệp vụ nội bộ.
   - Cơ chế bảo mật `X-API-Key` với mã băm SHA-256 và phân quyền scope.
   - Ghi nhật ký tra cứu bất đồng bộ theo lô (Async Batch Audit Log) không gây nghẽn API.

---

## 2. Kiến trúc Khai Thác & Đối Soát Tuần 3

```
+---------------------------------------------------------------------------------------------------+
|                                 TUẦN 3: ĐỐI SOÁT - BI - REST API C360                             |
+---------------------------------------------------------------------------------------------------+
|                                                                                                   |
|  [VSS_HO_GD_NHANKHAU] <==== (Tầng 1 <-> 2) =====> [MIR_VSS_HO_GD_NHANKHAU]                        |
|                                                          |                                        |
|                                              (Tầng 2 <-> 3: COUNT DISTINCT)                       |
|                                                          |                                        |
|                                                          v                                        |
|  +------------------------------------ [C360_THONG_TIN_CA_NHAN] -------------------------------+  |
|  |                                               |                                              |  |
|  v                                               v                                              v  |
|  [Job Đối Soát 3 Tầng]                   [BI Data Mart Views]                         [FastAPI Service]    |
|  reconcile_3_tiers.py                   views_bi_datamart.sql                         p3_.../api/main.py  |
|  - Nguồn vs Mirror                      ├── V_BI_THONG_KE_DIA_BAN                     ├── API 1: GET {ma} |
|  - Mirror vs C360 (Khớp 100%)           ├── V_BI_CHAT_LUONG_DU_LIEU                   ├── API 2: Batch    |
|  - Phân tích cờ DQ_FLAGS                └── V_BI_GIAM_SAT_DONG_BO                     ├── API 3: Search   |
|         |                                        |                                    └── API 4: Thay đổi |
|         v                                        v                                              |  |
|  [Reconciliation Report]                [Metabase Dashboards]                                   v  |
|   data/reconciliation_report.json        - 3 Nhóm biểu đồ tương tác                 [Async Audit Log]     |
|                                                                                     C360_API_AUDIT_LOG    |
+---------------------------------------------------------------------------------------------------+
```

---

## 3. Đặc tả 4 Dịch vụ REST API Tra Cứu C360

Hệ thống API được xây dựng bằng **FastAPI**, kết nối DB qua Connection Pool tối ưu, bảo vệ bởi Header `X-API-Key`:

### 3.1. API 1 – Tra cứu cá nhân theo mã (`GET /api/v1/ca-nhan/{ma}`)
- **Quy tắc dò cascade 3 tầng (Tự động dừng ở tầng đầu tiên tìm thấy):**
  1. Dò theo Mã số BHXH hiện hành (`MA_CD = :ma`).
  2. Dò theo Mã số BHXH cũ (`MA_CD_OLD = :ma`).
  3. Dò theo Số định danh cá nhân 12 số (`SO_DINH_DANH = :ma`).
- **Xử lý kết quả:**
  - `200 OK`: Tìm thấy duy nhất 1 công dân hợp lệ. Trả về toàn bộ thông tin đã chuẩn hóa và danh sách cờ lỗi `DQ_FLAGS`.
  - `404 Not Found`: Không tìm thấy bản ghi nào khớp ở cả 3 tầng dò.
  - `409 Conflict`: Trường hợp mã cũ hoặc số CCCD khớp với $\ge 2$ công dân khác nhau (do dữ liệu nguồn nhập nhầm CCCD). Trả về danh sách các ứng viên trùng lặp để phía nghiệp vụ xử lý thủ công.

### 3.2. API 2 – Tra cứu theo lô (`POST /api/v1/ca-nhan/tra-cuu-lo`)
- **Body Input:** JSON chứa danh sách mã `{"ma_list": ["0123456789", "001095012345", ...]}`.
- **Ràng buộc:** Giới hạn tối đa **1.000 mã/lần gọi**.
- **Output:** Danh sách kết quả chi tiết từng mã kèm trạng thái `FOUND`, `NOT_FOUND`, hoặc `CONFLICT`.

### 3.3. API 3 – Tìm kiếm theo Họ tên + Ngày sinh (`GET /api/v1/ca-nhan/tim-kiem`)
- **Tham số Query:** `ho_ten` (bắt buộc) và `ngay_sinh` (tùy chọn).
- **Quy tắc xử lý:**
  - Tự động chuẩn hóa `ho_ten` nhập vào về dạng không dấu viết hoa (`HO_TEN_KHONG_DAU`), tìm kiếm tận dụng chỉ mục B-Tree / Function-based Index.
  - Tham số `ngay_sinh` linh hoạt: chấp nhận dạng năm `YYYY` (ví dụ `1995`), dạng tháng `YYYYMM` (ví dụ `199508`), hoặc dạng ngày `YYYYMMDD` (ví dụ `19950815`). Khớp chính xác cả những người chỉ có năm/tháng sinh rút gọn trên hệ thống.

### 3.4. API 4 – Lấy danh sách bản ghi thay đổi (`GET /api/v1/ca-nhan/thay-doi`)
- **Tham số Query:** `cursor_date`, `cursor_id`, `limit` (mặc định 100, tối đa 1.000).
- **Phân trang an toàn theo con trỏ (Cursor-based):** Kết hợp cặp `(SYNC_DATE, ID)` chống sót bản ghi khi có nhiều bản ghi cùng mili-giây.
- **Cơ chế Độ trễ An toàn (Safe Watermark Lag):**
  - Chỉ trả về các bản ghi có:  
    `SYNC_DATE <= CURRENT_TIMESTAMP - INTERVAL '120' SECOND`
  - Giúp loại trừ hoàn toàn các bản ghi thuộc các lô Near Real-time đang commit dở dang trong cùng thời điểm.

### 3.5. Bảo Mật & Async Batch Audit Log
- **Xác thực:** Mỗi client được cấp 1 `API_KEY`. Phía DB chỉ lưu mã băm SHA-256 trong bảng `C360_API_CLIENTS` kèm các quyền (`scopes: read:single, read:batch, read:search, read:cdc`).
- **Ghi log bất đồng bộ:** Sử dụng Queue trong bộ nhớ (`asyncio.Queue`) và gom lô (batch 100 records hoặc mỗi 3 giây) để ghi vào bảng `C360_API_AUDIT_LOG`, không làm chậm độ trễ phản hồi của API (< 20ms).

---

## 4. Thiết Kế 3 Nhóm Dashboard Báo Cáo BI (Metabase)

Kết nối trực tiếp vào schema qua tài khoản `C360_BI_READONLY` để khai thác các View tổng hợp:

### Nhóm 1: Thống Kê Nhân Khẩu & Định Danh 2025 (`V_BI_THONG_KE_DIA_BAN`)
1. **Bản đồ / Phân bổ dân số:** Tổng số công dân theo 36 Tỉnh/Thành phố và 3.322 Xã/Phường sau sáp nhập năm 2025.
2. **Cơ cấu nhân khẩu học:** Phân bổ theo nhóm tuổi (Dưới 18, 18-35, 36-60, Trên 60), tháp giới tính và tỷ lệ còn sống (`IS_ALIVE = '1'`).
3. **Mức độ phủ CCCD 12 số:** Biểu đồ phần trăm công dân đã có CCCD 12 số chuẩn hóa so với công dân chỉ có CMND 9 số hoặc chưa có định danh trên từng địa phương.

### Nhóm 2: Báo Cáo Chất Lượng Dữ Liệu Data Quality (`V_BI_CHAT_LUONG_DU_LIEU`)
1. **Thống kê theo 11 mã cờ `DQ_FLAGS`:** Số lượng và tỷ lệ % dính từng cờ (`TINH_KHONG_MAP`, `XA_MO_HO`, `TINH_LECH_XA`, `NGAY_SINH_LOI`, `SDT_LOI`...).
2. **Tỷ lệ dữ liệu sạch hoàn toàn:** Tỷ lệ công dân có `DQ_FLAGS IS NULL` đạt bao nhiêu % trên toàn quốc.
3. **So sánh xếp hạng tỉnh có nhiều dữ liệu lỗi:** Xác định các tỉnh/xã có tỷ lệ lỗi địa bàn hoặc ngày sinh cao nhất để ban hành kế hoạch làm sạch dữ liệu nguồn.

### Nhóm 3: Giám Sát Vận Hành Đồng Bộ Near Real-time (`V_BI_GIAM_SAT_DONG_BO`)
1. **Lưu lượng đồng bộ theo thời gian:** Số khóa `MA_CD` được đẩy qua Queue và xử lý trong mỗi phút.
2. **Thống kê thao tác:** Tỷ lệ giữa thao tác `MERGE` (thêm mới/cập nhật) và `DELETE` (hủy công dân khỏi C360).
3. **Độ trễ xử lý (Execution Latency):** Thời gian xử lý trung bình của từng lô (mục tiêu < 5 giây/lô) và danh sách cảnh báo nếu có lô bị quá hạn hoặc gặp exception.

---

## 5. Quy Trình & Kịch Bản Đối Soát 3 Tầng (`reconcile_3_tiers.py`)

Kịch bản đối soát thực hiện tự động qua 4 bước kiểm chứng:
1. **Đối soát Tầng 1 (Nguồn $\leftrightarrow$ Mirror):** So sánh `COUNT(*)` toàn bảng và `COUNT(*)` theo từng `MA_TINH`. Độ lệch cho phép = 0.
2. **Đối soát Tầng 2 (Mirror $\leftrightarrow$ C360):**  
   $$\text{COUNT(DISTINCT MA\_CD)}_{\text{Mirror, Valid}} == \text{COUNT(*)}_{\text{C360}}$$
   với điều kiện Mirror thỏa mãn: `TYPEID NOT IN ('GT','GM','GG','GK')` và `TRANG_THAI_DUYET NOT IN (1,3,4)`.
3. **Kiểm tra mẫu tính nhất quán (Consistency Sample Test):**
   - Lấy ngẫu nhiên 100 công dân có nhiều hộ trên Mirror, đối chiếu thông tin địa chỉ/CCCD trên C360 xem có khớp 100% với dòng có `MODIFY_DATE` mới nhất không.
   - Quét toàn bộ C360 xem có bất kỳ dòng nào mang `TYPEID` giảm hoặc hồ sơ chưa duyệt không (kết quả mong đợi: 0 bản ghi).
4. **Kết xuất báo cáo JSON:** Ghi nhận toàn bộ thông số vào [`data/reconciliation_report.json`](../data/reconciliation_report.json).

---

## 6. Danh Sách Script & File Dự Kiến Triển Khai Tuần 3

```text
p3_reconciliation_bi_api/
├── README.md                           # Tài liệu chi tiết tuần 3 (file hiện tại)
├── reconcile_3_tiers.py                # Script đối soát tự động 3 tầng (Nguồn - Mirror - C360)
├── views_bi_datamart.sql               # DDL 3 Views Data Mart phục vụ Metabase BI
├── metabase_setup.py                   # Script khởi tạo Dashboard & Card trên Metabase tự động
└── api/
    ├── __init__.py
    ├── main.py                         # Ứng dụng FastAPI chính với 4 Endpoints
    ├── auth.py                         # X-API-Key SHA-256 verification & Scopes
    ├── audit.py                        # Async Batch Audit Logger vào C360_API_AUDIT_LOG
    └── schemas.py                      # Pydantic Schemas cho Request & Response
```

