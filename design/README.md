# Bộ Tài Liệu Thiết Kế Hệ Thống (`HGD -> Mirror -> C360 -> BI & API`)

Thư mục này chứa bộ tài liệu **Thiết kế cơ bản (Basic Design)** và **Thiết kế chi tiết (Detail Design)** chuẩn hóa cho dự án **Xây dựng Pipeline Dữ liệu Hộ gia đình - Nhân khẩu (`HDG -> Mirror -> C360 -> BI & API`)**, đồng bộ 100% giữa định dạng Markdown (`.md`) phục vụ kiểm soát phiên bản trên Git và định dạng bảng tính Excel (`.xlsx`) phục vụ trình duyệt nghiệm thu.

## 1. Danh mục tài liệu

| STT | Tên file | Mã tài liệu | Mô tả |
| --- | --- | --- | --- |
| 1 | `ThietKeCoBan_BasicDesign_HGD_C360.md` | `C360-BD-001` | Thiết kế cơ bản (15 sheets/mục): Tổng quan, Kiến trúc 3 tầng, Luồng nghiệp vụ, Danh sách Chức năng/Màn hình BI/API/Batch, Danh sách Bảng & Định nghĩa Bảng vật lý (Nguồn `VSS`, `MIRROR`, `C360`), Phi chức năng, Danh sách Thông báo và **Sheet QA** giải trình kết quả khảo sát dữ liệu danh mục thực tế. |
| 2 | `ThietKeCoBan_BasicDesign_HGD_C360.xlsx` | `C360-BD-001` | Bản Excel 15 sheets được sinh tự động từ `ThietKeCoBan_BasicDesign_HGD_C360.md`. |
| 3 | `ThietKeChiTiet_DetailDesign_HGD_C360.md` | `C360-DD-001` | Thiết kế chi tiết (11 sheets/mục): Đặc tả I/O, Thuật toán xử lý từng bước (`1`, `1.1`, `1.2`...), Đặc tả SQL 5 trường hợp kết quả (`SQL-C360-001..012`), Ma trận Validation & 11 mã `DQ_FLAGS`, Xử lý Ngoại lệ/Concurrency, Kịch bản Unit Test (`UT-P1..P4`) và **Sheet QA** kỹ thuật. |
| 4 | `ThietKeChiTiet_DetailDesign_HGD_C360.xlsx` | `C360-DD-001` | Bản Excel 11 sheets được sinh tự động từ `ThietKeChiTiet_DetailDesign_HGD_C360.md`. |
| 5 | `generate_excel_designs.py` | - | Script Python chuyển đổi tự động từ 2 file `.md` sang 2 file `.xlsx` có định dạng bảng biểu, màu sắc tiêu đề và độ rộng cột chuẩn. |

## 2. Điểm nhấn khảo sát dữ liệu thực tế (Sheet QA)

Qua quá trình khảo sát trực tiếp 3 file CSV danh mục giải nén (`DM_DBHC_202611091512.csv`, `DM_DAN_TOC_202611091513.csv`, `DM_QUOC_TICH_202611091512.csv`), các khác biệt giữa tài liệu mô tả sơ bộ và dữ liệu thực tế đã được ghi nhận tại **Sheet `QA`** và xử lý triệt để trong thiết kế:
1. **`DM_DBHC` (`15.325` dòng):**
   - Có **36 đơn vị cấp Tỉnh đang hiệu lực** (`NGAY_HET_HIEU_LUC = '2099-12-31...'`, `TINH_TRANG = '1'`, `IS_CAP_XA = '0'`, `IS_LT = '1'`), có `MA_DBHC` gồm **2 chữ số** (`'01'`, `'04'`, ..., `'33'`, `'79'`, `'96'`, `'97'`, `'98'`) và `MA_CHA = '0003'` (không phải `'01TTT'` và `MA_CHA IS NULL` như ví dụ minh họa).
   - Có **3.322 đơn vị cấp Xã/Phường/Đặc khu đang hiệu lực** (`IS_CAP_XA = '1'`), có `MA_DBHC` 5 chữ số và `MA_CHA` 2 chữ số trỏ trực tiếp lên 36 tỉnh.
   - Bổ sung cơ chế chuẩn hóa mã Tỉnh 5 ký tự kiểu `'01TTT'`/`'79TTT'` về mã 2 ký tự (`SUBSTR(TRIM(MA_TINH), 1, 2)`) trước khi tra cứu bảng `C360_MAP_DBHC_TINH`.
   - Trong CSV gốc hiện có `0` mã xã cũ bị trùng lặp (`MO_HO`). Script nạp danh mục (`load_danhmuc.py`) hỗ trợ tiêm có kiểm soát 2 mã xã cũ bị tách đôi (`'00999'` cùng tỉnh `'01'` và `'00998'` khác tỉnh) để kiểm thử đầy đủ nhánh Ưu tiên 3 (`MO_HO`).
2. **`DM_DAN_TOC` (`59` dòng):**
   - Khóa chính `ID` thực tế nằm trong dải **`171..225`** và **`246, 248, 249, 250`** (Dân tộc Kinh có `ID = 171`, `MA = '01'`; dòng `ID = 248` đã hết hiệu lực). Script sinh dữ liệu sử dụng đúng dải `ID` thực tế này.
3. **`DM_QUOC_TICH` (`243` dòng):**
   - Việt Nam có `ID = 233`, `MA = 'VN'`, `TEN = 'Việt Nam'`.

## 3. Lệnh cập nhật lại file Excel khi chỉnh sửa Markdown

```powershell
python design/generate_excel_designs.py
```
