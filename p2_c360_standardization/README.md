# TUẦN 2: CHUẨN HÓA DỮ LIỆU CÔNG DÂN (C360) & ĐỒNG BỘ NEAR REAL-TIME (MIRROR -> C360)

> **Trạng thái:** `HOÀN THÀNH 100% (Passed Toàn Bộ Test Suite & Triển Khai Thực Tế)`  
> **Thư mục quản lý:** `p2_c360_standardization/`  
> **Tài liệu thiết kế liên kết:** [`design/ThietKeChiTiet_DetailDesign_HGD_C360.md`](../design/ThietKeChiTiet_DetailDesign_HGD_C360.md) (Mục 2: Chi tiết xử lý C360, Mục 4: Bảng lỗi DQ_FLAGS & Sheet QA)  
> **Kết quả nghiệm thu:** `pytest p4_airflow_and_e2e_test/tests/test_week2_p2.py` (3 passed in 2.00s)

---

## 1. Mục tiêu và Phạm vi Tuần 2

Tuần 2 đảm nhận nhiệm vụ chuyển đổi dữ liệu thô, đa dòng và chứa lỗi từ vùng Mirror sang bảng chuẩn hóa thông tin cá nhân công dân duy nhất (**Customer 360 - C360**). Trọng tâm gồm 4 nhóm giải pháp:
1. **Thiết kế View danh mục hiệu lực & 2 Bảng Map Địa bàn hành chính (`C360_MAP_DBHC_TINH`, `C360_MAP_DBHC_XA`):**
   - Tách chuỗi `MA_DBHC_SN` (nhiều mã phân cách bởi dấu `;`) thành bảng tra cứu phẳng có chỉ mục `PRIMARY KEY`.
   - Phân bổ theo 3 tầng ưu tiên map: `HIEN_HANH` (Ưu tiên 1), `SAP_NHAP` (Ưu tiên 2) và `MO_HO` (Ưu tiên 3 - xã tách ra $\ge 2$ xã mới).
   - Cơ chế làm mới bảng Map bằng `DELETE + INSERT` trong 1 transaction an toàn, không dùng `TRUNCATE`.
2. **Thiết kế Bảng đích chuẩn hóa `C360_THONG_TIN_CA_NHAN` & Các bảng phụ trợ:**
   - Đảm bảo tính duy nhất: 1 công dân chỉ có duy nhất 1 dòng (`MA_CD` là khóa chính/Unique).
   - Lập bảng hàng đợi `C360_SYNC_QUEUE`, bảng điều khiển `C360_SYNC_CONTROL` và bảng nhật ký `C360_SYNC_LOG`.
3. **Bộ Quy tắc Chuẩn hóa Dùng Chung (Single Source of Truth):**
   - Dùng chung 1 logic xử lý cho cả chạy Batch Full Load và chạy Near Real-time (tránh lệch kết quả).
   - Khử trùng theo `MA_CD` (chọn dòng mới nhất theo `MODIFY_DATE` -> `CREATED_DATE` -> `ID`).
   - Bộ lọc nghiệp vụ loại bỏ hồ sơ giảm (`TYPEID IN ('GT','GM','GG','GK')`) và hồ sơ chưa duyệt/hủy (`TRANG_THAI_DUYET IN (1,3,4)`).
   - Chuẩn hóa 3 cụm địa chỉ (Thường trú, Liên hệ, Nơi khai sinh), cờ `TINH_LECH_XA`, giữ nguyên giá trị gốc `*_GOC`.
   - Làm sạch Họ tên, Ngày sinh (`YYYYMMDD`/`YYYYMM`/`YYYY`), Số định danh 12 số, Số điện thoại (đầu `0`), Dân tộc và Quốc tịch.
   - Gán chuỗi mã cờ chất lượng dữ liệu `DQ_FLAGS` (11 mã cờ chuẩn hóa).
4. **Cơ chế Cập nhật Kép (Full Load & Near Real-time qua Queue):**
   - **Job Full Load:** Tải lại toàn bộ dữ liệu C360 với hiệu năng cao.
   - **Job Near Real-time (chạy mỗi phút):** Bắt sự kiện thay đổi từ Mirror thông qua Trigger siêu nhẹ, cắt lô an toàn `v_cut`, `MERGE` các công dân hợp lệ và `DELETE` các công dân bị xóa hoặc mất hiệu lực khỏi C360.
   - **Xử lý Đua tranh (Concurrency Control):** Cơ chế khóa dòng `SELECT ... FOR UPDATE` trên bảng `C360_SYNC_CONTROL` ngăn xung đột giữa Full Load và Near Real-time.

---

## 2. Kiến trúc Xử lý Dữ liệu Tuần 2

```
+---------------------------------------------------------------------------------------------------+
|                            TUẦN 2: CHUẨN HÓA C360 & NEAR REAL-TIME ENGINE                         |
+---------------------------------------------------------------------------------------------------+
|                                                                                                   |
|  [VÙNG MIRROR: MIR_VSS_*]                                                                         |
|   ├── MIR_VSS_DM_DBHC ---------> [build_map_dbhc.py] ------> [C360_MAP_DBHC_TINH] (36 Tỉnh 2025)  |
|   │                               (DELETE + INSERT)   ------> [C360_MAP_DBHC_XA]   (3.322 Xã)     |
|   │                                                                |                              |
|   └── MIR_VSS_HO_GD_NHANKHAU                                       | (Lookup Map)                 |
|        │                                                           |                              |
|        ├── (Cách 1: Batch Full Load) ------------------------------+                              |
|        │    c360_etl_full_load.py                                  |                              |
|        │    - Khử trùng MA_CD (MODIFY_DATE -> CREATED_DATE -> ID)  |                              |
|        │    - Lọc TYPEID & TRANG_THAI_DUYET                        |                              |
|        │    - Chuẩn hóa CCCD, Dob, SĐT, Dân tộc, Quốc tịch, ĐC     |                              |
|        │    - Gán 11 cờ DQ_FLAGS                                   v                              |
|        │    ------------------------------------------------> [C360_THONG_TIN_CA_NHAN]            |
|        │                                                      (1 dòng duy nhất/công dân)          |
|        │                                                           ^                              |
|        └── (Cách 2: Near Real-time mỗi phút)                       | (MERGE / DELETE)             |
|             │                                                      |                              |
|             ├── [Trigger TRG_MIR_NK_QUEUE] (INSERT :NEW/:OLD)      |                              |
|             │         |                                            |                              |
|             │         v                                            |                              |
|             │   [C360_SYNC_QUEUE] (Chỉ chứa ID, MA_CD, STATUS)     |                              |
|             │         |                                            |                              |
|             │         v                                            |                              |
|             └── [c360_nrt_worker.py] (Mỗi phút 1 lần) -------------+                              |
|                 - Cắt lô snapshot: v_cut = MAX(ID)                                                |
|                 - SELECT ... FOR UPDATE trên C360_SYNC_CONTROL                                    |
|                 - Tái xử lý danh sách MA_CD biến động                                             |
|                 - Ghi log vào C360_SYNC_LOG                                                       |
+---------------------------------------------------------------------------------------------------+
```

---

## 3. Đặc tả Kỹ thuật & Bảng Thiết kế Tuần 2

### 3.1. Thiết kế 2 Bảng Map Địa bàn hành chính (`C360_MAP_DBHC_*`)

Sau sáp nhập năm 2025, cấp Huyện được bãi bỏ, chỉ còn **36 Tỉnh/Thành phố** và **3.322 Xã/Phường/Đặc khu**.

#### A. Bảng Map Tỉnh: `C360_MAP_DBHC_TINH`
- `MA_TINH_GOC` (PK): Mã tỉnh cũ hoặc hiện hành (2 chữ số).
- `MA_TINH_CHUAN`: Mã tỉnh mới 3 chữ số (ví dụ `'01'` -> `'001'`, `'33'` -> `'033'`).
- `TEN_TINH_CHUAN`: Tên tỉnh chuẩn hóa.
- `NGUON_MAP`: `'HIEN_HANH'` (mã đang dùng) hoặc `'SAP_NHAP'` (mã tỉnh cũ sáp nhập vào tỉnh mới, ví dụ Thái Bình `'34'` sáp nhập vào Hưng Yên `'33'`).

#### B. Bảng Map Xã: `C360_MAP_DBHC_XA`
- `MA_XA_GOC` (PK): Mã xã cũ hoặc hiện hành (5 chữ số).
- `MA_XA_CHUAN`: Mã xã mới chuẩn hóa (NULL nếu rơi vào diện `MO_HO`).
- `TEN_XA_CHUAN`: Tên xã mới.
- `MA_TINH_CHUAN`: Mã tỉnh trực thuộc của xã mới (3 chữ số).
- `NGUON_MAP`:
  - `HIEN_HANH`: Mã xã đang hiệu lực.
  - `SAP_NHAP`: Mã xã cũ được sáp nhập 1 - 1 vào duy nhất 1 xã mới.
  - `MO_HO`: Mã xã cũ bị tách ra $\ge 2$ xã mới. Khi đó gán `MA_XA_CHUAN = NULL`. Nếu các xã mới cùng thuộc 1 tỉnh thì giữ `MA_TINH_CHUAN`, nếu khác tỉnh thì cả 2 đều `NULL`.

### 3.2. Bảng Đích: `C360_THONG_TIN_CA_NHAN`

| Cột | Kiểu dữ liệu | Ràng buộc | Mô tả |
| :--- | :--- | :--- | :--- |
| `MA_CD` | VARCHAR2(30) | PRIMARY KEY | Mã số BHXH / Mã công dân hiện hành |
| `MA_CD_OLD` | VARCHAR2(30) | INDEX | Mã số cũ (nếu có biến động đổi mã) |
| `SO_DINH_DANH` | VARCHAR2(12) | INDEX | Số định danh cá nhân 12 chữ số chuẩn hóa |
| `HO_TEN` | VARCHAR2(250) | NOT NULL | Họ tên đã cắt bỏ khoảng trắng thừa |
| `HO_TEN_KHONG_DAU` | VARCHAR2(250) | INDEX | Họ tên không dấu viết hoa (phục vụ API tìm kiếm) |
| `GIOI_TINH` | CHAR(1) | | 1: Nam, 2: Nữ |
| `NGAY_SINH` | VARCHAR2(8) | INDEX | Chuẩn hóa: `YYYYMMDD`, `YYYYMM` hoặc `YYYY` |
| `NGAY_SINH_GOC` | NUMBER | | Lưu giá trị gốc kiểu số để đối soát |
| `SO_CCCD` | VARCHAR2(50) | | Số CCCD gốc từ nguồn |
| `SO_CMTND` | VARCHAR2(50) | | Số CMTND gốc từ nguồn |
| `PHONE_NUMBER` | VARCHAR2(15) | | Số điện thoại chuẩn hóa đầu `0` (10-11 số) |
| `MA_TINH_TT` | VARCHAR2(3) | | Mã tỉnh thường trú chuẩn hóa (3 chữ số) |
| `MA_XA_TT` | VARCHAR2(5) | | Mã xã thường trú chuẩn hóa (5 chữ số) |
| `DIA_CHI_TT_FULL` | VARCHAR2(500) | | Địa chỉ thường trú đầy đủ |
| `MA_TINH_LH` | VARCHAR2(3) | | Mã tỉnh liên hệ chuẩn hóa |
| `MA_XA_LH` | VARCHAR2(5) | | Mã xã liên hệ chuẩn hóa |
| `MA_TINH_KS` | VARCHAR2(3) | | Mã tỉnh nơi khai sinh chuẩn hóa |
| `MA_XA_KS` | VARCHAR2(5) | | Mã xã nơi khai sinh chuẩn hóa |
| `DM_DAN_TOC_ID` | NUMBER(19) | | ID dân tộc join từ danh mục |
| `TEN_DAN_TOC` | VARCHAR2(100) | | Tên dân tộc chuẩn hóa |
| `QUOC_TICH_MA` | VARCHAR2(10) | | Mã quốc tịch chuẩn (VD: 'VN') |
| `TEN_QUOC_TICH` | VARCHAR2(100) | | Tên quốc tịch (VD: 'Việt Nam') |
| `IS_ALIVE` | CHAR(1) | | 1: Còn sống, 0: Đã mất |
| `DEAD_DATE` | NUMBER | | Ngày mất (nếu có) |
| `SOURCE_MA_HO_GD` | VARCHAR2(30) | | Mã hộ gia đình mới nhất được chọn |
| `SOURCE_MODIFY_DATE`| DATE | | Ngày cập nhật mới nhất ở nguồn |
| `DQ_FLAGS` | VARCHAR2(500) | INDEX | Chuỗi các cờ lỗi chất lượng dữ liệu |
| `SYNC_DATE` | TIMESTAMP | INDEX | Mốc thời gian đồng bộ vào C360 |
| `SYNC_BATCH_ID` | NUMBER(19) | | ID của lô Batch/NRT đồng bộ |

---

## 4. Ma Trận 11 Mã Cờ Chất Lượng Dữ Liệu (`DQ_FLAGS`)

Các bản ghi có dữ liệu lỗi vẫn được lưu trữ tại bảng C360 nhưng được đánh dấu tại cột `DQ_FLAGS` (ngăn cách bởi dấu `;`):

| STT | Mã cờ lỗi | Mức độ | Điều kiện kích hoạt | Hành động xử lý |
| :---: | :--- | :---: | :--- | :--- |
| 1 | `TINH_KHONG_MAP` | ERROR | Mã tỉnh không tìm thấy trong `C360_MAP_DBHC_TINH` | Để NULL mã tỉnh chuẩn |
| 2 | `XA_KHONG_MAP` | ERROR | Mã xã không tìm thấy trong `C360_MAP_DBHC_XA` | Để NULL mã xã chuẩn |
| 3 | `XA_MO_HO` | WARNING | Mã xã cũ bị tách ra $\ge 2$ xã mới (`NGUON_MAP = 'MO_HO'`) | Để NULL xã, giữ tỉnh nếu cùng tỉnh |
| 4 | `TINH_LECH_XA` | WARNING | Tỉnh suy ra từ Xã khác với tỉnh khai báo tại `MA_TINH` | Ưu tiên Tỉnh theo Xã, gắn cờ lệch |
| 5 | `NGAY_SINH_LOI` | ERROR | Ngày sai lịch (VD: 30/02) hoặc năm sinh < 1850 | Để NULL `NGAY_SINH`, giữ gốc |
| 6 | `SDT_LOI` | WARNING | SĐT không đúng định dạng VN (< 9 số, ký tự lạ) | Để NULL `PHONE_NUMBER`, giữ gốc |
| 7 | `KHONG_SO_DINH_DANH`| WARNING| Cả CCCD và CMTND đều không có dãy 12 số hợp lệ | Để NULL `SO_DINH_DANH` |
| 8 | `CANH_BAO_CMND9` | INFO | Chỉ có CMND 9 chữ số cũ | Cảnh báo chưa đổi sang CCCD |
| 9 | `DANTOC_KHONG_MAP` | WARNING | `DM_DAN_TOC_ID` không tồn tại hoặc đã hết hiệu lực | Để NULL tên dân tộc |
| 10 | `QUOCTICH_KHONG_MAP`| WARNING | Mã hoặc ID quốc tịch không tìm thấy trong danh mục | Để NULL tên quốc tịch |
| 11 | `HOTEN_THUA_KHOANG_TRANG` | INFO | Họ tên gốc có khoảng trắng thừa đầu/giữa/cuối | Tự động trim & chuẩn hóa 1 space |

---

## 5. Danh Sách Script & File Dự Kiến Triển Khai Tuần 2

```text
p2_c360_standardization/
├── README.md                           # Tài liệu chi tiết tuần 2 (file hiện tại)
├── ddl_c360_schema.sql                 # DDL tạo bảng Map, bảng C360, Queue, Control & Log
├── build_map_dbhc.py                   # Script sinh 2 bảng Map DBHC (DELETE + INSERT trong TX)
├── db_c360_cleaner.py                  # Module logic chuẩn hóa dùng chung (Cleaners & Validators)
├── c360_etl_full_load.py               # Job chạy Full Load toàn bộ C360
├── c360_nrt_worker.py                  # Worker Near Real-time đọc Queue mỗi phút (MERGE / DELETE)
└── __init__.py
```

---

## 6. Kế Hoạch Kiểm Thử Nghiệm Thu Tuần 2

1. **Kiểm thử Bảng Map (`test_map_dbhc`):**
   - Đảm bảo 36 tỉnh và 3.322 xã được map chính xác 100%.
   - Kiểm tra mã xã `'00999'` kích hoạt đúng cờ `XA_MO_HO` và giữ được mã tỉnh Hà Nội `'001'`.
   - Kiểm tra mã xã `'00998'` kích hoạt đúng cờ `XA_MO_HO` và cả tỉnh lẫn xã đều là `NULL`.
2. **Kiểm thử Khử trùng & Lọc nghiệp vụ (`test_c360_dedup_and_filter`):**
   - Với 52.500 dòng nguồn, sau khi lọc `TYPEID` (1.000) và `TRANG_THAI_DUYET` (1.000), còn đúng **48.000 công dân hợp lệ** trên C360.
   - 2.500 công dân có nhiều hộ phải lấy chính xác dòng có `MODIFY_DATE` mới nhất.
   - 500 công dân hòa `MODIFY_DATE` và `CREATED_DATE` phải lấy chính xác dòng có `ID` lớn nhất.
3. **Kiểm thử Near Real-time với Delta Changes (`test_c360_nrt_pipeline`):**
   - Chạy `gen_delta_changes.py` ở nguồn -> kiểm tra Queue nhận đủ sự kiện.
   - Kích hoạt `c360_nrt_worker.py` -> kiểm chứng bản ghi C360 được cập nhật ngay trong vòng < 60 giây.
   - Đổi `MA_CD`: Mã cũ bị xóa hoàn toàn khỏi C360, mã mới xuất hiện với `MA_CD_OLD` được ghi nhận.
   - Chuyển `TYPEID = 'GG'` hoặc xóa toàn bộ dòng nguồn: Công dân lập tức biến mất khỏi C360.

