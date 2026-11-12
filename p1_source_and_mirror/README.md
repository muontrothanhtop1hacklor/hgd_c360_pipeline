# TUẦN 1: KHẢO SÁT, THIẾT KẾ, SINH DỮ LIỆU GIẢ LẬP & ĐỒNG BỘ NGUỒN -> MIRROR

> **Trạng thái:** `HOÀN THÀNH 100% (Passed Toàn Bộ Unit & Integration Tests)`  
> **Thư mục quản lý:** `p1_source_and_mirror/`  
> **Tài liệu thiết kế liên kết:** [`design/ThietKeCoBan_BasicDesign_HGD_C360.md`](../design/ThietKeCoBan_BasicDesign_HGD_C360.md) & [`design/ThietKeChiTiet_DetailDesign_HGD_C360.md`](../design/ThietKeChiTiet_DetailDesign_HGD_C360.md)

---

## 1. Mục tiêu và Phạm vi Tuần 1

Tuần 1 tập trung xây dựng nền tảng vững chắc cho toàn bộ hệ thống pipeline Hộ gia đình – Nhân khẩu (`HGD -> C360 -> BI & API`), giải quyết trọn vẹn 5 nhiệm vụ cốt lõi:
1. **Khảo sát dữ liệu thực tế & Chốt tài liệu thiết kế:** Phân tích 3 file CSV danh mục thực tế (Dân tộc, Quốc tịch, Địa bàn hành chính 2025 sau sáp nhập), hoàn thiện bộ tài liệu Thiết kế cơ bản (15 sheets) và Thiết kế chi tiết (11 sheets) kèm Sheet `QA` giải trình kỹ thuật.
2. **Khởi tạo Schema & Nạp Danh mục (`load_danhmuc.py`):** Nạp đầy đủ 3 bảng danh mục nguồn `VSS_DM_*`, hỗ trợ cơ chế tiêm có kiểm soát dữ liệu mơ hồ (`MO_HO`) để kiểm thử.
3. **Sinh dữ liệu nguồn ban đầu - Initial Seed Generator (`gen_seed_nhankhau.py`):** Sinh giả lập 50.000 công dân (~52.500 dòng nhân khẩu `VSS_HO_GD_NHANKHAU`) bao phủ 100% các kịch bản sạch, bẩn và biên.
4. **Đồng bộ toàn lượng Full Load JDBC (`full_load_jdbc.py`):** Đồng bộ 4 bảng từ Nguồn sang vùng Mirror (`MIR_VSS_*`) với phân đoạn song song theo tiền tố `MA_CD`, ép kiểu tường minh, quản lý Unique Index và ghi nhận mốc SCN bàn giao CDC.
5. **Giả lập biến động Delta Changes & Cấu hình Debezium CDC (`gen_delta_changes.py` & `debezium/`):** Tạo đủ các kịch bản `INSERT`, `UPDATE`, đổi `MA_CD`, `DELETE` và chuẩn hóa cấu hình Debezium Source/Sink.

---

## 2. Kiến trúc & Luồng Dữ liệu Tuần 1

```
+---------------------------------------------------------------------------------------------------+
|                                        TUẦN 1: NGUỒN -> VÙNG MIRROR                               |
+---------------------------------------------------------------------------------------------------+
|                                                                                                   |
|  [3 File CSV Danh mục]                                                                            |
|   - DM_DAN_TOC.csv (59 dòng)                                                                      |
|   - DM_QUOC_TICH.csv (243 dòng)        --> [load_danhmuc.py] ---> [Bảng Nguồn VSS_DM_*]           |
|   - DM_DBHC.csv (15.325 dòng)                                           |                         |
|                                                                          |                         |
|  [Bộ Sinh Dữ Liệu Nguồn]                                                 | (Full Load JDBC)        |
|   - gen_seed_nhankhau.py (50k công dân) -> [VSS_HO_GD_NHANKHAU] --------+                         |
|   - gen_delta_changes.py (Delta events)         |                        |                         |
|                                                 |                        v                         |
|                                                 | (CDC Stream)    [Vùng Mirror: MIR_VSS_*]        |
|                                                 +---------------> - MIR_VSS_DM_DANTOC             |
|                                                 | (Debezium)      - MIR_VSS_DM_QUOC_TICH          |
|                                                 |                 - MIR_VSS_DM_DBHC               |
|                                                 v                 - MIR_VSS_HO_GD_NHANKHAU        |
|                                         [Kafka Topics]            - MIR_CDC_SCN_HANDOFF           |
|                                          - vss.nhankhau                  |                         |
|                                                 | (Sink Connector)       v                         |
|                                                 +----------------> [Sẵn sàng cho Tuần 2]          |
|                                                                    (Trigger & C360_SYNC_QUEUE)     |
+---------------------------------------------------------------------------------------------------+
```

---

## 3. Chi tiết các Module Thực hiện

### 3.1. Nạp danh mục thực tế (`load_danhmuc.py`)
- **Nguồn dữ liệu:** Đọc trực tiếp từ thư mục `data/danhmuc/`.
- **Số lượng bản ghi nạp thực tế:**
  - `VSS_DM_DANTOC`: **59** dòng (Dân tộc Kinh `ID = 171`, dải ID thực tế `171..250`).
  - `VSS_DM_QUOC_TICH`: **243** dòng (Việt Nam `ID = 233`, `MA = 'VN'`).
  - `VSS_DM_DBHC`: **15.325** dòng (36 Tỉnh/Thành phố đang hiệu lực sau sáp nhập 2025, 3.322 Xã/Phường đang hiệu lực).
- **Cơ chế tiêm mã mơ hồ (`inject_moho`):** Tự động tiêm 2 mã xã cũ bị tách vào nhiều xã mới (`'00999'` cùng tỉnh Hà Nội và `'00998'` liên tỉnh) nhằm phục vụ kiểm chứng quy tắc Ưu tiên 3 (`MO_HO`) ở Tuần 2.

### 3.2. Script 1 – Sinh dữ liệu ban đầu Initial Seed (`gen_seed_nhankhau.py`)
Sinh bộ dữ liệu chuẩn mẫu **50.000 công dân** (`num_persons_total`), phát sinh **52.500 dòng nhân khẩu** (`VSS_HO_GD_NHANKHAU`), xuất báo cáo kiểm chứng tại [`data/seed_ground_truth.json`](../data/seed_ground_truth.json):

| Nhóm kịch bản dữ liệu | Số lượng cài cắm | Mô tả kỹ thuật & Ý nghĩa nghiệp vụ |
| :--- | :---: | :--- |
| **Công dân hợp lệ dự kiến vào C360** | **48.000** | Tập dữ liệu sạch, hợp lệ về mặt nghiệp vụ sẽ lên bảng chuẩn hóa C360 |
| **Trùng công dân ở nhiều hộ** | **2.500** | 1 `MA_CD` xuất hiện ở 2 hộ khác nhau với `MODIFY_DATE` khác nhau |
| **Trùng ngày giờ sửa (Hòa ID)** | **500** | `MODIFY_DATE` và `CREATED_DATE` bằng nhau, kiểm thử tie-breaker bằng `ID` |
| **Hồ sơ bị loại trừ bởi TYPEID** | **1.000** | Thuộc diện giảm: `'GT'` (tử), `'GM'` (mất tích), `'GG'` (gộp), `'GK'` (khác) |
| **Hồ sơ chưa duyệt / Từ chối / Hủy** | **1.000** | `TRANG_THAI_DUYET IN (1, 3, 4)` để kiểm tra bộ lọc không đẩy lên C360 |
| **Mã Tỉnh dạng `NNTTT`** | **2.500** | Mã tỉnh có đuôi `'TTT'` (ví dụ `'01TTT'`, `'33TTT'`, `'79TTT'`) |
| **Mã Xã sáp nhập 1 - 1** | **4.000** | Mã xã cũ nằm trong `MA_DBHC_SN` của duy nhất 1 xã mới |
| **Mã Xã mơ hồ cùng Tỉnh (`00999`)** | **500** | Xã cũ bị tách sang 2 xã mới cùng Tỉnh Hà Nội (giữ tỉnh, xã = NULL) |
| **Mã Xã mơ hồ khác Tỉnh (`00998`)** | **500** | Xã cũ bị tách sang 2 xã mới ở 2 tỉnh khác nhau (tỉnh & xã = NULL) |
| **Tỉnh lệch Xã** | **1.000** | Xã thuộc Tỉnh A nhưng cột `MA_TINH` khai báo Tỉnh B (ưu tiên suy từ Xã) |
| **Suy diễn Tỉnh từ Xã** | **1.000** | Dòng bị khuyết/sai `MA_TINH` nhưng `MA_XA` đúng, tự động suy ra Tỉnh |
| **Địa chỉ sai / không map được** | **1.000** | Mã xã `'99999'`, mã tỉnh `'99TTT'` không tồn tại trong danh mục |
| **Ngày sinh chuẩn `YYYYMMDD`** | **43.500** | Ngày sinh 8 chữ số đúng lịch thiên văn |
| **Ngày sinh dạng chuỗi ngày `/` hoặc `-`** | **2.500** | Lưu dạng `'DD/MM/YYYY'` hoặc `'YYYY-MM-DD'` |
| **Chỉ có năm sinh (`YYYY0000` / 4 số)** | **1.000** | Chuẩn hóa về chuỗi 4 ký tự `YYYY` |
| **Ngày sinh sai lịch / Ngoài khoảng** | **500** | Ngày 30/02 (`19950230`) hoặc năm sinh bất thường (< 1850) |
| **CCCD đủ 12 chữ số hợp lệ** | **45.500** | Đúng format 12 ký tự số |
| **CCCD lấy từ cột `SO_CMTND` 12 số** | **1.000** | Cột `SO_CCCD` trống nhưng `SO_CMTND` lưu đủ 12 số |
| **Chỉ có CMND 9 chữ số** | **1.000** | Cảnh báo thiếu số định danh cá nhân (`CANH_BAO_CMND9`) |
| **Số điện thoại chuẩn / Quốc tế / Lỗi** | **48.000** | Bao gồm: chuẩn đầu 0, đầu `+84` / `84`, số sai < 9 chữ số |
| **Dân tộc hết hạn (`ID=248`) / Sai** | **1.000** | Kiểm tra join danh mục theo thời gian hiệu lực |
| **Quốc tịch dạng ID / Mã / Sai** | **2.000** | Hỗ trợ nguồn lưu `'VN'`, lưu ID `'233'` hoặc chuỗi sai `'XX'` |
| **Họ tên thừa khoảng trắng** | **2.500** | Khoảng trắng đầu/giữa/cuối chuỗi để kiểm tra trim & upper |

### 3.3. Job Nạp toàn bộ Full Load JDBC (`full_load_jdbc.py`)
- **Phân mảnh đọc song song (10 Partitions):** Cột `MA_CD` là chuỗi, chia 10 dải theo ký tự đầu (`MA_CD LIKE '0%'` .. `MA_CD LIKE '9%'`), tận dụng chỉ mục `IDX_VSS_NK_MACD`.
- **Ép kiểu tường minh (`CAST`):** Chống lỗi Spark/SQLite suy diễn `NUMBER` thành `Decimal(38,10)` gây tràn bộ nhớ.
- **Không dùng `.cache()` / `.persist()` toàn tập:** Truyền stream trực tiếp theo từng batch (`batch_size = 5000`) nhằm bảo vệ RAM dưới 300MB.
- **Chiến lược Unique Index:** Tự động Drop/Disable `UK_MIR_NK (MA_CD, MA_HO_GD)` trên Mirror trước khi nạp và Rebuild lại sau khi nạp xong.
- **Bàn giao SCN cho Debezium CDC (`MIR_CDC_SCN_HANDOFF`):** Chốt mốc SCN `1794466529406500` ngay tại thời điểm snapshot.
- **Báo cáo đối soát Full Load:** Xuất ra [`data/full_load_mirror_report.json`](../data/full_load_mirror_report.json):
  - `VSS_HO_GD_NHANKHAU`: **52.500** = `MIR_VSS_HO_GD_NHANKHAU`: **52.500** (Lệch: **0**, Status: `PASSED`).
  - Phân bổ 10 partition: `part_0`: 6.000, `part_1`: 5.000, `part_2`: 5.000, `part_3`: 5.500, `part_4`: 5.000, `part_5`: 6.000, `part_6`: 5.000, `part_7`: 5.000, `part_8`: 5.000, `part_9`: 5.000.

### 3.4. Script 2 – Giả lập biến động Delta Changes (`gen_delta_changes.py`)
Mô phỏng đầy đủ mọi tình huống biến động phát sinh trực tiếp tại nguồn, xuất ra [`data/delta_ground_truth.json`](../data/delta_ground_truth.json):
1. **`INSERT` nhân khẩu mới:** Thêm 5 công dân mới (`9900000001` .. `9900000005`).
2. **`INSERT` hộ mới cho công dân cũ:** Thêm hộ mới với `MODIFY_DATE` mới hơn cho công dân đã có (`0000000004` .. `0000000008`).
3. **`UPDATE` thông tin sạch lên bẩn & bẩn về sạch:** Sửa CCCD, sửa địa chỉ, sửa ngày sinh để kiểm tra việc gán/gỡ cờ `DQ_FLAGS`.
4. **`UPDATE` loại trừ & phục hồi:** Đổi `TYPEID = 'GG'` hoặc `TRANG_THAI_DUYET = 4` để loại khỏi C360, và duyệt hồ sơ `1 -> 2` để đưa vào C360.
5. **`UPDATE` đổi mã công dân (`MA_CD`):** Đổi `MA_CD` mới đồng thời lưu `MA_CD_OLD` để kiểm tra việc xóa mã cũ và thêm mã mới trên C360.
6. **`DELETE` dòng nhân khẩu:** Xóa 1 hộ trong số nhiều hộ và xóa toàn bộ các dòng của một công dân.

### 3.5. Cấu hình Debezium CDC (`debezium/`)
- `oracle-source-connector.json`: Cấu hình Debezium đọc Oracle LogMiner, tự động gắn `log.mining.scn.start` từ bảng `MIR_CDC_SCN_HANDOFF`.
- `jdbc-sink-connector.json`: Cấu hình Kafka JDBC Sink ghi vào bảng Mirror với `insert.mode = upsert`, `delete.enabled = true`, khóa bản ghi `pk.fields = MA_CD, MA_HO_GD`.
- `register_connectors.py`: Script tự động lấy SCN từ DB và đăng ký cấu hình lên Kafka Connect REST API (`POST /connectors`).

---

## 4. Hướng Dẫn Thực Thi Chi Tiết

Tất cả các lệnh chạy từ thư mục gốc dự án (`hgd_c360_pipeline`):

```powershell
# Bước 1: Nạp 3 bảng Danh mục thực tế vào DB Nguồn VSS
python p1_source_and_mirror/load_danhmuc.py

# Bước 2: Sinh dữ liệu giả lập ban đầu (50.000 công dân, 52.500 dòng nhân khẩu)
python p1_source_and_mirror/gen_seed_nhankhau.py --num-persons 50000 --batch-size 5000

# Bước 3: Thực hiện Full Load JDBC từ Nguồn sang vùng Mirror
python p1_source_and_mirror/full_load_jdbc.py --batch-size 5000

# Bước 4: Kiểm tra nhanh trạng thái và đối soát số liệu Tuần 1
python p1_source_and_mirror/check_week1_status.py

# Bước 5: (Tùy chọn) Chạy kịch bản biến động Delta để tạo dữ liệu CDC
python p1_source_and_mirror/gen_delta_changes.py

# Bước 6: Chạy bộ kiểm thử tự động (Unit & Integration Test Suite)
pytest p4_airflow_and_e2e_test/tests/test_week1_p1.py -v
```

---

## 5. Kết Quả Kiểm Tra Trạng Thái & Nghiệm Thu Tuần 1

Kết quả thực tế từ script `check_week1_status.py`:

```text
==============================================================================
 BÁO CÁO KIỂM TRA TRẠNG THÁI TUẦN 1 (HGD -> MIRROR -> C360 PIPELINE)
==============================================================================

[1] TÀI LIỆU THIẾT KẾ (design/):
  - ThietKeCoBan_BasicDesign_HGD_C360.md          : OK (52,047 bytes)
  - ThietKeCoBan_BasicDesign_HGD_C360.xlsx        : OK (43,923 bytes)
  - ThietKeChiTiet_DetailDesign_HGD_C360.md       : OK (50,587 bytes)
  - ThietKeChiTiet_DetailDesign_HGD_C360.xlsx     : OK (38,188 bytes)

[2] SỐ LƯỢNG BẢN GHI CÁC BẢNG NGUỒN (VSS_*) & MIRROR (MIR_VSS_*):
  - VSS_DM_DANTOC                :         59 dòng
  - VSS_DM_QUOC_TICH             :        243 dòng
  - VSS_DM_DBHC                  :     15,325 dòng
  - VSS_HO_GD_NHANKHAU           :     52,500 dòng
  - MIR_VSS_DM_DANTOC            :         59 dòng
  - MIR_VSS_DM_QUOC_TICH         :        243 dòng
  - MIR_VSS_DM_DBHC              :     15,325 dòng
  - MIR_VSS_HO_GD_NHANKHAU       :     52,500 dòng

[3] CHỈ MỤC MIRROR & MỐC SCN BÀN GIAO DEBEZIUM CDC:
  - Unique Index UK_MIR_NK (MA_CD, MA_HO_GD) : ACTIVE (UNIQUE)
  - Mốc SCN bàn giao (SNAPSHOT_SCN)          : 1794466529406500 (Status: PASSED)

[4] PHÂN BỔ ĐỌC SONG SONG THEO PHÂN VÙNG MA_CD (10 PARTITIONS):
  - part_0 :    6,000 dòng     - part_5 :    6,000 dòng
  - part_1 :    5,000 dòng     - part_6 :    5,000 dòng
  - part_2 :    5,000 dòng     - part_7 :    5,000 dòng
  - part_3 :    5,500 dòng     - part_8 :    5,000 dòng
  - part_4 :    5,000 dòng     - part_9 :    5,000 dòng
==============================================================================
```

> **Đánh giá Tuần 1:** Đạt 100% mục tiêu đề bài, bàn giao hoàn hảo sang Tuần 2 (Chuẩn hóa C360 và Cập nhật Near Real-time qua Queue).

