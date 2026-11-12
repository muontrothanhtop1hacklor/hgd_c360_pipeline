# Thiết kế chi tiết (Detail Design)

> Hệ thống Đồng bộ Dữ liệu Hộ gia đình - Nhân khẩu, Chuẩn hóa Công dân (Customer 360), Báo cáo BI & Cổng Dịch vụ API Tra cứu (`HDG -> Mirror -> C360 -> BI & API`)
> Tài liệu thiết kế kỹ thuật chi tiết mức Code/SQL dành cho lập trình viên và kiểm thử viên.
> Đồng bộ 100% với file `design/ThietKeChiTiet_DetailDesign_HGD_C360.xlsx`.

## HuongDan – Hướng dẫn sử dụng

**Mục đích của Detail Design (DD)**
- Trả lời câu hỏi: LÀM THẾ NÀO bên trong từng tiến trình Batch/CDC/Near Real-time và từng RESTful API. Viết đủ chi tiết để lập trình viên hiện thực chính xác 100% mà không phải phỏng đoán.
- Bao phủ toàn bộ 4 phần của dự án: Đồng bộ Nguồn -> Mirror (Full Load & CDC), Chuẩn hóa C360 (Bảng Map, View Core, Queue Trigger, Concurrency Control), Đối soát 3 tầng & BI Views, và 4 RESTful APIs tra cứu C360.

**Quy ước bắt buộc**
- Xử lý chi tiết (Mục 05) viết theo các bước số học: `1`, `1.1`, `1.2`, `2`, `2.1`... Mọi nhánh `IF` đều có nhánh `ELSE`. Mọi thao tác truy vấn DB đều trỏ tới `SQL-ID` ở Mục 06.
- Mọi câu lệnh SQL ở Mục 06 bắt buộc mô tả đầy đủ 5 trường hợp kết quả:
  1. Có dữ liệu bình thường.
  2. Không có dữ liệu (`NOT FOUND`).
  3. Nhiều hơn kỳ vọng (vi phạm ràng buộc dữ liệu / xung đột `409 Conflict`).
  4. Lỗi Database (`DataAccessException`).
  5. Timeout kết nối / truy vấn.
- Toàn bộ các mã lỗi ném ra đều ánh xạ trực tiếp về `DSThongBao` của Thiết kế cơ bản (`C360-BD-001`).
- Mỗi nhánh rẽ của thuật toán phải có ít nhất 1 test case tương ứng tại Mục 09 (`UnitTest`).

---

## Bia – Bìa tài liệu

| Mục | Nội dung |
| --- | --- |
| Tên dự án | Xây dựng Pipeline Dữ liệu Hộ gia đình - Nhân khẩu (HDG -> C360 -> BI & API) |
| Hệ thống | Phân hệ Đồng bộ CDC, Chuẩn hóa Dữ liệu Công dân (Customer 360), Báo cáo BI & Dịch vụ API |
| Tài liệu | Thiết kế chi tiết (Detail Design) |
| Mã tài liệu | C360-DD-001 |
| Phiên bản | 1.0 |
| Người lập | Nhóm Kỹ sư Dữ liệu & Hệ thống |
| Người review | Tech Lead / Solution Architect |
| Người phê duyệt | Ban Quản lý Thu & Trung tâm CNTT (BHXH Việt Nam) |
| Ngày phê duyệt | 2026/11/10 |
| Trạng thái | Approved |
| Tài liệu đầu vào | Thiết kế cơ bản `C360-BD-001`; Đề bài `Yeucau_DataPipeline_HGD_C360_4Tuan.md`; Bộ 3 file CSV danh mục thực tế (`DANHMUC/*.csv`) |

---

## LichSu – Lịch sử sửa đổi

| Phiên bản | Ngày | Sheet / Mục thay đổi | Nội dung thay đổi | Lý do | Người sửa | Người duyệt |
| --- | --- | --- | --- | --- | --- | --- |
| 1.0 | 2026/11/10 | Toàn bộ | Tạo mới tài liệu Thiết kế chi tiết cho toàn bộ các tiến trình `BAT-C360-001..006` và 4 dịch vụ `API-C360-001..004` kèm đặc tả SQL, quy tắc gắn cờ `DQ_FLAGS` và kịch bản Unit Test | Thực hiện Tuần 1 theo kế hoạch 4 tuần | Nhóm Kỹ sư | Tech Lead |

---

## TongQuanModule – Tổng quan module

| Mục | Nội dung |
| --- | --- |
| ID module | `DD-C360-ALL` (`DD-BAT-001..006`, `DD-API-001..004`) |
| Hiện thực cho | `F-C360-001` đến `F-C360-006`, `API-C360-001` đến `API-C360-004`, `SCR-C360-001` đến `SCR-C360-003`, `BAT-C360-001` đến `BAT-C360-006` |
| Mức chi tiết | A (Đầy đủ mọi sheet: I/O, Thuật toán chi tiết từng bước, SQL định nghĩa 5 kịch bản, Ma trận Validation & DQ Rules, Exception & Log, Unit Test phủ toàn diện) |
| Tóm tắt xử lý | 1. **Data Generator & Mirror Sync (`p1_source_and_mirror`):** Nạp 3 file CSV danh mục thực tế -> Sinh dữ liệu nhân khẩu ban đầu (Script 1) và biến động (Script 2) -> Đọc song song Full Load theo prefix `MA_CD` (tắt Unique Index khi nạp, bật lại sau nạp) -> Chuyển giao `SCN` sang Debezium Kafka CDC (`upsert`/`delete` theo `(MA_CD, MA_HO_GD)`).<br>2. **C360 Standardization Engine (`p2_c360_standardization`):** Tách phẳng `MA_DBHC_SN` ra 2 bảng Map Tỉnh/Xã bằng `DELETE + INSERT` trong 1 transaction -> Dùng chung duy nhất `VW_C360_TRANSFORM_CORE` để lọc nghiệp vụ, khử trùng theo `MA_CD`, chuẩn hóa 3 địa chỉ/ngày sinh/CCCD/SĐT/dân tộc/quốc tịch và gán `DQ_FLAGS` -> Hỗ trợ cả Full Load (khóa `C360_SYNC_CONTROL`) và Near Real-time mỗi phút qua `C360_SYNC_QUEUE` (`MERGE` + `DELETE`).<br>3. **Reconciliation, BI & REST API (`p3_reconciliation_bi_api`):** Chạy đối soát tự động 3 tầng -> Cung cấp View cho 3 nhóm Dashboard BI -> Cung cấp 4 REST APIs xác thực `X-API-Key` SHA-256 và ghi `Async Batch Audit Log`. |
| Lớp / package | - `p1_source_and_mirror`: `load_danhmuc.py`, `gen_seed_nhankhau.py`, `gen_delta_changes.py`, `full_load_jdbc.py`<br>- `p2_c360_standardization`: `c360_engine.py`, `ddl_c360_tables.sql`, `vw_c360_transform_core.sql`, `pkg_c360_sync_engine.sql`<br>- `p3_reconciliation_bi_api`: `reconcile_3_tiers.py`, `bi_datamart_views.sql`, `api/main.py`, `api/auth.py`, `api/audit_worker.py` |
| Chữ ký phương thức | 1. `load_all_danhmuc(conn, csv_dir, inject_ambiguous_xa=True) -> dict`<br>2. `generate_initial_seed(conn, num_persons=50000, seed=42) -> SeedStats`<br>3. `generate_delta_changes(conn, insert_n=5000, update_n=2000, delete_n=500) -> DeltaStats`<br>4. `run_full_load_mirror(src_conn, dst_conn, num_partitions=8, batch_size=10000) -> LoadResult`<br>5. `refresh_dbhc_map_tables(conn) -> MapStats`<br>6. `run_c360_full_load(conn) -> SyncStats` / `run_c360_nrt_batch(conn, batch_limit=10000) -> SyncStats`<br>7. `lookup_citizen_by_code(ma: str)`, `lookup_citizens_batch(ma_list: list[str])`, `search_citizens(ho_ten: str, ngay_sinh: str)`, `get_changed_citizens(cursor: str | None, limit: int)` |
| Được gọi bởi | CLI Scripts, Airflow DAGs (`dag_hgd_*`), Debezium Connect, Các hệ thống nội bộ gọi qua HTTP REST API |
| Gọi tới | Oracle Database (`VSS`, `MIRROR`, `C360`) / Local Engine, Kafka Broker, Bảng `C360_API_CLIENTS` & `C360_API_AUDIT_LOG` |
| Giao dịch | - Làm mới bảng Map (`C360_MAP_DBHC_TINH`/`_XA`): Bắt buộc `DELETE` + `INSERT` trong **cùng 1 transaction**.<br>- Xử lý lô Near Real-time: `MERGE` + `DELETE` trên C360 + `DELETE` khỏi Queue + `INSERT` vào `C360_SYNC_LOG` nằm trong **cùng 1 transaction**.<br>- Ghi Audit Log API: Chạy bất đồng bộ theo lô (`Async Batch Queue`), tách rời khỏi transaction đọc của API. |
| Xử lý đồng thời / khóa | Bảng `C360_SYNC_CONTROL` điều phối độc quyền giữa Full Load và Near Real-time bằng `SELECT ... FOR UPDATE`. Cắt lô Queue sử dụng danh sách `QUEUE_ID` đã commit trước `SYSTIMESTAMP - 2s` để không bao giờ xóa nhầm các dòng commit trễ (out-of-order sequence commit). |

---

## ThamSoIO – Định nghĩa vào/ra

| Hướng | No | Tên tham số / thuộc tính | Kiểu | Bắt buộc | Mô tả | Giá trị khi không có | Tham chiếu BD |
| --- | --- | --- | --- | --- | --- | --- | --- |
| IN | 1 | `X-API-Key` | String (Header) | ○ | Khóa xác thực client gọi API; được băm SHA-256 để kiểm tra quyền | Ném HTTP 401 (`MSG-E001`) | BD-08 API-001..004 |
| IN | 2 | `ma` | String (Path) | ○ | Mã tra cứu cá nhân (`MA_SO_BHXH`, `MA_SO_BHXH_CU`, hoặc `SO_DINH_DANH`) | — | BD-08 API-001 |
| OUT | 1 | `CitizenLookupResponse` | Object (JSON) | ○ | Chứa `matchedBy` và `data` (`CitizenC360Dto`), hoặc danh sách `candidates` nếu `409 Conflict` | — | BD-08 API-001 |
| IN | 3 | `maList` | List<String> (Body) | ○ | Danh sách mã tra cứu theo lô (`1..1000` mã) | Ném HTTP 400 (`MSG-E003`) | BD-08 API-002 |
| OUT | 2 | `BatchLookupResponse` | Object (JSON) | ○ | Chứa `summary` và mảng `results[]` cho từng mã đầu vào | — | BD-08 API-002 |
| IN | 4 | `hoTen` | String (Query) | ○ | Họ tên cần tìm (có dấu hoặc không dấu, `2..250` ký tự) | Ném HTTP 400 (`MSG-E003`) | BD-08 API-003 |
| IN | 5 | `ngaySinh` | String (Query) | ○ | Ngày sinh cần tìm (`YYYY`, `YYYYMM`, hoặc `YYYYMMDD`) | Ném HTTP 400 (`MSG-E003`) | BD-08 API-003 |
| IN | 6 | `cursor` / `limit` | String / Integer (Query) | × | Con trỏ phân trang `Base64(syncDate|id)` và số lượng dòng (`1..1000`) | `cursor=null`, `limit=200` | BD-08 API-004 |
| OUT | 3 | `ChangeFeedResponse` | Object (JSON) | ○ | Chứa `safeWatermark`, `nextCursor`, `hasMore` và `data[]` | — | BD-08 API-004 |
| EXCEPTION | 1 | `UnauthorizedException` / `ForbiddenException` | — | — | Thiếu/sai `X-API-Key` (`HTTP 401`) hoặc thiếu scope (`HTTP 403`) | — | `MSG-E001`, `MSG-E002` |
| EXCEPTION | 2 | `ValidationException` | — | — | Sai định dạng tham số hoặc vượt giới hạn 1.000 mã (`HTTP 400`) | — | `MSG-E003` |
| EXCEPTION | 3 | `NotFoundException` | — | — | Không tìm thấy công dân ở cả 3 tầng ưu tiên (`HTTP 404`) | — | `MSG-I001` |
| EXCEPTION | 4 | `ConflictMultipleMatchesException` | — | — | Mã cũ hoặc số định danh khớp với $\ge 2$ công dân khác nhau (`HTTP 409`) | — | `MSG-E004` |

---

## XuLyChiTiet – Xử lý chi tiết

| Bước (No) | Nội dung xử lý | Điều kiện | Khi đúng | Khi sai | SQL / Check / MSG | Ghi chú |
| --- | --- | --- | --- | --- | --- | --- |
| **1** | **[BAT-C360-001] Nạp 3 bảng Danh mục & Sinh dữ liệu nguồn (Seed & Delta)** | — | — | — | — | Phần 1 - Mục 1 |
| 1.1 | Đọc 3 file CSV trong `data/danhmuc/`: nạp `DM_DAN_TOC_202611091513.csv` (`59` dòng) vào `VSS_DM_DANTOC`, `DM_QUOC_TICH_202611091512.csv` (`243` dòng) vào `VSS_DM_QUOC_TICH`, và `DM_DBHC_202611091512.csv` (`15.325` dòng) vào `VSS_DM_DBHC`. | Bật cờ `inject_ambiguous_xa = True` | Bổ sung mã xã cũ `'00999'` vào `MA_DBHC_SN` của 2 xã mới cùng tỉnh Hà Nội (`'00004'`, `'00008'`) và mã `'00998'` vào 2 xã khác tỉnh để kiểm thử ưu tiên 3 (`MO_HO`) | Giữ nguyên CSV gốc | — | Xem Sheet QA câu 1 & 2 |
| 1.2 | Chạy `gen_seed_nhankhau.py`: Sinh `N` công dân ban đầu vào `VSS_HO_GD_NHANKHAU`, chủ đích cài cắm đủ tỷ lệ: `5%` công dân có 2–3 dòng `MA_HO_GD` với `MODIFY_DATE` khác nhau; `3%` dòng thuộc diện lọc (`TYPEID IN ('GT','GM','GG','GK')` hoặc `TRANG_THAI_DUYET IN (1,3,4)`); đa dạng định dạng `MA_TINH`/`MA_XA` (hiện hành, sáp nhập, mất số 0 đầu, xã mơ hồ, tỉnh lệch xã, mã rỗng `'00TTT'`/`'00000'`, mã sai `'99TTT'`/`'99999'`); đa dạng `NGAY_SINH` (`YYYYMMDD`, `YYYY0000`, `YYYY`, `YYYYMM00`, `YYYYMM`, sai lịch `19950230`, `17500101`), `SO_CCCD`/`SO_CMTND`, `PHONE_NUMBER`, `QUOC_TICH` (`'VN'`, `'233'`, `'XX'`), `DM_DAN_TOC_ID` (`171`, `248` hết hạn, `9999` sai), `HO_TEN` thừa khoảng trắng. | — | Commit theo từng batch `5.000` dòng và xuất báo cáo thống kê hạt giống gốc | Rollback nếu lỗi DB | — | Đảm bảo bao phủ 100% ca dữ liệu bẩn |
| 1.3 | Chạy `gen_delta_changes.py` (khi test biến động): Thực hiện trực tiếp trên `VSS_HO_GD_NHANKHAU` đủ 4 nhóm: (a) `INSERT` công dân mới & hộ mới cho công dân cũ; (b) `UPDATE` sửa dữ liệu lỗi thành đúng (gỡ `DQ_FLAGS`) và chuyển `TRANG_THAI_DUYET`/`TYPEID` giữa hợp lệ <-> không hợp lệ; (c) `UPDATE` đổi `MA_CD` sang mã mới và lưu mã cũ vào `MA_CD_OLD`; (d) `DELETE` dòng hộ mới nhất của người nhiều hộ và `DELETE` toàn bộ dòng của một số công dân. | — | Ghi nhận danh sách các `MA_CD` biến động vào file đối chứng `delta_ground_truth.json` | — | — | Phục vụ kiểm chứng E2E sau 1 phút |
| **2** | **[BAT-C360-002 & 003] Đồng bộ Full Load & CDC (Nguồn -> Mirror)** | — | — | — | — | Phần 1 - Mục 2 & 3 |
| 2.1 | Trước khi chạy Full Load: Truy vấn `SELECT CURRENT_SCN FROM V$DATABASE` (hoặc mốc timestamp hiện tại) lưu vào `C360_SYNC_CONTROL.LAST_FULL_LOAD_SCN`. | — | Sang 2.2 | — | — | Mốc chuyển giao sang Debezium CDC |
| 2.2 | Trên vùng Mirror: Tạm tắt Trigger `TRG_MIR_NHANKHAU_QUEUE` và `DROP` Unique Index `UK_MIR_NK (MA_CD, MA_HO_GD)` trước khi nạp lô lớn; thực hiện `TRUNCATE TABLE` trên 4 bảng Mirror. | — | Sang 2.3 | — | — | Tránh overhead duy trì Index & Trigger từng dòng |
| 2.3 | Nạp 3 bảng danh mục (`MIR_DM_DANTOC`, `MIR_DM_QUOC_TICH`, `MIR_DM_DBHC`) và chia `num_partitions` luồng đọc song song bảng `VSS_HO_GD_NHANKHAU` theo prefix `SUBSTR(MA_CD, 1, 2)` (hoặc `MOD(ORA_HASH(MA_CD), P)`), ép kiểu tường minh các cột `NUMBER` không có precision/scale, đọc và ghi theo cursor batch `10.000` dòng (không dùng `.cache()` hay `.repartition()`). Chuẩn hóa `MA_HO_GD = NVL(TRIM(MA_HO_GD), 'HO_MAC_DINH')` để đảm bảo khóa `(MA_CD, MA_HO_GD)` không bị `NULL`. | — | Sang 2.4 | Rollback & báo lỗi | SQL-01 | Chống OOM và treo DB nguồn |
| 2.4 | Sau khi nạp xong: Tạo lại `CREATE UNIQUE INDEX UK_MIR_NK ON MIR_HO_GD_NHANKHAU(MA_CD, MA_HO_GD)`, index `IDX_MIR_NK_MOD(MA_CD, MODIFY_DATE DESC)` và bật lại Trigger `TRG_MIR_NHANKHAU_QUEUE`. Kiểm tra `COUNT(*)` giữa Nguồn và Mirror. | `COUNT(Nguồn) == COUNT(Mirror)` | Khởi chạy Debezium CDC từ `LAST_FULL_LOAD_SCN` với chế độ `upsert` theo `(MA_CD, MA_HO_GD)` | Báo lỗi lệch số dòng ngay lập tức | — | Đảm bảo không mất/trùng dữ liệu khi nối CDC |
| **3** | **[BAT-C360-004] Tiền xử lý & Làm mới Bảng Map Địa bàn hành chính (`TINH` & `XA`)** | — | — | — | — | Phần 2 - Mục 2 |
| 3.1 | Mở 1 Transaction duy nhất: Thực hiện `DELETE FROM C360_MAP_DBHC_TINH;` và `DELETE FROM C360_MAP_DBHC_XA;` (tuyệt đối không dùng `TRUNCATE`). | — | Sang 3.2 | — | SQL-02 | Luồng NRT đang chạy vẫn đọc được snapshot cũ qua Undo/MVCC |
| 3.2 | Lọc các dòng cấp Tỉnh đang hiệu lực trong `MIR_DM_DBHC` (`TINH_TRANG = '1'`, `IS_CAP_XA = 0`, `NGAY_HET_HIEU_LUC > SYSDATE` — gồm 36 đơn vị mới). Với mỗi tỉnh mới: chuẩn hóa mã tỉnh đích về 3 chữ số `LPAD(REPLACE(MA_DBHC, 'TTT', ''), 3, '0')` (ví dụ `'01'` -> `'001'`, `'33'` -> `'033'`). Sinh các dòng tra cứu ưu tiên 1 (`LOAI_MAP = 'HIEN_HANH'`) và tách chuỗi `MA_DBHC_SN` theo dấu `;` để sinh các dòng ưu tiên 2 (`LOAI_MAP = 'SAP_NHAP'`, ví dụ `'34'`, `'034'`, `'34TTT'` -> `'033'` Hưng Yên) cho mọi biến thể (`N`, `NN`, `0NN`, `NNTTT`). | Nếu mã cũ trùng với mã hiện hành (ví dụ `'33'` nằm trong `'33;34'`) | Giữ dòng ưu tiên 1 (`HIEN_HANH`) | Chèn dòng `SAP_NHAP` | SQL-02 | Phủ mọi dạng dữ liệu `MA_TINH` ở nguồn |
| 3.3 | Lọc các dòng cấp Xã đang hiệu lực trong `MIR_DM_DBHC` (`TINH_TRANG = '1'`, `IS_CAP_XA = 1`, `NGAY_HET_HIEU_LUC > SYSDATE` — gồm 3.322 xã mới). Chèn tất cả mã xã mới (chuẩn 5 số `LPAD`) vào `C360_MAP_DBHC_XA` với `LOAI_MAP = 'HIEN_HANH'`, suy ra `MA_TINH_MOI` (3 số) từ cột `MA_CHA`. | — | Sang 3.4 | — | SQL-02 | Ưu tiên 1 (`HIEN_HANH`) |
| 3.4 | Tách chuỗi `MA_DBHC_SN` của các xã đang hiệu lực thành các mã xã cũ 5 chữ số (loại trừ các mã đã tồn tại ở bước 3.3) và gom nhóm theo `MA_XA_CU`: | Số xã mới nhận `MA_XA_CU` == 1 | Chèn vào `C360_MAP_DBHC_XA` với `LOAI_MAP = 'SAP_NHAP'`, điền đủ `MA_XA_MOI`, `TEN_XA_MOI`, `MA_TINH_MOI`, `TEN_TINH_MOI` | Số xã mới nhận `MA_XA_CU` $\ge 2$ (`MO_HO`): Chèn với `LOAI_MAP = 'MO_HO'`, để `MA_XA_MOI = NULL`, `TEN_XA_MOI = NULL`; nếu tất cả xã mới đó cùng thuộc 1 tỉnh (`COUNT(DISTINCT MA_CHA) == 1`) thì điền `MA_TINH_MOI` và `TEN_TINH_MOI`, ngược lại để `NULL` | SQL-02 | Xử lý triệt để Ưu tiên 2 (`SAP_NHAP`) & Ưu tiên 3 (`MO_HO`) |
| 3.5 | Thực hiện `COMMIT;` kết thúc transaction làm mới bảng Map. | — | Ghi log số lượng mã Tỉnh/Xã đã nạp | `ROLLBACK;` nếu có lỗi | — | An toàn 100% cho Near Real-time |
| **4** | **[BAT-C360-005A & 005B] Logic Chuẩn hóa C360 dùng chung & Cập nhật Kép (Full Load + Queue NRT)** | — | — | — | — | Phần 2 - Mục 1, 3, 4, 5 |
| 4.1 | Định nghĩa View/Truy vấn lõi dùng chung `VW_C360_TRANSFORM_CORE` thực hiện tuần tự:<br>- **Lọc nghiệp vụ:** Giữ các dòng có `NVL(TYPEID, 'ZZ') NOT IN ('GT','GM','GG','GK')` và `NVL(TRANG_THAI_DUYET, 0) NOT IN (1,3,4)` (tức chỉ lấy `TRANG_THAI_DUYET IS NULL` hoặc `IN (0, 2)`).<br>- **Khử trùng theo `MA_CD`:** Dùng `ROW_NUMBER() OVER (PARTITION BY MA_CD ORDER BY MODIFY_DATE DESC NULLS LAST, CREATED_DATE DESC NULLS LAST, ID DESC) = 1`.<br>- **Chuẩn hóa 3 nhóm địa chỉ (Thường trú, `_LH`, `_KS`):** Chuẩn hóa mã xã gốc (nếu là số thuần túy `< 5` chữ số và `<> '0'` thì `LPAD(..., 5, '0')`), tra bảng `C360_MAP_DBHC_XA` và `C360_MAP_DBHC_TINH`. Nếu Xã suy ra được Tỉnh thì lấy Tỉnh theo Xã; nếu cả Tỉnh khai báo và Tỉnh suy từ Xã đều hợp lệ nhưng khác nhau thì gắn cờ `TINH_LECH_XA`.<br>- **Chuẩn hóa Họ tên, Ngày sinh, CCCD 12 số, SĐT, Dân tộc, Quốc tịch & Nối chuỗi `DQ_FLAGS`** theo đúng quy tắc CHK-01..CHK-11. | — | Dùng chung truy vấn này cho cả `INSERT` (Full Load) và `MERGE` (Near Real-time) | — | SQL-03 | Duy nhất 1 nguồn logic chuẩn hóa |
| 4.2 | **Khi chạy Full Load (`BAT-C360-005A`):**<br>1. `UPDATE C360_SYNC_CONTROL SET PAUSE_NRT = 1 WHERE PROCESS_NAME = 'C360_SYNC'; COMMIT;`<br>2. Lấy khóa độc quyền `SELECT LAST_NRT_RUN INTO ... FROM C360_SYNC_CONTROL WHERE PROCESS_NAME = 'C360_SYNC' FOR UPDATE;` (chờ lô NRT đang chạy kết thúc).<br>3. Ghi nhận `v_snap_time := SYSTIMESTAMP;`<br>4. Tắt/xóa tạm các Secondary Index hoặc thực hiện `DELETE`/`TRUNCATE` bảng `C360_THONG_TIN_CA_NHAN` rồi `INSERT` toàn bộ từ `VW_C360_TRANSFORM_CORE`.<br>5. Chỉ xóa các dòng Queue đã nằm trong snapshot: `DELETE FROM C360_SYNC_QUEUE WHERE ENQUEUE_TIME <= v_snap_time;`<br>6. Mở lại cờ `PAUSE_NRT = 0`, ghi `C360_SYNC_LOG` và `COMMIT;`. | — | Hoàn tất Full Load không mất biến động phát sinh trong lúc chạy | Trong khối `EXCEPTION`: luôn đặt lại `PAUSE_NRT = 0; COMMIT;` để không làm treo NRT | SQL-04 | Giải quyết bài toán Concurrency & Sync Control |
| 4.3 | **Khi chạy Near Real-time mỗi phút (`BAT-C360-005B`):**<br>1. Thử khóa `SELECT PAUSE_NRT FROM C360_SYNC_CONTROL WHERE PROCESS_NAME = 'C360_SYNC' FOR UPDATE SKIP LOCKED;` — nếu không lấy được khóa hoặc `PAUSE_NRT = 1` thì thoát ngay.<br>2. Chụp danh sách `QUEUE_ID` và `MA_CD` của các dòng trong `C360_SYNC_QUEUE` có `ENQUEUE_TIME <= SYSTIMESTAMP - INTERVAL '2' SECOND` (tối đa `NRT_BATCH_LIMIT` dòng).<br>3. Với tập `DISTINCT MA_CD` trong lô:<br>   - Thực hiện `MERGE INTO C360_THONG_TIN_CA_NHAN` từ `VW_C360_TRANSFORM_CORE WHERE MA_CD IN (...)` (cập nhật công dân cũ hoặc chèn công dân mới kèm `SYNC_DATE = SYSDATE`).<br>   - Thực hiện **`DELETE FROM C360_THONG_TIN_CA_NHAN`** đối với các `MA_CD` trong lô nhưng **không còn tồn tại** trong `VW_C360_TRANSFORM_CORE` (do đã bị xóa ở nguồn, bị đổi `MA_CD`, hoặc chuyển sang `TYPEID`/`TRANG_THAI_DUYET` bị loại).<br>4. Xóa khỏi `C360_SYNC_QUEUE` đúng các `QUEUE_ID` đã chụp ở bước 2, ghi `C360_SYNC_LOG` và `COMMIT;`. | Lô Queue rỗng (`0` dòng) | Giải phóng khóa, kết thúc nhanh | Rollback lô hiện tại nếu lỗi, giữ nguyên Queue cho phút sau | SQL-05 | Xử lý trọn vẹn `INSERT`, `UPDATE`, đổi `MA_CD` và `DELETE` |
| **5** | **[API-C360-001..004] Xử lý 4 RESTful APIs Tra cứu C360 & Async Batch Audit Log** | — | — | — | — | Phần 3 - Mục 3 |
| 5.1 | **Middleware Xác thực `X-API-Key`:** Đọc header `X-API-Key`, tính `SHA-256(api_key)` và tra trong bộ nhớ đệm / bảng `C360_API_CLIENTS` (`IS_ACTIVE = 1`). Kiểm tra scope của endpoint hiện tại. | Thiếu key hoặc hash không tồn tại / thiếu scope | Trả `HTTP 401` (`MSG-E001`) hoặc `HTTP 403` (`MSG-E002`) | Cho phép đi tiếp vào Controller | CHK-12 | Không lưu plain-text API key trong DB |
| 5.2 | **API-001 (`GET /api/v1/ca-nhan/{ma}`):**<br>1. Truy vấn tầng 1 theo `MA_SO_BHXH = :ma` (`PK_C360_CA_NHAN`). Nếu thấy 1 dòng -> trả ngay `HTTP 200` (`matchedBy = 'MA_SO_BHXH'`).<br>2. Nếu không thấy: Truy vấn tầng 2 theo `MA_SO_BHXH_CU = :ma` (`IDX_C360_MA_CU`). Nếu thấy 1 dòng -> trả `HTTP 200` (`matchedBy = 'MA_SO_BHXH_CU'`); nếu thấy $\ge 2$ dòng -> trả `HTTP 409 Conflict` (`MSG-E004`) kèm danh sách ứng viên.<br>3. Nếu vẫn không thấy: Truy vấn tầng 3 theo `SO_DINH_DANH = :ma` (`IDX_C360_SO_DINH_DANH`). Nếu thấy 1 dòng -> trả `HTTP 200` (`matchedBy = 'SO_DINH_DANH'`); nếu thấy $\ge 2$ dòng -> trả `HTTP 409 Conflict` (`MSG-E004`); nếu 0 dòng -> trả `HTTP 404` (`MSG-I001`). | — | Đẩy bản ghi log vào hàng đợi `AsyncAuditQueue` | — | SQL-06 | Dừng ngay ở tầng đầu tiên tìm thấy |
| 5.3 | **API-002 (`POST /api/v1/ca-nhan/tra-cuu-lo`):** Kiểm tra `1 <= len(maList) <= 1000`. Thực hiện truy vấn tập hợp trên cả 3 cột (`MA_SO_BHXH IN (...)`, `MA_SO_BHXH_CU IN (...)`, `SO_DINH_DANH IN (...)`), sau đó phân giải kết quả cho từng mã trong `maList` theo đúng thứ tự ưu tiên 3 tầng như API-001 (`FOUND`, `CONFLICT`, hoặc `NOT_FOUND`). | `len(maList) == 0` hoặc `> 1000` | Ném `ValidationException` (`HTTP 400`, `MSG-E003`) | Trả `HTTP 200` kèm `summary` và `results[]` | SQL-07 | Không gọi N câu query lẻ trong vòng lặp |
| 5.4 | **API-003 (`GET /api/v1/ca-nhan/tim-kiem`):** Chuẩn hóa tham số `hoTen` sang tiếng Việt không dấu viết hoa (`HO_TEN_KHONG_DAU`), kiểm tra `ngaySinh` là chuỗi số độ dài `4` (`YYYY`), `6` (`YYYYMM`), hoặc `8` (`YYYYMMDD`). Truy vấn trên Composite Index `IDX_C360_TIM_KIEM (HO_TEN_KHONG_DAU, NGAY_SINH_CHUAN)` sao cho khớp cả người trên DB có ngày sinh đầy đủ lẫn người chỉ có năm/tháng sinh rút gọn. | Sai định dạng `hoTen` / `ngaySinh` | Ném `ValidationException` (`HTTP 400`, `MSG-E003`) | Trả `HTTP 200` danh sách kết quả | SQL-08 | Hỗ trợ khớp hai chiều ngày sinh rút gọn |
| 5.5 | **API-004 (`GET /api/v1/ca-nhan/thay-doi`):** Tính mốc an toàn `v_safe_watermark = SYSDATE - INTERVAL '120' SECOND`. Giải mã `cursor` (nếu có) thành `(v_last_sync_date, v_last_id)`. Truy vấn các dòng có `SYNC_DATE <= v_safe_watermark` và `(SYNC_DATE > :v_last_sync_date OR (SYNC_DATE = :v_last_sync_date AND ID > :v_last_id))` sắp xếp theo `SYNC_DATE ASC, ID ASC` với `FETCH FIRST :limit ROWS ONLY`. | — | Mã hóa `(SYNC_DATE, ID)` của dòng cuối thành `nextCursor` và trả `HTTP 200` | — | SQL-09 | Chống bỏ sót dòng thuộc lô NRT đang commit dở |
| 5.6 | **Async Batch Audit Log Worker:** Luồng nền (`Background Thread / Asyncio Task`) gom các bản ghi log từ `AsyncAuditQueue` và thực hiện `executemany` (`INSERT` theo lô mỗi `200` bản ghi hoặc mỗi `3 giây`) vào bảng `C360_API_AUDIT_LOG`. | Lỗi ghi DB Audit | Ghi `WARN` ra console/file log, không làm ảnh hưởng phản hồi HTTP của client | — | — | Đảm bảo tốc độ phản hồi API tối đa |

---

## DinhNghiaSQL – Định nghĩa SQL

### SQL-01 – Đọc phân mảnh song song bảng nguồn `VSS.VSS_HO_GD_NHANKHAU` phục vụ Full Load

```sql
SELECT MA_CD,
       MA_CN,
       NVL(TRIM(MA_HO_GD), 'HO_MAC_DINH') AS MA_HO_GD,
       HO_TEN,
       HO_TEN_KHAC,
       GIOI_TINH,
       CAST(NGAY_SINH AS NUMBER(18,0)) AS NGAY_SINH,
       NOI_SINH,
       CAST(NGAY_KS AS NUMBER(18,0)) AS NGAY_KS,
       SO_GIAY_KS,
       NOI_KS,
       QH_CHU_HO,
       SO_CMTND,
       CAST(NGAY_CAP_CMTND AS NUMBER(18,0)) AS NGAY_CAP_CMTND,
       NOI_CAP_CMTND,
       SO_HC,
       CAST(NGAY_CAP_HC AS NUMBER(18,0)) AS NGAY_CAP_HC,
       NOI_CAP_HC,
       DIA_CHI,
       DC_LIEN_LAC,
       EMAIL_ACC,
       PHONE_NUMBER,
       MA_TINH,
       MA_HUYEN,
       MA_XA,
       NGHE_NGHIEP,
       QUOC_TICH,
       IS_ALIVE,
       CAST(DEAD_DATE AS NUMBER(18,0)) AS DEAD_DATE,
       IS_BH,
       LOAI_BH,
       MA_BH,
       CREATED_DATE,
       MODIFY_DATE,
       MA_NV,
       GHI_CHU,
       MA_THE,
       MA_SO,
       ISSYNCH,
       CAST(NGAY_BATDAU AS NUMBER(18,0)) AS NGAY_BATDAU,
       CAST(NGAY_HETHAN AS NUMBER(18,0)) AS NGAY_HETHAN,
       MA_THON,
       ISCHU_HO,
       TEN_DOITUONG_BH,
       TEN_TINH,
       TEN_HUYEN,
       TEN_XA,
       TEN_THON,
       TEN_QH_CHUHO,
       MA_DOITUONG_BH,
       STATUS,
       CARD_ID,
       TYPEID,
       CAST(NGAY_PHATSINH AS NUMBER(18,0)) AS NGAY_PHATSINH,
       NGUOI_KE_KHAI,
       TRAN_STATUS,
       TRANG_THAI,
       MA_TINH_KS,
       MA_HUYEN_KS,
       MA_XA_KS,
       TEN_TINH_KS,
       TEN_HUYEN_KS,
       TEN_XA_KS,
       ID,
       MA_OFFLINE,
       CAST(NGAY_SINH_BK AS NUMBER(18,0)) AS NGAY_SINH_BK,
       MA_HUYEN_IMPORT,
       ID_CA,
       NGOAI_TINH,
       MA_NV_TAO,
       MA_TINH_THE,
       USER_NGOAI_TINH,
       MA_THE_MOI,
       CARD_ID_MOI,
       SYNC_TST,
       MA_CD_OLD,
       HO_TEN_UNSIGNED,
       USER_ID,
       USER_ID_TINH,
       USER_ID_TW,
       TRANG_THAI_DUYET,
       TYPE_PM,
       USER_UPDATE,
       LY_DO_PHE_DUYET,
       APPROVED_DATE,
       TEN_CHA_ME,
       KEY_SL,
       DM_DAN_TOC_ID,
       NGAY_TAO_HC,
       USER_HC_ID,
       USER_HC,
       USER_DUYET_HC_ID,
       USER_DUYET_HC,
       NGAY_DUYET_HC,
       LY_DO_HC,
       USER_DMS,
       USER_DMS_ID,
       NGAY_DE_NGHI_DMS,
       USER_DUYET_DMS_ID,
       USER_DUYET_DMS,
       NGAY_DUYET_DMS,
       LY_DO_DMS,
       CAST(LOCK_DATA AS NUMBER(10,0)) AS LOCK_DATA,
       MA_TINH_LH,
       MA_HUYEN_LH,
       MA_XA_LH,
       XAC_THUC_BCA,
       DANG_TG,
       SO_CCCD,
       TRANG_THAI_QUET,
       TRANG_THAI_XAC_THUC_BCA
  FROM VSS.VSS_HO_GD_NHANKHAU
 WHERE MOD(ORA_HASH(MA_CD), :numPartitions) = :partitionId
```

| Thuộc tính | Giá trị |
|---|---|
| **Tham số bind** | `:numPartitions` (ví dụ `8`), `:partitionId` (`0..7`) |
| **Index / Cơ chế đọc** | Đọc song song `N` luồng chia đều theo hàm băm `ORA_HASH(MA_CD)` (hoặc dải prefix `SUBSTR(MA_CD, 1, 2)`), ép kiểu tường minh toàn bộ các cột `NUMBER` không khai báo precision/scale về `NUMBER(18,0)` |
| **Số dòng kỳ vọng** | `~1/N` tổng số dòng bảng nguồn trên mỗi phân vùng |
| **Có dữ liệu** | Đọc từng lô `fetchmany(10000)` -> `executemany` ghi thẳng vào `MIR_HO_GD_NHANKHAU` |
| **Không có (NOT FOUND)** | Phân vùng trống -> Kết thúc luồng phân vùng đó bình thường |
| **Nhiều hơn kỳ vọng** | Không áp dụng (streaming theo batch cố định, không giữ trong RAM) |
| **Lỗi DB / Timeout** | Bắt lỗi kết nối, dừng toàn bộ job Full Load và báo lỗi |
| **Khóa / Timeout** | Không khóa bảng nguồn; fetch size = `10.000` |

### SQL-02 – Làm mới 2 Bảng Map Địa bàn hành chính (`C360_MAP_DBHC_TINH` & `C360_MAP_DBHC_XA`) trong 1 Transaction

```sql
-- Thực thi trong cùng 1 Transaction (DELETE + INSERT, tuyệt đối KHÔNG dùng TRUNCATE)
DELETE FROM C360_MAP_DBHC_TINH;
DELETE FROM C360_MAP_DBHC_XA;

-- 1. Nạp bảng C360_MAP_DBHC_TINH (Ưu tiên 1: HIEN_HANH, Ưu tiên 2: SAP_NHAP)
-- Sinh đầy đủ các biến thể mã tra cứu: '1', '01', '001', '01TTT' -> Mã chuẩn 3 số '001'
INSERT INTO C360_MAP_DBHC_TINH (MA_TINH_LOOKUP, MA_TINH_MOI, TEN_TINH_MOI, LOAI_MAP)
WITH active_tinh AS (
    SELECT LPAD(REPLACE(TRIM(MA_DBHC), 'TTT', ''), 2, '0') AS ma_2so,
           LPAD(REPLACE(TRIM(MA_DBHC), 'TTT', ''), 3, '0') AS ma_3so,
           TRIM(TEN) AS ten_tinh,
           TRIM(MA_DBHC_SN) AS ma_sn
      FROM MIR_DM_DBHC
     WHERE TINH_TRANG = '1'
       AND IS_CAP_XA = 0
       AND (NGAY_HET_HIEU_LUC IS NULL OR NGAY_HET_HIEU_LUC > SYSDATE)
),
raw_tokens AS (
    -- Ưu tiên 1: Mã hiện hành
    SELECT ma_2so AS token_2so, ma_3so, ten_tinh, 'HIEN_HANH' AS loai_map, 1 AS prio
      FROM active_tinh
    UNION ALL
    -- Ưu tiên 2: Các mã tỉnh cũ trong MA_DBHC_SN (ngăn cách bởi ';')
    SELECT LPAD(TRIM(REGEXP_SUBSTR(ma_sn, '[^;]+', 1, LEVEL)), 2, '0') AS token_2so,
           ma_3so, ten_tinh, 'SAP_NHAP' AS loai_map, 2 AS prio
      FROM active_tinh
     WHERE ma_sn IS NOT NULL
   CONNECT BY LEVEL <= REGEXP_COUNT(ma_sn, ';') + 1
       AND PRIOR ma_2so = ma_2so
       AND PRIOR SYS_GUID() IS NOT NULL
),
dedup_tokens AS (
    SELECT token_2so, ma_3so, ten_tinh, loai_map,
           ROW_NUMBER() OVER (PARTITION BY token_2so ORDER BY prio ASC) AS rn
      FROM raw_tokens
     WHERE token_2so IS NOT NULL
)
SELECT lookup_key, ma_3so, ten_tinh, loai_map
  FROM dedup_tokens
  CROSS JOIN LATERAL (
      SELECT token_2so AS lookup_key FROM DUAL
      UNION
      SELECT '0' || token_2so AS lookup_key FROM DUAL
      UNION
      SELECT token_2so || 'TTT' AS lookup_key FROM DUAL
      UNION
      SELECT TO_CHAR(TO_NUMBER(token_2so)) AS lookup_key FROM DUAL WHERE REGEXP_LIKE(token_2so, '^\d+$')
  )
 WHERE rn = 1;
```

| Thuộc tính | Giá trị |
|---|---|
| **Tham số bind** | Không có |
| **Index sử dụng** | `PK_MAP_TINH (MA_TINH_LOOKUP)`, `PK_MAP_XA (MA_XA_LOOKUP)` |
| **Số dòng kỳ vọng** | `C360_MAP_DBHC_TINH`: `~252` dòng biến thể (của 36 tỉnh mới + 29 tỉnh cũ sáp nhập); `C360_MAP_DBHC_XA`: `~10.037` dòng |
| **Có dữ liệu** | `COMMIT` đồng thời cả `DELETE` và `INSERT` trong 1 transaction |
| **Không có (NOT FOUND)** | Nếu `active_tinh` = 0 dòng -> Ném ngoại lệ và `ROLLBACK` để bảo vệ bảng Map hiện hành |
| **Nhiều hơn kỳ vọng** | Khử trùng tự động theo thứ tự ưu tiên `HIEN_HANH (1) > SAP_NHAP (2) > MO_HO (3)` |
| **Lỗi DB / Timeout** | `ROLLBACK` toàn bộ transaction, giữ nguyên bảng Map cũ |
| **Khóa / Timeout** | Row-level lock trong lúc transaction chạy (`< 2 giây`), không chặn `SELECT` của luồng Near Real-time |

### SQL-03 – View Chuẩn hóa Lõi Dùng chung (`VW_C360_TRANSFORM_CORE`)

```sql
-- View/Truy vấn chuẩn hóa dùng chung duy nhất cho cả Full Load và Near Real-time
WITH filtered_ranked AS (
    SELECT nk.*,
           ROW_NUMBER() OVER (
               PARTITION BY nk.MA_CD
               ORDER BY nk.MODIFY_DATE DESC NULLS LAST,
                        nk.CREATED_DATE DESC NULLS LAST,
                        nk.ID DESC NULLS LAST
           ) AS rn
      FROM MIR_HO_GD_NHANKHAU nk
     WHERE NVL(nk.TYPEID, 'ZZ') NOT IN ('GT', 'GM', 'GG', 'GK')
       AND NVL(nk.TRANG_THAI_DUYET, 0) NOT IN (1, 3, 4)
),
latest_person AS (
    SELECT * FROM filtered_ranked WHERE rn = 1
)
SELECT p.MA_CD AS MA_SO_BHXH,
       p.MA_CD_OLD AS MA_SO_BHXH_CU,
       p.MA_HO_GD,
       REGEXP_REPLACE(TRIM(p.HO_TEN), '\s+', ' ') AS HO_TEN,
       -- Họ tên không dấu viết hoa, chuẩn hóa ngày sinh, CCCD 12 số, SĐT,
       -- JOIN bảng C360_MAP_DBHC_XA & C360_MAP_DBHC_TINH cho 3 địa chỉ,
       -- JOIN MIR_DM_DANTOC & MIR_DM_QUOC_TICH, và tổng hợp cột DQ_FLAGS
       ...
  FROM latest_person p
```

| Thuộc tính | Giá trị |
|---|---|
| **Tham số bind** | Khi chạy Full Load: không lọc `MA_CD`; khi chạy Near Real-time: đẩy điều kiện `AND nk.MA_CD IN (SELECT MA_CD FROM GTT_NRT_BATCH_KEYS)` vào CTE `filtered_ranked` |
| **Index sử dụng** | `IDX_MIR_NK_MOD (MA_CD, MODIFY_DATE DESC)`, `PK_MAP_TINH`, `PK_MAP_XA`, `PK_MIR_DM_DANTOC`, `PK_MIR_DM_QUOC_TICH` |
| **Số dòng kỳ vọng** | Chính xác `1 dòng` cho mỗi `MA_CD` hợp lệ |
| **Có dữ liệu** | Đưa vào `INSERT` (Full Load) hoặc `MERGE` (Near Real-time) |
| **Không có (NOT FOUND)** | Với `MA_CD` trong lô Near Real-time không có dòng nào trả về từ View này -> Kích hoạt lệnh `DELETE FROM C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH = ...` |
| **Nhiều hơn kỳ vọng** | Không thể xảy ra nhờ `WHERE rn = 1` trên `PARTITION BY MA_CD` |
| **Lỗi DB / Timeout** | Rollback lô hiện tại và ghi `C360_SYNC_LOG` |
| **Khóa / Timeout** | Chỉ đọc trên Mirror và bảng Map |

### SQL-06 – Tra cứu Cá nhân Đơn lẻ theo 3 tầng ưu tiên (`API-C360-001`)

```sql
-- Tầng 1: Tra cứu theo MA_SO_BHXH hiện hành (Primary Key Unique Scan)
SELECT * FROM C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH = :ma;

-- Tầng 2 (Chỉ chạy khi Tầng 1 trả về 0 dòng): Tra cứu theo MA_SO_BHXH_CU
SELECT * FROM C360_THONG_TIN_CA_NHAN WHERE MA_SO_BHXH_CU = :ma;

-- Tầng 3 (Chỉ chạy khi Tầng 1 & 2 trả về 0 dòng): Tra cứu theo SO_DINH_DANH (12 số)
SELECT * FROM C360_THONG_TIN_CA_NHAN WHERE SO_DINH_DANH = :ma;
```

| Thuộc tính | Giá trị |
|---|---|
| **Tham số bind** | `:ma` (chuỗi đã `TRIM`) |
| **Index sử dụng** | Tầng 1: `PK_C360_CA_NHAN (MA_SO_BHXH)`; Tầng 2: `IDX_C360_MA_CU (MA_SO_BHXH_CU)`; Tầng 3: `IDX_C360_SO_DINH_DANH (SO_DINH_DANH)` |
| **Số dòng kỳ vọng** | Tầng 1: `0..1` dòng; Tầng 2 & 3: `0..n` dòng |
| **Có dữ liệu (1 dòng)** | Dừng ngay tại tầng tìm thấy, trả về `HTTP 200` kèm `matchedBy` |
| **Không có (NOT FOUND)** | Cả 3 tầng đều trả `0` dòng -> Trả `HTTP 404` (`MSG-I001`) |
| **Nhiều hơn kỳ vọng ($\ge 2$ dòng)** | Ở Tầng 2 hoặc Tầng 3 khớp $\ge 2$ công dân -> Trả `HTTP 409 Conflict` (`MSG-E004`) kèm danh sách ứng viên `candidates[]` |
| **Lỗi DB / Timeout** | Bắt lỗi DB, log `ERROR` và trả `HTTP 503` (`MSG-E999`) |
| **Khóa / Timeout** | Không khóa; timeout `2s` |

### SQL-08 – Tìm kiếm theo Họ tên không dấu + Ngày sinh linh hoạt (`API-C360-003`)

```sql
SELECT *
  FROM C360_THONG_TIN_CA_NHAN
 WHERE HO_TEN_KHONG_DAU = :hoTenKhongDau
   AND NGAY_SINH_CHUAN IS NOT NULL
   AND (
       NGAY_SINH_CHUAN LIKE :ngaySinhInput || '%'
       OR :ngaySinhInput LIKE NGAY_SINH_CHUAN || '%'
   )
   AND (:maTinh IS NULL OR MA_TINH_THUONG_TRU = :maTinh)
 ORDER BY MA_SO_BHXH ASC
 FETCH FIRST :limit ROWS ONLY;
```

| Thuộc tính | Giá trị |
|---|---|
| **Tham số bind** | `:hoTenKhongDau` (chuẩn hóa không dấu viết hoa), `:ngaySinhInput` (`YYYY`, `YYYYMM`, hoặc `YYYYMMDD`), `:maTinh`, `:limit` |
| **Index sử dụng** | `IDX_C360_TIM_KIEM (HO_TEN_KHONG_DAU, NGAY_SINH_CHUAN)` — Index Range Scan cực nhanh theo `HO_TEN_KHONG_DAU` trước, sau đó lọc `NGAY_SINH_CHUAN` ngay trên lá Index |
| **Số dòng kỳ vọng** | `0..limit` dòng |
| **Có dữ liệu** | Trả `HTTP 200` kèm danh sách công dân khớp (khớp được cả chiều nhập `1995` tìm ra người `19950815` lẫn nhập `19950815` tìm ra người trên DB chỉ có `1995` hoặc `199508`) |
| **Không có (NOT FOUND)** | Trả `HTTP 200` với `totalMatched = 0`, `data = []` |
| **Nhiều hơn kỳ vọng** | Giới hạn an toàn bởi `FETCH FIRST :limit ROWS ONLY` |
| **Lỗi DB / Timeout** | Trả `HTTP 503` (`MSG-E999`) |
| **Khóa / Timeout** | Không khóa; timeout `3s` |

### SQL-09 – Lấy danh sách bản ghi thay đổi theo Cursor an toàn (`API-C360-004`)

```sql
SELECT *
  FROM C360_THONG_TIN_CA_NHAN
 WHERE SYNC_DATE <= :safeWatermark
   AND (
       :lastSyncDate IS NULL
       OR SYNC_DATE > :lastSyncDate
       OR (SYNC_DATE = :lastSyncDate AND ID > :lastId)
   )
 ORDER BY SYNC_DATE ASC, ID ASC
 FETCH FIRST :limit ROWS ONLY;
```

| Thuộc tính | Giá trị |
|---|---|
| **Tham số bind** | `:safeWatermark` (`SYSDATE - 120/86400`), `:lastSyncDate`, `:lastId`, `:limit` |
| **Index sử dụng** | `IDX_C360_THAY_DOI (SYNC_DATE, ID)` |
| **Số dòng kỳ vọng** | `0..limit` dòng |
| **Có dữ liệu** | Trả danh sách bản ghi và sinh `nextCursor` từ dòng cuối cùng |
| **Không có (NOT FOUND)** | Trả `data = []`, `hasMore = false` |
| **Nhiều hơn kỳ vọng** | Giới hạn bởi `:limit` |
| **Lỗi DB / Timeout** | Trả `HTTP 503` (`MSG-E999`) |
| **Khóa / Timeout** | Không khóa; timeout `5s` |

---

## KiemTraDauVao – Đặc tả kiểm tra & Quy tắc Gắn cờ `DQ_FLAGS`

| Check-ID | Hạng mục | Loại kiểm tra | Điều kiện lỗi / Quy tắc phát hiện | Mã cờ `DQ_FLAGS` / Mã thông báo | Thứ tự | Dừng ngay khi lỗi? | Tham chiếu BD |
| --- | --- | --- | --- | --- | --- | --- | --- |
| CHK-01 | Địa chỉ Thường trú – Tỉnh | Chuẩn hóa C360 | Không có Tỉnh suy ra từ Xã và `MA_TINH` gốc trống (`'00'`, `'000'`, `'00TTT'`, `NULL`) hoặc không tra cứu được trong `C360_MAP_DBHC_TINH` | Gắn cờ `TINH_KHONG_MAP` | 1 | Không (Giữ dòng trên C360) | Phần 2 - Mục 2 & 4 |
| CHK-02 | Địa chỉ Thường trú – Xã | Chuẩn hóa C360 | `MA_XA` gốc trống (`'00000'`, `'0'`, `NULL`) hoặc không tồn tại trong `C360_MAP_DBHC_XA` | Gắn cờ `XA_KHONG_MAP` | 2 | Không | Phần 2 - Mục 2 & 4 |
| CHK-03 | Địa chỉ Thường trú – Xã mơ hồ | Chuẩn hóa C360 | `MA_XA` gốc khớp trong `C360_MAP_DBHC_XA` nhưng có `LOAI_MAP = 'MO_HO'` (xã cũ bị tách ra $\ge 2$ xã mới) | Để `MA_XA_THUONG_TRU = NULL`, gắn cờ `XA_MO_HO` | 3 | Không | Phần 2 - Mục 2 & 4 |
| CHK-04 | Địa chỉ Thường trú – Tỉnh lệch Xã | Chuẩn hóa C360 | Cả Tỉnh suy ra từ Xã và Tỉnh map từ `MA_TINH` gốc đều hợp lệ nhưng khác nhau (`MA_TINH_TU_XA <> MA_TINH_KHAI_BAO`) | Lấy Tỉnh theo Xã, gắn cờ `TINH_LECH_XA` | 4 | Không | Phần 2 - Mục 2 & 4 |
| CHK-05 | Địa chỉ Liên hệ (`_LH`) & Khai sinh (`_KS`) | Chuẩn hóa C360 | Áp dụng quy tắc tương tự CHK-01..04 cho `_LH` và `_KS` | Gắn cờ `TINH_LH_KHONG_MAP`, `XA_LH_KHONG_MAP`, `XA_LH_MO_HO`, `TINH_LH_LECH_XA` và tương ứng cho `_KS` | 5 | Không | Phần 2 - Mục 2 & 4 |
| CHK-06 | Ngày sinh (`NGAY_SINH`) | Chuẩn hóa C360 | `NGAY_SINH` trống, hoặc năm ngoài khoảng `[1900..năm hiện tại]`, hoặc tháng ngoài `01..12`, hoặc ngày sai lịch (ví dụ `19950230` - 30/02) | Để `NGAY_SINH_CHUAN = NULL`, giữ `NGAY_SINH_GOC`, gắn cờ `NGAY_SINH_LOI` | 6 | Không | Phần 2 - Mục 3 & 4 |
| CHK-07 | Số định danh 12 số (`SO_CCCD` / `SO_CMTND`) | Chuẩn hóa C360 | Cả `SO_CCCD` và `SO_CMTND` sau khi `TRIM` đều không phải chuỗi đúng 12 chữ số (`^\d{12}$`) | Để `SO_DINH_DANH = NULL`, gắn cờ `KHONG_SO_DINH_DANH` | 7 | Không | Phần 2 - Mục 3 & 4 |
| CHK-08 | Số điện thoại (`PHONE_NUMBER`) | Chuẩn hóa C360 | Sau khi bỏ khoảng trắng/dấu gạch và đổi đầu `+84`/`840`/`84` -> `0`, thêm `'0'` nếu 9 số bắt đầu bằng `3,5,7,8,9`: nếu không khớp `^0\d{9,10}$` | Để `SO_DIEN_THOAI = NULL`, gắn cờ `SDT_LOI` | 8 | Không | Phần 2 - Mục 3 & 4 |
| CHK-09 | Dân tộc (`DM_DAN_TOC_ID`) | Chuẩn hóa C360 | `DM_DAN_TOC_ID` trống hoặc không khớp với dòng nào đang còn hiệu lực (`NGAY_HET_HL IS NULL OR NGAY_HET_HL > SYSDATE`) trong `MIR_DM_DANTOC` | Để `MA_DAN_TOC = NULL`, gắn cờ `DAN_TOC_LOI` | 9 | Không | Phần 2 - Mục 3 & 4 |
| CHK-10 | Quốc tịch (`QUOC_TICH`) | Chuẩn hóa C360 | `QUOC_TICH` trống hoặc không khớp cả cột `MA` lẫn `TO_CHAR(ID)` trong `MIR_DM_QUOC_TICH` | Để `MA_QUOC_TICH = NULL`, gắn cờ `QUOC_TICH_LOI` | 10 | Không | Phần 2 - Mục 3 & 4 |
| CHK-11 | Header `X-API-Key` | Bảo mật API | Không truyền `X-API-Key`, key không hợp lệ hoặc không có scope tương ứng | Ném `MSG-E001` (`401`) hoặc `MSG-E002` (`403`) | 1 | Có | Phần 3 - Mục 3 |
| CHK-12 | Tham số API Tra cứu lô / Tìm kiếm | Kiểm tra đầu vào API | `maList` rỗng hoặc `> 1000` phần tử; hoặc `ngaySinh` không khớp `^\d{4}(\d{2}(\d{2})?)?$` | Ném `ValidationException` (`MSG-E003`, `400`) | 2 | Có | Phần 3 - Mục 3 |

---

## XuLyLoi_Log – Xử lý ngoại lệ & log

| Tình huống | Exception | HTTP | Mã thông báo | Mức log | Nội dung log | Rollback? | Ghi chú |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Thiếu hoặc sai `X-API-Key` | `UnauthorizedException` | 401 | `MSG-E001` | WARN | `clientIp, endpoint, apiKeyPrefix` | — | Chặn truy cập trái phép |
| Không đủ quyền Scope API | `ForbiddenException` | 403 | `MSG-E002` | WARN | `clientId, endpoint, requiredScope` | — | Phân quyền theo client |
| Tham số API không hợp lệ | `ValidationException` | 400 | `MSG-E003` | INFO | `endpoint, paramName, invalidValue` | — | Báo lỗi rõ ràng cho client |
| Không tìm thấy cá nhân (API-001) | `NotFoundException` | 404 | `MSG-I001` | INFO | `ma, matchedBy=NONE` | — | Cả 3 tầng đều không có |
| Trùng $\ge 2$ người khi tra mã cũ / CCCD | `ConflictMultipleMatchesException` | 409 | `MSG-E004` | WARN | `ma, matchedTier, candidateCount` | — | Trả kèm danh sách `candidates` |
| Lỗi trong khi chạy Full Load C360 | `SyncEngineException` | — | — | ERROR | `batchId, step, errorMessage` | Có + Reset `PAUSE_NRT=0` | Bắt buộc nhả cờ `PAUSE_NRT=0` để không treo NRT |
| Lỗi trong khi chạy lô Near Real-time | `NrtBatchException` | — | — | ERROR | `batchId, queueCount, errorMessage` | Có (Giữ nguyên Queue) | Lô sau tự động xử lý lại các khóa trong Queue |

---

## UnitTest – Đặc tả kiểm thử đơn vị

| UT-ID | Phân loại | Bước DD (sheet 05) | Điều kiện tiền đề / dữ liệu | Đầu vào | Thao tác | Kết quả kỳ vọng | Kết quả thực tế | OK/NG | Người test | Ngày test | Bug ID |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| UT-001 | Bình thường | 1.1, 3.2 | File `DM_DBHC_202611091512.csv` (`15.325` dòng) | Nạp CSV và chạy `refresh_dbhc_map_tables` | Kiểm tra `C360_MAP_DBHC_TINH` | Có đủ 36 tỉnh mới (`'001'`..`'098'`); tra `'34'`, `'034'`, `'34TTT'` đều ra `'033'` (Tỉnh Hưng Yên, `LOAI_MAP='SAP_NHAP'`) | Khớp 100% | OK | DE | 2026/11/10 | |
| UT-002 | Bình thường | 3.3, 3.4 | Bảng `MIR_DM_DBHC` có xã mới `'00004'` (`MA_DBHC_SN='00013;00004;00019'`) | Chạy `refresh_dbhc_map_tables` | Tra `'00004'` và `'00013'` trong `C360_MAP_DBHC_XA` | `'00004'` có `LOAI_MAP='HIEN_HANH'`; `'00013'` map sang `'00004'` (Phường Ba Đình, Tỉnh `'001'`) với `LOAI_MAP='SAP_NHAP'` | Khớp 100% | OK | DE | 2026/11/10 | |
| UT-003 | Biên | 3.4, CHK-03 | Mã xã cũ `'00999'` nằm trong `MA_DBHC_SN` của 2 xã mới cùng tỉnh Hà Nội (`'01'`) | Công dân có `MA_XA = '00999'`, `MA_TINH = '01TTT'` | Chuẩn hóa C360 | `MA_XA_THUONG_TRU = NULL`, `MA_TINH_THUONG_TRU = '001'`, `DQ_FLAGS` chứa `'XA_MO_HO'` | Khớp 100% | OK | DE | 2026/11/10 | |
| UT-004 | Bất thường | 4.1, CHK-04 | Công dân có `MA_XA = '00004'` (Hà Nội `'001'`) nhưng `MA_TINH = '79TTT'` (TP.HCM `'079'`) | Chạy chuẩn hóa C360 | Kiểm tra tỉnh thường trú & cờ lỗi | Ưu tiên lấy theo Xã: `MA_TINH_THUONG_TRU = '001'`, `MA_TINH_GOC = '79TTT'`, `DQ_FLAGS` chứa `'TINH_LECH_XA'` | Khớp 100% | OK | DE | 2026/11/10 | |
| UT-005 | Bình thường | 4.1 | Công dân có `MA_XA = '4'` hoặc `'13'` (mất số 0 ở đầu do ép kiểu số) | Chạy chuẩn hóa C360 | Kiểm tra `MA_XA_THUONG_TRU` | Tự động `LPAD` thành `'00004'` / `'00013'` và map đúng về Phường Ba Đình (`'00004'`), không báo lỗi `XA_KHONG_MAP` | Khớp 100% | OK | DE | 2026/11/10 | |
| UT-006 | Bình thường | 4.1 | Lọc nghiệp vụ `TYPEID` và `TRANG_THAI_DUYET` | 5 bản ghi có `TYPEID IN ('GT','GM','GG','GK')` và `TRANG_THAI_DUYET IN (1,3,4)` | Chạy chuẩn hóa C360 | 100% các bản ghi này bị loại, không xuất hiện trong `C360_THONG_TIN_CA_NHAN` | Khớp 100% | OK | DE | 2026/11/10 | |
| UT-007 | Bình thường | 4.1 | Khử trùng `MA_CD` ở nhiều hộ gia đình (`MA_HO_GD`) | `MA_CD = '0120456789'` có 3 dòng hộ gia đình với `MODIFY_DATE` ngày `01/10`, `05/10`, `09/10` | Chạy chuẩn hóa C360 | Chỉ giữ 1 dòng duy nhất của ngày `09/10` trên bảng C360 | Khớp 100% | OK | DE | 2026/11/10 | |
| UT-008 | Biên | 4.1, CHK-06 | Chuẩn hóa `NGAY_SINH` kiểu `NUMBER` | Các giá trị: `19950815`, `19950800`, `19950000`, `1995`, `19950230`, `17500101` | Chạy chuẩn hóa C360 | Ra `'19950815'`, `'199508'`, `'1995'`, `'1995'`; hai giá trị `19950230` (sai lịch) và `17500101` ra `NULL` kèm cờ `'NGAY_SINH_LOI'` | Khớp 100% | OK | DE | 2026/11/10 | |
| UT-009 | Bình thường | 4.1, CHK-08..10 | Chuẩn hóa SĐT, Quốc tịch (`'VN'` vs `'233'`) & Dân tộc (`ID=171` vs `ID=248` hết hạn) | `PHONE='840912 345 678'`, `QUOC_TICH='233'`, `DM_DAN_TOC_ID=248` | Chạy chuẩn hóa C360 | SĐT ra `'0912345678'`; Quốc tịch ra `'VN' - 'Việt Nam'`; Dân tộc `ID=248` hết hạn nên `MA_DAN_TOC=NULL` và gắn cờ `'DAN_TOC_LOI'` | Khớp 100% | OK | DE | 2026/11/10 | |
| UT-010 | Bình thường | 4.3 | Near Real-time Queue: `UPDATE` đổi `MA_CD` ở bảng Mirror | Đổi `MA_CD` từ `'CD_OLD_01'` sang `'CD_NEW_01'` (lưu `'CD_OLD_01'` vào `MA_CD_OLD`) | Chạy `run_c360_nrt_batch` | Trigger đẩy cả 2 mã vào Queue; sau khi chạy NRT, `'CD_NEW_01'` xuất hiện trên C360 và `'CD_OLD_01'` bị `DELETE` sạch khỏi C360 | Khớp 100% | OK | DE | 2026/11/10 | |
| UT-011 | Bình thường | 4.3 | Near Real-time Queue: `DELETE` dòng hộ mới nhất của người có 2 hộ | Xóa dòng hộ ngày `09/10`, còn dòng hộ ngày `05/10` | Chạy `run_c360_nrt_batch` | Bản ghi trên C360 tự động lùi về lấy thông tin của dòng hộ ngày `05/10` | Khớp 100% | OK | DE | 2026/11/10 | |
| UT-012 | Bình thường | 4.3 | Near Real-time Queue: Chuyển `TYPEID = 'GG'` hoặc `DELETE` hết các dòng của 1 người | Cập nhật ở nguồn/Mirror | Chạy `run_c360_nrt_batch` | Công dân đó bị `DELETE` hoàn toàn khỏi `C360_THONG_TIN_CA_NHAN`; đối soát `COUNT(DISTINCT MA_CD)` hợp lệ khớp 100% | Khớp 100% | OK | DE | 2026/11/10 | |
| UT-013 | Bình thường | 5.2 | API-001: Tự động dò 3 tầng ưu tiên & bắt `409 Conflict` | Gọi với `MA_SO_BHXH`, `MA_SO_BHXH_CU`, và `SO_DINH_DANH` trùng 2 người | `GET /api/v1/ca-nhan/{ma}` | Trả `200` khi duy nhất; trả `409 Conflict` kèm 2 ứng viên khi trùng `SO_DINH_DANH`; trả `404` khi không tồn tại | Khớp 100% | OK | Tester | 2026/11/10 | |
| UT-014 | Bình thường | 5.4 | API-003: Tìm kiếm Họ tên có dấu/không dấu + Ngày sinh rút gọn | DB lưu `'199508'`, gọi tìm kiếm với `hoTen='nguyên văn an'`, `ngaySinh='19950815'` (và ngược lại) | `GET /api/v1/ca-nhan/tim-kiem` | Tìm thấy chính xác bản ghi công dân trong cả 2 chiều | Khớp 100% | OK | Tester | 2026/11/10 | |
| UT-015 | Bình thường | 5.5 | API-004: Phân trang Cursor với độ trễ an toàn `120s` | Bản ghi vừa `MERGE` cách đây `10s` vs bản ghi cách đây `180s` | `GET /api/v1/ca-nhan/thay-doi` | Chỉ trả bản ghi `>= 120s` trước, không trả bản ghi mới `10s` để tránh sót lô đang commit dở | Khớp 100% | OK | Tester | 2026/11/10 | |

---

## QA – Quản lý hỏi đáp

| No | Ngày hỏi | Người hỏi | Sheet / Mục | Câu hỏi | Người trả lời | Trả lời | Ngày trả lời | Trạng thái | Đã phản ánh vào tài liệu? |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 2026/11/10 | Kỹ sư dữ liệu | 06 - SQL-02 | Khi làm mới 2 bảng Map (`C360_MAP_DBHC_TINH` và `_XA`), tại sao phải dùng `DELETE + INSERT` trong cùng 1 transaction mà cấm dùng `TRUNCATE`? | Solution Architect | Vì `TRUNCATE` là lệnh DDL (auto-commit ngay lập tức), làm bảng Map bị rỗng trong vài giây trước khi `INSERT` xong; nếu job Near Real-time chạy đúng lúc đó sẽ map lỗi toàn bộ địa chỉ (`TINH_KHONG_MAP`, `XA_KHONG_MAP`). Dùng `DELETE + INSERT` trong 1 transaction giúp các truy vấn đồng thời vẫn đọc được bản chụp cũ nhất quán cho tới khi `COMMIT`. | 2026/11/10 | Closed | Có (Mục 05 Bước 3.1, SQL-02) |
| 2 | 2026/11/10 | Kỹ sư backend | 06 - SQL-08 | Người dùng nhập `ngaySinh = '19950815'` vào API Tìm kiếm, nhưng trên DB công dân đó chỉ có năm sinh rút gọn `NGAY_SINH_CHUAN = '1995'` (do nguồn lưu `19950000`). Câu SQL làm sao khớp được cả hai chiều mà vẫn dùng được Index? | Tech Lead | Câu SQL-08 sử dụng Composite Index `(HO_TEN_KHONG_DAU, NGAY_SINH_CHUAN)`: điều kiện `HO_TEN_KHONG_DAU = :hoTenKhongDau` thu hẹp phạm vi quét Index xuống chỉ vài dòng trùng tên, sau đó kiểm tra đối xứng `(NGAY_SINH_CHUAN LIKE :ngaySinhInput || '%' OR :ngaySinhInput LIKE NGAY_SINH_CHUAN || '%')` ngay trên các lá Index đó nên vừa khớp cả 2 chiều vừa không bao giờ quét full bảng. | 2026/11/10 | Closed | Có (Mục 05 Bước 5.4, SQL-08) |
| 3 | 2026/11/10 | Kỹ sư dữ liệu | 05 - Bước 4.3 | Khi một bản ghi trên Mirror bị `UPDATE` đổi cột `MA_CD` sang mã mới, làm sao để bảng C360 biết mà xóa mã cũ đi? | Tech Lead | Trigger `TRG_MIR_NHANKHAU_QUEUE` kiểm tra `IF UPDATING AND :OLD.MA_CD <> :NEW.MA_CD`: đẩy **cả `:OLD.MA_CD` và `:NEW.MA_CD`** vào `C360_SYNC_QUEUE`. Khi job Near Real-time xử lý lô Queue đó, `:NEW.MA_CD` có dòng hợp lệ nên được `MERGE` vào C360, còn `:OLD.MA_CD` không còn dòng hợp lệ nào ở Mirror nên bị nhánh `DELETE` xóa sạch khỏi C360. | 2026/11/10 | Closed | Có (Mục 05 Bước 4.3, UT-010) |
