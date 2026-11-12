#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BAT-C360-001 (Script 1 - Initial Seed Generator):
Sinh dữ liệu giả lập ban đầu cho bảng nguồn VSS.VSS_HO_GD_NHANKHAU.
- Sử dụng trực tiếp mã thực tế từ 3 bảng danh mục đã nạp (VSS_DM_DBHC, VSS_DM_DANTOC, VSS_DM_QUOC_TICH).
- Bao phủ đầy đủ 100% kịch bản dữ liệu sạch, dữ liệu bẩn và biên (Edge Cases) theo mục 3 - Phần 1.1.
- Xuất báo cáo thống kê số lượng dòng theo từng kịch bản vào data/seed_ground_truth.json.
"""

import argparse
import json
import random
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from p1_source_and_mirror.db_env import (
    NHANKHAU_COLUMNS,
    get_connection,
    get_db_engine,
    init_source_and_mirror_schema,
)

HO_LIST = ["NGUYỄN", "TRẦN", "LÊ", "PHẠM", "HOÀNG", "HUỲNH", "PHAN", "VŨ", "VÕ", "ĐẶNG", "BÙI", "ĐỖ"]
DEM_NAM = ["VĂN", "HỮU", "ĐỨC", "MINH", "QUỐC", "THÀNH", "TUẤN", "ANH", "XUÂN", "TRỌNG"]
DEM_NU = ["THỊ", "NGỌC", "THU", "PHƯƠNG", "THANH", "MỸ", "KIM", "HOÀI", "DIỆU", "KHÁNH"]
TEN_NAM = ["AN", "BÌNH", "CƯỜNG", "DŨNG", "ĐẠT", "HÙNG", "KHÁNH", "LONG", "MINH", "NAM", "PHÚC", "QUÂN", "SƠN", "TÙNG", "VIỆT"]
TEN_NU = ["ANH", "CHÂU", "DUNG", "GIANG", "HÀ", "HƯƠNG", "LAN", "LINH", "MAI", "NGA", "NHUNG", "OANH", "THẢO", "TRANG", "VY"]

HO_KD_MAP = str.maketrans(
    "ÀÁẢÃẠĂẰẮẲẴẶÂẦẤẨẪẬÈÉẺẼẸÊỀẾỂỄỆÌÍỈĨỊÒÓỎÕỌÔỒỐỔỖỘƠỜỚỞỠỢÙÚỦŨỤƯỪỨỬỮỰỲÝỶỸỴĐ"
    "àáảãạăằắẳẵặâầấẩẫậèéẻẽẹêềếểễệìíỉĩịòóỏõọôồốổỗộơờớởỡợùúủũụưừứửữựỳýỷỹỵđ",
    "AAAAAAAAAAAAAAAAAEEEEEEEEEEEIIIIIOOOOOOOOOOOOOOOOOUUUUUUUUUUUYYYYYD"
    "aaaaaaaaaaaaaaaaaeeeeeeeeeeeiiiiiooooooooooooooooouuuuuuuuuuuyyyyyd",
)


def remove_accents(s: str) -> str:
    return s.translate(HO_KD_MAP)


def fetch_reference_pools(conn: Any) -> Dict[str, Any]:
    """Đọc các tập mã thực tế từ 3 bảng danh mục VSS_DM_* để sinh dữ liệu khớp thực tế."""
    cur = conn.cursor()

    # 1. Tỉnh đang hiệu lực (36 tỉnh 2025)
    cur.execute(
        """
        SELECT MA_DBHC, TEN, MA_DBHC_SN
        FROM VSS_DM_DBHC
        WHERE TINH_TRANG = '1' AND IS_CAP_XA = 0 AND IS_LT = 1
          AND NGAY_HET_HIEU_LUC LIKE '2099-12-31%'
        ORDER BY MA_DBHC
        """
    )
    active_provinces = []
    merged_provinces = []  # (old_code, new_code)
    for ma_dbhc, ten, ma_sn in cur.fetchall():
        active_provinces.append((ma_dbhc, ten))
        if ma_sn:
            for token in ma_sn.split(";"):
                t = token.strip()
                if t and t != ma_dbhc:
                    merged_provinces.append((t, ma_dbhc))

    # 2. Xã đang hiệu lực (3.322 xã 2025)
    cur.execute(
        """
        SELECT MA_DBHC, MA_CHA, TEN, MA_DBHC_SN
        FROM VSS_DM_DBHC
        WHERE TINH_TRANG = '1' AND IS_CAP_XA = 1
          AND NGAY_HET_HIEU_LUC LIKE '2099-12-31%'
        ORDER BY MA_DBHC
        """
    )
    active_wards = []  # (ma_xa, ma_tinh, ten)
    sn_counts: Dict[str, List[Tuple[str, str]]] = {}
    for ma_xa, ma_tinh, ten, ma_sn in cur.fetchall():
        active_wards.append((ma_xa, ma_tinh, ten))
        if ma_sn:
            for token in ma_sn.split(";"):
                t = token.strip()
                if t and t != ma_xa:
                    sn_counts.setdefault(t, []).append((ma_xa, ma_tinh))

    merged_wards_1to1 = [(old_xa, targets[0][0], targets[0][1]) for old_xa, targets in sn_counts.items() if len(targets) == 1]

    # 3. Dân tộc đang hiệu lực
    cur.execute(
        """
        SELECT ID, MA, TEN
        FROM VSS_DM_DANTOC
        WHERE TRANG_THAI = 1 AND (NGAY_HET_HL IS NULL OR NGAY_HET_HL = '')
        ORDER BY ID
        """
    )
    valid_dantoc = [r[0] for r in cur.fetchall()]
    if not valid_dantoc:
        valid_dantoc = [171, 172, 173, 174]

    return {
        "active_provinces": active_provinces,
        "merged_provinces": merged_provinces,
        "active_wards": active_wards,
        "merged_wards_1to1": merged_wards_1to1,
        "valid_dantoc": valid_dantoc,
    }


def make_empty_row() -> Dict[str, Any]:
    return {col: None for col, _ in NHANKHAU_COLUMNS}


def generate_seed_dataset(
    conn: Any,
    num_persons: int = 50000,
    seed: int = 42,
    batch_size: int = 5000,
    out_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    t0 = time.perf_counter()
    rng = random.Random(seed)
    pools = fetch_reference_pools(conn)

    active_wards = pools["active_wards"]
    merged_wards = pools["merged_wards_1to1"]
    merged_provinces = pools["merged_provinces"]
    valid_dantoc = pools["valid_dantoc"]

    cur = conn.cursor()
    cur.execute("DELETE FROM VSS_HO_GD_NHANKHAU")
    conn.commit()

    col_names = [c for c, _ in NHANKHAU_COLUMNS]
    placeholders = ", ".join(["?"] * len(col_names))
    insert_sql = f"INSERT INTO VSS_HO_GD_NHANKHAU ({', '.join(col_names)}) VALUES ({placeholders})"

    stats = {
        "num_persons_total": num_persons,
        "num_rows_total": 0,
        "expected_valid_c360_persons": 0,
        "filtered_out_persons_typeid_d": 0,
        "filtered_out_persons_unapproved": 0,
        "multi_household_persons": 0,
        "multi_household_tie_id_persons": 0,
        "inactive_or_deceased_persons": 0,
        "addr_chinh_xac": 0,
        "addr_5char_tinh_ttt": 0,
        "addr_sap_nhap_1to1": 0,
        "addr_moho_same_province_00999": 0,
        "addr_moho_cross_province_00998": 0,
        "addr_mismatch_tinh_xa": 0,
        "addr_infer_tinh_from_xa": 0,
        "addr_invalid_unmapped": 0,
        "dob_standard_yyyymmdd": 0,
        "dob_slash_or_dash": 0,
        "dob_year_only": 0,
        "dob_from_ngay_sinh_dt": 0,
        "dob_invalid": 0,
        "cccd_valid_12": 0,
        "cccd_from_cmtnd_12": 0,
        "cccd_9_digits_warn": 0,
        "cccd_invalid_err": 0,
        "phone_valid": 0,
        "phone_intl_84": 0,
        "phone_invalid_warn": 0,
        "dantoc_expired_248": 0,
        "dantoc_invalid_9999": 0,
        "quoctich_id_or_name": 0,
        "quoctich_invalid": 0,
        "hoten_extra_spaces": 0,
    }

    row_id = 1
    batch_rows: List[Tuple[Any, ...]] = []

    for p_idx in range(1, num_persons + 1):
        ma_cd = f"{p_idx:010d}"
        gioi_tinh = 1 if (p_idx % 2 == 1) else 2
        ho = HO_LIST[p_idx % len(HO_LIST)]
        dem = DEM_NAM[p_idx % len(DEM_NAM)] if gioi_tinh == 1 else DEM_NU[p_idx % len(DEM_NU)]
        ten = TEN_NAM[p_idx % len(TEN_NAM)] if gioi_tinh == 1 else TEN_NU[p_idx % len(TEN_NU)]

        # 5% Họ tên thừa khoảng trắng
        if p_idx % 20 == 0:
            ho_ten = f"  {ho}   {dem}   {ten}  "
            stats["hoten_extra_spaces"] += 1
        else:
            ho_ten = f"{ho} {dem} {ten}"
        ho_ten_kd = remove_accents(f"{ho} {dem} {ten}")
        ten_kd = remove_accents(ten)

        # Phân bổ kịch bản lọc nghiệp vụ (TYPEID & TRANG_THAI_DUYET)
        # - p_idx % 50 == 1 (2%): TYPEID = 'D' -> bị loại hoàn toàn
        # - p_idx % 50 == 2 (2%): TRANG_THAI_DUYET = 0 -> bị loại hoàn toàn
        # - Còn lại 96%: Công dân hợp lệ đưa vào C360
        is_typeid_d = (p_idx % 50 == 1)
        is_unapproved = (p_idx % 50 == 2)

        if is_typeid_d:
            stats["filtered_out_persons_typeid_d"] += 1
        elif is_unapproved:
            stats["filtered_out_persons_unapproved"] += 1
        else:
            stats["expected_valid_c360_persons"] += 1

        # Số hộ gia đình của công dân này:
        # - p_idx % 25 == 0 (4%, thuộc nhóm hợp lệ): có 2 hộ gia đình (1 dòng cũ, 1 dòng mới)
        # - p_idx % 100 == 3 (1%, thuộc nhóm hợp lệ): có 2 hộ gia đình trùng NGAY_SUA & NGAY_TAO -> phân định bằng ID DESC
        num_households = 1
        is_tie_id_case = False
        if not is_typeid_d and not is_unapproved:
            if p_idx % 25 == 0:
                num_households = 2
                stats["multi_household_persons"] += 1
            elif p_idx % 100 == 3:
                num_households = 2
                is_tie_id_case = True
                stats["multi_household_persons"] += 1
                stats["multi_household_tie_id_persons"] += 1

        # Chọn kịch bản địa chỉ thường trú cho dòng chính (dòng mới nhất) của công dân
        ward_new, tinh_new, ward_name = active_wards[p_idx % len(active_wards)]
        ma_tinh_hk = tinh_new
        ma_xa_hk = ward_new
        dia_chi_hk = f"Số {p_idx % 200 + 1}, {ward_name}"

        # Chọn địa chỉ LH và KS mặc định sạch
        ward_lh, tinh_lh, ward_lh_name = active_wards[(p_idx + 7) % len(active_wards)]
        ma_tinh_lh, ma_xa_lh, dia_chi_lh = tinh_lh, ward_lh, f"LH Số {p_idx % 100 + 1}, {ward_lh_name}"
        ward_ks, tinh_ks, ward_ks_name = active_wards[(p_idx + 13) % len(active_wards)]
        ma_tinh_ks, ma_xa_ks, dia_chi_ks = tinh_ks, ward_ks, f"KS {ward_ks_name}"

        if not is_typeid_d and not is_unapproved:
            addr_Bucket = p_idx % 100
            if addr_Bucket in (10, 11, 12, 13, 14):
                # 5%: Mã tỉnh 5 ký tự '01TTT' / '79TTT' (Khảo sát thực tế)
                ma_tinh_hk = f"{tinh_new}TTT"
                stats["addr_5char_tinh_ttt"] += 1
            elif addr_Bucket in (15, 16, 17, 18, 19, 20, 21, 22) and merged_wards:
                # 8%: Mã cũ sáp nhập 1-1 (SAP_NHAP)
                old_xa, new_xa, new_tinh = merged_wards[p_idx % len(merged_wards)]
                old_tinh = merged_provinces[p_idx % len(merged_provinces)][0] if merged_provinces else new_tinh
                ma_tinh_hk = new_tinh
                ma_xa_hk = old_xa
                # Đồng thời gán tỉnh cũ sáp nhập ở địa chỉ LH để kiểm thử bảng Map Tỉnh
                ma_tinh_lh = old_tinh
                stats["addr_sap_nhap_1to1"] += 1
            elif addr_Bucket == 23:
                # 1%: Mã xã cũ MO_HO cùng tỉnh Hà Nội ('00999' -> tách ra '00004' và '00007' cùng thuộc '01')
                ma_tinh_hk = None
                ma_xa_hk = "00999"
                stats["addr_moho_same_province_00999"] += 1
            elif addr_Bucket == 24:
                # 1%: Mã xã cũ MO_HO khác tỉnh ('00998' -> tách ra '01' và '33')
                ma_tinh_hk = None
                ma_xa_hk = "00998"
                stats["addr_moho_cross_province_00998"] += 1
            elif addr_Bucket in (26, 27):
                # 2%: Xã hợp lệ nhưng không thuộc Tỉnh ghi trên hồ sơ (ERR_XA_KHONG_THUOC_TINH)
                wrong_tinh = "01" if tinh_new != "01" else "79"
                ma_tinh_hk = wrong_tinh
                ma_xa_hk = ward_new
                stats["addr_mismatch_tinh_xa"] += 1
            elif addr_Bucket in (28, 29):
                # 2%: Xã hợp lệ nhưng Tỉnh bị để trống (WARN_TINH_SUY_TU_XA)
                ma_tinh_hk = None
                ma_xa_hk = ward_new
                stats["addr_infer_tinh_from_xa"] += 1
            elif addr_Bucket in (30, 31):
                # 2%: Mã Tỉnh / Xã rác không ánh xạ được (ERR_TINH_KHONG_HOP_LE, ERR_XA_KHONG_HOP_LE)
                ma_tinh_hk = "99"
                ma_xa_hk = "99999"
                stats["addr_invalid_unmapped"] += 1
            else:
                stats["addr_chinh_xac"] += 1

        # Kịch bản NGAY_SINH & NGAY_SINH_DT
        birth_year = 1955 + (p_idx % 65)
        birth_month = (p_idx % 12) + 1
        birth_day = (p_idx % 28) + 1
        ngay_sinh = f"{birth_year:04d}{birth_month:02d}{birth_day:02d}"
        ngay_sinh_dt = f"{birth_year:04d}-{birth_month:02d}-{birth_day:02d} 00:00:00"

        if not is_typeid_d and not is_unapproved:
            dob_bucket = p_idx % 100
            if dob_bucket in (32, 33, 34):
                ngay_sinh = f"{birth_day:02d}/{birth_month:02d}/{birth_year:04d}"
                stats["dob_slash_or_dash"] += 1
            elif dob_bucket in (35, 36):
                ngay_sinh = f"{birth_year:04d}-{birth_month:02d}-{birth_day:02d}"
                stats["dob_slash_or_dash"] += 1
            elif dob_bucket in (37, 38):
                # Chỉ có năm sinh
                ngay_sinh = f"{birth_year:04d}" if dob_bucket == 37 else f"{birth_year:04d}0000"
                ngay_sinh_dt = None
                stats["dob_year_only"] += 1
            elif dob_bucket == 39:
                # NGAY_SINH rỗng nhưng cứu được từ NGAY_SINH_DT
                ngay_sinh = None
                stats["dob_from_ngay_sinh_dt"] += 1
            elif dob_bucket == 40:
                # Ngày sinh sai hoàn toàn
                ngay_sinh = "18509999" if (p_idx % 2 == 0) else "INVALID_DATE"
                ngay_sinh_dt = None
                stats["dob_invalid"] += 1
            else:
                stats["dob_standard_yyyymmdd"] += 1

        # Kịch bản SO_CCCD & SO_CMTND
        cccd_clean = f"0{tinh_new[:2].zfill(2)}{gioi_tinh - 1}{str(birth_year)[-2:]}{p_idx % 1000000:06d}"
        so_cccd: Optional[str] = cccd_clean
        so_cmtnd: Optional[str] = None
        if not is_typeid_d and not is_unapproved:
            cccd_bucket = p_idx % 100
            if cccd_bucket in (41, 42):
                # CCCD để trống, 12 số nằm ở SO_CMTND kèm khoảng trắng
                so_cccd = None
                so_cmtnd = f" {cccd_clean} "
                stats["cccd_from_cmtnd_12"] += 1
            elif cccd_bucket in (43, 44):
                # CMND 9 số cũ
                so_cccd = None
                so_cmtnd = f"{100000000 + (p_idx % 899999999):09d}"
                stats["cccd_9_digits_warn"] += 1
            elif cccd_bucket == 45:
                # CCCD sai định dạng (toàn số 0 hoặc chứa chữ)
                so_cccd = "000000000000" if (p_idx % 2 == 0) else "001090123ABC"
                stats["cccd_invalid_err"] += 1
            else:
                stats["cccd_valid_12"] += 1

        # Kịch bản PHONE_NUMBER & SO_DIEN_THOAI
        phone_clean = f"09{p_idx % 100000000:08d}"
        phone_number: Optional[str] = phone_clean
        so_dien_thoai: Optional[str] = None
        if not is_typeid_d and not is_unapproved:
            ph_bucket = p_idx % 100
            if ph_bucket in (46, 47):
                phone_number = f"+849{p_idx % 100000000:08d}"
                stats["phone_intl_84"] += 1
            elif ph_bucket == 48:
                phone_number = None
                so_dien_thoai = f"091.{p_idx % 1000:03d}.{p_idx % 10000:04d}"
                stats["phone_valid"] += 1
            elif ph_bucket == 49:
                phone_number = "0000000000" if (p_idx % 2 == 0) else "012345"
                stats["phone_invalid_warn"] += 1
            else:
                stats["phone_valid"] += 1

        # Kịch bản QUOC_TICH & DM_DAN_TOC_ID
        quoc_tich = "VN"
        dm_dan_toc_id = 171 if (p_idx % 5 != 0) else valid_dantoc[p_idx % len(valid_dantoc)]
        if not is_typeid_d and not is_unapproved:
            qt_bucket = p_idx % 100
            if qt_bucket == 53:
                quoc_tich = "233"  # ID của Việt Nam trong DM_QUOC_TICH
                stats["quoctich_id_or_name"] += 1
            elif qt_bucket == 54:
                quoc_tich = "Việt Nam"
                stats["quoctich_id_or_name"] += 1
            elif qt_bucket == 55:
                quoc_tich = "ZZ_INVALID"
                stats["quoctich_invalid"] += 1

            if qt_bucket == 56:
                dm_dan_toc_id = 248  # Dân tộc 'Khác' đã hết hiệu lực
                stats["dantoc_expired_248"] += 1
            elif qt_bucket == 57:
                dm_dan_toc_id = 9999  # Không tồn tại trong DM_DANTOC
                stats["dantoc_invalid_9999"] += 1

        # Kịch bản TRANG_THAI & TINH_TRANG_SONG
        trang_thai = 1
        tinh_trang_song = 1
        ngay_chet = None
        if not is_typeid_d and not is_unapproved and (p_idx % 100 == 58):
            tinh_trang_song = 0
            ngay_chet = "20241015"
            stats["inactive_or_deceased_persons"] += 1

        # Sinh các dòng hộ gia đình cho công dân này
        for h_idx in range(num_households):
            r = make_empty_row()
            r["ID"] = row_id
            row_id += 1

            # Nếu có 2 hộ gia đình: h_idx=0 là hộ cũ, h_idx=1 là hộ mới nhất (được chọn vào C360)
            is_older_household = (num_households > 1 and h_idx == 0)
            r["MA_CD"] = ma_cd
            r["MA_HO_GD"] = f"HGD{p_idx:09d}_{h_idx + 1}"
            r["HO_TEN"] = ho_ten
            r["HO_TEN_KD"] = ho_ten_kd
            r["TEN_KD"] = ten_kd
            r["SO_SO_BHXH"] = ma_cd
            r["GIOI_TINH"] = gioi_tinh
            r["NGAY_SINH"] = ngay_sinh
            r["NGAY_SINH_DT"] = ngay_sinh_dt
            r["QUOC_TICH"] = quoc_tich
            r["DM_DAN_TOC_ID"] = dm_dan_toc_id
            r["SO_CCCD"] = so_cccd
            r["SO_CMTND"] = so_cmtnd
            r["PHONE_NUMBER"] = phone_number
            r["SO_DIEN_THOAI"] = so_dien_thoai
            r["EMAIL"] = f"cd{ma_cd}@vss.gov.vn"
            r["MQH_CHU_HO"] = "01" if (p_idx % 3 == 0) else "03"
            r["SO_THE_BHYT"] = f"GD4{tinh_new[:2].zfill(2)}{ma_cd}"
            r["MA_DVI"] = f"DV{tinh_new[:2].zfill(2)}01"
            r["MA_DT"] = "GD"
            r["MA_KCB_TINH"] = tinh_new
            r["MA_KCB_BV"] = "001"
            r["MA_CQBHXH"] = f"{tinh_new[:2].zfill(2)}00"
            r["TRANG_THAI"] = trang_thai
            r["TINH_TRANG_SONG"] = tinh_trang_song
            r["NGAY_CHET"] = ngay_chet

            if is_typeid_d:
                r["TYPEID"] = "D"
                r["TRANG_THAI_DUYET"] = 1
            elif is_unapproved:
                r["TYPEID"] = None
                r["TRANG_THAI_DUYET"] = 0
            else:
                r["TYPEID"] = None if (p_idx % 2 == 0) else "1"
                r["TRANG_THAI_DUYET"] = 1

            if is_older_household:
                # Dòng hộ cũ có địa chỉ cũ khác để kiểm chứng khử trùng chọn đúng dòng mới nhất
                old_w, old_t, old_wname = active_wards[(p_idx + 19) % len(active_wards)]
                r["MA_TINH"] = old_t
                r["MA_XA"] = old_w
                r["DIA_CHI"] = f"Hộ cũ của {ma_cd}, {old_wname}"
                r["MA_TINH_HK"] = old_t
                r["MA_XA_HK"] = old_w
                r["DIA_CHI_HK"] = r["DIA_CHI"]
                r["MA_TINH_LH"] = old_t
                r["MA_XA_LH"] = old_w
                r["DIA_CHI_LH"] = r["DIA_CHI"]
                r["MA_TINH_KS"] = old_t
                r["MA_XA_KS"] = old_w
                r["DIA_CHI_KS"] = r["DIA_CHI"]
                if is_tie_id_case:
                    r["NGAY_TAO"] = "2024-06-01 08:00:00"
                    r["NGAY_SUA"] = "2025-09-15 10:00:00"
                else:
                    r["NGAY_TAO"] = "2021-01-10 08:00:00"
                    r["NGAY_SUA"] = "2022-05-20 09:00:00"
            else:
                r["MA_TINH"] = ma_tinh_hk
                r["MA_XA"] = ma_xa_hk
                r["DIA_CHI"] = dia_chi_hk
                r["MA_TINH_HK"] = ma_tinh_hk
                r["MA_XA_HK"] = ma_xa_hk
                r["DIA_CHI_HK"] = dia_chi_hk
                r["MA_TINH_LH"] = ma_tinh_lh
                r["MA_XA_LH"] = ma_xa_lh
                r["DIA_CHI_LH"] = dia_chi_lh
                r["MA_TINH_KS"] = ma_tinh_ks
                r["MA_XA_KS"] = ma_xa_ks
                r["DIA_CHI_KS"] = dia_chi_ks
                if is_tie_id_case:
                    r["NGAY_TAO"] = "2024-06-01 08:00:00"
                    r["NGAY_SUA"] = "2025-09-15 10:00:00"
                else:
                    r["NGAY_TAO"] = "2023-03-15 08:30:00"
                    r["NGAY_SUA"] = "2025-10-01 14:20:00"

            r["NGUOI_TAO"] = "SYSTEM_SEED"
            r["NGUOI_SUA"] = "SYSTEM_SEED"

            batch_rows.append(tuple(r[c] for c in col_names))
            stats["num_rows_total"] += 1

            if len(batch_rows) >= batch_size:
                cur.executemany(insert_sql, batch_rows)
                conn.commit()
                batch_rows.clear()

    if batch_rows:
        cur.executemany(insert_sql, batch_rows)
        conn.commit()
        batch_rows.clear()

    elapsed = round(time.perf_counter() - t0, 2)
    stats["elapsed_sec"] = elapsed

    target_dir = Path(out_dir) if out_dir else (PROJECT_ROOT / "data")
    gt_path = target_dir / "seed_ground_truth.json"
    gt_path.parent.mkdir(parents=True, exist_ok=True)
    gt_path.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        f"[GEN_SEED_NHANKHAU] Đã sinh thành công {stats['num_rows_total']:,} dòng "
        f"({stats['num_persons_total']:,} công dân, kỳ vọng {stats['expected_valid_c360_persons']:,} công dân hợp lệ ở C360) "
        f"trong {elapsed}s.\n"
        f"  -> Báo cáo Ground Truth lưu tại: {gt_path}"
    )
    return stats


def main():
    parser = argparse.ArgumentParser(description="Script 1: Sinh dữ liệu giả lập ban đầu cho VSS_HO_GD_NHANKHAU")
    parser.add_argument("--persons", type=int, default=50000, help="Số lượng công dân (MA_CD) cần sinh")
    parser.add_argument("--seed", type=int, default=42, help="Random seed cố định để tái lập kết quả")
    parser.add_argument("--batch-size", type=int, default=5000, help="Kích thước lô commit")
    parser.add_argument("--db-path", type=str, default=None, help="Đường dẫn file SQLite DB")
    args = parser.parse_args()

    conn = get_connection(sqlite_path=Path(args.db_path) if args.db_path else None)
    init_source_and_mirror_schema(conn, engine=get_db_engine())
    generate_seed_dataset(conn, num_persons=args.persons, seed=args.seed, batch_size=args.batch_size)
    conn.close()


if __name__ == "__main__":
    main()
