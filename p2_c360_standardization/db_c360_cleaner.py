#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Single Source of Truth cho toàn bộ nghiệp vụ chuẩn hóa dữ liệu C360 (BAT-C360-005).
Dùng chung 100% logic cho cả:
  - Batch Full Load (c360_etl_full_load.py)
  - Near Real-time Worker (c360_nrt_worker.py)
  - Kiểm tra Validation & Test Cases (test_week2_p2.py)
"""

import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

# Bảng dịch ký tự tiếng Việt có dấu sang không dấu
VN_ACCENTS_MAP = str.maketrans(
    "ÀÁẢÃẠĂẰẮẲẴẶÂẦẤẨẪẬÈÉẺẼẸÊỀẾỂỄỆÌÍỈĨỊÒÓỎÕỌÔỒỐỔỖỘƠỜỚỞỠỢÙÚỦŨỤƯỪỨỬỮỰỲÝỶỸỴĐ"
    "àáảãạăằắẳẵặâầấẩẫậèéẻẽẹêềếểễệìíỉĩịòóỏõọôồốổỗộơờớởỡợùúủũụưừứửữựỳýỷỹỵđ",
    "AAAAAAAAAAAAAAAAAEEEEEEEEEEEIIIIIOOOOOOOOOOOOOOOOOUUUUUUUUUUUYYYYYD"
    "aaaaaaaaaaaaaaaaaeeeeeeeeeeeiiiiiooooooooooooooooouuuuuuuuuuuyyyyyd",
)


def remove_vietnamese_accents(text: str) -> str:
    """Chuyển đổi chuỗi tiếng Việt có dấu thành không dấu viết hoa."""
    if not text:
        return ""
    return text.translate(VN_ACCENTS_MAP).upper().strip()


def clean_hoten(hoten_raw: Optional[str]) -> Tuple[str, str, bool]:
    """
    Chuẩn hóa Họ và tên.
    Trả về: (ho_ten_clean, ho_ten_khong_dau, has_extra_spaces)
    """
    if not hoten_raw:
        return ("", "", False)

    raw_str = str(hoten_raw)
    cleaned = re.sub(r"\s+", " ", raw_str).strip()
    has_extra_spaces = raw_str != cleaned
    khong_dau = remove_vietnamese_accents(cleaned)
    return (cleaned, khong_dau, has_extra_spaces)


def is_valid_calendar_date(year: int, month: int, day: int) -> bool:
    """Kiểm tra ngày tháng năm có hợp lệ theo lịch thiên văn không."""
    if year < 1850 or year > 2026:
        return False
    if month < 1 or month > 12:
        return False
    if day < 1:
        return False

    days_in_month = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    # Năm nhuận
    if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0):
        days_in_month[2] = 29

    return day <= days_in_month[month]


def clean_ngaysinh(ngaysinh_raw: Any, ngaysinh_dt: Optional[str] = None) -> Tuple[Optional[str], Optional[str], bool]:
    """
    Chuẩn hóa Ngày sinh kiểu NUMBER hoặc chuỗi.
    Trả về: (ngay_sinh_chuan, ngay_sinh_goc, is_error)
    """
    goc_str = str(ngaysinh_raw).strip() if ngaysinh_raw is not None else ""
    if not goc_str and ngaysinh_dt:
        goc_str = str(ngaysinh_dt).strip()

    if not goc_str or goc_str in ("0", "None", "NULL"):
        return (None, goc_str or None, True)

    # Xử lý định dạng ngày có dấu gạch chéo hoặc gạch nối: DD/MM/YYYY hoặc YYYY-MM-DD
    if "/" in goc_str:
        parts = goc_str.split("/")
        if len(parts) == 3:
            try:
                d, m, y = int(parts[0]), int(parts[1]), int(parts[2])
                if is_valid_calendar_date(y, m, d):
                    return (f"{y:04d}{m:02d}{d:02d}", goc_str, False)
                return (None, goc_str, True)
            except ValueError:
                return (None, goc_str, True)
    elif "-" in goc_str and len(goc_str) == 10:
        parts = goc_str.split("-")
        if len(parts) == 3:
            try:
                y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
                if is_valid_calendar_date(y, m, d):
                    return (f"{y:04d}{m:02d}{d:02d}", goc_str, False)
                return (None, goc_str, True)
            except ValueError:
                return (None, goc_str, True)

    # Xử lý chuỗi số thuần túy
    digits = re.sub(r"\D", "", goc_str)
    if not digits:
        return (None, goc_str, True)

    # 1. Định dạng 8 chữ số YYYYMMDD
    if len(digits) == 8:
        y = int(digits[0:4])
        m = int(digits[4:6])
        d = int(digits[6:8])

        # Trường hợp chỉ có năm: YYYY0000
        if m == 0 and d == 0:
            if 1850 <= y <= 2026:
                return (f"{y:04d}", goc_str, False)
            return (None, goc_str, True)

        # Trường hợp chỉ có năm và tháng: YYYYMM00
        if m in range(1, 13) and d == 0:
            if 1850 <= y <= 2026:
                return (f"{y:04d}{m:02d}", goc_str, False)
            return (None, goc_str, True)

        # Đủ ngày tháng năm
        if is_valid_calendar_date(y, m, d):
            return (f"{y:04d}{m:02d}{d:02d}", goc_str, False)
        return (None, goc_str, True)

    # 2. Định dạng 6 chữ số YYYYMM
    if len(digits) == 6:
        y = int(digits[0:4])
        m = int(digits[4:6])
        if 1850 <= y <= 2026 and 1 <= m <= 12:
            return (f"{y:04d}{m:02d}", goc_str, False)
        return (None, goc_str, True)

    # 3. Định dạng 4 chữ số YYYY
    if len(digits) == 4:
        y = int(digits)
        if 1850 <= y <= 2026:
            return (f"{y:04d}", goc_str, False)
        return (None, goc_str, True)

    return (None, goc_str, True)


def _is_valid_12_digit_id(val: Optional[str]) -> bool:
    """Kiểm tra chuỗi đúng 12 chữ số và không phải chuỗi lặp toàn 1 ký tự (vd: '000000000000')."""
    return bool(val and len(val) == 12 and val.isdigit() and len(set(val)) > 1)


def clean_cccd(
    so_cccd_raw: Optional[str],
    so_cmtnd_raw: Optional[str],
) -> Tuple[Optional[str], Optional[str], Optional[str], bool, bool]:
    """
    Chuẩn hóa Số định danh cá nhân 12 chữ số.
    Ưu tiên SO_CCCD 12 số hợp lệ -> SO_CMTND 12 số hợp lệ.
    Loại trừ chuỗi giả mạo toàn số giống nhau (vd: 000000000000).
    Trả về: (so_dinh_danh, so_cccd_clean, so_cmtnd_clean, flag_khong_sdd, flag_cmnd9)
    """
    cccd_clean = re.sub(r"\D", "", str(so_cccd_raw or "")).strip() or None
    cmtnd_clean = re.sub(r"\D", "", str(so_cmtnd_raw or "")).strip() or None

    # 1. Tìm 12 số hợp lệ
    if _is_valid_12_digit_id(cccd_clean):
        return (cccd_clean, cccd_clean, cmtnd_clean, False, False)

    if _is_valid_12_digit_id(cmtnd_clean):
        return (cmtnd_clean, cccd_clean, cmtnd_clean, False, False)

    # 2. Không có 12 số hợp lệ -> kiểm tra cảnh báo CMND 9 số
    has_cmnd9 = False
    if (cmtnd_clean and len(cmtnd_clean) == 9 and len(set(cmtnd_clean)) > 1) or (
        cccd_clean and len(cccd_clean) == 9 and len(set(cccd_clean)) > 1
    ):
        has_cmnd9 = True

    return (None, cccd_clean, cmtnd_clean, True, has_cmnd9)


def clean_phone(phone_raw: Optional[str]) -> Tuple[Optional[str], bool]:
    """
    Chuẩn hóa Số điện thoại VN:
      - Di động: 0[3|5|7|8|9] + 8 chữ số (tổng 10 số)
      - Cố định: 02 + 8..9 chữ số (tổng 10-11 số)
      - Loại trừ chuỗi lặp toàn 1 chữ số (vd: 0000000000).
    Trả về: (phone_clean, flag_sdt_loi)
    """
    if not phone_raw:
        return (None, True)

    p = re.sub(r"[\s\.\-\(\)]", "", str(phone_raw)).strip()
    if p.startswith("+84"):
        p = "0" + p[3:]
    elif p.startswith("840"):
        p = "0" + p[3:]
    elif p.startswith("84") and len(p) in (11, 12):
        p = "0" + p[2:]

    # Bổ sung số 0 đầu nếu người dùng nhập 9 số bắt đầu bằng 3, 5, 7, 8, 9
    if len(p) == 9 and p[0] in ("3", "5", "7", "8", "9") and p.isdigit():
        p = "0" + p

    # Kiểm tra regex số điện thoại hợp lệ tại VN & không phải chuỗi toàn số giống nhau
    if re.match(r"^0(?:[35789]\d{8}|2\d{8,9})$", p) and len(set(p)) > 1:
        return (p, False)

    return (None, True)


def clean_address_group(
    ma_xa_raw: Optional[str],
    ma_tinh_raw: Optional[str],
    dia_chi_raw: Optional[str],
    map_xa: Dict[str, Tuple[Optional[str], Optional[str], Optional[str], Optional[str], str]],
    map_tinh: Dict[str, Tuple[str, str, str]],
    prefix: str = "",
) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], List[str]]:
    """
    Chuẩn hóa nhóm địa chỉ (Thường trú, Liên hệ, Khai sinh) dựa trên 2 bảng Map DBHC.
    Trả về: (ma_tinh_chuan, ten_tinh_chuan, ma_xa_chuan, ten_xa_chuan, dia_chi_full, flags)
    """
    flags = []

    # Chuẩn hóa mã đầu vào để tra cứu
    xa_token = str(ma_xa_raw or "").strip()
    if xa_token.isdigit() and len(xa_token) < 5:
        xa_token = xa_token.zfill(5)

    tinh_token = str(ma_tinh_raw or "").strip().upper()

    # 1. Tra cứu cấp Xã
    xa_info = map_xa.get(xa_token)
    ma_xa_chuan = None
    ten_xa_chuan = None
    tinh_tu_xa = None
    ten_tinh_tu_xa = None

    if not xa_token or xa_token in ("0", "00000"):
        flag_name = f"XA_{prefix}_KHONG_MAP" if prefix else "XA_KHONG_MAP"
        flags.append(flag_name)
    elif xa_info:
        ma_xa_chuan, ten_xa_chuan, tinh_tu_xa, ten_tinh_tu_xa, loai_map = xa_info
        if loai_map == "MO_HO":
            flag_name = f"XA_{prefix}_MO_HO" if prefix else "XA_MO_HO"
            flags.append(flag_name)
    else:
        flag_name = f"XA_{prefix}_KHONG_MAP" if prefix else "XA_KHONG_MAP"
        flags.append(flag_name)

    # 2. Tra cứu cấp Tỉnh từ MA_TINH gốc
    tinh_info = map_tinh.get(tinh_token)
    ma_tinh_khai_bao = None
    ten_tinh_khai_bao = None

    if tinh_info:
        ma_tinh_khai_bao, ten_tinh_khai_bao, _ = tinh_info

    # 3. Phân xử Tỉnh: Ưu tiên Tỉnh suy từ Xã
    ma_tinh_chuan = None
    ten_tinh_chuan = None

    if tinh_tu_xa:
        ma_tinh_chuan = tinh_tu_xa
        ten_tinh_chuan = ten_tinh_tu_xa
        if ma_tinh_khai_bao and ma_tinh_khai_bao != tinh_tu_xa:
            flag_name = f"TINH_{prefix}_LECH_XA" if prefix else "TINH_LECH_XA"
            flags.append(flag_name)
    elif ma_tinh_khai_bao:
        ma_tinh_chuan = ma_tinh_khai_bao
        ten_tinh_chuan = ten_tinh_khai_bao
    else:
        flag_name = f"TINH_{prefix}_KHONG_MAP" if prefix else "TINH_KHONG_MAP"
        flags.append(flag_name)

    # 4. Xây dựng địa chỉ đầy đủ
    dia_chi_parts = []
    if dia_chi_raw and str(dia_chi_raw).strip():
        dia_chi_parts.append(str(dia_chi_raw).strip())
    if ten_xa_chuan:
        dia_chi_parts.append(ten_xa_chuan)
    if ten_tinh_chuan:
        dia_chi_parts.append(ten_tinh_chuan)

    dia_chi_full = ", ".join(dia_chi_parts) if dia_chi_parts else None

    return (ma_tinh_chuan, ten_tinh_chuan, ma_xa_chuan, ten_xa_chuan, dia_chi_full, flags)


def clean_dantoc(
    dantoc_id_raw: Any,
    map_dantoc: Dict[int, Tuple[str, str, bool]],
) -> Tuple[Optional[str], Optional[str], bool]:
    """
    Chuẩn hóa Dân tộc.
    Trả về: (ma_dantoc, ten_dantoc, is_error)
    """
    if dantoc_id_raw is None or str(dantoc_id_raw).strip() in ("", "0"):
        return (None, None, True)

    try:
        dt_id = int(dantoc_id_raw)
    except ValueError:
        return (None, None, True)

    info = map_dantoc.get(dt_id)
    if not info:
        return (None, None, True)

    ma_dt, ten_dt, is_active = info
    if not is_active:
        return (None, None, True)

    return (ma_dt, ten_dt, False)


def clean_quoctich(
    quoctich_raw: Any,
    map_quoctich_code: Dict[str, Tuple[str, str]],
    map_quoctich_id: Dict[str, Tuple[str, str]],
    map_quoctich_name: Optional[Dict[str, Tuple[str, str]]] = None,
) -> Tuple[Optional[str], Optional[str], bool]:
    """
    Chuẩn hóa Quốc tịch. Hỗ trợ cả mã ('VN'), ID ('233') và tên quốc gia ('Việt Nam' / 'VIET NAM').
    Trả về: (ma_quoctich, ten_quoctich, is_error)
    """
    raw_clean = str(quoctich_raw or "").strip()
    if not raw_clean:
        return (None, None, True)

    raw_upper = raw_clean.upper()
    if raw_upper in map_quoctich_code:
        return (map_quoctich_code[raw_upper][0], map_quoctich_code[raw_upper][1], False)

    if raw_upper in map_quoctich_id:
        return (map_quoctich_id[raw_upper][0], map_quoctich_id[raw_upper][1], False)

    if map_quoctich_name:
        if raw_upper in map_quoctich_name:
            return (map_quoctich_name[raw_upper][0], map_quoctich_name[raw_upper][1], False)
        raw_kd = remove_vietnamese_accents(raw_clean)
        if raw_kd in map_quoctich_name:
            return (map_quoctich_name[raw_kd][0], map_quoctich_name[raw_kd][1], False)

    return (None, None, True)


def load_standardization_lookups(conn: Any) -> Dict[str, Any]:
    """Tải toàn bộ bộ nhớ đệm tra cứu danh mục & bản đồ vào RAM để tối đa hóa tốc độ nạp."""
    cur = conn.cursor()

    # 1. Map Tỉnh
    cur.execute("SELECT MA_TINH_LOOKUP, MA_TINH_MOI, TEN_TINH_MOI, LOAI_MAP FROM C360_MAP_DBHC_TINH;")
    map_tinh = {row[0]: (row[1], row[2], row[3]) for row in cur.fetchall()}

    # 2. Map Xã
    cur.execute("SELECT MA_XA_LOOKUP, MA_XA_MOI, TEN_XA_MOI, MA_TINH_MOI, TEN_TINH_MOI, LOAI_MAP FROM C360_MAP_DBHC_XA;")
    map_xa = {row[0]: (row[1], row[2], row[3], row[4], row[5]) for row in cur.fetchall()}

    # 3. Danh mục Dân tộc
    tbl_dantoc = "MIR_VSS_DM_DANTOC"
    cur.execute(f"SELECT COUNT(*) FROM {tbl_dantoc}")
    if cur.fetchone()[0] == 0:
        tbl_dantoc = "VSS_DM_DANTOC"

    cur.execute(
        f"""
        SELECT ID, MA, TEN,
               CASE WHEN NGAY_HET_HL IS NULL OR NGAY_HET_HL >= '2026-01-01' THEN 1 ELSE 0 END AS IS_ACTIVE
          FROM {tbl_dantoc}
        """
    )
    map_dantoc = {row[0]: (row[1], row[2], bool(row[3])) for row in cur.fetchall()}

    # 4. Danh mục Quốc tịch (index theo MA, ID và TEN có dấu / không dấu)
    tbl_qt = "MIR_VSS_DM_QUOC_TICH"
    cur.execute(f"SELECT COUNT(*) FROM {tbl_qt}")
    if cur.fetchone()[0] == 0:
        tbl_qt = "VSS_DM_QUOC_TICH"

    cur.execute(f"SELECT ID, MA, TEN FROM {tbl_qt}")
    map_qt_code = {}
    map_qt_id = {}
    map_qt_name = {}
    for row in cur.fetchall():
        qid = str(row[0]).strip()
        qma = str(row[1]).strip().upper()
        qten = str(row[2]).strip()
        map_qt_code[qma] = (qma, qten)
        map_qt_id[qid] = (qma, qten)
        if qten:
            map_qt_name[qten.upper()] = (qma, qten)
            map_qt_name[remove_vietnamese_accents(qten)] = (qma, qten)

    return {
        "map_tinh": map_tinh,
        "map_xa": map_xa,
        "map_dantoc": map_dantoc,
        "map_qt_code": map_qt_code,
        "map_qt_id": map_qt_id,
        "map_qt_name": map_qt_name,
    }


def transform_citizen_record(
    raw_dict: Dict[str, Any],
    lookups: Dict[str, Any],
    sync_batch_id: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Hàm chuẩn hóa hoàn chỉnh một bản ghi nhân khẩu thành bản ghi C360.
    Tổng hợp đầy đủ các mã cờ lỗi DQ_FLAGS và lưu trữ đầy đủ các cột *_GOC để đối soát.
    """
    dq_flags: List[str] = []

    # 1. Mã định danh & Mã cũ
    ma_cd = str(raw_dict.get("MA_CD") or "").strip()
    ma_cd_old = str(raw_dict.get("MA_CD_OLD") or "").strip() or None

    # 2. Họ tên
    ho_ten_clean, ho_ten_kd, has_extra_spaces = clean_hoten(raw_dict.get("HO_TEN"))
    if has_extra_spaces:
        dq_flags.append("HOTEN_THUA_KHOANG_TRANG")

    # 3. Giới tính
    gioi_tinh = raw_dict.get("GIOI_TINH")
    if gioi_tinh is not None:
        try:
            gioi_tinh = int(gioi_tinh)
        except ValueError:
            gioi_tinh = None

    # 4. Ngày sinh & Năm sinh
    ngay_sinh_chuan, ngay_sinh_goc, dob_error = clean_ngaysinh(
        raw_dict.get("NGAY_SINH"),
        raw_dict.get("NGAY_SINH_DT"),
    )
    nam_sinh = int(ngay_sinh_chuan[:4]) if ngay_sinh_chuan and len(ngay_sinh_chuan) >= 4 else None
    if dob_error:
        dq_flags.append("NGAY_SINH_LOI")

    # 5. CCCD / CMTND / Số định danh 12 số
    so_dinh_danh, cccd_clean, cmtnd_clean, khong_sdd, cmnd9_warn = clean_cccd(
        raw_dict.get("SO_CCCD"),
        raw_dict.get("SO_CMTND"),
    )
    if khong_sdd:
        dq_flags.append("KHONG_SO_DINH_DANH")
    if cmnd9_warn:
        dq_flags.append("CANH_BAO_CMND9")

    # 6. Số điện thoại (ưu tiên PHONE_NUMBER, fallback SO_DIEN_THOAI)
    phone_raw = raw_dict.get("PHONE_NUMBER")
    if phone_raw is None or not str(phone_raw).strip():
        phone_raw = raw_dict.get("SO_DIEN_THOAI")
    phone_goc = str(phone_raw).strip() if phone_raw is not None and str(phone_raw).strip() else None
    phone_clean, phone_error = clean_phone(phone_goc)
    if phone_error:
        dq_flags.append("SDT_LOI")

    # 7. Địa chỉ Thường trú
    ma_tinh_goc = str(raw_dict.get("MA_TINH") or "").strip() or None
    ma_xa_goc = str(raw_dict.get("MA_XA") or "").strip() or None
    tinh_tt, ten_tinh_tt, xa_tt, ten_xa_tt, dc_tt_full, addr_tt_flags = clean_address_group(
        ma_xa_goc,
        ma_tinh_goc,
        raw_dict.get("DIA_CHI"),
        lookups["map_xa"],
        lookups["map_tinh"],
        prefix="",
    )
    dq_flags.extend(addr_tt_flags)

    # 8. Địa chỉ Liên hệ (chỉ gắn DQ_FLAGS nếu có nhập mã tỉnh/xã liên hệ nhưng bị lỗi)
    ma_tinh_lh_goc = str(raw_dict.get("MA_TINH_LH") or "").strip() or None
    ma_xa_lh_goc = str(raw_dict.get("MA_XA_LH") or "").strip() or None
    tinh_lh, ten_tinh_lh, xa_lh, ten_xa_lh, dc_lh_full, addr_lh_flags = clean_address_group(
        ma_xa_lh_goc,
        ma_tinh_lh_goc,
        raw_dict.get("DIA_CHI_LH"),
        lookups["map_xa"],
        lookups["map_tinh"],
        prefix="LH",
    )
    if ma_tinh_lh_goc or ma_xa_lh_goc:
        dq_flags.extend(addr_lh_flags)

    # 9. Địa chỉ Khai sinh (chỉ gắn DQ_FLAGS nếu có nhập mã tỉnh/xã khai sinh nhưng bị lỗi)
    ma_tinh_ks_goc = str(raw_dict.get("MA_TINH_KS") or "").strip() or None
    ma_xa_ks_goc = str(raw_dict.get("MA_XA_KS") or "").strip() or None
    tinh_ks, ten_tinh_ks, xa_ks, ten_xa_ks, dc_ks_full, addr_ks_flags = clean_address_group(
        ma_xa_ks_goc,
        ma_tinh_ks_goc,
        raw_dict.get("DIA_CHI_KS"),
        lookups["map_xa"],
        lookups["map_tinh"],
        prefix="KS",
    )
    if ma_tinh_ks_goc or ma_xa_ks_goc:
        dq_flags.extend(addr_ks_flags)

    # 10. Dân tộc
    dt_id = raw_dict.get("DM_DAN_TOC_ID")
    ma_dt, ten_dt, dt_error = clean_dantoc(dt_id, lookups["map_dantoc"])
    if dt_error:
        dq_flags.append("DAN_TOC_LOI")

    # 11. Quốc tịch
    qt_raw = raw_dict.get("QUOC_TICH")
    ma_qt, ten_qt, qt_error = clean_quoctich(
        qt_raw,
        lookups["map_qt_code"],
        lookups["map_qt_id"],
        lookups.get("map_qt_name"),
    )
    if qt_error:
        dq_flags.append("QUOC_TICH_LOI")

    # 12. Tình trạng sống
    dead_date = str(raw_dict.get("NGAY_CHET") or "").strip() or None
    is_alive = raw_dict.get("TINH_TRANG_SONG")
    if is_alive is None:
        is_alive = 0 if dead_date else 1
    else:
        try:
            is_alive = int(is_alive)
        except ValueError:
            is_alive = 0 if dead_date else 1

    # Khóa nguồn để đối soát
    source_ma_ho = str(raw_dict.get("MA_HO_GD") or "").strip()
    source_modify_date = str(raw_dict.get("MODIFY_DATE") or raw_dict.get("NGAY_SUA") or "").strip()
    source_created_date = str(raw_dict.get("CREATED_DATE") or raw_dict.get("NGAY_TAO") or "").strip()

    dq_flags_str = ";".join(dq_flags) if dq_flags else None

    return {
        "MA_SO_BHXH": ma_cd,
        "MA_SO_BHXH_CU": ma_cd_old,
        "SO_DINH_DANH": so_dinh_danh,
        "HO_TEN": ho_ten_clean,
        "HO_TEN_KHONG_DAU": ho_ten_kd,
        "GIOI_TINH": gioi_tinh,
        "NGAY_SINH_CHUAN": ngay_sinh_chuan,
        "NAM_SINH": nam_sinh,
        "NGAY_SINH_GOC": ngay_sinh_goc,
        "SO_CCCD": cccd_clean,
        "SO_CMTND": cmtnd_clean,
        "SO_DIEN_THOAI": phone_clean,
        "SO_DIEN_THOAI_GOC": phone_goc,
        "EMAIL": str(raw_dict.get("EMAIL") or "").strip() or None,
        "MA_TINH_THUONG_TRU": tinh_tt,
        "TEN_TINH_THUONG_TRU": ten_tinh_tt,
        "MA_XA_THUONG_TRU": xa_tt,
        "TEN_XA_THUONG_TRU": ten_xa_tt,
        "DIA_CHI_THUONG_TRU": dc_tt_full,
        "MA_TINH_GOC": ma_tinh_goc,
        "MA_XA_GOC": ma_xa_goc,
        "MA_TINH_LIEN_HE": tinh_lh,
        "TEN_TINH_LIEN_HE": ten_tinh_lh,
        "MA_XA_LIEN_HE": xa_lh,
        "TEN_XA_LIEN_HE": ten_xa_lh,
        "DIA_CHI_LIEN_HE": dc_lh_full,
        "MA_TINH_LH_GOC": ma_tinh_lh_goc,
        "MA_XA_LH_GOC": ma_xa_lh_goc,
        "MA_TINH_KHAI_SINH": tinh_ks,
        "TEN_TINH_KHAI_SINH": ten_tinh_ks,
        "MA_XA_KHAI_SINH": xa_ks,
        "TEN_XA_KHAI_SINH": ten_xa_ks,
        "DIA_CHI_KHAI_SINH": dc_ks_full,
        "MA_TINH_KS_GOC": ma_tinh_ks_goc,
        "MA_XA_KS_GOC": ma_xa_ks_goc,
        "DM_DAN_TOC_ID": dt_id if not dt_error else None,
        "MA_DAN_TOC": ma_dt,
        "TEN_DAN_TOC": ten_dt,
        "QUOC_TICH_RAW": str(qt_raw).strip() if qt_raw else None,
        "MA_QUOC_TICH": ma_qt,
        "TEN_QUOC_TICH": ten_qt,
        "IS_ALIVE": is_alive,
        "DEAD_DATE": dead_date,
        "SOURCE_MA_HO_GD": source_ma_ho,
        "SOURCE_MODIFY_DATE": source_modify_date,
        "SOURCE_CREATED_DATE": source_created_date,
        "DQ_FLAGS": dq_flags_str,
        "SYNC_BATCH_ID": sync_batch_id,
    }
