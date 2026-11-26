#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pydantic Schemas cho 4 Dịch vụ REST APIs Tra cứu C360.
"""

from typing import List, Optional
from pydantic import BaseModel, Field


class CitizenResponse(BaseModel):
    id: Optional[int] = Field(None, description="ID tự tăng hệ thống")
    ma_so_bhxh: str = Field(..., description="Mã số BHXH hiện hành (10 số)")
    ma_so_bhxh_cu: Optional[str] = Field(None, description="Mã số BHXH cũ (nếu có)")
    so_dinh_danh: Optional[str] = Field(None, description="Số định danh cá nhân 12 số chuẩn hóa")
    ho_ten: str = Field(..., description="Họ và tên chuẩn hóa")
    ho_ten_khong_dau: str = Field(..., description="Họ và tên không dấu viết hoa")
    gioi_tinh: Optional[int] = Field(None, description="1: Nam, 2: Nữ")
    ngay_sinh_chuan: Optional[str] = Field(None, description="Ngày sinh chuẩn (YYYY, YYYYMM, hoặc YYYYMMDD)")
    nam_sinh: Optional[int] = Field(None, description="Năm sinh chuẩn hóa")
    ngay_sinh_goc: Optional[str] = Field(None, description="Ngày sinh gốc")
    so_cccd: Optional[str] = Field(None, description="Số CCCD gốc đã lọc chữ số")
    so_cmtnd: Optional[str] = Field(None, description="Số CMTND gốc đã lọc chữ số")
    so_dien_thoai: Optional[str] = Field(None, description="Số điện thoại chuẩn hóa (đầu 0)")
    so_dien_thoai_goc: Optional[str] = Field(None, description="Số điện thoại gốc ở nguồn")
    email: Optional[str] = Field(None, description="Email liên hệ")
    ma_tinh_thuong_tru: Optional[str] = Field(None, description="Mã tỉnh thường trú chuẩn 3 số")
    ten_tinh_thuong_tru: Optional[str] = Field(None, description="Tên tỉnh thường trú")
    ma_xa_thuong_tru: Optional[str] = Field(None, description="Mã xã thường trú chuẩn 5 số")
    ten_xa_thuong_tru: Optional[str] = Field(None, description="Tên xã thường trú")
    dia_chi_thuong_tru: Optional[str] = Field(None, description="Địa chỉ thường trú đầy đủ")
    ma_tinh_goc: Optional[str] = Field(None, description="Mã tỉnh thường trú gốc")
    ma_xa_goc: Optional[str] = Field(None, description="Mã xã thường trú gốc")
    ma_tinh_lien_he: Optional[str] = Field(None, description="Mã tỉnh liên hệ chuẩn 3 số")
    ten_tinh_lien_he: Optional[str] = Field(None, description="Tên tỉnh liên hệ")
    ma_xa_lien_he: Optional[str] = Field(None, description="Mã xã liên hệ chuẩn 5 số")
    ten_xa_lien_he: Optional[str] = Field(None, description="Tên xã liên hệ")
    dia_chi_lien_he: Optional[str] = Field(None, description="Địa chỉ liên hệ đầy đủ")
    ma_tinh_khai_sinh: Optional[str] = Field(None, description="Mã tỉnh khai sinh chuẩn 3 số")
    ten_tinh_khai_sinh: Optional[str] = Field(None, description="Tên tỉnh khai sinh")
    ma_xa_khai_sinh: Optional[str] = Field(None, description="Mã xã khai sinh chuẩn 5 số")
    ten_xa_khai_sinh: Optional[str] = Field(None, description="Tên xã khai sinh")
    dia_chi_khai_sinh: Optional[str] = Field(None, description="Địa chỉ khai sinh đầy đủ")
    ma_dan_toc: Optional[str] = Field(None, description="Mã dân tộc")
    ten_dan_toc: Optional[str] = Field(None, description="Tên dân tộc")
    ma_quoc_tich: Optional[str] = Field(None, description="Mã quốc tịch")
    ten_quoc_tich: Optional[str] = Field(None, description="Tên quốc tịch")
    is_alive: int = Field(1, description="1: Còn sống, 0: Đã mất")
    dead_date: Optional[str] = Field(None, description="Ngày mất (nếu có)")
    source_ma_ho_gd: Optional[str] = Field(None, description="Mã hộ gia đình nguồn mới nhất")
    source_modify_date: Optional[str] = Field(None, description="Ngày cập nhật mới nhất ở nguồn")
    dq_flags: Optional[str] = Field(None, description="Chuỗi cờ lỗi chất lượng dữ liệu ngăn cách bởi dấu ;")
    dq_flags_list: List[str] = Field(default_factory=list, description="Danh sách mảng các cờ lỗi")
    sync_date: Optional[str] = Field(None, description="Mốc thời gian đồng bộ vào C360")


class SingleCitizenLookupResponse(BaseModel):
    matched_by: str = Field(..., description="Tầng dò tìm thấy: MA_SO_BHXH | MA_SO_BHXH_CU | SO_DINH_DANH")
    data: CitizenResponse


class ConflictCandidateResponse(BaseModel):
    code: str = Field("MSG-E004", description="Mã thông báo xung đột")
    message: str = Field("Phát hiện trùng lặp nhiều cá nhân với mã tra cứu", description="Mô tả lỗi 409 Conflict")
    matched_tier: str = Field(..., description="Tầng phát sinh xung đột: MA_SO_BHXH_CU | SO_DINH_DANH")
    candidate_count: int
    candidates: List[CitizenResponse]


class BatchQueryRequest(BaseModel):
    ma_list: List[str] = Field(
        ...,
        min_length=1,
        max_length=1000,
        description="Danh sách tối đa 1.000 mã tra cứu (MA_SO_BHXH, MA_SO_BHXH_CU, hoặc SO_DINH_DANH)",
    )


class BatchQueryResultItem(BaseModel):
    query_key: str
    status: str = Field(..., description="FOUND | NOT_FOUND | CONFLICT")
    matched_by: Optional[str] = None
    data: Optional[CitizenResponse] = None
    candidates: Optional[List[CitizenResponse]] = None


class BatchQueryResponse(BaseModel):
    total_requested: int
    found_count: int
    not_found_count: int
    conflict_count: int
    results: List[BatchQueryResultItem]


class SearchCitizensResponse(BaseModel):
    query_name: str
    query_dob: Optional[str] = None
    total_matched: int
    data: List[CitizenResponse]


class ChangeFeedResponse(BaseModel):
    safe_watermark: str = Field(..., description="Mốc trễ an toàn SYSDATE - lag_seconds")
    next_cursor: Optional[str] = Field(None, description="Con trỏ phân trang Base64(sync_date|id) cho trang kế tiếp")
    has_more: bool
    data: List[CitizenResponse]
