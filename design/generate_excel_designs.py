#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script chuyển đổi tài liệu Thiết kế cơ bản (.md) và Thiết kế chi tiết (.md)
sang định dạng bảng tính Excel (.xlsx) nhiều sheets chuẩn hóa.
"""

import re
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter


def parse_markdown_sections(md_text: str):
    """Tách file markdown theo các tiêu đề cấp 2 (## SheetName – Mô tả)."""
    lines = md_text.splitlines()
    sections = []
    current_sheet = None
    current_title = None
    current_lines = []

    for line in lines:
        m = re.match(r"^##\s+([^\s–-]+)\s*[–-]\s*(.+)$", line.strip())
        if m:
            if current_sheet is not None:
                sections.append((current_sheet, current_title, current_lines))
            raw_name = m.group(1).strip()
            # Excel sheet name max 31 chars, no invalid chars
            sheet_name = re.sub(r"[\[\]\*\?\/\\:]", "_", raw_name)[:31]
            current_sheet = sheet_name
            current_title = f"{raw_name} – {m.group(2).strip()}"
            current_lines = []
        else:
            if current_sheet is not None:
                current_lines.append(line)

    if current_sheet is not None:
        sections.append((current_sheet, current_title, current_lines))

    return sections


def parse_blocks(lines):
    """Phân tích danh sách dòng trong 1 section thành các khối: heading, table, code, text."""
    blocks = []
    i = 0
    n = len(lines)
    while i < n:
        line = lines[i]
        stripped = line.strip()

        if not stripped or stripped == "---":
            i += 1
            continue

        # Code block
        if stripped.startswith("```"):
            code_lines = []
            i += 1
            while i < n and not lines[i].strip().startswith("```"):
                code_lines.append(lines[i])
                i += 1
            i += 1  # skip closing ```
            blocks.append(("code", "\n".join(code_lines)))
            continue

        # Markdown table
        if stripped.startswith("|") and stripped.endswith("|"):
            table_rows = []
            while i < n and lines[i].strip().startswith("|") and lines[i].strip().endswith("|"):
                row_str = lines[i].strip()[1:-1]
                cells = [c.strip().replace("<br>", "\n") for c in row_str.split("|")]
                # Skip separator row | --- | --- |
                if all(re.match(r"^:?-{2,}:?$", c.strip()) for c in cells if c.strip() != ""):
                    i += 1
                    continue
                table_rows.append(cells)
                i += 1
            if table_rows:
                blocks.append(("table", table_rows))
            continue

        # Sub-heading
        if stripped.startswith("###"):
            blocks.append(("heading", stripped.lstrip("#").strip()))
            i += 1
            continue

        # Normal text / bullet
        blocks.append(("text", stripped.replace("<br>", "\n")))
        i += 1

    return blocks


def build_excel_from_md(md_path: Path, xlsx_path: Path):
    md_text = md_path.read_text(encoding="utf-8")
    sections = parse_markdown_sections(md_text)

    wb = Workbook()
    # Remove default sheet
    wb.remove(wb.active)

    title_font = Font(name="Segoe UI", size=13, bold=True, color="FFFFFF")
    title_fill = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")

    sub_font = Font(name="Segoe UI", size=11, bold=True, color="1F4E78")
    sub_fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")

    th_font = Font(name="Segoe UI", size=10, bold=True, color="FFFFFF")
    th_fill = PatternFill(start_color="2F5597", end_color="2F5597", fill_type="solid")

    td_font = Font(name="Segoe UI", size=10, bold=False, color="000000")
    zebra_fill = PatternFill(start_color="F9FAFB", end_color="F9FAFB", fill_type="solid")

    code_font = Font(name="Consolas", size=9.5, bold=False, color="1E293B")
    code_fill = PatternFill(start_color="F1F5F9", end_color="F1F5F9", fill_type="solid")

    thin_side = Side(border_style="thin", color="CBD5E1")
    grid_border = Border(left=thin_side, right=thin_side, top=thin_side, bottom=thin_side)

    for sheet_name, section_title, section_lines in sections:
        ws = wb.create_sheet(title=sheet_name)
        ws.views.sheetView[0].showGridLines = True

        # Sheet banner
        ws.cell(row=1, column=1, value=section_title)
        ws.cell(row=1, column=1).font = title_font
        ws.cell(row=1, column=1).fill = title_fill
        ws.cell(row=1, column=1).alignment = Alignment(vertical="center", wrap_text=True)
        ws.row_dimensions[1].height = 28

        row_idx = 3
        max_cols_in_sheet = 1
        blocks = parse_blocks(section_lines)

        for btype, bcontent in blocks:
            if btype == "heading":
                cell = ws.cell(row=row_idx, column=1, value=bcontent)
                cell.font = sub_font
                cell.fill = sub_fill
                cell.alignment = Alignment(vertical="center", wrap_text=True)
                ws.row_dimensions[row_idx].height = 22
                row_idx += 1

            elif btype == "text":
                cell = ws.cell(row=row_idx, column=1, value=bcontent)
                cell.font = td_font
                cell.alignment = Alignment(vertical="top", wrap_text=True)
                row_idx += 1

            elif btype == "code":
                cell = ws.cell(row=row_idx, column=1, value=bcontent)
                cell.font = code_font
                cell.fill = code_fill
                cell.border = grid_border
                cell.alignment = Alignment(vertical="top", wrap_text=True)
                line_count = bcontent.count("\n") + 1
                ws.row_dimensions[row_idx].height = min(max(line_count * 14, 24), 380)
                row_idx += 2

            elif btype == "table":
                for r_i, row_cells in enumerate(bcontent):
                    max_cols_in_sheet = max(max_cols_in_sheet, len(row_cells))
                    max_lines_in_row = 1
                    for c_i, val in enumerate(row_cells, start=1):
                        c = ws.cell(row=row_idx, column=c_i, value=val)
                        c.border = grid_border
                        if r_i == 0:
                            c.font = th_font
                            c.fill = th_fill
                            c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
                        else:
                            c.font = td_font
                            if r_i % 2 == 0:
                                c.fill = zebra_fill
                            c.alignment = Alignment(vertical="top", wrap_text=True)
                        max_lines_in_row = max(max_lines_in_row, val.count("\n") + 1, len(val) // 65 + 1)
                    ws.row_dimensions[row_idx].height = (
                        24 if r_i == 0 else min(max(max_lines_in_row * 15, 20), 260)
                    )
                    row_idx += 1
                row_idx += 1

        if max_cols_in_sheet > 1:
            ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=max_cols_in_sheet)

        # Auto-fit column widths
        for col_idx in range(1, max_cols_in_sheet + 1):
            col_letter = get_column_letter(col_idx)
            max_len = 14
            for r in range(2, row_idx):
                val = ws.cell(row=r, column=col_idx).value
                if val:
                    first_line = str(val).split("\n")[0]
                    max_len = max(max_len, min(len(first_line) + 4, 58))
            ws.column_dimensions[col_letter].width = max_len

    wb.save(xlsx_path)
    print(f"[OK] Đã tạo file Excel: {xlsx_path} ({len(sections)} sheets)")


def main():
    design_dir = Path(__file__).resolve().parent
    bd_md = design_dir / "ThietKeCoBan_BasicDesign_HGD_C360.md"
    bd_xlsx = design_dir / "ThietKeCoBan_BasicDesign_HGD_C360.xlsx"

    dd_md = design_dir / "ThietKeChiTiet_DetailDesign_HGD_C360.md"
    dd_xlsx = design_dir / "ThietKeChiTiet_DetailDesign_HGD_C360.xlsx"

    build_excel_from_md(bd_md, bd_xlsx)
    build_excel_from_md(dd_md, dd_xlsx)


if __name__ == "__main__":
    main()
