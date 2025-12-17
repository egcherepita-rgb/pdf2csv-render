# parser.py
from __future__ import annotations

import io
import re
from dataclasses import dataclass
from typing import Dict, List, Tuple

import pdfplumber
from openpyxl import load_workbook


# -------------------------
# Helpers
# -------------------------

_WS_RE = re.compile(r"\s+")


def _norm(s: str) -> str:
    """Нормализация строки для сравнения."""
    s = (s or "").strip()
    s = _WS_RE.sub(" ", s)
    return s


def _is_int(s: str) -> bool:
    return bool(re.fullmatch(r"\d+", (s or "").strip()))


def _group_lines(words: List[dict], y_tol: float = 2.5) -> List[dict]:
    """
    Группируем слова в "строки" по координате top.
    words: элементы из pdfplumber.extract_words()
    """
    ws = sorted(words, key=lambda w: (w["top"], w["x0"]))
    lines: List[dict] = []

    for w in ws:
        y = float(w["top"])
        placed = False
        for line in lines:
            if abs(line["y"] - y) <= y_tol:
                line["words"].append(w)
                # пересчитать усреднённый y
                n = len(line["words"])
                line["y"] = (line["y"] * (n - 1) + y) / n
                placed = True
                break
        if not placed:
            lines.append({"y": y, "words": [w]})

    for line in lines:
        line["words"] = sorted(line["words"], key=lambda w: w["x0"])

    lines = sorted(lines, key=lambda l: l["y"])
    return lines


# -------------------------
# Items.xlsx loader (openpyxl)
# -------------------------

def _load_items(items_xlsx_path: str) -> Dict[str, str]:
    """
    Загружаем список допустимых наименований из items.xlsx (1-я колонка, 1-й лист).
    Возвращаем dict: norm_name -> original_name
    """
    wb = load_workbook(items_xlsx_path, read_only=True, data_only=True)
    ws = wb.worksheets[0]

    out: Dict[str, str] = {}
    for row in ws.iter_rows(min_row=1, max_col=1, values_only=True):
        val = row[0]
        if val is None:
            continue
        name = _norm(str(val))
        if not name:
            continue
        out[_norm(name)] = name

    return out


# -------------------------
# PDF row extraction by columns
# -------------------------

@dataclass
class PdfRow:
    name: str
    qty: int
    y: float


def _page_has_table(page) -> bool:
    """
    Считаем страницу "товарной", если есть слово 'Товар' (заголовок таблицы).
    """
    try:
        text = page.extract_text() or ""
    except Exception:
        return False
    return "Товар" in text


def _extract_rows_from_page(
    page,
    *,
    name_x: Tuple[float, float] = (70.0, 250.0),
    qty_x: Tuple[float, float] = (440.0, 505.0),
    y_tol: float = 2.5,
) -> List[PdfRow]:
    """
    Достаём строки (наименование + количество) со страницы PDF по координатам колонок.
    """
    words = page.extract_words(keep_blank_chars=False, use_text_flow=True) or []
    if not words:
        return []

    # отрежем шапку до строки с "Товар"
    header_cut = 0.0
    for w in words:
        if w.get("text") == "Товар":
            header_cut = float(w.get("bottom", w.get("top", 0.0))) + 2.0
            break

    data_words = [w for w in words if float(w.get("top", 0.0)) > header_cut]
    if not data_words:
        return []

    lines = _group_lines(data_words, y_tol=y_tol)

    # найдём "числовые" линии, где в колонке кол-ва стоит целое число
    qty_lines: List[Tuple[float, int]] = []
    for line in lines:
        qty_tokens = [
            w for w in line["words"]
            if qty_x[0] <= float(w["x0"]) <= qty_x[1] and _is_int(w.get("text", ""))
        ]
        if qty_tokens:
            qty_lines.append((float(line["y"]), int(qty_tokens[0]["text"])))

    if not qty_lines:
        return []

    ys = [y for y, _q in qty_lines]
    rows: List[PdfRow] = []

    # Окно строки: между серединами соседних qty-линий (чтобы захватить переносы + цвет)
    for i, (y, q) in enumerate(qty_lines):
        y_prev = ys[i - 1] if i > 0 else (y - 60.0)
        y_next = ys[i + 1] if i + 1 < len(ys) else (y + 60.0)

        top = (y_prev + y) / 2.0
        bottom = (y + y_next) / 2.0

        band_words = [
            w for w in data_words
            if (top - 1.0) <= float(w["top"]) <= (bottom + 1.0)
            and name_x[0] <= float(w["x0"]) <= name_x[1]
        ]
        band_words = sorted(band_words, key=lambda w: (w["top"], w["x0"]))

        name = _norm(" ".join(w.get("text", "") for w in band_words))
        if name:
            rows.append(PdfRow(name=name, qty=q, y=y))

    return rows


def _extract_rows_from_pdf(pdf_bytes: bytes) -> List[PdfRow]:
    """
    Собираем строки со всех страниц PDF (только со страниц с таблицей "Товар").
    """
    rows: List[PdfRow] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            if not _page_has_table(page):
                continue
            rows.extend(_extract_rows_from_page(page))
    return rows


# -------------------------
# Public API
# -------------------------

def build_csv_from_pdf(
    *,
    pdf_bytes: bytes,
    items_xlsx_path: str,
    delimiter: str = ";",
    include_header: bool = True,
) -> str:
    """
    Строит CSV:
    - берём строки из PDF в порядке появления
    - фильтруем по items.xlsx (только то, что есть в списке)
    - суммируем количества, если один и тот же товар встретился несколько раз
    - порядок сохраняем как в PDF (по первому появлению)
    """
    allowed = _load_items(items_xlsx_path)  # norm -> original
    pdf_rows = _extract_rows_from_pdf(pdf_bytes)

    # accumulator с сохранением порядка
    order: List[str] = []          # norm_name в порядке первого появления
    qty_map: Dict[str, int] = {}   # norm_name -> total qty

    for r in pdf_rows:
        key = _norm(r.name)
        if key not in allowed:
            # Если товара нет в items.xlsx — не выводим вообще
            continue
        if key not in qty_map:
            qty_map[key] = 0
            order.append(key)
        qty_map[key] += int(r.qty)

    lines: List[str] = []
    if include_header:
        lines.append(delimiter.join(["Наименование", "Количество"]))

    for key in order:
        name = allowed[key]
        qty = qty_map.get(key, 0)
        # нули обычно не нужны, но на всякий случай
        if qty <= 0:
            continue
        lines.append(delimiter.join([name, str(qty)]))

    return "\n".join(lines) + "\n"
