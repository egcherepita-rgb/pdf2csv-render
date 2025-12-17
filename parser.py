import io
import re
import csv
from typing import Dict, List, Optional, Tuple

import pdfplumber
from openpyxl import load_workbook


# ---------------- НОРМАЛИЗАЦИЯ ----------------

def _norm(s: str) -> str:
    s = "" if s is None else str(s)
    s = s.replace("\u00a0", " ").replace("ё", "е")
    s = s.lower()
    s = re.sub(r"\s+", " ", s).strip()
    # приводим все варианты "x/×/*" к кириллической "х"
    s = s.replace("x", "х").replace("×", "х").replace("*", "х")
    return s


# ---------------- РЕГЕКСЫ ----------------

# коды типа GR-65 / GS-150 / GBr-40 / GFB-60 / GOV-60 / GPd-30 / GSd-40 и т.п.
_CODE_RE = re.compile(r"\b(g[a-z]{1,3}-?\d{2,3}|grb|grp|grc|gbm|gsh)\b", re.IGNORECASE)

# размеры типа 60х40, 90х40, 60х20, 30х10 (а НЕ 650х48 и НЕ 25х1528)
_SIZE_RE = re.compile(r"\b(\d{2,3})\s*х\s*(\d{2,3})\b", re.IGNORECASE)

# строка где есть "цена за шт", "кол-во", "сумма"
# примеры из твоего PDF: "290.00 ₽ 1 290 ₽", "640.00 ₽ 2 1280 ₽", "220.00 ₽ 12 2640 ₽"
_QTY_LINE_RE = re.compile(r"\b\d+[.,]\d+\s*₽\s*(\d+)\s+[\d\s]+\s*₽\b")


# ---------------- EXCEL ----------------

def _load_items(items_xlsx_path: str) -> Dict[str, str]:
    """
    Возвращает {key -> оригинальное_наименование_из_excel}
    """
    wb = load_workbook(items_xlsx_path, read_only=True, data_only=True)
    ws = wb.active

    items_map: Dict[str, str] = {}

    for (v,) in ws.iter_rows(min_row=1, max_col=1, values_only=True):
        if v is None:
            continue
        name = str(v).strip()
        if not name:
            continue
        if _norm(name) in ("наименование", "товар", "позиция"):
            continue

        key = _make_key_from_text(name)
        if key:
            items_map[key] = name

    wb.close()
    return items_map


# ---------------- КЛЮЧ ИЗ ТЕКСТА ----------------

def _pick_small_size(text: str) -> Optional[str]:
    """
    Берём "малый размер" (типа 60х40, 90х40, 60х20, 30х10),
    а габариты типа 650х48 или 25х1528 игнорируем.
    """
    t = _norm(text)
    for m in _SIZE_RE.finditer(t):
        a = int(m.group(1))
        b = int(m.group(2))
        mx, mn = max(a, b), min(a, b)

        # отсечка больших габаритов
        if mx > 200:
            continue

        # более “товарные” размеры (по твоему ассортименту)
        # (чтобы 178х167 из чертежей/параметров не путало)
        if mn in (10, 20, 30, 40, 50, 60, 90, 100, 120) or mx in (10, 20, 30, 40, 50, 60, 90, 100, 120):
            return f"{a}х{b}"

        # запасной вариант: маленькие, но не совсем случайные
        if mx <= 120 and mn <= 90:
            return f"{a}х{b}"

    return None


def _make_key_from_text(text: str) -> Optional[str]:
    t = _norm(text)
    codes = [m.group(1).lower() for m in _CODE_RE.finditer(t)]
    if not codes:
        return None

    code = codes[-1]  # ВАЖНО: берём ПОСЛЕДНИЙ код в буфере (он относится к текущей позиции)

    size = _pick_small_size(t)
    # размер нужен для "размерных" товаров (полки/корзины/разделители)
    if size and code in ("gsh", "gbm", "gpd", "gps"):
        return f"{code}:{size}"

    return code


# ---------------- PDF -> CSV ----------------

def _extract_lines(pdf_bytes: bytes) -> List[str]:
    lines: List[str] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            lines.extend(text.splitlines())
    return lines


def build_csv_from_pdf(pdf_bytes: bytes, items_xlsx_path: str, delimiter: str = ";") -> str:
    items_map = _load_items(items_xlsx_path)  # key -> excel_name

    lines = _extract_lines(pdf_bytes)

    # КОПИМ БУФЕР СТРОК ОДНОГО ТОВАРА, ДО ТЕХ ПОР, ПОКА НЕ ВСТРЕТИЛИ qty-строку
    buffer: List[str] = []
    found_qty: Dict[str, int] = {}

    for raw in lines:
        line = (raw or "").strip()
        if not line:
            continue

        # пропускаем мусор/шапки
        if line.startswith("Фото Товар"):
            buffer = []
            continue
        if line.startswith("Страница:"):
            continue
        if line in ("18995 ₽", "Общий вес", "Максимальный габарит заказа", "Адрес:", "Телефон:", "Email"):
            continue

        buffer.append(line)

        m = _QTY_LINE_RE.search(line)
        if not m:
            continue

        qty = int(m.group(1))

        # ключ ищем по ВСЕМУ буферу, но берём ПОСЛЕДНИЙ код (см _make_key_from_text)
        key = _make_key_from_text(" ".join(buffer))

        # очистка буфера ВСЕГДА после qty-строки — это убирает “смещение”
        buffer = []

        if not key:
            continue

        # выводим ТОЛЬКО то, что есть в Excel
        if key not in items_map:
            continue

        found_qty[key] = found_qty.get(key, 0) + qty

    # CSV: только найденные, в порядке Excel
    out = io.StringIO()
    writer = csv.writer(out, delimiter=delimiter)
    writer.writerow(["Наименование", "Кол-во"])

    for key, excel_name in items_map.items():
        if key in found_qty:
            writer.writerow([excel_name, found_qty[key]])

    return out.getvalue()
