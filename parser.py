import io
import re
import csv
from typing import Dict, List, Optional

import pdfplumber
from openpyxl import load_workbook


# ---------------- НОРМАЛИЗАЦИЯ ----------------

def _norm(s: str) -> str:
    s = "" if s is None else str(s)
    s = s.replace("\u00a0", " ").replace("ё", "е")
    s = s.lower()
    s = re.sub(r"\s+", " ", s).strip()
    # приводим все варианты x/×/* к кириллической "х"
    s = s.replace("x", "х").replace("×", "х").replace("*", "х")
    return s


# ---------------- РЕГЕКСЫ ----------------

# коды типа GR-65 / GS-150 / GBr-40 / GFB-60 / GOV-60 / GRB / GRC / GRP / GSh / GBM ...
_CODE_RE = re.compile(r"\b(g[a-z]{1,3}-?\d{2,3}|grb|grp|grc|gbm|gsh)\b", re.IGNORECASE)

# размеры типа 60х40, 90х40, 60х20, 30х10
_SIZE_RE = re.compile(r"\b(\d{2,3})\s*х\s*(\d{2,3})\b", re.IGNORECASE)

# строка где есть "цена за шт ... ₽  кол-во  сумма ₽"
# ВАЖНО: без \b в конце
_QTY_LINE_RE = re.compile(r"\d+[.,]\d+\s*₽\s*(\d+)\s+[\d\s]+\s*₽")


# ---------------- EXCEL ----------------

def _pick_small_size(text: str) -> Optional[str]:
    """
    Берём "малый размер" (типа 60х40, 90х40, 60х20),
    большие габариты типа 650х48 отсекаем.
    """
    t = _norm(text)
    for m in _SIZE_RE.finditer(t):
        a = int(m.group(1))
        b = int(m.group(2))
        mx = max(a, b)

        # отсечка больших габаритов из строк с габаритами (650х48 и т.п.)
        if mx > 200:
            continue

        return f"{a}х{b}"

    return None


def _make_key_from_text(text: str) -> Optional[str]:
    t = _norm(text)
    codes = [m.group(1).lower() for m in _CODE_RE.finditer(t)]
    if not codes:
        return None

    # берём ПОСЛЕДНИЙ код (наиболее “близкий” к товару)
    code = codes[-1]

    size = _pick_small_size(t)

    # Для размерных товаров добавляем размер в ключ
    if size and code in ("gsh", "gbm", "gpd", "gps"):
        return f"{code}:{size}"

    return code


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


# ---------------- PDF ----------------

def _extract_lines(pdf_bytes: bytes) -> List[str]:
    lines: List[str] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            lines.extend(text.splitlines())
    return lines


def _make_key_for_qty_line(lines: List[str], i: int) -> Optional[str]:
    """
    В твоём PDF код товара идёт ПОСЛЕ строки с количеством.
    Поэтому берём окно: 1 строка ДО qty и 4 строки ПОСЛЕ qty.
    Приоритет: найти код в части "после qty" (i..i+4).
    """
    start = max(0, i - 1)
    end = min(len(lines), i + 5)

    ctx = lines[start:end]
    forward = lines[i:end]

    forward_text = _norm(" ".join(forward))
    codes_fwd = [m.group(1).lower() for m in _CODE_RE.finditer(forward_text)]

    if codes_fwd:
        code = codes_fwd[0]  # первый код после qty
        size = _pick_small_size(" ".join(ctx))
        if size and code in ("gsh", "gbm", "gpd", "gps"):
            return f"{code}:{size}"
        return code

    # запасной вариант: пробуем общий контекст
    return _make_key_from_text(" ".join(ctx))


# ---------------- CSV ----------------

def build_csv_from_pdf(pdf_bytes: bytes, items_xlsx_path: str, delimiter: str = ";") -> str:
    items_map = _load_items(items_xlsx_path)  # key -> excel_name
    lines = _extract_lines(pdf_bytes)

    found_qty: Dict[str, int] = {}
    order: List[str] = []  # порядок по PDF (первое появление)

    for i, raw in enumerate(lines):
        line = (raw or "").strip()
        if not line:
            continue

        # пропуски мусора/шапок
        if line.startswith("Фото Товар"):
            continue
        if line.startswith("Страница:"):
            continue
        if line in ("18995 ₽", "Общий вес", "Максимальный габарит заказа", "Адрес:", "Телефон:", "Email"):
            continue

        m = _QTY_LINE_RE.search(line)
        if not m:
            continue

        qty = int(m.group(1))
        key = _make_key_for_qty_line(lines, i)

        # выводим ТОЛЬКО то, что есть в Excel
        if not key or key not in items_map:
            continue

        # фиксируем порядок первого появления в PDF
        if key not in found_qty:
            order.append(key)

        found_qty[key] = found_qty.get(key, 0) + qty

    # CSV: только найденные, в порядке PDF
    out = io.StringIO()
    writer = csv.writer(out, delimiter=delimiter)
    writer.writerow(["Наименование", "Кол-во"])

    for key in order:
        writer.writerow([items_map[key], found_qty[key]])

    return out.getvalue()
