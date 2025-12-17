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


def _norm_code(code: str) -> str:
    """
    Нормализуем код:
    - приводим к lower
    - кириллическую 'с' внутри кода заменяем на латинскую 'c'
      (чтобы GFBс-60 совпадал с GFBc-60 / GFBс-60 из Excel/PDF).
    """
    c = _norm(code)
    c = c.replace("с", "c")  # важно: кириллическая 'с'
    return c


# ---------------- РЕГЕКСЫ ----------------
# Расширили список кодов под твой PDF:
# GR-126, GS-50, GS-200, GS-230
# GBr-40, GBr-50, GRB, GRBn, GRP
# GBrW-50, GFB-60, GFBc-60 (и GFBс-60), GOV-60
# GSh, GShW, GBM, GHB-30, GP (без цифр, но с размером 60х40)
_CODE_RE = re.compile(
    r"\b("
    r"gr-\d{2,3}"
    r"|gs-\d{2,3}"
    r"|gbrw-\d{2,3}"
    r"|gbrn"
    r"|gbr-\d{2,3}"
    r"|grb"
    r"|grbn"
    r"|grp"
    r"|gbm"
    r"|gshw"
    r"|gsh"
    r"|gfb[сc]?-\d{2,3}"
    r"|gov-\d{2,3}"
    r"|ghb-\d{2,3}"
    r"|gp"
    r")\b",
    re.IGNORECASE
)

_SIZE_RE = re.compile(r"\b(\d{2,3})\s*х\s*(\d{2,3})\b", re.IGNORECASE)

# строка с количеством: "450.00 ₽ 1 450 ₽"
_QTY_LINE_RE = re.compile(r"\d+[.,]\d+\s*₽\s*(\d+)\s+[\d\s]+\s*₽")


# ---------------- ЦВЕТ ----------------

def _extract_color(text: str) -> Optional[str]:
    t = _norm(text)
    if "графит" in t:
        return "графит"
    if "бел" in t:
        return "белый"
    if "оцинк" in t:
        return "оцинк"
    return None


# ---------------- РАЗМЕР ----------------

def _pick_small_size(text: str) -> Optional[str]:
    """
    Берём "малый размер" типа 60х40, 60х20, 60х50.
    Большие габариты типа 1260х48, 25х2008 и т.п. игнорируем.
    """
    t = _norm(text)
    for m in _SIZE_RE.finditer(t):
        a = int(m.group(1))
        b = int(m.group(2))
        if max(a, b) > 200:
            continue
        return f"{a}х{b}"
    return None


# ---------------- КЛЮЧ ТОВАРА ----------------

def _make_key(code: str, ctx_text: str) -> str:
    code = _norm_code(code)
    size = _pick_small_size(ctx_text)
    color = _extract_color(ctx_text)

    parts = [code]

    # для размерных позиций добавляем размер:
    # - GSh, GShW, GBM, GP обычно с 60х40/60х50/60х20
    if size and code in ("gsh", "gshw", "gbm", "gp"):
        parts.append(size)

    # добавляем цвет, если есть
    if color:
        parts.append(color)

    return ":".join(parts)


def _make_key_from_text(text: str) -> Optional[str]:
    t = _norm(text)
    codes = [m.group(1) for m in _CODE_RE.finditer(t)]
    if not codes:
        return None
    # если в строке несколько кодов, берём последний
    code = codes[-1]
    return _make_key(code, t)


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


# ---------------- PDF ----------------

def _extract_lines(pdf_bytes: bytes) -> List[str]:
    lines: List[str] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            lines.extend(text.splitlines())
    return lines


def _find_code_near_qty(lines: List[str], i: int) -> Optional[str]:
    """
    В твоём PDF код товара может быть:
    - до строки с ценой/кол-вом
    - или после неё (поэтому смотрим и вперёд, и назад)
    """
    start = max(0, i - 4)
    end = min(len(lines), i + 6)
    ctx = " ".join(lines[start:end])
    ctx_n = _norm(ctx)

    codes = [m.group(1) for m in _CODE_RE.finditer(ctx_n)]
    if not codes:
        return None
    # как правило, нужный код ближе к qty → берём последний найденный
    return codes[-1]


# ---------------- CSV ----------------

def build_csv_from_pdf(pdf_bytes: bytes, items_xlsx_path: str, delimiter: str = ";") -> str:
    items_map = _load_items(items_xlsx_path)
    lines = _extract_lines(pdf_bytes)

    found_qty: Dict[str, int] = {}
    order: List[str] = []  # порядок как в PDF (первое появление)

    for i, raw in enumerate(lines):
        line = (raw or "").strip()
        if not line:
            continue

        # шапки/мусор
        if line.startswith("Фото Товар"):
            continue
        if line.startswith("Страница:"):
            continue
        if line in ("Общий вес", "Максимальный габарит заказа", "Адрес:", "Телефон:", "Email"):
            continue

        m = _QTY_LINE_RE.search(line)
        if not m:
            continue

        qty = int(m.group(1))

        # контекст вокруг qty (чтобы взять цвет/размер)
        start = max(0, i - 4)
        end = min(len(lines), i + 6)
        ctx_text = _norm(" ".join(lines[start:end]))

        code = _find_code_near_qty(lines, i)
        if not code:
            continue

        key = _make_key(code, ctx_text)

        # выводим ТОЛЬКО то, что есть в Excel
        if key not in items_map:
            continue

        if key not in found_qty:
            order.append(key)

        found_qty[key] = found_qty.get(key, 0) + qty

    out = io.StringIO()
    writer = csv.writer(out, delimiter=delimiter)
    writer.writerow(["Наименование", "Кол-во"])

    for key in order:
        writer.writerow([items_map[key], found_qty[key]])

    return out.getvalue()
