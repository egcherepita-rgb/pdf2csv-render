import re
from io import StringIO
from typing import Dict, List, Optional, Tuple

import pdfplumber
from openpyxl import load_workbook


# --- Нормализация текста ---
def _norm(text: str) -> str:
    if text is None:
        return ""
    s = str(text).lower().replace("ё", "е").replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    # приводим все варианты "x/*/×" к кириллической "х"
    s = s.replace("x", "х").replace("×", "х").replace("*", "х")
    return s


# --- Регексы для ключей ---
# Ловим коды типа GR-65, GS-150, GBr-40, GPd-30, GSd-40, GFB-60 и т.п.
_CODE_RE = re.compile(r"\b(g[a-z]{1,3}-?\d{2,3}|grb|grp|grc|gbm|gsh)\b", re.IGNORECASE)

# Размеры: 60х40, 90x40, 90*40 и т.п.
_SIZE_RE = re.compile(r"\b(\d{2,3})\s*[хx×\*]\s*(\d{2,3})\b", re.IGNORECASE)

# Строка с количеством: "... 290.00 ₽ 1 290 ₽" (в середине — КОЛ-ВО)
# Важно: сумма может быть с пробелами "1 290"
_QTY_LINE_RE = re.compile(r"\d+[.,]\d+\s*₽\s*(\d+)\s+[\d\s]+\s*₽")


def _make_key_from_text(text: str) -> Optional[str]:
    """
    Делаем ключ:
      - если нашли код: берем его
      - если нашли размер: добавляем как code:AxB (например gsh:60х40)
    """
    t = _norm(text)
    mc = _CODE_RE.search(t)
    if not mc:
        return None
    code = mc.group(1).lower()

    ms = _SIZE_RE.search(t)
    if ms:
        a = int(ms.group(1))
        b = int(ms.group(2))
        size = f"{a}х{b}"
        return f"{code}:{size}"

    return code


def _load_items(items_xlsx_path: str) -> Dict[str, str]:
    """
    Читает Excel (1 колонка: наименования).
    Возвращает map: key -> оригинальное наименование (как в Excel).
    """
    wb = load_workbook(items_xlsx_path, read_only=True, data_only=True)
    ws = wb.active

    items_map: Dict[str, str] = {}
    for (val,) in ws.iter_rows(min_row=1, max_col=1, values_only=True):
        if val is None:
            continue
        name = str(val).strip()
        if not name:
            continue
        # пропускаем заголовок если он есть
        if _norm(name) in ("наименование", "товар", "позиция"):
            continue

        key = _make_key_from_text(name)
        if key:
            items_map[key] = name

    wb.close()
    return items_map


def _extract_lines_from_pdf(pdf_bytes: bytes) -> List[str]:
    lines: List[str] = []
    with pdfplumber.open(StringIO("")) as _:
        pass  # просто чтобы линтер не ругался, pdfplumber реально откроем ниже

    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            lines.extend(text.splitlines())
    return lines


# pdfplumber требует BytesIO, поэтому импорт здесь
from io import BytesIO


def _is_qty_line(s: str) -> bool:
    return bool(_QTY_LINE_RE.search(s))


def _block_around_qty(lines: List[str], qty_idx: int, max_back: int = 7, max_fwd: int = 10) -> List[str]:
    """
    Собираем "блок" вокруг строки с количеством, чтобы в блок попали:
    - название
    - размеры/вес
    - код
    - цвет
    Даже если код стоит на следующей строке (как в твоём PDF).
    """
    start = qty_idx
    steps = 0

    # назад: берём несколько строк, но останавливаемся на предыдущей строке с ₽ (другая позиция)
    while start > 0 and steps < max_back:
        prev = lines[start - 1].strip()
        if not prev:
            break
        if _is_qty_line(prev):
            break
        # если там "кодовая" строка (GR-65/GS-150) — чаще это хвост предыдущей позиции, не лезем дальше
        if _CODE_RE.search(_norm(prev)) and "₽" not in prev and not any(ch.isalpha() for ch in prev[:1]):
            break
        start -= 1
        steps += 1

    end = qty_idx
    steps = 0

    # вперёд: берём ещё строки после количества (там часто код/цвет/размер)
    while end + 1 < len(lines) and steps < max_fwd:
        nxt = lines[end + 1].strip()
        if not nxt:
            break
        if _is_qty_line(nxt):
            break
        end += 1
        steps += 1

    return lines[start : end + 1]


def build_csv_from_pdf(pdf_bytes: bytes, items_xlsx_path: str, delimiter: str = ";") -> str:
    """
    Главная функция:
      - загрузить список из Excel
      - распарсить PDF
      - сопоставить ключи
      - вернуть CSV (только найденные позиции)
    """
    items_map = _load_items(items_xlsx_path)

    # 1) вытаскиваем все строки PDF
    lines: List[str] = []
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            lines.extend(text.splitlines())

    # 2) находим все qty-строки и строим найденное
    found_qty_by_key: Dict[str, int] = {}

    for i, line in enumerate(lines):
        m = _QTY_LINE_RE.search(line)
        if not m:
            continue
        qty = int(m.group(1))

        block = _block_around_qty(lines, i)
        key = _make_key_from_text(" ".join(block))
        if not key:
            continue

        # ВАЖНО: выводим ТОЛЬКО то, что есть в Excel
        if key not in items_map:
            continue

        found_qty_by_key[key] = found_qty_by_key.get(key, 0) + qty

    # 3) формируем CSV
    out = StringIO()
    out.write(f"Наименование{delimiter}Кол-во\n")

    # порядок: как в Excel
    for key, name in items_map.items():
        if key in found_qty_by_key:
            out.write(f"{name}{delimiter}{found_qty_by_key[key]}\n")

    return out.getvalue()
