import io
import re
import csv
import pdfplumber
from openpyxl import load_workbook


# ---------- НОРМАЛИЗАЦИЯ И КЛЮЧ ----------

def _norm(text: str) -> str:
    if not text:
        return ""
    text = str(text).lower().replace("ё", "е")
    text = text.replace("\u00a0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    text = text.replace("x", "х").replace("×", "х")
    return text


def _make_key(text: str) -> str | None:
    """
    Делает ключ:
      - gr-65
      - gs-200
      - gbr-40
      - gsh:60х40
    """
    t = _norm(text)

    # код изделия (под твой PDF)
    m_code = re.search(r"\b(g[a-z]{1,3}-?\d{2,3}|grb|grp|grc|gbm|gsh)\b", t)
    if not m_code:
        return None
    code = m_code.group(1)

    # размер (для полок/корзин)
    m_size = re.search(r"\b(\d{2,3}х\d{2,3})\b", t)
    if m_size:
        return f"{code}:{m_size.group(1)}"

    return code


# ---------- EXCEL ----------

def _load_items(xlsx_path: str) -> dict[str, str]:
    """
    Читает 1-ю колонку (A) начиная с 1-й строки.
    Возвращает { key -> оригинальное_название }
    """
    wb = load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb.active

    items: dict[str, str] = {}

    for row in ws.iter_rows(min_row=1, max_col=1, values_only=True):
        v = row[0]
        if v is None:
            continue
        name = str(v).strip()
        if not name:
            continue

        # если вдруг в первой строке заголовок
        if _norm(name) in ("наименование", "товар", "позиция"):
            continue

        key = _make_key(name)
        if key:
            items[key] = name

    wb.close()
    return items


# ---------- PDF ----------

_QTY_RE = re.compile(r"\b\d+[.,]\d+\s*₽\s*(\d+)\s+\d+\s*₽\b")
_PRICE_START_RE = re.compile(r"\b\d+[.,]\d+\s*₽\b")


def _name_from_buffer(buffer_lines: list[str]) -> str:
    """
    Собирает название товара из набора строк.
    ВАЖНО: если цена/₽ в той же строке — мы всё равно берём часть ДО цены.
    """
    joined = " ".join([x.strip() for x in buffer_lines if x and x.strip()])
    joined = re.sub(r"\s+", " ", joined).strip()

    # отрезаем всё, что начинается с цены "500.00 ₽"
    parts = _PRICE_START_RE.split(joined, maxsplit=1)
    left = parts[0].strip() if parts else joined

    # дополнительная “чистка”: часто после названия идут габариты/вес
    # режем по первому " мм" или " кг" если они встретились
    cut_pos = None
    for token in [" мм", " кг", " кг.", "мм", "кг"]:
        p = left.find(token)
        if p != -1:
            cut_pos = p if cut_pos is None else min(cut_pos, p)
    if cut_pos is not None:
        left = left[:cut_pos].strip()

    return left


def _extract_blocks(lines: list[str]) -> list[tuple[str, int]]:
    """
    Возвращает [(название_товара, кол-во)].
    Без сдвигов: каждый раз, когда встречаем строку с ценой/кол-вом/суммой,
    берём название из накопленного буфера (включая текущую строку).
    """
    blocks: list[tuple[str, int]] = []
    buffer: list[str] = []

    for raw in lines:
        line = (raw or "").strip()
        if not line:
            continue

        # шапка таблицы
        if line.startswith("Фото Товар"):
            continue
        if line.startswith("Страница:"):
            continue

        buffer.append(line)

        m = _QTY_RE.search(line)
        if m:
            qty = int(m.group(1))
            name = _name_from_buffer(buffer)

            if name:
                blocks.append((name, qty))

            buffer = []

    return blocks


# ---------- ГЛАВНАЯ ФУНКЦИЯ ----------

def build_csv_from_pdf(pdf_bytes: bytes, items_xlsx_path: str, delimiter: str = ";") -> str:
    items = _load_items(items_xlsx_path)  # {key -> excel_name}
    result: dict[str, int] = {}

    # читаем PDF в строки
    lines: list[str] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            lines.extend(text.splitlines())

    blocks = _extract_blocks(lines)

    # сопоставляем строго по ключу
    for found_name, qty in blocks:
        key = _make_key(found_name)
        if not key:
            continue

        if key in items:
            excel_name = items[key]
            result[excel_name] = result.get(excel_name, 0) + qty

    # CSV: только найденные
    out = io.StringIO()
    writer = csv.writer(out, delimiter=delimiter)
    writer.writerow(["Наименование", "Кол-во"])
    for name, qty in result.items():
        if qty > 0:
            writer.writerow([name, qty])

    return out.getvalue()
