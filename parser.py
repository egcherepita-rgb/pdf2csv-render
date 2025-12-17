import io
import re
import csv
import pdfplumber
from openpyxl import load_workbook


# ----------------------------
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ----------------------------

def _norm(text: str) -> str:
    if not text:
        return ""
    text = text.lower().replace("ё", "е")
    text = text.replace("\u00a0", " ")
    text = re.sub(r"\s+", " ", text)
    text = text.replace("x", "х").replace("×", "х")
    return text.strip()


def _make_key(text: str) -> str | None:
    """
    Делает ключ вида:
    - gr-65
    - gs-200
    - gfb-60
    - gsh:60х40
    """
    t = _norm(text)

    # код изделия
    m_code = re.search(r"\b(g[a-z]{1,3}-?\d{2,3}|grb|grp|grc|gbm|gsh)\b", t)
    if not m_code:
        return None
    code = m_code.group(1)

    # размер (если есть)
    m_size = re.search(r"\b(\d{2,3}х\d{2,3})\b", t)
    if m_size:
        return f"{code}:{m_size.group(1)}"

    return code


# ----------------------------
# ЗАГРУЗКА EXCEL
# ----------------------------

def _load_items(xlsx_path: str) -> dict:
    """
    Загружает Excel и возвращает:
    { key -> оригинальное_название }
    """
    wb = load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb.active

    items = {}

    for row in ws.iter_rows(min_row=2, values_only=True):
        name = str(row[0]).strip()
        if not name:
            continue

        key = _make_key(name)
        if key:
            items[key] = name

    wb.close()
    return items


# ----------------------------
# ПАРСИНГ PDF
# ----------------------------

def _extract_blocks(lines: list[str]) -> list[tuple[str, int]]:
    """
    Возвращает список:
    [(название, количество)]
    """
    blocks = []
    buffer = []

    qty_re = re.compile(r"\b\d+[.,]\d+\s*₽\s*(\d+)\s+\d+\s*₽")

    for line in lines:
        line = line.strip()
        if not line:
            continue

        if line.startswith("Фото Товар"):
            continue

        buffer.append(line)

        m = qty_re.search(line)
        if m:
            qty = int(m.group(1))

            name_parts = []
            for x in buffer:
                if "₽" in x:
                    break
                name_parts.append(x)

            name = " ".join(name_parts).strip()
            if name:
                blocks.append((name, qty))

            buffer = []

    return blocks


# ----------------------------
# ГЛАВНАЯ ФУНКЦИЯ
# ----------------------------

def build_csv_from_pdf(
    pdf_bytes: bytes,
    items_xlsx_path: str,
    delimiter: str = ";"
) -> str:

    items = _load_items(items_xlsx_path)
    result: dict[str, int] = {}

    # читаем PDF
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        lines = []
        for page in pdf.pages:
            text = page.extract_text() or ""
            lines.extend(text.split("\n"))

    blocks = _extract_blocks(lines)

    for found_name, qty in blocks:
        key = _make_key(found_name)
        if not key:
            continue

        if key in items:
            name = items[key]
            result[name] = result.get(name, 0) + qty

    # формируем CSV
    output = io.StringIO()
    writer = csv.writer(output, delimiter=delimiter)
    writer.writerow(["Наименование", "Кол-во"])

    for name, qty in result.items():
        if qty > 0:
            writer.writerow([name, qty])

    return output.getvalue()
