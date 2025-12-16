import io
import re
import pdfplumber
from openpyxl import load_workbook

def _norm(s: str) -> str:
    s = s.lower().replace("ё", "е")
    s = re.sub(r"\s+", " ", s)
    s = s.replace("\u00a0", " ").strip()
    return s

def _load_items(items_xlsx_path: str) -> list[str]:
    wb = load_workbook(items_xlsx_path, read_only=True, data_only=True)
    ws = wb.active  # первый лист
    items = []

    # читаем только первую колонку (A)
    for row in ws.iter_rows(min_row=1, max_col=1, values_only=True):
        v = row[0]
        if v is None:
            continue
        s = str(v).strip()
        if s:
            items.append(s)

    wb.close()
    return items

def _extract_product_blocks_from_text(lines: list[str]) -> list[tuple[str, int]]:
    """
    Склеиваем строки товара до строки с ценой/кол-вом/суммой.
    Кол-во обычно стоит после символа ₽:
    "... 290.00 ₽ 1 290 ₽"
    """
    blocks = []
    buf = []

    qty_line_re = re.compile(r"₽\s+(\d+)\s+\d")  # ловим qty после "₽"

    for raw in lines:
        line = (raw or "").strip()
        if not line:
            continue

        # заголовки/мусор
        if line.startswith("Фото Товар") or line.startswith("Страница:"):
            continue

        buf.append(line)

        m = qty_line_re.search(line)
        if m:
            qty = int(m.group(1))

            name_parts = []
            for x in buf:
                # стоп-слова/строки, после которых обычно начинается тех. инфа/цены
                if "₽" in x:
                    break
                name_parts.append(x)

            name = " ".join(name_parts).strip()
            if name:
                blocks.append((name, qty))

            buf = []

    return blocks

def build_csv_from_pdf(pdf_bytes: bytes, items_xlsx_path: str, delimiter: str = ";") -> str:
    items = _load_items(items_xlsx_path)
    items_norm = [_norm(x) for x in items]

    # читаем весь текст PDF
    all_lines = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            all_lines.extend(text.splitlines())

    blocks = _extract_product_blocks_from_text(all_lines)

    # считаем количества по списку из Excel
    counts = {item: 0 for item in items}

    for found_name, qty in blocks:
        fn = _norm(found_name)

        # 1) пробуем сопоставить по коду типа GR-65 / GS-200 и т.п.
        code = None
        m = re.search(r"\b([A-Za-zА-Яа-я]{1,4}-?\d{2,3})\b", found_name)
        if m:
            code = _norm(m.group(1))

        matched_idx = None

        if code:
            for i, itn in enumerate(items_norm):
                if code in itn:
                    matched_idx = i
                    break

        # 2) запасной вариант: по подстроке названия
        if matched_idx is None:
            for i, itn in enumerate(items_norm):
                if itn and itn in fn:
                    matched_idx = i
                    break

        if matched_idx is not None:
            counts[items[matched_idx]] += qty

    # CSV
    out = io.StringIO()
    out.write(f"Наименование{delimiter}Кол-во\n")
    for item in items:
        out.write(f"{item}{delimiter}{counts[item]}\n")
    return out.getvalue()
