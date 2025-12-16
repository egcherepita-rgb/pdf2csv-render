import io
import re
import pandas as pd
import pdfplumber

def _norm(s: str) -> str:
    s = s.lower().replace("ё", "е")
    s = re.sub(r"\s+", " ", s)
    s = s.replace("\u00a0", " ").strip()
    return s

def _load_items(items_xlsx_path: str) -> list[str]:
    df = pd.read_excel(items_xlsx_path, header=None)
    items = []
    for v in df.iloc[:, 0].dropna().astype(str).tolist():
        v = v.strip()
        if v:
            items.append(v)
    return items

def _extract_product_blocks_from_text(lines: list[str]) -> list[tuple[str, int]]:
    """
    Пытаемся восстановить товары из текстовых строк PDF.
    Логика:
    - товар идёт несколькими строками
    - строка с ценой/кол-вом/суммой обычно содержит: "... ₽ <qty> <sum> ₽"
    """
    blocks = []
    buf = []

    qty_line_re = re.compile(r"₽\s+(\d+)\s+\d")  # ловим qty после "₽"
    for raw in lines:
        line = raw.strip()
        if not line:
            continue

        # заголовки/мусор пропускаем
        if line.startswith("Фото Товар") or line.startswith("Страница:"):
            continue

        buf.append(line)

        m = qty_line_re.search(line)
        if m:
            qty = int(m.group(1))
            name_parts = []
            for x in buf:
                # отрезаем технические строки
                if re.search(r"\d+x\d+x?\d*\s*мм", x.lower()):
                    break
                if "кг." in x.lower():
                    break
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

    # соберём весь текст PDF постранично
    all_lines = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            all_lines.extend(text.splitlines())

    blocks = _extract_product_blocks_from_text(all_lines)

    # Сопоставление:
    # 1) нормализуем найденное название
    # 2) ищем вхождение каждого "эталонного" товара из Excel
    counts = {item: 0 for item in items}

    for found_name, qty in blocks:
        fn = _norm(found_name)

        # лучший вариант: ищем совпадение по коду типа GR-65 / GFB-60 / GBr-40 и т.п.
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

        # запасной вариант: по подстроке названия
        if matched_idx is None:
            for i, itn in enumerate(items_norm):
                if itn and itn in fn:
                    matched_idx = i
                    break

        if matched_idx is not None:
            counts[items[matched_idx]] += qty

    # CSV: Наименование;Кол-во
    out = io.StringIO()
    out.write(f"Наименование{delimiter}Кол-во\n")
    for item in items:
        out.write(f"{item}{delimiter}{counts[item]}\n")
    return out.getvalue()
