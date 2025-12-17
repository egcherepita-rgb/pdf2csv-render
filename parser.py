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
    items = [v.strip() for v in df.iloc[:, 0].dropna().astype(str).tolist() if v.strip()]
    return items

def build_csv_from_pdf(pdf_bytes: bytes, items_xlsx_path: str, delimiter: str = ";") -> str:
    items = _load_items(items_xlsx_path)
    items_norm = [_norm(x) for x in items]
    data = []  # Список (название, кол-во)

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            # Сначала пытаемся извлечь таблицу (для стр. 3)
            table = page.extract_table()
            if table:
                for row in table[1:]:  # Пропуск заголовка
                    if len(row) >= 7:
                        item = row[1].replace('\n', ' ').strip()
                        qty = row[5].strip()
                        if qty.isdigit():
                            data.append((item, int(qty)))

            # Затем текст (для стр. 4 и если таблица не найдена)
            text = page.extract_text()
            if text:
                lines = text.splitlines()
                buf = []
                qty_line_re = re.compile(r"₽\s+(\d+)\s+\d+ ₽")  # Улучшенный regex
                for line in lines:
                    line = line.strip()
                    if not line or line.startswith("Фото Товар") or line.startswith("Страница:"):
                        continue
                    buf.append(line)
                    m = qty_line_re.search(' '.join(buf))  # Ищем в склеенном buf
                    if m:
                        qty = int(m.group(1))
                        name_parts = []
                        full_buf = ' '.join(buf)
                        # Вырезаем имя до размеров/веса/цены
                        name_match = re.match(r"^(.*?)(?:\d+x\d+x?\d*\s*мм|$)", full_buf)
                        if name_match:
                            name = name_match.group(1).strip()
                            if name:
                                data.append((name, qty))
                        buf = []

    # Сопоставление с Excel (улучшено: учитываем коды вроде GBr-40 U, GFBс-60)
    counts = {item: 0 for item in items}
    for found_name, qty in data:
        fn = _norm(found_name)
        code = None
        # Regex для кодов (учитывает U, c, x30 и т.д.)
        m = re.search(r"\b([A-ZА-Я]{1,4}[a-zа-я]?-?\d{1,3}(?:\s*[a-zа-яА-Я]|\s*x\d+)?)\b", found_name)
        if m:
            code = _norm(m.group(1))
        matched_idx = None
        if code:
            for i, itn in enumerate(items_norm):
                if code in itn:
                    matched_idx = i
                    break
        if matched_idx is None:
            for i, itn in enumerate(items_norm):
                if itn in fn:
                    matched_idx = i
                    break
        if matched_idx is not None:
            counts[items[matched_idx]] += qty

    # CSV (только найденные с >0, но можно все)
    out = io.StringIO()
    out.write(f"Товар{delimiter}Кол-во\n")
    for item in items:
        if counts[item] > 0:  # Только с кол-вом >0, чтобы не мусорить
            out.write(f"{item}{delimiter}{counts[item]}\n")
    return out.getvalue()
