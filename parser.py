import io
import re
import pandas as pd
import pdfplumber
from thefuzz import fuzz, process

def _norm(s: str) -> str:
    replacements = {
        'х': 'x', 'Х': 'X',
        'с': 'c', 'С': 'C',
        'р': 'g', 'Р': 'G',  # CR → GR
        'о': 'o', 'О': 'O',
        'а': 'a', 'А': 'A',
        'е': 'e', 'Е': 'E',
        'ё': 'e',
        'в': 'b', 'В': 'B',
        'у': 'y', 'У': 'Y',
        'к': 'k', 'К': 'K'  # на всякий
    }
    for rus, lat in replacements.items():
        s = s.replace(rus, lat)
    s = s.lower()
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _load_items(items_xlsx_path: str) -> list[str]:
    df = pd.read_excel(items_xlsx_path, header=None)
    items = [str(v).strip() for v in df.iloc[:, 0].dropna().tolist() if str(v).strip()]
    return items

def build_csv_from_pdf(pdf_bytes: bytes, items_xlsx_path: str, delimiter: str = ";") -> str:
    items = _load_items(items_xlsx_path)
    items_norm = [_norm(x) for x in items]
    item_map = dict(zip(items_norm, items))

    data = []

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            # Таблица
            table = page.extract_table()
            if table:
                for row in table[1:]:
                    if len(row) >= 7:
                        item = (row[1] or "").replace('\n', ' ').strip()
                        qty_str = (row[5] or "").strip()
                        if qty_str.isdigit() and item:
                            data.append((item, int(qty_str)))

            # Текст (улучшенный парсинг стр. 4)
            text = page.extract_text()
            if text:
                blocks = re.split(r'\n\s*\n', text)  # разбиваем на блоки товаров
                for block in blocks:
                    lines = [l.strip() for l in block.split('\n') if l.strip()]
                    if len(lines) >= 2:
                        item_name = lines[0]
                        detail_line = ' '.join(lines[1:])
                        # Ищем количество перед суммой
                        match = re.search(r'(\d+)\s+\d+\s*₽$', detail_line)
                        if match:
                            qty = int(match.group(1))
                            data.append((item_name, qty))

    # Fuzzy с порогом 85%
    counts = {item: 0 for item in items}
    for found_name, qty in data:
        norm_found = _norm(found_name)
        if not norm_found:
            continue
        best_match, score = process.extractOne(norm_found, items_norm, scorer=fuzz.token_sort_ratio)
        if score >= 85:
            original_item = item_map[best_match]
            counts[original_item] += qty

    # CSV
    out = io.StringIO()
    out.write(f"Товар{delimiter}Кол-во\n")
    for item in items:
        if counts[item] > 0:
            out.write(f"{item}{delimiter}{counts[item]}\n")
    return out.getvalue()
