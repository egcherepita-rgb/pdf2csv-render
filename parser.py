import io
import re
import pandas as pd
import pdfplumber
from thefuzz import fuzz, process

def _norm(s: str) -> str:
    # Нормализуем ТОЛЬКО коды и размеры, цвет оставляем как есть
    replacements = {
        'х': 'x', 'Х': 'X',
        'с': 'c', 'С': 'C',
        'р': 'g', 'Р': 'G',  # CR → GR
        'о': 'o', 'О': 'O',
        'а': 'a', 'А': 'A',
        'е': 'e', 'Е': 'E',
        'ё': 'e'
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
    item_map = dict(zip(items_norm, items))  # для обратного маппинга на оригинальное название

    data = []  # (найденное имя из PDF, количество)

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            # Таблица (стр. 3)
            table = page.extract_table()
            if table:
                for row in table[1:]:
                    if len(row) >= 7 and row[5] and row[5].strip().isdigit():
                        item = row[1].replace('\n', ' ').strip()
                        qty = int(row[5].strip())
                        data.append((item, qty))

            # Текст (стр. 4)
            text = page.extract_text()
            if text:
                lines = text.splitlines()
                current_item = []
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue
                    # Ищем строки с ценой и количеством вида "3090.00 ₽ 1 3090 ₽"
                    if re.search(r'\d+\.\d{2}\s*₽\s*\d+\s+\d+', line):
                        parts = line.split()
                        if len(parts) >= 4:
                            qty = None
                            for i, p in enumerate(parts):
                                if p.isdigit() and i > 0 and parts[i-1].endswith('₽'):
                                    qty = int(p)
                                    break
                            if qty and current_item:
                                item_name = ' '.join(current_item).strip()
                                data.append((item_name, qty))
                                current_item = []
                    else:
                        current_item.append(line)

    # Нечёткое сопоставление
    counts = {item: 0 for item in items}
    for found_name, qty in data:
        norm_found = _norm(found_name)
        # Ищем лучшее совпадение с высоким порогом
        best_match, score = process.extractOne(norm_found, items_norm, scorer=fuzz.token_sort_ratio)
        if score >= 90:
            original_item = item_map[best_match]
            counts[original_item] += qty

    # Формируем CSV только с найденными (>0)
    out = io.StringIO()
    out.write(f"Товар{delimiter}Кол-во\n")
    for item in items:
        if counts[item] > 0:
            out.write(f"{item}{delimiter}{counts[item]}\n")
    return out.getvalue()
