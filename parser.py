import io
import re
import pandas as pd
import pdfplumber
from thefuzz import fuzz, process

def _norm(s: str) -> str:
    """
    Нормализация только для кодов и размеров.
    Цвет ("белый", "графит", "белая") остаётся без изменений.
    """
    replacements = {
        'х': 'x', 'Х': 'X',
        'с': 'c', 'С': 'C',
        'р': 'g', 'Р': 'G',  # CR → GR
        'о': 'o', 'О': 'O',
        'а': 'a', 'А': 'A',
        'е': 'e', 'Е': 'E',
        'ё': 'e',
        'в': 'b', 'В': 'B',  # иногда B вместо В
        'у': 'y', 'У': 'Y'
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
    item_map = dict(zip(items_norm, items))  # оригинальное название ← нормализованное

    data = []  # список (найденное имя из PDF, количество)

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            # 1. Таблица на странице 3
            table = page.extract_table()
            if table:
                for row in table[1:]:  # пропускаем заголовок
                    if len(row) >= 7:
                        item_cell = row[1] if row[1] is not None else ""
                        qty_cell = row[5] if row[5] is not None else ""
                        item = item_cell.replace('\n', ' ').strip()
                        if qty_cell.strip().isdigit():
                            qty = int(qty_cell.strip())
                            if item:
                                data.append((item, qty))

            # 2. Текстовая часть на странице 4
            text = page.extract_text()
            if text:
                lines = text.splitlines()
                current_item_lines = []
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue
                    # Если строка содержит цену и количество (пример: "3090.00 ₽ 1 3090 ₽")
                    if re.search(r'\d+\.\d{2}\s*₽.*?\s\d+\s+\d+', line):
                        # Извлекаем количество (второе число перед последней суммой)
                        parts = re.split(r'\s+', line)
                        qty = None
                        for i in range(len(parts) - 2, -1, -1):
                            if parts[i].isdigit():
                                qty = int(parts[i])
                                break
                        if qty and current_item_lines:
                            item_name = ' '.join(current_item_lines).strip()
                            data.append((item_name, qty))
                        current_item_lines = []
                    else:
                        current_item_lines.append(line)

    # 3. Сопоставление с fuzzy (порог 88%)
    counts = {item: 0 for item in items}
    for found_name, qty in data:
        norm_found = _norm(found_name)
        if not norm_found:
            continue
        # Находим лучшее совпадение
        best_match, score = process.extractOne(norm_found, items_norm, scorer=fuzz.token_sort_ratio)
        if score >= 88:  # 88 — оптимально для ваших примеров
            original_item = item_map[best_match]
            counts[original_item] += qty

    # 4. Формируем CSV (только с количеством > 0)
    out = io.StringIO()
    out.write(f"Товар{delimiter}Кол-во\n")
    for item in items:
        if counts[item] > 0:
            out.write(f"{item}{delimiter}{counts[item]}\n")
    return out.getvalue()
