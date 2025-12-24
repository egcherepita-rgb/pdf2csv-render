import io
import re
from collections import OrderedDict
from typing import List, Tuple, Optional

import fitz  # PyMuPDF
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import Response, HTMLResponse


app = FastAPI(title="PDF → CSV (товар / кол-во)", version="1.5.1")

RX_INT = re.compile(r"^\d+$")
RX_DIM = re.compile(r"^\d{2,}[xх]\d{2,}([xх]\d{1,})?$", re.IGNORECASE)  # 950x48x9 / 1250x25x25


# -------------------------
# УТИЛИТЫ
# -------------------------

def normalize_text(text: str) -> str:
    text = text.replace("\u00a0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def find_word(words: list, target: str) -> Optional[tuple]:
    t = target.lower()
    for w in words:
        if (w[4] or "").lower() == t:
            return w
    return None


def find_footer_cutoff_y(words: list, page_height: float) -> float:
    """
    Возвращает Y-координату, выше которой контент, а ниже — футер.
    Если на странице есть "Страница:", берём её y0 - небольшой отступ.
    Иначе fallback: page_height - 55.
    """
    footer = None
    for w in words:
        if (w[4] or "").lower().startswith("страница"):
            footer = w
            break
    if footer is None:
        return page_height - 55.0
    return float(footer[1]) - 4.0


def compute_layout_from_header(words: list) -> Optional[dict]:
    """
    Ищем шапку таблицы: Фото | ... | Габариты | ... | Кол-во | Сумма
    Границы:
      - name_left: после "Фото"
      - qty_left/qty_right: по "Кол-во" и "Сумма"
    ВАЖНО: правую границу "Товара" в строках считаем динамически (по габаритам внутри строки),
    поэтому здесь фиксируем только name_left и qty.
    """
    w_foto = find_word(words, "Фото")
    w_qty = find_word(words, "Кол-во")
    w_sum = find_word(words, "Сумма")
    w_gab = find_word(words, "Габариты")  # только для проверки, что это таблица

    if not (w_foto and w_gab and w_qty and w_sum):
        return None

    header_bottom = float(max(w_foto[3], w_gab[3], w_qty[3], w_sum[3]))

    name_left = float(w_foto[2]) + 4.0
    qty_left = float(w_qty[0]) - 4.0
    qty_right = float(w_sum[0]) - 4.0

    if qty_right <= qty_left:
        return None

    return {
        "header_bottom": header_bottom,
        "name_left": name_left,
        "qty_left": qty_left,
        "qty_right": qty_right,
    }


def make_csv_cp1251(rows: List[Tuple[str, int]]) -> bytes:
    out = io.StringIO()
    out.write("Товар;Кол-во\n")
    for name, qty in rows:
        safe = name.replace('"', '""')
        if ";" in safe or "\n" in safe:
            safe = f'"{safe}"'
        out.write(f"{safe};{qty}\n")
    return out.getvalue().encode("cp1251", errors="replace")


# -------------------------
# ПАРСИНГ ТАБЛИЦЫ
# -------------------------

def extract_rows_on_page(page: fitz.Page, layout: dict, header_present: bool) -> List[Tuple[str, int]]:
    words = page.get_text("words")
    if not words:
        return []

    hb = layout["header_bottom"] if header_present else 0.0
    name_left = layout["name_left"]
    qty_left, qty_right = layout["qty_left"], layout["qty_right"]

    # динамический низ контента (чтобы футер не прилипал и не резал последнюю позицию)
    max_content_y = find_footer_cutoff_y(words, float(page.rect.height))

    # 1) Ищем количества в колонке "Кол-во"
    qty_cells = []
    for w in words:
        s = w[4] or ""
        if not RX_INT.fullmatch(s):
            continue
        x0, y0 = float(w[0]), float(w[1])
        if y0 <= hb + 1:
            continue
        if y0 >= max_content_y:
            continue
        if not (qty_left <= x0 <= qty_right):
            continue

        q = int(s)
        if 1 <= q <= 500:
            qty_cells.append(w)

    if not qty_cells:
        return []

    qty_cells.sort(key=lambda w: (w[1], w[0]))
    y_centers = [((float(w[1]) + float(w[3])) / 2.0) for w in qty_cells]

    rows: List[Tuple[str, int]] = []

    for i, qw in enumerate(qty_cells):
        y = y_centers[i]

        top = (y_centers[i - 1] + y) / 2.0 if i > 0 else max(hb + 2.0, y - 90.0)
        bottom = (y + y_centers[i + 1]) / 2.0 if i < len(y_centers) - 1 else min(y + 220.0, max_content_y)
        top = max(top, hb + 2.0)

        # Слова в полосе строки
        band = []
        for w in words:
            x0, y0, y1 = float(w[0]), float(w[1]), float(w[3])
            if y0 <= hb + 1 or y0 >= max_content_y:
                continue
            yc = (y0 + y1) / 2.0
            if yc < top or yc > bottom:
                continue
            band.append(w)

        # 2) Находим начало габаритов В ЭТОЙ ЖЕ строке:
        # если есть "мм", то ищем самое левое DIM-слово (950x48x9) — это и есть старт габаритов
        has_mm = any((w[4] or "").lower() == "мм" for w in band)
        gab_x_min = None
        if has_mm:
            dim_xs = [float(w[0]) for w in band if RX_DIM.fullmatch((w[4] or "").strip())]
            if dim_xs:
                gab_x_min = min(dim_xs)

        # Правая граница названия:
        # - если нашли габариты -> режем до них
        # - иначе режем до колонки количества (это безопасно)
        right_limit = (gab_x_min - 3.0) if gab_x_min is not None else (qty_left - 8.0)

        # 3) Собираем название товара: ВСЁ слева от right_limit (без фикса на "колонку габаритов"),
        # чтобы не потерять сдвинутые строки типа "ПРАКТИК Home GOV-60"
        name_words = []
        for w in band:
            x0 = float(w[0])
            if x0 < name_left:
                continue
            if x0 >= right_limit:
                continue
            # не берём явные единицы измерения, чтобы не прилипло
            if (w[4] or "").lower() in {"мм", "кг.", "кг"}:
                continue
            name_words.append(w)

        name_words.sort(key=lambda w: (w[1], w[0]))
        name = normalize_text(" ".join(w[4] for w in name_words))

        # Чистка мусора
        name = re.sub(r"^Фото\s*", "", name, flags=re.IGNORECASE).strip()
        name = re.sub(r"^Товар\s*", "", name, flags=re.IGNORECASE).strip()
        name = re.sub(r"Страница:.*$", "", name, flags=re.IGNORECASE).strip()

        if not name:
            continue

        rows.append((name, int(qw[4])))

    return rows


def extract_items_from_pdf(pdf_bytes: bytes) -> List[Tuple[str, int]]:
    """
    Порядок сохраняется как в PDF.
    Если товар повторится — суммируем, но позиция остаётся по первому появлению.
    """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")

    layout = None
    ordered = OrderedDict()  # name -> qty

    for page in doc:
        words = page.get_text("words")
        if not words:
            continue

        new_layout = compute_layout_from_header(words)
        header_present = False
        if new_layout is not None:
            layout = new_layout
            header_present = True

        if layout is None:
            continue

        page_rows = extract_rows_on_page(page, layout, header_present=header_present)
        for name, qty in page_rows:
            if name in ordered:
                ordered[name] += qty
            else:
                ordered[name] = qty

    return [(k, v) for k, v in ordered.items()]


# -------------------------
# HTML
# -------------------------

HOME_HTML = r"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>PDF → CSV</title>
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; padding: 24px; background:#fafafa; }
    .card { max-width: 860px; margin: 0 auto; background:#fff; border: 1px solid #e5e5e5; border-radius: 14px; padding: 22px; }
    h1 { margin: 0 0 10px; font-size: 28px; }
    p { margin: 8px 0; color:#333; }
    .row { display:flex; gap: 12px; flex-wrap: wrap; align-items: center; margin-top: 14px; }
    input[type="file"] { padding: 10px; border: 1px solid #ddd; border-radius: 10px; background:#fff; }
    button { padding: 10px 14px; border: 0; border-radius: 10px; cursor: pointer; font-weight: 600; }
    button.primary { background: #111; color: #fff; }
    button.primary:disabled { opacity: .55; cursor:not-allowed; }
    .status { margin-top: 12px; font-size: 14px; white-space: pre-wrap; }
    .ok { color: #0a7a2f; }
    .err { color: #b00020; }
    .hint { color:#666; font-size: 14px; }
  </style>
</head>
<body>
  <div class="card">
    <h1>PDF → CSV</h1>
    <p>Загрузите PDF и получите CSV (2 колонки: <b>Товар</b> и <b>Кол-во</b>) в порядке как в PDF.</p>
    <p class="hint">CSV: кодировка <b>Windows-1251</b>, разделитель <b>;</b>.</p>

    <div class="row">
      <input id="pdf" type="file" accept="application/pdf,.pdf" />
      <button id="btn" class="primary" disabled>Получить CSV</button>
    </div>

    <div id="status" class="status"></div>
  </d
