import io
import re
from collections import Counter, defaultdict
from typing import List, Tuple

import fitz  # PyMuPDF
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import Response


app = FastAPI(title="PDF → CSV (товар/кол-во)", version="1.0.0")


def _normalize_text(s: str) -> str:
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _choose_qty_x(words: List[tuple]) -> float | None:
    """
    Находим X-позицию колонки "Кол-во" автоматически:
    берем все чисто-числовые слова и ищем наиболее частую координату x0.
    В таблице обычно две самые частые x0 среди чисел — это "Кол-во" и "Сумма".
    "Кол-во" левее, поэтому берем min из топ-2.
    """
    digit_words = [w for w in words if re.fullmatch(r"\d+", w[4] or "")]
    if not digit_words:
        return None

    xs = [round(w[0], 1) for w in digit_words]  # x0
    top = Counter(xs).most_common(2)
    if not top:
        return None

    # две самые частые колонки чисел: qty и sum
    qty_x = min(x for x, _ in top)
    return qty_x


def _extract_items_from_page(page: fitz.Page) -> List[Tuple[str, int]]:
    """
    Возвращает список (товар, кол-во) с одной страницы.
    Строим "якоря" по колонке Кол-во (координата x0),
    а товар собираем слева в соответствующем вертикальном диапазоне.
    """
    words = page.get_text("words")  # (x0,y0,x1,y1,word,block,line,word_no)
    if not words:
        return []

    qty_x = _choose_qty_x(words)
    if qty_x is None:
        return []

    # anchors: числа именно из колонки "Кол-во" (по x0 с допуском)
    digit_words = [w for w in words if re.fullmatch(r"\d+", w[4] or "")]
    anchors = [w for w in digit_words if abs(w[0] - qty_x) < 2.0]
    anchors.sort(key=lambda w: (w[1], w[0]))

    if not anchors:
        return []

    # Центры по Y для расчета границ "строки товара"
    ys = [(w[1] + w[3]) / 2 for w in anchors]

    items: List[Tuple[str, int]] = []

    for i, a in enumerate(anchors):
        y = ys[i]
        top = (ys[i - 1] + y) / 2 if i > 0 else y - 80
        bot = (y + ys[i + 1]) / 2 if i < len(anchors) - 1 else y + 100

        # собираем товар из левой части строки
        # 0.42 ширины A4 у твоего PDF отлично отделяет колонку "Товар" от "Габариты/Вес/Цена"
        left_limit = page.rect.width * 0.42
        band = [
            w for w in words
            if top <= (w[1] + w[3]) / 2 <= bot and w[0] < left_limit
        ]
        band.sort(key=lambda w: (w[1], w[0]))

        text = _normalize_text(" ".join(w[4] for w in band))
        # убрать возможные остатки заголовка таблицы
        text = re.sub(r"^(Фото\s+)?Товар\s+", "", text, flags=re.IGNORECASE).strip()

        try:
            qty = int(a[4])
        except ValueError:
            continue

        # фильтр от мусора
        if not text or text.lower() in {"фото", "товар"}:
            continue

        items.append((text, qty))

    return items


def extract_items_from_pdf(pdf_bytes: bytes) -> List[Tuple[str, int]]:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    all_items: List[Tuple[str, int]] = []

    for page in doc:
        all_items.extend(_extract_items_from_page(page))

    # Суммируем одинаковые товары
    agg = defaultdict(int)
    for name, qty in all_items:
        agg[name] += qty

    # стабильная сортировка для одинакового результата
    result = sorted(agg.items(), key=lambda x: x[0].lower())
    return result


def to_csv_semicolon(rows: List[Tuple[str, int]]) -> bytes:
    out = io.StringIO()
    out.write("Товар;Кол-во\n")
    for name, qty in rows:
        # Экранируем кавычки + оборачиваем, если вдруг встречаются ; или переносы
        safe = name.replace('"', '""')
        if ";" in safe or "\n" in safe:
            safe = f'"{safe}"'
        out.write(f"{safe};{qty}\n")
    return out.getvalue().encode("utf-8-sig")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/extract", summary="Загрузить PDF и получить CSV (товар/кол-во)")
async def extract(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Загрузите PDF файл.")

    pdf_bytes = await file.read()
    try:
        rows = extract_items_from_pdf(pdf_bytes)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Не удалось распарсить PDF: {e}")

    if not rows:
        raise HTTPException(
            status_code=422,
            detail="Не найдено ни одной позиции. Проверьте, что в PDF есть таблица с колонкой 'Кол-во'."
        )

    csv_bytes = to_csv_semicolon(rows)
    return Response(
        content=csv_bytes,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="items.csv"'},
    )
