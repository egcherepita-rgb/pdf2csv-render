import io
import re
from collections import Counter, defaultdict
from typing import List, Tuple

import fitz  # PyMuPDF
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import Response, HTMLResponse


app = FastAPI(
    title="PDF → CSV (товар / кол-во)",
    version="1.0.0"
)


# -------------------------
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# -------------------------

def normalize_text(text: str) -> str:
    text = text.replace("\u00a0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def detect_qty_column_x(words: list) -> float | None:
    """
    Автоматически определяем X-координату колонки 'Кол-во'
    Берём наиболее частую координату чисел
    """
    digit_words = [w for w in words if re.fullmatch(r"\d+", w[4] or "")]
    if not digit_words:
        return None

    xs = [round(w[0], 1) for w in digit_words]
    most_common = Counter(xs).most_common(2)
    if not most_common:
        return None

    # колонка количества обычно левее колонки суммы
    return min(x for x, _ in most_common)


def extract_from_page(page: fitz.Page) -> List[Tuple[str, int]]:
    words = page.get_text("words")
    if not words:
        return []

    qty_x = detect_qty_column_x(words)
    if qty_x is None:
        return []

    anchors = [
        w for w in words
        if re.fullmatch(r"\d+", w[4] or "") and abs(w[0] - qty_x) < 2.0
    ]
    anchors.sort(key=lambda w: (w[1], w[0]))

    if not anchors:
        return []

    y_centers = [(w[1] + w[3]) / 2 for w in anchors]
    results: List[Tuple[str, int]] = []

    for i, anchor in enumerate(anchors):
        y = y_centers[i]
        top = (y_centers[i - 1] + y) / 2 if i > 0 else y - 80
        bottom = (y + y_centers[i + 1]) / 2 if i < len(anchors) - 1 else y + 100

        left_limit = page.rect.width * 0.42

        name_words = [
            w for w in words
            if top <= (w[1] + w[3]) / 2 <= bottom and w[0] < left_limit
        ]
        name_words.sort(key=lambda w: (w[1], w[0]))

        name = normalize_text(" ".join(w[4] for w in name_words))
        name = re.sub(r"^Товар\s*", "", name, flags=re.IGNORECASE)

        try:
            qty = int(anchor[4])
        except ValueError:
            continue

        if name:
            results.append((name, qty))

    return results


def extract_items_from_pdf(pdf_bytes: bytes) -> List[Tuple[str, int]]:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    aggregated = defaultdict(int)

    for page in doc:
        for name, qty in extract_from_page(page):
            aggregated[name] += qty

    return sorted(aggregated.items(), key=lambda x: x[0].lower())


def make_csv(rows: List[Tuple[str, int]]) -> bytes:
    output = io.StringIO()
    output.write("Товар;Кол-во\n")
    for name, qty in rows:
        safe = name.replace('"', '""')
        if ";" in safe or "\n" in safe:
            safe = f'"{safe}"'
        output.write(f"{safe};{qty}\n")
    return output.getvalue().encode("utf-8-sig")


# -------------------------
# API
# -------------------------

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/info")
def info():
    return {
        "status": "ok",
        "docs": "/docs",
        "health": "/health",
        "extract": "/extract"
    }


@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
def home():
    return """
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <title>PDF → CSV</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial; padding: 24px; }
    .box { max-width: 760px; margin: auto; border: 1px solid #ddd; border-radius: 12px; padding: 20px; }
    a { margin-right: 12px; }
    code { background: #f5f5f5; padding: 2px 6px; border-radius: 6px; }
  </style>
</head>
<body>
  <div class="box">
    <h1>PDF → CSV</h1>
    <p>Сервис конвертации PDF в CSV (товар / количество).</p>
    <p>
      <a href="/docs">Swagger</a>
      <a href="/health">Health</a>
      <a href="/info">Info (JSON)</a>
    </p>
    <p><code>POST /extract</code> — загрузка PDF, результат: CSV с разделителем <b>;</b></p>
  </div>
</body>
</html>
"""


@app.post("/extract", summary="Загрузить PDF и получить CSV")
async def extract(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Загрузите PDF файл")

    pdf_bytes = await file.read()
    rows = extract_items_from_pdf(pdf_bytes)

    if not rows:
        raise HTTPException(
            status_code=422,
            detail="Не удалось найти товары и количество в PDF"
        )

    csv_bytes = make_csv(rows)
    return Response(
        content=csv_bytes,
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": 'attachment; filename="items.csv"'
        },
    )
