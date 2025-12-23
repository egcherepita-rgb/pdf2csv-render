import io
import re
from collections import Counter, defaultdict
from typing import List, Tuple, Optional

import fitz  # PyMuPDF
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import Response, HTMLResponse


app = FastAPI(title="PDF → CSV (товар / кол-во)", version="1.1.0")


# -------------------------
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# -------------------------

def normalize_text(text: str) -> str:
    text = text.replace("\u00a0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def detect_qty_column_x(words: list) -> Optional[float]:
    """
    Определяем X-координату колонки 'Кол-во' по наиболее частым x0 у чисел.
    Обычно две самые частые "числовые" колонки — Кол-во и Сумма.
    Кол-во левее → берём min из топ-2.
    """
    digit_words = [w for w in words if re.fullmatch(r"\d+", w[4] or "")]
    if not digit_words:
        return None

    xs = [round(w[0], 1) for w in digit_words]
    top = Counter(xs).most_common(2)
    if not top:
        return None

    return min(x for x, _ in top)


def extract_from_page(page: fitz.Page) -> List[Tuple[str, int]]:
    words = page.get_text("words")  # (x0,y0,x1,y1,word,block,line,word_no)
    if not words:
        return []

    qty_x = detect_qty_column_x(words)
    if qty_x is None:
        return []

    # "якоря" — числа в колонке количества
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

        # Левая часть строки — колонка "Товар"
        left_limit = page.rect.width * 0.42

        name_words = [
            w for w in words
            if top <= (w[1] + w[3]) / 2 <= bottom and w[0] < left_limit
        ]
        name_words.sort(key=lambda w: (w[1], w[0]))

        name = normalize_text(" ".join(w[4] for w in name_words))
        name = re.sub(r"^Товар\s*", "", name, flags=re.IGNORECASE).strip()

        try:
            qty = int(anchor[4])
        except ValueError:
            continue

        if not name or name.lower() in {"товар", "фото"}:
            continue

        results.append((name, qty))

    return results


def extract_items_from_pdf(pdf_bytes: bytes) -> List[Tuple[str, int]]:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    aggregated = defaultdict(int)

    for page in doc:
        for name, qty in extract_from_page(page):
            aggregated[name] += qty

    return sorted(aggregated.items(), key=lambda x: x[0].lower())


def make_csv_cp1251(rows: List[Tuple[str, int]]) -> bytes:
    """
    CSV с разделителем ';' и кодировкой cp1251 (Windows-1251),
    чтобы Excel открывал двойным кликом без "кракозябр".
    """
    output = io.StringIO()
    output.write("Товар;Кол-во\n")

    for name, qty in rows:
        safe = name.replace('"', '""')
        if ";" in safe or "\n" in safe:
            safe = f'"{safe}"'
        output.write(f"{safe};{qty}\n")

    return output.getvalue().encode("cp1251", errors="replace")


# -------------------------
# ROUTES
# -------------------------

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/info")
def info():
    return {"status": "ok", "docs": "/docs", "health": "/health", "extract": "/extract"}


@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
def home():
    # Простая страница: выбор PDF → отправка → скачивание CSV
    return """
<!doctype html>
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
    .links a { margin-right: 14px; color:#0b57d0; text-decoration: none; }
    .links a:hover { text-decoration: underline; }
    .hint { color:#666; font-size: 14px; }
    .status { margin-top: 12px; font-size: 14px; }
    .ok { color: #0a7a2f; }
    .err { color: #b00020; white-space: pre-wrap; }
    .small { font-size: 12px; color:#777; margin-top: 6px; }
    .kbd { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
           background:#f3f3f3; border: 1px solid #e4e4e4; padding: 1px 6px; border-radius: 6px; }
  </style>
</head>
<body>
  <div class="card">
    <h1>PDF → CSV</h1>
    <p>Загрузите PDF и получите CSV с двумя колонками: <b>Товар</b> и <b>Кол-во</b>.</p>
    <p class="hint">CSV отдаётся в кодировке <b>Windows-1251</b> и с разделителем <b>;</b> — Excel открывает без “кракозябр”.</p>

    <div class="row">
      <input id="pdf" type="file" accept="application/pdf,.pdf" />
      <button id="btn" class="primary" disabled>Получить CSV</button>
    </div>

    <div id="status" class="status"></div>

    <p class="small">
      Диагностика: <span class="links">
        <a href="/docs">Swagger</a>
        <a href="/health">Health</a>
        <a href="/info">Info</a>
      </span>
      API: <span class="kbd">POST /extract</span>
    </p>
  </div>

<script>
  const input = document.getElementById('pdf');
  const btn = document.getElementById('btn');
  const statusEl = document.getElementById('status');

  function setStatusOk(msg) {
    statusEl.className = "status ok";
    statusEl.textContent = msg;
  }
  function setStatusErr(msg) {
    statusEl.className = "status err";
    statusEl.textContent = msg;
  }
  function setStatusNeutral(msg) {
    statusEl.className = "status";
    statusEl.textContent = msg || "";
  }

  input.addEventListener('change', () => {
    const f = input.files && input.files[0];
    btn.disabled = !f;
    setStatusNeutral(f ? ("Выбран файл: " + f.name) : "");
  });

  btn.addEventListener('click', async () => {
    const f = input.files && input.files[0];
    if (!f) return;

    btn.disabled = true;
    setStatusNeutral("Обработка PDF…");

    try {
      const fd = new FormData();
      fd.append("file", f);

      const resp = await fetch("/extract", { method: "POST", body: fd });

      if (!resp.ok) {
        // FastAPI обычно отдаёт JSON с detail
        let text = await resp.text();
        try {
          const j = JSON.parse(text);
          text = j.detail ? String(j.detail) : text;
        } catch {}
        throw new Error("Ошибка " + resp.status + ": " + text);
      }

      const blob = await resp.blob();

      // Имя файла: items.csv или производное от PDF
      const base = (f.name || "items.pdf").replace(/\\.pdf$/i, "");
      const filename = base + ".csv";

      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);

      setStatusOk("Готово! CSV скачан: " + filename);
    } catch (e) {
      setStatusErr(String(e.message || e));
    } finally {
      btn.disabled = !(input.files && input.files[0]);
    }
  });
</script>
</body>
</html>
"""


@app.post("/extract", summary="Загрузить PDF и получить CSV (cp1251, ';')")
async def extract(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Загрузите PDF файл (.pdf).")

    pdf_bytes = await file.read()

    try:
        rows = extract_items_from_pdf(pdf_bytes)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Не удалось распарсить PDF: {e}")

    if not rows:
        raise HTTPException(status_code=422, detail="Не удалось найти товары и количество в PDF.")

    csv_bytes = make_csv_cp1251(rows)

    return Response(
        content=csv_bytes,
        media_type="text/csv; charset=windows-1251",
        headers={"Content-Disposition": 'attachment; filename="items.csv"'},
    )
