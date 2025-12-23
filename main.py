import io
import re
from collections import defaultdict
from typing import List, Tuple, Optional

import fitz  # PyMuPDF
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import Response, HTMLResponse


app = FastAPI(title="PDF → CSV (товар / кол-во)", version="1.3.1")


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


def layout_from_header(words: list) -> Optional[dict]:
    """
    Ищем шапку таблицы:
    Фото | Товар | Габариты | ... | Кол-во | Сумма
    Берём "Фото", "Габариты", "Кол-во", "Сумма".
    """
    w_foto = find_word(words, "Фото")
    w_gab = find_word(words, "Габариты")
    w_qty = find_word(words, "Кол-во")
    w_sum = find_word(words, "Сумма")

    if not (w_foto and w_gab and w_qty and w_sum):
        return None

    header_bottom = float(max(w_foto[3], w_gab[3], w_qty[3], w_sum[3]))

    # Колонка "Товар" начинается после "Фото" и заканчивается перед "Габариты"
    name_left = float(w_foto[2]) + 6.0
    name_right = float(w_gab[0]) - 6.0

    # Колонка "Кол-во" заканчивается перед "Сумма"
    qty_left = float(w_qty[0]) - 3.0
    qty_right = float(w_sum[0]) - 6.0

    if name_right <= name_left or qty_right <= qty_left:
        return None

    return {
        "header_bottom": header_bottom,
        "name_left": name_left,
        "name_right": name_right,
        "qty_left": qty_left,
        "qty_right": qty_right,
    }


def extract_items_from_page(page: fitz.Page, layout: dict, header_is_present: bool) -> List[Tuple[str, int]]:
    words = page.get_text("words")
    if not words:
        return []

    header_bottom = layout["header_bottom"] if header_is_present else 0.0
    name_left, name_right = layout["name_left"], layout["name_right"]
    qty_left, qty_right = layout["qty_left"], layout["qty_right"]

    # Числа из колонки Кол-во
    qty_cells = []
    for w in words:
        s = w[4] or ""
        if not re.fullmatch(r"\d+", s):
            continue

        x0, y0, y1 = float(w[0]), float(w[1]), float(w[3])
        if y0 <= header_bottom + 1:
            continue
        if not (qty_left <= x0 <= qty_right):
            continue

        q = int(s)
        # защита от "Стоимость проекта 42869"
        if 1 <= q <= 500:
            qty_cells.append((float(w[0]), float(w[1]), float(w[2]), float(w[3]), q))

    if not qty_cells:
        return []

    qty_cells.sort(key=lambda t: (t[1], t[0]))
    y_centers = [(q[1] + q[3]) / 2 for q in qty_cells]

    items: List[Tuple[str, int]] = []

    for i, q in enumerate(qty_cells):
        y = y_centers[i]
        top = (y_centers[i - 1] + y) / 2 if i > 0 else (header_bottom + 2)
        bottom = (y + y_centers[i + 1]) / 2 if i < len(y_centers) - 1 else (y + 140)
        top = max(top, header_bottom + 2)

        name_words = []
        for w in words:
            x0, y0, y1 = float(w[0]), float(w[1]), float(w[3])
            yc = (y0 + y1) / 2

            if yc < top or yc > bottom:
                continue
            if y0 <= header_bottom + 1:
                continue
            if not (name_left <= x0 <= name_right):
                continue
            name_words.append(w)

        name_words.sort(key=lambda w: (w[1], w[0]))
        name = normalize_text(" ".join(w[4] for w in name_words))

        # чистка
        name = re.sub(r"^Фото\s*", "", name, flags=re.IGNORECASE).strip()
        name = re.sub(r"^Товар\s*", "", name, flags=re.IGNORECASE).strip()

        low = name.lower()
        if not name:
            continue
        if "стоимость проекта" in low or "развертка стены" in low:
            continue

        items.append((name, q[4]))

    return items


def extract_items_from_pdf(pdf_bytes: bytes) -> List[Tuple[str, int]]:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")

    aggregated = defaultdict(int)
    current_layout = None

    for page in doc:
        words = page.get_text("words")
        if not words:
            continue

        header_layout = layout_from_header(words)
        if header_layout is not None:
            current_layout = header_layout
            page_items = extract_items_from_page(page, current_layout, header_is_present=True)
        else:
            if current_layout is None:
                continue
            page_items = extract_items_from_page(page, current_layout, header_is_present=False)

        for name, qty in page_items:
            aggregated[name] += qty

    return sorted(aggregated.items(), key=lambda x: x[0].lower())


def make_csv_cp1251(rows: List[Tuple[str, int]]) -> bytes:
    output = io.StringIO()
    output.write("Товар;Кол-во\n")
    for name, qty in rows:
        safe = name.replace('"', '""')
        if ";" in safe or "\n" in safe:
            safe = f'"{safe}"'
        output.write(f"{safe};{qty}\n")
    return output.getvalue().encode("cp1251", errors="replace")


# -------------------------
# HTML (вынесено в константу, чтобы не ломалось)
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
    <p class="hint">CSV: кодировка <b>Windows-1251</b>, разделитель <b>;</b> (Excel открывает без кракозябр).</p>

    <div class="row">
      <input id="pdf" type="file" accept="application/pdf,.pdf" />
      <button id="btn" class="primary" disabled>Получить CSV</button>
    </div>

    <div id="status" class="status"></div>

    <p class="small">
      Диагностика:
      <span class="links">
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

  function setOk(msg){ statusEl.className="status ok"; statusEl.textContent=msg; }
  function setErr(msg){ statusEl.className="status err"; statusEl.textContent=msg; }
  function setNeutral(msg){ statusEl.className="status"; statusEl.textContent=msg||""; }

  input.addEventListener('change', () => {
    const f = input.files && input.files[0];
    btn.disabled = !f;
    setNeutral(f ? ("Выбран файл: " + f.name) : "");
  });

  btn.addEventListener('click', async () => {
    const f = input.files && input.files[0];
    if (!f) return;

    btn.disabled = true;
    setNeutral("Обработка PDF…");

    try {
      const fd = new FormData();
      fd.append("file", f);

      const resp = await fetch("/extract", { method: "POST", body: fd });

      if (!resp.ok) {
        let text = await resp.text();
        try { const j = JSON.parse(text); if (j.detail) text = String(j.detail); } catch {}
        throw new Error("Ошибка " + resp.status + ": " + text);
      }

      const blob = await resp.blob();
      const base = (f.name || "items.pdf").replace(/\.pdf$/i, "");
      const filename = base + ".csv";

      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);

      setOk("Готово! CSV скачан: " + filename);
    } catch (e) {
      setErr(String(e.message || e));
    } finally {
      btn.disabled = !(input.files && input.files[0]);
    }
  });
</script>
</body>
</html>
"""


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
    return HOME_HTML


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
        raise HTTPException(status_code=422, detail="Не удалось найти таблицу товаров и количества в PDF.")

    csv_bytes = make_csv_cp1251(rows)

    return Response(
        content=csv_bytes,
        media_type="text/csv; charset=windows-1251",
        headers={"Content-Disposition": 'attachment; filename="items.csv"'},
    )
