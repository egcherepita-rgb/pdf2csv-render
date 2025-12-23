import io
import re
from collections import defaultdict
from typing import List, Tuple, Optional

import fitz  # PyMuPDF
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import Response, HTMLResponse


app = FastAPI(title="PDF → CSV (товар / кол-во)", version="1.2.0")


# -------------------------
# УТИЛИТЫ
# -------------------------

def normalize_text(text: str) -> str:
    text = text.replace("\u00a0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def find_first_word(words: list, target: str) -> Optional[tuple]:
    """
    Ищем слово (токен) в words (page.get_text("words")).
    Возвращаем первый найденный токен: (x0,y0,x1,y1,word,block,line,word_no)
    """
    t = target.lower()
    for w in words:
        if (w[4] or "").lower() == t:
            return w
    return None


def table_layout_from_header(words: list) -> Optional[dict]:
    """
    На странице ищем шапку таблицы:
    "Товар", "Габариты", "Кол-во", "Сумма"
    Если нашлась — возвращаем координаты колонок и нижнюю границу шапки.
    """
    w_tovar = find_first_word(words, "Товар")
    w_gab = find_first_word(words, "Габариты")
    w_qty = find_first_word(words, "Кол-во")
    w_sum = find_first_word(words, "Сумма")

    if not (w_tovar and w_gab and w_qty and w_sum):
        return None

    header_bottom = max(w_tovar[3], w_gab[3], w_qty[3], w_sum[3])

    # Границы колонок по X:
    # Название товара: между "Товар" и "Габариты"
    name_left = w_tovar[0] - 2
    name_right = w_gab[0] - 2

    # Кол-во: между "Кол-во" и "Сумма"
    qty_left = w_qty[0] - 2
    qty_right = w_sum[0] - 2

    # На всякий случай, если PDF чуть “плывёт”
    if name_right <= name_left or qty_right <= qty_left:
        return None

    return {
        "header_bottom": header_bottom,
        "name_left": name_left,
        "name_right": name_right,
        "qty_left": qty_left,
        "qty_right": qty_right,
    }


def extract_items_from_page(page: fitz.Page) -> List[Tuple[str, int]]:
    """
    Извлекаем товары только из таблицы (если есть шапка).
    """
    words = page.get_text("words")
    if not words:
        return []

    layout = table_layout_from_header(words)
    if layout is None:
        # Нет шапки таблицы -> это не страница со списком товаров
        return []

    header_bottom = layout["header_bottom"]
    name_left, name_right = layout["name_left"], layout["name_right"]
    qty_left, qty_right = layout["qty_left"], layout["qty_right"]

    # 1) Находим числа в колонке "Кол-во" НИЖЕ шапки
    qty_words = []
    for w in words:
        txt = w[4] or ""
        if not re.fullmatch(r"\d+", txt):
            continue

        x0, y0, x1, y1 = w[0], w[1], w[2], w[3]
        if y0 <= header_bottom + 1:
            continue
        if not (qty_left <= x0 <= qty_right):
            continue

        qty = int(txt)
        # фильтр от мусора: в проектах кол-во обычно не тысячи
        if 1 <= qty <= 500:
            qty_words.append((x0, y0, x1, y1, qty))

    if not qty_words:
        return []

    # Сортировка сверху вниз
    qty_words.sort(key=lambda t: (t[1], t[0]))

    # 2) Строим "строки" по Y вокруг каждого qty
    y_centers = [(q[1] + q[3]) / 2 for q in qty_words]

    items: List[Tuple[str, int]] = []

    for i, q in enumerate(qty_words):
        y = y_centers[i]

        # границы строки — середина между соседними количествами
        top = (y_centers[i - 1] + y) / 2 if i > 0 else (header_bottom + 2)
        bottom = (y + y_centers[i + 1]) / 2 if i < len(y_centers) - 1 else (y + 120)

        # top не должен заходить на шапку
        top = max(top, header_bottom + 2)

        # 3) Собираем название товара строго из колонки "Товар"
        name_words = []
        for w in words:
            txt = w[4] or ""
            x0, y0, x1, y1 = w[0], w[1], w[2], w[3]
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

        # доп. чистка от случайных прилипаний
        name = re.sub(r"^Фото\s*", "", name, flags=re.IGNORECASE).strip()
        name = re.sub(r"^Товар\s*", "", name, flags=re.IGNORECASE).strip()

        if not name:
            continue

        items.append((name, q[4]))

    return items


def extract_items_from_pdf(pdf_bytes: bytes) -> List[Tuple[str, int]]:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    aggregated = defaultdict(int)

    for page in doc:
        for name, qty in extract_items_from_page(page):
            aggregated[name] += qty

    return sorted(aggregated.items(), key=lambda x: x[0].lower())


def make_csv_cp1251(rows: List[Tuple[str, int]]) -> bytes:
    """
    CSV с разделителем ';' и кодировкой cp1251 (Windows-1251),
    чтобы Excel открывал двойным кликом без кракозябр.
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
    <p class="hint">CSV: кодировка <b>Windows-1251</b>, разделитель <b>;</b> (Excel открывает без кракозябр).</p>

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
        raise HTTPException(status_code=422, detail="Не удалось найти таблицу товаров (шапку 'Товар/Габариты/Кол-во/Сумма').")

    csv_bytes = make_csv_cp1251(rows)

    return Response(
        content=csv_bytes,
        media_type="text/csv; charset=windows-1251",
        headers={"Content-Disposition": 'attachment; filename="items.csv"'},
    )
