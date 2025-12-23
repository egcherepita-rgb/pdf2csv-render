import io
import re
from collections import OrderedDict
from typing import List, Tuple, Optional

import fitz  # PyMuPDF
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import Response, HTMLResponse


app = FastAPI(title="PDF → CSV (товар / кол-во)", version="1.4.0")


# -------------------------
# УТИЛИТЫ
# -------------------------

RX_QTY = re.compile(r"^\d+$")
RX_GAB = re.compile(r"^\d{2,}[xх]\d{2,}([xх]\d{1,})?$", re.IGNORECASE)  # 950x48x9 / 1250x25x25 и т.п.

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

    Возвращаем:
    - header_bottom
    - границы колонки кол-во (по "Кол-во" и "Сумма")
    - ориентиры по X для названия (левая граница после "Фото", правая - перед "Габариты")
      (правая граница используется как запасная, основное отсечение делаем по "мм" в строке)
    """
    w_foto = find_word(words, "Фото")
    w_gab = find_word(words, "Габариты")
    w_qty = find_word(words, "Кол-во")
    w_sum = find_word(words, "Сумма")

    if not (w_foto and w_gab and w_qty and w_sum):
        return None

    header_bottom = float(max(w_foto[3], w_gab[3], w_qty[3], w_sum[3]))

    # Название товара начинается после "Фото"
    name_left = float(w_foto[2]) + 6.0
    # Правая граница — перед заголовком "Габариты" (запасная)
    name_right = float(w_gab[0]) - 8.0

    qty_left = float(w_qty[0]) - 4.0
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


def make_csv_cp1251(rows: List[Tuple[str, int]]) -> bytes:
    """
    CSV с разделителем ';' и кодировкой cp1251 (Windows-1251),
    чтобы Excel открывал двойным кликом без кракозябр.
    """
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

def extract_rows_from_page(page: fitz.Page, layout: dict, header_is_present: bool) -> List[Tuple[str, int]]:
    """
    Идём по "якорям" количества и собираем слова в той же строке.
    Ключевое: если в строке есть "мм", то отсекаем габариты (950x48x9 мм) из колонки "Габариты".
    """
    words = page.get_text("words")
    if not words:
        return []

    header_bottom = layout["header_bottom"] if header_is_present else 0.0
    name_left, name_right = layout["name_left"], layout["name_right"]
    qty_left, qty_right = layout["qty_left"], layout["qty_right"]

    # 1) Находим кол-ва в колонке "Кол-во"
    qty_cells = []
    for w in words:
        s = w[4] or ""
        if not RX_QTY.match(s):
            continue
        x0, y0 = float(w[0]), float(w[1])
        if y0 <= header_bottom + 1:
            continue
        if not (qty_left <= x0 <= qty_right):
            continue

        q = int(s)
        # защита от "Стоимость проекта: 42869"
        if 1 <= q <= 500:
            qty_cells.append(w)

    if not qty_cells:
        return []

    qty_cells.sort(key=lambda w: (w[1], w[0]))
    y_centers = [ (float(w[1]) + float(w[3])) / 2 for w in qty_cells ]

    rows: List[Tuple[str, int]] = []

    for i, qword in enumerate(qty_cells):
        q = int(qword[4])
        y = y_centers[i]

        # вертикальный диапазон "строки"
        top = (y_centers[i - 1] + y) / 2 if i > 0 else (header_bottom + 2)
        bottom = (y + y_centers[i + 1]) / 2 if i < len(y_centers) - 1 else (y + 160)
        top = max(top, header_bottom + 2)

        # все слова в диапазоне строки (нужны, чтобы определить "мм" и где начинаются габариты)
        band = []
        for w in words:
            x0, y0, y1 = float(w[0]), float(w[1]), float(w[3])
            yc = (y0 + y1) / 2
            if yc < top or yc > bottom:
                continue
            if y0 <= header_bottom + 1:
                continue
            band.append(w)

        # 2) Определяем, где начинаются габариты по признаку "мм" в строке
        has_mm = any((bw[4] or "").lower() == "мм" for bw in band)
        # иногда вместо "мм" можно ловить по "кг." — но габариты лучше резать по "мм"
        gab_x_min = None
        if has_mm:
            # ищем самое левое "950x48x9" / "1250x25x25" в этой строке
            candidates = []
            for bw in band:
                token = (bw[4] or "").strip()
                if RX_GAB.match(token):
                    candidates.append(float(bw[0]))
            if candidates:
                gab_x_min = min(candidates)

        # 3) Собираем название товара: только из колонки слева, до начала габаритов
        name_words = []
        for bw in band:
            x0 = float(bw[0])
            # базовая зона названия
            if not (name_left <= x0 <= name_right):
                continue
            # если нашли начало габаритов (по "мм"), режем всё правее
            if gab_x_min is not None and x0 >= (gab_x_min - 6.0):
                continue
            name_words.append(bw)

        name_words.sort(key=lambda w: (w[1], w[0]))
        name = normalize_text(" ".join(w[4] for w in name_words))

        # подчистка мусора
        name = re.sub(r"^Фото\s*", "", name, flags=re.IGNORECASE).strip()
        name = re.sub(r"^Товар\s*", "", name, flags=re.IGNORECASE).strip()
        low = name.lower()
        if not name:
            continue
        if "стоимость проекта" in low or "развертка стены" in low:
            continue

        rows.append((name, q))

    return rows


def extract_items_from_pdf(pdf_bytes: bytes) -> List[Tuple[str, int]]:
    """
    Возвращает позиции в ПОРЯДКЕ PDF.
    Если товар повторился — суммируем, но позиция сохраняется по первому появлению.
    """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    current_layout = None

    # ordered sum
    ordered = OrderedDict()  # name -> qty

    for page in doc:
        words = page.get_text("words")
        if not words:
            continue

        header_layout = layout_from_header(words)
        if header_layout is not None:
            current_layout = header_layout
            page_rows = extract_rows_from_page(page, current_layout, header_is_present=True)
        else:
            if current_layout is None:
                continue
            page_rows = extract_rows_from_page(page, current_layout, header_is_present=False)

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
    .links a { margin-right: 14px; color:#0b57d0; text-decoration: none; }
    .links a:hover { text-decoration: underline; }
    .hint { color:#666; font-size: 14px; }
    .status { margin-top: 12px; font-size: 14px; }
    .ok { color: #0a7a2f; }
    .err { color: #b00020; white-space: pre-wrap; }
    .small { font-size: 12px; color:#777; margin-top: 6px; }
  </style>
</head>
<body>
  <div class="card">
    <h1>PDF → CSV</h1>
    <p>Загрузите PDF и получите CSV: <b>Товар</b> / <b>Кол-во</b> (в порядке как в PDF).</p>
    <p class="hint">CSV: кодировка <b>Windows-1251</b>, разделитель <b>;</b> (Excel открывает без кракозябр).</p>

    <div class="row">
      <input id="pdf" type="file" accept="application/pdf,.pdf" />
      <button id="btn" class="primary" disabled>Получить CSV</button>
    </div>

    <div id="status" class="status"></div>

    <p class="small">
      <span class="links">
        <a href="/docs">Swagger</a>
        <a href="/health">Health</a>
      </span>
    </p>
  </div>

<script>
  const input = document.getElementById('pdf');
  const btn = document.getElementById('btn');
  const statusEl = document.getElementById('status');

  function ok(msg){ statusEl.className="status ok"; statusEl.textContent=msg; }
  function err(msg){ statusEl.className="status err"; statusEl.textContent=msg; }
  function neutral(msg){ statusEl.className="status"; statusEl.textContent=msg||""; }

  input.addEventListener('change', () => {
    const f = input.files && input.files[0];
    btn.disabled = !f;
    neutral(f ? ("Выбран файл: " + f.name) : "");
  });

  btn.addEventListener('click', async () => {
    const f = input.files && input.files[0];
    if (!f) return;

    btn.disabled = true;
    neutral("Обработка PDF…");

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

      ok("Готово! CSV скачан: " + filename);
    } catch (e) {
      err(String(e.message || e));
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


@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
def home():
    return HOME_HTML


@app.post("/extract")
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
