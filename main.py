import io
import re
from collections import OrderedDict
from typing import List, Tuple, Optional

import fitz  # PyMuPDF
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import Response, HTMLResponse


app = FastAPI(title="PDF → CSV (товар / кол-во)", version="1.5.0")


RX_INT = re.compile(r"^\d+$")
RX_DIM = re.compile(r"^\d{2,}[xх]\d{2,}([xх]\d{1,})?$", re.IGNORECASE)  # 950x48x9 / 1250x25x25


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


def compute_layout_from_header(words: list) -> Optional[dict]:
    """
    Ищем шапку таблицы. Нам нужны:
    Фото | ... | Габариты | ... | Кол-во | Сумма

    ВАЖНО: реальная колонка "Габариты" в твоём PDF начинается левее заголовка "Габариты",
    поэтому границу определяем по строкам, где встречается "мм".
    """
    w_foto = find_word(words, "Фото")
    w_gab_hdr = find_word(words, "Габариты")
    w_qty = find_word(words, "Кол-во")
    w_sum = find_word(words, "Сумма")

    if not (w_foto and w_gab_hdr and w_qty and w_sum):
        return None

    header_bottom = float(max(w_foto[3], w_gab_hdr[3], w_qty[3], w_sum[3]))

    # Колонка "Товар" начинается после "Фото"
    name_left = float(w_foto[2]) + 4.0

    # Находим реальный старт колонки "Габариты":
    # берем строки (block,line), где есть "мм", и в них ищем первое DIM-слово (950x48x9)
    mm_tokens = [w for w in words if float(w[1]) > header_bottom and (w[4] or "").lower() == "мм"]
    gab_start_x = None
    if mm_tokens:
        dim_xs = []
        for mm in mm_tokens:
            b, ln = mm[5], mm[6]
            for w in words:
                if w[5] == b and w[6] == ln and RX_DIM.fullmatch(w[4] or ""):
                    dim_xs.append(float(w[0]))
        if dim_xs:
            gab_start_x = min(dim_xs)

    # fallback: если по каким-то причинам мм/габариты не нашлись
    if gab_start_x is None:
        gab_start_x = float(w_gab_hdr[0])

    # всё, что правее gab_start_x — это уже "Габариты" и дальше
    name_right = gab_start_x - 6.0

    qty_left = float(w_qty[0]) - 4.0
    qty_right = float(w_sum[0]) - 4.0

    if name_right <= name_left or qty_right <= qty_left:
        return None

    return {
        "header_bottom": header_bottom,
        "name_left": name_left,
        "name_right": name_right,
        "qty_left": qty_left,
        "qty_right": qty_right,
    }


def extract_rows_on_page(page: fitz.Page, layout: dict, header_present: bool) -> List[Tuple[str, int]]:
    words = page.get_text("words")
    if not words:
        return []

    hb = layout["header_bottom"] if header_present else 0.0
    name_left, name_right = layout["name_left"], layout["name_right"]
    qty_left, qty_right = layout["qty_left"], layout["qty_right"]

    # берём только то, что точно не футер (чтобы "Страница: 3 из 4" не прилипала)
    max_content_y = float(page.rect.height) - 65.0

    # 1) ищем количества в колонке "Кол-во"
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
        # фильтр от мусора вроде итоговых сумм
        if 1 <= q <= 500:
            qty_cells.append(w)

    if not qty_cells:
        return []

    qty_cells.sort(key=lambda w: (w[1], w[0]))
    y_centers = [((float(w[1]) + float(w[3])) / 2.0) for w in qty_cells]

    rows: List[Tuple[str, int]] = []

    # 2) для каждого кол-ва выделяем "строку" по Y и собираем название из колонки Товар
    for i, qw in enumerate(qty_cells):
        y = y_centers[i]
        top = (y_centers[i - 1] + y) / 2.0 if i > 0 else max(hb + 2.0, y - 80.0)
        bottom = (y + y_centers[i + 1]) / 2.0 if i < len(y_centers) - 1 else min(y + 120.0, max_content_y)
        top = max(top, hb + 2.0)

        name_words = []
        for w in words:
            x0, y0, y1 = float(w[0]), float(w[1]), float(w[3])
            if y0 <= hb + 1:
                continue
            if y0 >= max_content_y:
                continue

            yc = (y0 + y1) / 2.0
            if yc < top or yc > bottom:
                continue

            if not (name_left <= x0 <= name_right):
                continue

            name_words.append(w)

        name_words.sort(key=lambda w: (w[1], w[0]))
        name = normalize_text(" ".join(w[4] for w in name_words))

        # чистка от возможных прилипаний
        name = re.sub(r"^Фото\s*", "", name, flags=re.IGNORECASE).strip()
        name = re.sub(r"^Товар\s*", "", name, flags=re.IGNORECASE).strip()
        name = re.sub(r"Страница:.*$", "", name, flags=re.IGNORECASE).strip()

        if not name:
            continue

        rows.append((name, int(qw[4])))

    return rows


def extract_items_from_pdf(pdf_bytes: bytes) -> List[Tuple[str, int]]:
    """
    Порядок сохраняем как в PDF.
    Если один и тот же товар повторится — суммируем, но позиция остается по первому появлению.
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


def make_csv_cp1251(rows: List[Tuple[str, int]]) -> bytes:
    out = io.StringIO()
    out.write("Товар;Кол-во\n")
    for name, qty in rows:
        safe = name.replace('"', '""')
        if ";" in safe or "\n" in safe:
            safe = f'"{safe}"'
        out.write(f"{safe};{qty}\n")
    return out.getvalue().encode("cp1251", errors="replace")


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
    <p class="hint">CSV: кодировка <b>Windows-1251</b>, разделитель <b>;</b> (Excel открывает без “кракозябр”).</p>

    <div class="row">
      <input id="pdf" type="file" accept="application/pdf,.pdf" />
      <button id="btn" class="primary" disabled>Получить CSV</button>
    </div>

    <div id="status" class="status"></div>
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
