import io
import re
from collections import OrderedDict
from typing import List, Tuple, Optional, Dict

import fitz  # PyMuPDF
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import Response, HTMLResponse


app = FastAPI(title="PDF → CSV (товар / кол-во)", version="2.3.1")

RX_DIM_LINE = re.compile(r"\b\d{2,}[xх]\d{2,}(?:[xх]\d{1,})?\b.*\bмм\b", re.IGNORECASE)
RX_WEIGHT_LINE = re.compile(r"\b\d+(?:[.,]\d+)?\s*кг\.?\b", re.IGNORECASE)
RX_PRICE_LINE = re.compile(r"\b\d+(?:[.,]\d+)\s*₽\b")
RX_INT = re.compile(r"^\d+$")
RX_ANY_RUB = re.compile(r"₽")
RX_SUM_LINE = re.compile(r"^\d+(?:[ \u00a0]\d{3})*\s*₽$")


def norm_token(s: str) -> str:
    s = (s or "").strip().lower()
    return s.replace("–", "-").replace("—", "-")


def normalize_space(s: str) -> str:
    s = (s or "").replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def split_lines(page: fitz.Page) -> List[str]:
    txt = page.get_text("text") or ""
    lines = [normalize_space(x) for x in txt.splitlines()]
    return [x for x in lines if x]


def is_footer_or_noise(line: str) -> bool:
    low = norm_token(line)
    if low.startswith("страница:"):
        return True
    if low.startswith("ваш проект"):
        return True
    if "проект создан" in low:
        return True
    if "развертка стены" in low:
        return True
    if "стоимость проекта" in low:
        return True
    return False


def is_end_block(line: str) -> bool:
    low = norm_token(line)
    return (
        low.startswith("общий вес")
        or low.startswith("максимальный габарит")
        or low.startswith("адрес:")
        or low.startswith("телефон:")
        or low.startswith("email")
    )


def is_project_total_only(line: str) -> bool:
    return bool(re.fullmatch(r"\d+\s*₽", normalize_space(line)))


def is_header_token(line: str) -> bool:
    low = norm_token(line)
    return low in {"фото", "товар", "габариты", "вес", "цена за шт", "кол-во", "сумма"}


def clean_name(lines: List[str]) -> str:
    name = normalize_space(" ".join(lines))
    name = re.sub(r"^Фото\s*", "", name, flags=re.IGNORECASE).strip()
    name = re.sub(r"^Товар\s*", "", name, flags=re.IGNORECASE).strip()
    name = re.sub(r"Страница:.*$", "", name, flags=re.IGNORECASE).strip()
    return name


# -------------------------
# Parser A: line-chain
# -------------------------
def extract_item_after_dim(lines: List[str], dim_idx: int, name_buf: List[str]) -> Tuple[Optional[Tuple[str, int]], int]:
    name = clean_name(name_buf)
    name_buf.clear()

    end = min(len(lines), dim_idx + 14)
    i = dim_idx + 1

    if i < end and RX_WEIGHT_LINE.search(lines[i]):
        i += 1

    price_idx = None
    for j in range(i, end):
        if RX_PRICE_LINE.search(lines[j]):
            price_idx = j
            break
    if price_idx is None:
        return None, dim_idx + 1

    qty_idx = None
    for j in range(price_idx + 1, end):
        if RX_INT.fullmatch(lines[j]):
            qty_idx = j
            break
    if qty_idx is None:
        return None, dim_idx + 1

    qty = int(lines[qty_idx])
    if not (1 <= qty <= 500):
        return None, dim_idx + 1

    sum_idx = None
    for j in range(qty_idx + 1, end):
        if "₽" in lines[j]:
            sum_idx = j
            break

    if not name:
        back = []
        k = dim_idx - 1
        while k >= 0 and len(back) < 7:
            if is_footer_or_noise(lines[k]) or is_end_block(lines[k]) or is_project_total_only(lines[k]):
                break
            if RX_DIM_LINE.search(lines[k]) or RX_WEIGHT_LINE.search(lines[k]) or RX_PRICE_LINE.search(lines[k]) or RX_INT.fullmatch(lines[k]) or RX_SUM_LINE.fullmatch(lines[k]):
                break
            if is_header_token(lines[k]):
                break
            back.append(lines[k])
            k -= 1
        back.reverse()
        name = clean_name(back)

    if not name:
        return None, dim_idx + 1

    next_idx = (sum_idx + 1) if sum_idx is not None else (qty_idx + 1)
    return (name, qty), max(next_idx, dim_idx + 1)


def parse_by_lines(doc: fitz.Document) -> Tuple[List[Tuple[str, int]], Dict[str, int]]:
    ordered = OrderedDict()
    name_buf: List[str] = []

    stats = {"dim": 0, "price": 0, "qty_int": 0, "rub_any": 0, "pages_scanned": 0}

    for page in doc:
        stats["pages_scanned"] += 1
        lines = split_lines(page)
        if not lines:
            continue

        for ln in lines:
            if RX_DIM_LINE.search(ln):
                stats["dim"] += 1
            if RX_PRICE_LINE.search(ln):
                stats["price"] += 1
            if RX_INT.fullmatch(ln):
                stats["qty_int"] += 1
            if RX_ANY_RUB.search(ln):
                stats["rub_any"] += 1

        i = 0
        while i < len(lines):
            line = lines[i]

            if is_footer_or_noise(line) or is_header_token(line):
                i += 1
                continue

            if is_end_block(line) or is_project_total_only(line):
                name_buf.clear()
                i += 1
                continue

            if RX_DIM_LINE.search(line):
                item, next_i = extract_item_after_dim(lines, i, name_buf)
                if item is not None:
                    name, qty = item
                    low = name.lower()
                    if "стоимость проекта" not in low and "развертка стены" not in low and len(name) >= 3:
                        if name in ordered:
                            ordered[name] += qty
                        else:
                            ordered[name] = qty
                    i = next_i
                    continue

                i += 1
                continue

            name_buf.append(line)
            i += 1

    return list(ordered.items()), stats


# -------------------------
# Parser B: coordinate fallback
# -------------------------
def find_word_any(words: list, variants: List[str]) -> Optional[tuple]:
    vset = {norm_token(v) for v in variants}
    for w in words:
        if norm_token(w[4]) in vset:
            return w
    return None


def compute_layout_from_header(words: list) -> Optional[dict]:
    w_foto = find_word_any(words, ["Фото"])
    w_qty = find_word_any(words, ["Кол-во", "Кол–во", "Кол—во", "Колво", "Кол-ва"])
    w_sum = find_word_any(words, ["Сумма"])
    w_gab = find_word_any(words, ["Габариты"])

    if not (w_foto and w_qty and w_sum and w_gab):
        return None

    header_bottom = float(max(w_foto[3], w_qty[3], w_sum[3], w_gab[3]))
    name_left = float(w_foto[2]) + 4.0
    qty_left = float(w_qty[0]) - 4.0
    qty_right = float(w_sum[0]) - 4.0
    if qty_right <= qty_left:
        return None

    return {"header_bottom": header_bottom, "name_left": name_left, "qty_left": qty_left, "qty_right": qty_right}


def find_footer_cutoff_y(words: list, page_height: float) -> float:
    for w in words:
        if norm_token(w[4]).startswith("страница"):
            return float(w[1]) - 4.0
    return page_height - 55.0


def parse_by_coords(doc: fitz.Document) -> List[Tuple[str, int]]:
    ordered = OrderedDict()
    layout = None

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

        hb = layout["header_bottom"] if header_present else 0.0
        name_left = layout["name_left"]
        qty_left, qty_right = layout["qty_left"], layout["qty_right"]
        max_y = find_footer_cutoff_y(words, float(page.rect.height))

        qty_cells = []
        for w in words:
            t = (w[4] or "").strip()
            if not RX_INT.fullmatch(t):
                continue
            x0, y0 = float(w[0]), float(w[1])
            if y0 <= hb + 1 or y0 >= max_y:
                continue
            if not (qty_left <= x0 <= qty_right):
                continue
            q = int(t)
            if 1 <= q <= 500:
                qty_cells.append(w)

        qty_cells.sort(key=lambda w: (w[1], w[0]))
        y_centers = [((float(w[1]) + float(w[3])) / 2.0) for w in qty_cells]

        for i, qw in enumerate(qty_cells):
            y = y_centers[i]
            top = (y_centers[i - 1] + y) / 2.0 if i > 0 else max(hb + 2.0, y - 90.0)
            bottom = (y + y_centers[i + 1]) / 2.0 if i < len(y_centers) - 1 else min(y + 220.0, max_y)
            top = max(top, hb + 2.0)

            band = []
            for w in words:
                x0, y0, y1 = float(w[0]), float(w[1]), float(w[3])
                if y0 <= hb + 1 or y0 >= max_y:
                    continue
                yc = (y0 + y1) / 2.0
                if yc < top or yc > bottom:
                    continue
                band.append(w)

            has_mm = any(norm_token(w[4]) == "мм" for w in band)
            gab_x_min = None
            if has_mm:
                dim_xs = []
                for w in band:
                    s = (w[4] or "").strip()
                    if re.fullmatch(r"\d{2,}[xх]\d{2,}(?:[xх]\d{1,})?", s, flags=re.IGNORECASE):
                        dim_xs.append(float(w[0]))
                if dim_xs:
                    gab_x_min = min(dim_xs)

            right_limit = (gab_x_min - 3.0) if gab_x_min is not None else (qty_left - 8.0)

            name_words = []
            for w in band:
                x0 = float(w[0])
                if x0 < name_left or x0 >= right_limit:
                    continue
                if norm_token(w[4]) in {"мм", "кг", "кг."}:
                    continue
                if norm_token(w[4]).startswith("страница"):
                    continue
                name_words.append(w)

            name_words.sort(key=lambda w: (w[1], w[0]))
            name = normalize_space(" ".join(w[4] for w in name_words))
            if not name:
                continue

            qty = int(qw[4])
            if name in ordered:
                ordered[name] += qty
            else:
                ordered[name] = qty

    return list(ordered.items())


def make_csv_cp1251(rows: List[Tuple[str, int]]) -> bytes:
    out = io.StringIO()
    out.write("Товар;Кол-во\n")
    for name, qty in rows:
        safe = (name or "").replace('"', '""')
        if ";" in safe or "\n" in safe:
            safe = f'"{safe}"'
        out.write(f"{safe};{qty}\n")
    return out.getvalue().encode("cp1251", errors="replace")


HOME_HTML = "\n".join([
    "<!doctype html>",
    "<html lang='ru'>",
    "<head>",
    "  <meta charset='utf-8' />",
    "  <meta name='viewport' content='width=device-width, initial-scale=1' />",
    "  <title>PDF → CSV</title>",
    "  <style>",
    "    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; padding: 24px; background:#fafafa; }",
    "    .card { max-width: 860px; margin: 0 auto; background:#fff; border: 1px solid #e5e5e5; border-radius: 14px; padding: 22px; }",
    "    h1 { margin: 0 0 10px; font-size: 28px; }",
    "    p { margin: 8px 0; color:#333; }",
    "    .row { display:flex; gap: 12px; flex-wrap: wrap; align-items: center; margin-top: 14px; }",
    "    input[type=file] { padding: 10px; border: 1px solid #ddd; border-radius: 10px; background:#fff; }",
    "    button { padding: 10px 14px; border: 0; border-radius: 10px; cursor: pointer; font-weight: 600; }",
    "    button.primary { background: #111; color: #fff; }",
    "    button.primary:disabled { opacity: .55; cursor:not-allowed; }",
    "    .status { margin-top: 12px; font-size: 14px; white-space: pre-wrap; }",
    "    .ok { color: #0a7a2f; }",
    "    .err { color: #b00020; }",
    "    .hint { color:#666; font-size: 14px; }",
    "  </style>",
    "</head>",
    "<body>",
    "  <div class='card'>",
    "    <h1>PDF → CSV</h1>",
    "    <p>Загрузите PDF и получите CSV: <b>Товар</b> / <b>Кол-во</b> (порядок как в PDF).</p>",
    "    <p class='hint'>CSV: кодировка <b>Windows-1251</b>, разделитель <b>;</b>.</p>",
    "    <div class='row'>",
    "      <input id='pdf' type='file' accept='application/pdf,.pdf' />",
    "      <button id='btn' class='primary' disabled>Получить CSV</button>",
    "    </div>",
    "    <div id='status' class='status'></div>",
    "  </div>",
    "  <script>",
    "    const input = document.getElementById('pdf');",
    "    const btn = document.getElementById('btn');",
    "    const statusEl = document.getElementById('status');",
    "    function ok(msg){ statusEl.className='status ok'; statusEl.textContent=msg; }",
    "    function err(msg){ statusEl.className='status err'; statusEl.textContent=msg; }",
    "    function neutral(msg){ statusEl.className='status'; statusEl.textContent=msg||''; }",
    "    input.addEventListener('change', () => {",
    "      const f = input.files && input.files[0];",
    "      btn.disabled = !f;",
    "      neutral(f ? ('Выбран файл: ' + f.name) : '');",
    "    });",
    "    btn.addEventListener('click', async () => {",
    "      const f = input.files && input.files[0];",
    "      if (!f) return;",
    "      btn.disabled = true;",
    "      neutral('Обработка PDF…');",
    "      try {",
    "        const fd = new FormData();",
    "        fd.append('file', f);",
    "        const resp = await fetch('/extract', { method: 'POST', body: fd });",
    "        if (!resp.ok) {",
    "          let text = await resp.text();",
    "          try { const j = JSON.parse(text); if (j.detail) text = String(j.detail); } catch(e) {}",
    "          throw new Error('Ошибка ' + resp.status + ': ' + text);",
    "        }",
    "        const blob = await resp.blob();",
    "        const base = (f.name || 'items.pdf').replace(/\\.pdf$/i, '');",
    "        const filename = base + '.csv';",
    "        const url = URL.createObjectURL(blob);",
    "        const a = document.createElement('a');",
    "        a.href = url;",
    "        a.download = filename;",
    "        document.body.appendChild(a);",
    "        a.click();",
    "        a.remove();",
    "        URL.revokeObjectURL(url);",
    "        ok('Готово! CSV скачан: ' + filename);",
    "      } catch(e) {",
    "        err(String(e.message || e));",
    "      } finally {",
    "        btn.disabled = !(input.files && input.files[0]);",
    "      }",
    "    });",
    "  </script>",
    "</body>",
    "</html>",
])


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
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Не удалось открыть PDF: {e}")

    rows, stats = parse_by_lines(doc)

    if not rows:
        rows = parse_by_coords(doc)

    if not rows:
        raise HTTPException(
            status_code=422,
            detail=(
                "Не удалось найти таблицу товаров и количества в PDF. "
                f"(debug: pages={stats.get('pages_scanned')}, dim_lines={stats.get('dim')}, "
                f"price_lines={stats.get('price')}, int_lines={stats.get('qty_int')}, rub_lines={stats.get('rub_any')})"
            ),
        )

    csv_bytes = make_csv_cp1251(rows)
    return Response(
        content=csv_bytes,
        media_type="text/csv; charset=windows-1251",
        headers={"Content-Disposition": 'attachment; filename=\"items.csv\"'},
    )
