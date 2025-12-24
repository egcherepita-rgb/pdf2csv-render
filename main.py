import io
import re
from collections import OrderedDict
from typing import List, Tuple, Optional, Dict

import fitz  # PyMuPDF
from fastapi import FastAPI, File, UploadFile, HTTPException, Query
from fastapi.responses import Response, HTMLResponse


app = FastAPI(title="PDF → CSV (товар / кол-во)", version="3.2.0")

# -------------------------
# Robust regex (важно для Render)
# -------------------------
# Пример: 650x48x9 мм  / 650x48x9мм  / 650х48х9мм
RX_DIM_MM = re.compile(r"\d{2,}[xх]\d{2,}(?:[xх]\d{1,})?\s*мм", re.IGNORECASE)
RX_WEIGHT = re.compile(r"\b\d+(?:[.,]\d+)?\s*кг\.?\b", re.IGNORECASE)
RX_PRICE = re.compile(r"\b\d+(?:[.,]\d+)\s*₽\b")  # 290.00 ₽ / 290,00 ₽
RX_INT = re.compile(r"^\d+$")
RX_ANY_RUB = re.compile(r"₽")
RX_SUM = re.compile(r"^\d+(?:[ \u00a0]\d{3})*\s*₽$")  # 290 ₽ / 1 080 ₽
RX_HEADER_JOIN = re.compile(r"фото.*товар.*габарит.*сумма", re.IGNORECASE)


def normalize_space(s: str) -> str:
    s = (s or "").replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def split_lines(page: fitz.Page) -> List[str]:
    txt = page.get_text("text") or ""
    lines = [normalize_space(x) for x in txt.splitlines()]
    return [x for x in lines if x]


def is_noise(line: str) -> bool:
    low = (line or "").strip().lower()
    if not low:
        return True
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


def is_totals_block(line: str) -> bool:
    low = (line or "").strip().lower()
    return (
        low.startswith("общий вес")
        or low.startswith("максимальный габарит заказа")
        or low.startswith("адрес:")
        or low.startswith("телефон:")
        or low.startswith("email")
    )


def is_project_total_only(line: str) -> bool:
    return bool(re.fullmatch(r"\d+\s*₽", normalize_space(line)))


def is_header_token(line: str) -> bool:
    low = normalize_space(line).lower().replace("–", "-").replace("—", "-")
    return low in {"фото", "товар", "габариты", "вес", "цена за шт", "кол-во", "сумма"}


def is_header_line(line: str) -> bool:
    # иногда шапка идёт одной строкой (в редких случаях)
    return bool(RX_HEADER_JOIN.search(line))


def clean_name(lines: List[str]) -> str:
    name = normalize_space(" ".join(lines))
    name = re.sub(r"^Фото\s*", "", name, flags=re.IGNORECASE).strip()
    name = re.sub(r"^Товар\s*", "", name, flags=re.IGNORECASE).strip()
    return name


def try_parse_item(lines: List[str], dim_idx: int, name_buf: List[str]) -> Tuple[Optional[Tuple[str, int]], int]:
    """
    dim_idx -> строка с габаритами (650x48x9мм)
    Дальше: (вес?) -> цена -> qty -> сумма(опционально)
    """
    name = clean_name(name_buf)
    name_buf.clear()

    end = min(len(lines), dim_idx + 14)
    i = dim_idx + 1

    # вес опционально
    if i < end and RX_WEIGHT.search(lines[i]):
        i += 1

    # цена
    price_idx = None
    for j in range(i, end):
        if RX_PRICE.search(lines[j]):
            price_idx = j
            break
    if price_idx is None:
        return None, dim_idx + 1

    # qty
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

    # сумма (для перехода)
    sum_idx = None
    for j in range(qty_idx + 1, end):
        if RX_ANY_RUB.search(lines[j]):
            sum_idx = j
            break

    # если имя пустое — пробуем взять строки перед dim_idx
    if not name:
        back = []
        k = dim_idx - 1
        while k >= 0 and len(back) < 7:
            ln = lines[k]
            if is_noise(ln) or is_totals_block(ln) or is_project_total_only(ln) or is_header_token(ln) or is_header_line(ln):
                break
            if RX_DIM_MM.search(ln) or RX_WEIGHT.search(ln) or RX_PRICE.search(ln) or RX_INT.fullmatch(ln) or RX_SUM.fullmatch(ln):
                break
            back.append(ln)
            k -= 1
        back.reverse()
        name = clean_name(back)

    if not name:
        return None, dim_idx + 1

    next_i = (sum_idx + 1) if sum_idx is not None else (qty_idx + 1)
    return (name, qty), max(next_i, dim_idx + 1)


def extract_items(pdf_bytes: bytes) -> Tuple[List[Tuple[str, int]], Dict[str, int]]:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")

    ordered = OrderedDict()
    name_buf: List[str] = []  # НЕ сбрасываем на стыке страниц
    in_totals = False

    stats = {"pages": 0, "dim_lines": 0, "price_lines": 0, "int_lines": 0, "rub_lines": 0}

    for page in doc:
        stats["pages"] += 1
        lines = split_lines(page)
        if not lines:
            continue

        # статистика для дебага
        for ln in lines:
            if RX_DIM_MM.search(ln):
                stats["dim_lines"] += 1
            if RX_PRICE.search(ln):
                stats["price_lines"] += 1
            if RX_INT.fullmatch(ln):
                stats["int_lines"] += 1
            if RX_ANY_RUB.search(ln):
                stats["rub_lines"] += 1

        i = 0
        while i < len(lines):
            line = lines[i]

            if is_noise(line) or is_header_token(line) or is_header_line(line):
                i += 1
                continue

            if is_project_total_only(line) or is_totals_block(line):
                in_totals = True
                name_buf.clear()
                i += 1
                continue

            if in_totals:
                i += 1
                continue

            # габариты
            if RX_DIM_MM.search(line):
                item, next_i = try_parse_item(lines, i, name_buf)
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

            # часть названия
            name_buf.append(line)
            i += 1

    return list(ordered.items()), stats


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
async def extract(
    file: UploadFile = File(...),
    debug: int = Query(0, description="1 = include debug stats in 422 error"),
):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Загрузите PDF файл (.pdf).")

    pdf_bytes = await file.read()

    try:
        rows, stats = extract_items(pdf_bytes)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Не удалось распарсить PDF: {e}")

    if not rows:
        msg = "Не удалось найти позиции по шаблону (габариты → цена → кол-во)."
        if debug:
            msg += f" debug={stats}"
        raise HTTPException(status_code=422, detail=msg)

    csv_bytes = make_csv_cp1251(rows)
    return Response(
        content=csv_bytes,
        media_type="text/csv; charset=windows-1251",
        headers={"Content-Disposition": 'attachment; filename="items.csv"'},
    )
