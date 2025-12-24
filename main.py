import io
import re
from collections import OrderedDict
from typing import List, Tuple, Optional

import fitz  # PyMuPDF
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import Response, HTMLResponse


app = FastAPI(title="PDF → CSV (товар / кол-во)", version="2.2.0")

# -------------------------
# Regex
# -------------------------
RX_DIM_LINE = re.compile(r"\b\d{2,}[xх]\d{2,}(?:[xх]\d{1,})?\b.*\bмм\b", re.IGNORECASE)
RX_WEIGHT_LINE = re.compile(r"\b\d+(?:[.,]\d+)?\s*кг\.?\b", re.IGNORECASE)
RX_PRICE_LINE = re.compile(r"\b\d+(?:[.,]\d+)\s*₽\b")  # 290.00 ₽ / 290,00 ₽
RX_INT = re.compile(r"^\d+$")
RX_SUM_LINE = re.compile(r"^\d+(?:[ \u00a0]\d{3})*\s*₽$")  # 8580 ₽ / 8 580 ₽

# -------------------------
# Utils
# -------------------------
def normalize_space(s: str) -> str:
    s = (s or "").replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def split_lines(page: fitz.Page) -> List[str]:
    txt = page.get_text("text") or ""
    lines = [normalize_space(x) for x in txt.splitlines()]
    return [x for x in lines if x]


def is_footer_or_noise(line: str) -> bool:
    low = line.lower()
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
    low = line.lower()
    return (
        low.startswith("общий вес")
        or low.startswith("максимальный габарит")
        or low.startswith("адрес:")
        or low.startswith("телефон:")
        or low.startswith("email")
    )


def is_project_total_only(line: str) -> bool:
    # строка вида "63376 ₽"
    return bool(re.fullmatch(r"\d+\s*₽", normalize_space(line)))


def clean_name(lines: List[str]) -> str:
    name = normalize_space(" ".join(lines))
    name = re.sub(r"^Фото\s*", "", name, flags=re.IGNORECASE).strip()
    name = re.sub(r"^Товар\s*", "", name, flags=re.IGNORECASE).strip()
    name = re.sub(r"Страница:.*$", "", name, flags=re.IGNORECASE).strip()
    return name


def extract_item_after_dim(lines: List[str], dim_idx: int, name_buf: List[str]) -> Tuple[Optional[Tuple[str, int]], int]:
    """
    dim_idx — индекс строки с габаритами (есть мм и размер).
    В окне следующих строк ищем:
      цена -> qty -> сумма.
    Возвращаем (item, next_idx).
    """
    name = clean_name(name_buf)
    name_buf.clear()

    # окно поиска (в твоих PDF хватает 8, но берём 12 с запасом)
    end = min(len(lines), dim_idx + 12)
    i = dim_idx + 1

    # пропускаем возможный вес
    if i < end and RX_WEIGHT_LINE.search(lines[i]):
        i += 1

    # ищем цену
    price_idx = None
    for j in range(i, end):
        if RX_PRICE_LINE.search(lines[j]):
            price_idx = j
            break
    if price_idx is None:
        return None, dim_idx + 1

    # ищем количество (отдельная строка с целым числом)
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

    # ищем сумму после qty (строка с ₽)
    sum_idx = None
    for j in range(qty_idx + 1, end):
        if "₽" in lines[j]:
            sum_idx = j
            break

    # Если имя пустое — пробуем взять строки прямо перед dim_idx (иногда буфер мог быть сброшен шумом)
    if not name:
        back = []
        k = dim_idx - 1
        while k >= 0 and len(back) < 6:
            if is_footer_or_noise(lines[k]) or is_totals_block(lines[k]) or is_project_total_only(lines[k]):
                break
            if RX_DIM_LINE.search(lines[k]) or RX_WEIGHT_LINE.search(lines[k]) or RX_PRICE_LINE.search(lines[k]) or RX_INT.fullmatch(lines[k]) or RX_SUM_LINE.fullmatch(lines[k]):
                break
            back.append(lines[k])
            k -= 1
        back.reverse()
        name = clean_name(back)

    if not name:
        return None, dim_idx + 1

    next_idx = (sum_idx + 1) if sum_idx is not None else (qty_idx + 1)
    return (name, qty), max(next_idx, dim_idx + 1)


def extract_items_from_pdf(pdf_bytes: bytes) -> List[Tuple[str, int]]:
    """
    Парсим весь документ без поиска шапки.
    Правило: товар = название (несколько строк) + строка с габаритами (мм) + цена + qty + сумма.
    Порядок сохраняем как в PDF. Повторы суммируем по первому появлению.
    """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")

    ordered = OrderedDict()  # name -> qty
    name_buf: List[str] = []

    for page in doc:
        lines = split_lines(page)
        if not lines:
            continue

        i = 0
        while i < len(lines):
            line = lines[i]

            if is_footer_or_noise(line):
                i += 1
                continue

            # если пошли итоги — сбрасываем буфер, но продолжаем скан (вдруг дальше ещё страницы с таблицей)
            if is_totals_block(line) or is_project_total_only(line):
                name_buf.clear()
                i += 1
                continue

            if RX_DIM_LINE.search(line):
                item, next_i = extract_item_after_dim(lines, i, name_buf)
                if item is not None:
                    name, qty = item
                    low = name.lower()
                    # страховка от мусора
                    if "стоимость проекта" not in low and "развертка стены" not in low and len(name) >= 3:
                        if name in ordered:
                            ordered[name] += qty
                        else:
                            ordered[name] = qty
                    i = next_i
                    continue

                # не получилось распарсить — просто идём дальше
                i += 1
                continue

            # обычная строка — часть названия (копим, включая переносы на следующую страницу)
            # но не копим очевидные заголовочные слова
            low = line.lower().replace("–", "-").replace("—", "-")
            if low in {"фото", "товар", "габариты", "вес", "цена за шт", "кол-во", "сумма"}:
                i += 1
                continue

            name_buf.append(line)
            i += 1

        # не очищаем name_buf на границе страниц — это как раз помогает "Обувнице" на разрыве

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


# -------------------------
# HTML (без triple quotes)
# -------------------------
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
