import io
import re
from collections import OrderedDict
from typing import List, Tuple, Optional

import fitz  # PyMuPDF
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import Response, HTMLResponse


app = FastAPI(title="PDF → CSV (товар / кол-во)", version="2.1.0")

# --- regex / helpers ---
RX_DIM_LINE = re.compile(r"\b\d{2,}[xх]\d{2,}(?:[xх]\d{1,})?\b.*\bмм\b", re.IGNORECASE)
RX_WEIGHT = re.compile(r"\b\d+(?:[.,]\d+)?\s*кг\.?\b", re.IGNORECASE)
RX_PRICE = re.compile(r"\b\d+(?:[.,]\d+)\s*₽\b")  # 290.00 ₽
RX_INT = re.compile(r"^\d+$")
RX_SUM = re.compile(r"^\d+(?:[ \u00a0]\d{3})*\s*₽$")  # 8 580 ₽ или 8580 ₽


def normalize_space(s: str) -> str:
    s = (s or "").replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def split_lines(page: fitz.Page) -> List[str]:
    txt = page.get_text("text") or ""
    lines = [normalize_space(x) for x in txt.splitlines()]
    return [x for x in lines if x]


def detect_table_start(lines: List[str]) -> bool:
    # В твоих PDF шапка идёт отдельными строками: Фото / Товар / ...
    head = " ".join(lines[:30]).lower().replace("–", "-").replace("—", "-")
    return ("фото" in head) and ("товар" in head) and ("габариты" in head) and (
        ("кол-во" in head) or ("кол-ва" in head) or ("колво" in head) or ("кол во" in head)
    )


def is_end_of_table(line: str) -> bool:
    low = line.lower()
    return (
        low.startswith("общий вес")
        or low.startswith("максимальный габарит")
        or low.startswith("адрес:")
        or low.startswith("телефон:")
        or low.startswith("email")
    )


def is_footer(line: str) -> bool:
    return line.lower().startswith("страница:")


def is_header_token(line: str) -> bool:
    # строки шапки таблицы, которые не являются товарами
    low = line.lower().replace("–", "-").replace("—", "-")
    return low in {
        "фото",
        "товар",
        "габариты",
        "вес",
        "цена за шт",
        "кол-во",
        "сумма",
    }


def clean_name(name_lines: List[str]) -> str:
    # склеиваем, убираем мусор
    name = normalize_space(" ".join(name_lines))
    name = re.sub(r"^Фото\s*", "", name, flags=re.IGNORECASE).strip()
    name = re.sub(r"^Товар\s*", "", name, flags=re.IGNORECASE).strip()
    name = re.sub(r"Страница:.*$", "", name, flags=re.IGNORECASE).strip()
    return name


def extract_item_from_lines(lines: List[str], start_idx: int, name_buf: List[str]) -> Tuple[Optional[Tuple[str, int]], int]:
    """
    start_idx указывает на строку с габаритами (в ней есть 'мм' и размер).
    Возвращает (item, next_idx). item = (name, qty) или None если не получилось.
    """
    # имя товара — всё, что накопили ДО строки с габаритами
    name = clean_name(name_buf)
    name_buf.clear()

    # если имя пустое — пытаемся собрать имя из строк сразу перед габаритами (иногда буфер мог быть пуст)
    if not name:
        # максимум 3 строки выше (но это редкость)
        pass

    # После строки с габаритами могут быть:
    # - вес отдельной строкой (0.4 кг.)
    # - цена (290.00 ₽)
    # - кол-во (1)
    # - сумма (290 ₽)
    i = start_idx + 1
    max_i = min(len(lines), start_idx + 8)

    # пропускаем вес (может быть сразу в строке габаритов или отдельной строкой)
    if i < len(lines) and RX_WEIGHT.search(lines[i]):
        i += 1

    # ищем цену в окне
    price_idx = None
    for j in range(i, max_i):
        if RX_PRICE.search(lines[j]):
            price_idx = j
            break
    if price_idx is None:
        return None, start_idx + 1

    # ищем количество: первое целое число после цены
    qty_idx = None
    for j in range(price_idx + 1, max_i):
        if RX_INT.fullmatch(lines[j]):
            qty_idx = j
            break
    if qty_idx is None:
        return None, start_idx + 1

    qty = int(lines[qty_idx])
    if not (1 <= qty <= 500):
        return None, start_idx + 1

    # ищем сумму после qty (не обязательно, но как контроль)
    sum_idx = None
    for j in range(qty_idx + 1, max_i):
        if "₽" in lines[j]:
            sum_idx = j
            break

    # Если имя всё ещё пустое — значит буфер не собрался: пробуем взять 1–4 строки перед габаритами
    if not name:
        # берём строки назад до предыдущего “блока”
        back = []
        k = start_idx - 1
        while k >= 0 and len(back) < 5:
            if is_header_token(lines[k]) or is_footer(lines[k]) or is_end_of_table(lines[k]):
                break
            # останавливаемся если встречаем строку, похожую на цену/сумму/кол-во/вес/габариты
            if RX_DIM_LINE.search(lines[k]) or RX_WEIGHT.search(lines[k]) or RX_PRICE.search(lines[k]) or RX_INT.fullmatch(lines[k]) or RX_SUM.fullmatch(lines[k]):
                break
            back.append(lines[k])
            k -= 1
        back.reverse()
        name = clean_name(back)

    if not name:
        return None, start_idx + 1

    # next idx — после суммы (если нашли), иначе после qty
    next_idx = (sum_idx + 1) if sum_idx is not None else (qty_idx + 1)
    return (name, qty), next_idx


def extract_items_from_pdf(pdf_bytes: bytes) -> List[Tuple[str, int]]:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")

    ordered = OrderedDict()  # name -> qty
    in_table = False
    name_buf: List[str] = []

    for page in doc:
        lines = split_lines(page)
        if not lines:
            continue

        if detect_table_start(lines):
            in_table = True
            name_buf.clear()

        if not in_table:
            continue

        i = 0
        while i < len(lines):
            line = lines[i]

            if is_footer(line):
                i += 1
                continue

            if is_end_of_table(line):
                in_table = False
                name_buf.clear()
                break

            # пропускаем шапку таблицы
            if is_header_token(line):
                i += 1
                continue

            # строки "Ваш проект / Стоимость проекта" (они бывают на первых страницах)
            low = line.lower()
            if low.startswith("ваш проект") or "стоимость проекта" in low or "проект создан" in low or "развертка стены" in low:
                i += 1
                continue

            # если встретили строку габаритов — фиксируем товар
            if RX_DIM_LINE.search(line):
                item, next_i = extract_item_from_lines(lines, i, name_buf)
                if item is not None:
                    name, qty = item
                    if name in ordered:
                        ordered[name] += qty
                    else:
                        ordered[name] = qty
                    i = max(next_i, i + 1)
                    continue
                # если не смогли распарсить — просто идём дальше, но буфер очищен в extract_item
                i += 1
                continue

            # иначе это часть названия (в т.ч. переносы: "Обувница выдвижная" / "ПРАКТИК Home GOV-60" / "белая")
            name_buf.append(line)
            i += 1

        # если страница закончилась, а имя копится — оставляем буфер, чтобы склеить с продолжением на след. странице
        # (это как раз лечит переносы типа "Обувница..." на разрыве)

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
