import io
import re
from collections import OrderedDict
from typing import List, Tuple

import fitz  # PyMuPDF
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import Response, HTMLResponse


app = FastAPI(title="PDF → CSV (товар / кол-во)", version="2.0.0")

# --- regex ---
RX_DIM = re.compile(r"\b\d{2,}[xх]\d{2,}(?:[xх]\d{1,})?\b", re.IGNORECASE)  # 950x48x9 / 25x2008x25
RX_KG = re.compile(r"\b\d+(?:[.,]\d+)?\s*кг\.?\b", re.IGNORECASE)
RX_RUB = re.compile(r"₽")
RX_MMRU = re.compile(r"\bмм\b", re.IGNORECASE)

# В строке деталей всегда есть "мм" и минимум 2 знака ₽
def is_detail_line(s: str) -> bool:
    return bool(RX_MMRU.search(s)) and (s.count("₽") >= 2)

def normalize_space(s: str) -> str:
    s = (s or "").replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def split_lines(page: fitz.Page) -> List[str]:
    txt = page.get_text("text") or ""
    lines = [normalize_space(x) for x in txt.splitlines()]
    return [x for x in lines if x]

def detect_table_start(lines: List[str]) -> bool:
    """
    В твоих PDF заголовок бывает либо одной строкой:
      "Фото Товар Габариты Вес Цена за шт Кол-во Сумма"
    либо может быть разбит.
    Поэтому ищем наличие ключевых слов в первых ~15 строках страницы.
    """
    head = " ".join(lines[:20]).lower()
    head = head.replace("–", "-").replace("—", "-")
    # достаточно "фото", "товар", "кол-во/кол-ва/колво" и "сумма"
    return ("фото" in head) and ("товар" in head) and ("сумма" in head) and (
        ("кол-во" in head) or ("кол-ва" in head) or ("колво" in head) or ("кол во" in head)
    )

def is_footer_or_noise(line: str) -> bool:
    low = line.lower()
    if low.startswith("страница:"):
        return True
    if "развертка стены" in low:
        return True
    if "ваш проект" in low or "проект создан" in low:
        return True
    return False

def is_totals_block_start(line: str) -> bool:
    low = line.lower()
    # после таблицы в твоих PDF идёт это
    return (
        low.startswith("общий вес")
        or low.startswith("максимальный габарит")
        or low.startswith("адрес:")
        or low.startswith("телефон:")
        or low.startswith("email")
        or low.startswith("praktik-home")
    )

def is_project_total_only(line: str) -> bool:
    # строка вида "63376 ₽" (без других символов)
    s = normalize_space(line)
    return bool(re.fullmatch(r"\d+\s*₽", s))

def extract_qty_from_detail_line(line: str) -> int:
    """
    Самый устойчивый способ для твоего формата:
    берём сегмент между последними двумя "₽":
      "... 360.00 ₽ 3 1080 ₽" -> между ₽ и ₽: " 3 1080 " -> qty=3
    """
    parts = line.split("₽")
    if len(parts) < 3:
        raise ValueError("not enough ₽")
    mid = parts[-2]
    nums = re.findall(r"\d+", mid)
    if not nums:
        raise ValueError("qty not found")
    qty = int(nums[0])
    if not (1 <= qty <= 500):
        raise ValueError("qty out of range")
    return qty

def clean_name(name: str) -> str:
    name = normalize_space(name)
    # не тащим служебное
    name = re.sub(r"^Фото\s*", "", name, flags=re.IGNORECASE).strip()
    name = re.sub(r"^Товар\s*", "", name, flags=re.IGNORECASE).strip()
    name = re.sub(r"Страница:.*$", "", name, flags=re.IGNORECASE).strip()
    return name

def maybe_add_inline_prefix_from_detail(line: str, name_buf: List[str]) -> None:
    """
    Иногда часть названия идёт в строке деталей ДО габаритов, например:
    "Стойка ПРАКТИК Home GS-200 белая 25x2008x25 мм ..."

    Тогда добавляем префикс до DIM в буфер названия.
    """
    m = RX_DIM.search(line)
    if not m:
        return
    prefix = clean_name(line[:m.start()])
    if not prefix:
        return
    # не дублируем
    if not name_buf or (name_buf and prefix not in name_buf):
        name_buf.append(prefix)

def extract_items_from_pdf(pdf_bytes: bytes) -> List[Tuple[str, int]]:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")

    ordered = OrderedDict()  # name -> qty (сохранение порядка)
    in_table = False
    name_buf: List[str] = []

    for page in doc:
        lines = split_lines(page)
        if not lines:
            continue

        # детект шапки (в т.ч. когда она на этой странице)
        if detect_table_start(lines):
            in_table = True
            name_buf = []
            # не продолжаем — просто даём обработке идти дальше по строкам этой же страницы

        if not in_table:
            continue

        for line in lines:
            if not line:
                continue

            if is_footer_or_noise(line):
                continue

            if is_totals_block_start(line):
                in_table = False
                name_buf = []
                break

            if is_project_total_only(line):
                # после этого обычно сразу итоги; заканчиваем таблицу
                in_table = False
                name_buf = []
                break

            if is_detail_line(line):
                # 1) qty
                try:
                    qty = extract_qty_from_detail_line(line)
                except Exception:
                    name_buf = []
                    continue

                # 2) добираем часть названия, если она внутри этой же строки до DIM
                maybe_add_inline_prefix_from_detail(line, name_buf)

                # 3) итоговое имя
                name = clean_name(" ".join(name_buf))
                name_buf = []

                # отсекаем мусор
                low = name.lower()
                if not name:
                    continue
                if "стоимость проекта" in low:
                    continue
                if "развертка стены" in low:
                    continue

                # 4) пишем в OrderedDict (суммируем повторы)
                if name in ordered:
                    ordered[name] += qty
                else:
                    ordered[name] = qty

                continue

            # обычная строка: часть наименования (переносы)
            # не копим строки с заголовками таблицы
            low = line.lower().replace("–", "-").replace("—", "-")
            if ("фото" in low and "товар" in low and "сумма" in low) or ("кол-во" in low and "сумма" in low):
                continue

            name_buf.append(line)

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


# -------------------------
# routes
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
