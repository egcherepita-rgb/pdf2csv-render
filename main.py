import io
import re
from collections import OrderedDict
from typing import List, Tuple

import fitz  # PyMuPDF
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import Response, HTMLResponse


app = FastAPI(title="PDF → CSV (товар / кол-во)", version="1.6.0")

# 950x48x9 / 25x2008x25 / 607x14x405 и т.п.
RX_DIM = re.compile(r"\b\d{2,}[xх]\d{2,}(?:[xх]\d{1,})?\b", re.IGNORECASE)

# Строка таблицы обычно содержит "мм" и минимум 2 раза "₽" (цена и сумма)
def is_item_detail_line(line: str) -> bool:
    low = line.lower()
    return ("₽" in line) and ("мм" in low) and (line.count("₽") >= 2)

def normalize_space(s: str) -> str:
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def split_lines(page: fitz.Page) -> List[str]:
    txt = page.get_text("text") or ""
    lines = [normalize_space(x) for x in txt.splitlines()]
    return [x for x in lines if x]

def extract_qty_from_detail_line(line: str) -> int:
    """
    Берём сегмент между последними двумя "₽":
      "... 290.00 ₽ 3 1080 ₽" -> сегмент " 3 1080 " -> qty=3
    """
    parts = line.split("₽")
    if len(parts) < 3:
        raise ValueError("no enough currency marks")
    qty_seg = parts[-2]
    ints = re.findall(r"\d+", qty_seg)
    if not ints:
        raise ValueError("qty not found")
    qty = int(ints[0])
    # защита от мусора типа "63376 ₽"
    if not (1 <= qty <= 500):
        raise ValueError("qty out of range")
    return qty

def extract_name_from_buffer(name_lines: List[str]) -> str:
    name = normalize_space(" ".join(name_lines))
    # На всякий случай режем мусорные заголовки/итоги
    low = name.lower()
    if low.startswith("фото товар"):
        return ""
    if "стоимость проекта" in low or "развертка стены" in low:
        return ""
    if low.startswith("страница:"):
        return ""
    return name

def extract_items_from_pdf(pdf_bytes: bytes) -> List[Tuple[str, int]]:
    """
    Возвращает [(товар, кол-во)] в порядке PDF.
    Повторы суммируются, но порядок сохраняется по первому появлению.
    """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")

    ordered = OrderedDict()  # name -> qty

    in_table = False
    name_buf: List[str] = []

    for page in doc:
        lines = split_lines(page)

        for line in lines:
            low = line.lower()

            # Старт таблицы (шапка есть на первой странице таблицы)
            if ("фото" in low and "товар" in low and "кол-во" in low and "сумма" in low):
                in_table = True
                name_buf = []
                continue

            if not in_table:
                continue

            # Конец таблицы: после товаров в твоём PDF идут итоги/адрес/контакты
            # Как только увидели "Общий вес" или "Адрес:" — таблица закончилась.
            if low.startswith("общий вес") or low.startswith("максимальный габарит") or low.startswith("адрес:") or low.startswith("телефон:") or low.startswith("email"):
                in_table = False
                name_buf = []
                continue

            # Игнорируем футер страниц
            if low.startswith("страница:"):
                continue

            # Иногда встречается строка только с суммой проекта типа "63376 ₽" — это уже не товар.
            if re.fullmatch(r"\d+\s*₽", line):
                # после этого обычно идут итоги — считаем таблицу законченной
                in_table = False
                name_buf = []
                continue

            # Если это строка с деталями (габариты/вес/цены/qty)
            if is_item_detail_line(line):
                # qty
                try:
                    qty = extract_qty_from_detail_line(line)
                except Exception:
                    # если не смогли вытащить qty — пропускаем (защита от мусора)
                    name_buf = []
                    continue

                # Если в этой же строке есть часть названия ДО габаритов (например: "GS-200 белая 25x2008x25 мм ...")
                # то добавляем префикс в буфер названия
                m = RX_DIM.search(line)
                if m:
                    prefix = normalize_space(line[:m.start()])
                    # prefix может быть "GS-200 белая" — это часть названия
                    if prefix and prefix.lower() not in {"фото", "товар"}:
                        # но не дублируем если уже есть
                        if not name_buf or (name_buf and prefix not in name_buf):
                            name_buf.append(prefix)

                name = extract_name_from_buffer(name_buf)
                name_buf = []

                if not name:
                    continue

                # аккуратная чистка: если вдруг прилипли хвосты после названия (редко)
                name = re.sub(r"\s+мм.*$", "", name, flags=re.IGNORECASE).strip()
                name = re.sub(r"\s+кг\..*$", "", name, flags=re.IGNORECASE).strip()

                if name in ordered:
                    ordered[name] += qty
                else:
                    ordered[name] = qty

                continue

            # Обычная строка — это часть названия (в т.ч. переносы: "Обувница выдвижная" + "ПРАКТИК Home GOV-60" + "белая")
            # Отсекаем явно мусорные строки
            if "стоимость проекта" in low or "развертка стены" in low:
                continue

            # Внутри таблицы копим название до строки с деталями
            name_buf.append(line)

    return list(ordered.items())

def make_csv_cp1251(rows: List[Tuple[str, int]]) -> bytes:
    out = io.StringIO()
    out.write("Товар;Кол-во\n")
    for name, qty in rows:
        safe = name.replace('"', '""')
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
