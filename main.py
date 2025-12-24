import io
import re
from collections import OrderedDict
from typing import List, Tuple, Optional

import fitz  # PyMuPDF
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import Response, HTMLResponse


app = FastAPI(title="PDF → CSV (товар / кол-во)", version="3.0.0")

# -------------------------
# Regex / helpers
# -------------------------
RX_DIM = re.compile(r"\b\d{2,}[xх]\d{2,}(?:[xх]\d{1,})?\b", re.IGNORECASE)  # 650x48x9
RX_MM = re.compile(r"\bмм\b", re.IGNORECASE)
RX_RUB = re.compile(r"₽")

def normalize_space(s: str) -> str:
    s = (s or "").replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()

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
    if "стоимость проекта" in low:
        return True
    if "развертка стены" in low:
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
        or low.startswith("praktik-home")
    )

def is_project_total_only(line: str) -> bool:
    # строка вида "63376 ₽"
    return bool(re.fullmatch(r"\d+\s*₽", normalize_space(line)))

def is_header_line(line: str) -> bool:
    low = normalize_space(line).lower().replace("–", "-").replace("—", "-")
    # шапка таблицы в 1.pdf: "Фото Товар Габариты Вес Цена за шт Кол-во Сумма"
    return ("фото" in low and "товар" in low and "габариты" in low and "сумма" in low)

def looks_like_detail_line(line: str) -> bool:
    # Строка деталей: обязательно содержит "мм" + "₽" и размер 650x48x9
    if not (RX_MM.search(line) and RX_RUB.search(line) and RX_DIM.search(line)):
        return False
    # В норме минимум 2 знака ₽ (цена и сумма)
    return line.count("₽") >= 2

def extract_qty_from_detail_line(line: str) -> Optional[int]:
    """
    Надёжно для формата:
      "... 360.00 ₽ 3 1080 ₽"
    Берём сегмент между последними двумя "₽" -> там сначала qty, потом сумма.
    """
    parts = line.split("₽")
    if len(parts) < 3:
        return None
    mid = parts[-2]  # между предпоследним и последним ₽
    nums = re.findall(r"\d+", mid)
    if not nums:
        return None
    qty = int(nums[0])
    if 1 <= qty <= 500:
        return qty
    return None

def prefix_name_from_detail_line(line: str) -> str:
    """
    Иногда часть названия стоит в той же строке, что и габариты:
      "Стойка ... GS-200 белая 25x2008x25 мм ..."
    Тогда берём текст ДО первого размера.
    """
    m = RX_DIM.search(line)
    if not m:
        return ""
    return normalize_space(line[:m.start()])

def clean_name(name: str) -> str:
    name = normalize_space(name)
    # убрать случайные слова шапки
    name = re.sub(r"^Фото\s*", "", name, flags=re.IGNORECASE).strip()
    name = re.sub(r"^Товар\s*", "", name, flags=re.IGNORECASE).strip()
    return name

def split_lines_from_page(page: fitz.Page) -> List[str]:
    txt = page.get_text("text") or ""
    lines = [normalize_space(x) for x in txt.splitlines()]
    return [x for x in lines if x]


# -------------------------
# Main parser (тот самый "рабочий", + фикс стыка страниц)
# -------------------------
def extract_items(pdf_bytes: bytes) -> List[Tuple[str, int]]:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")

    ordered = OrderedDict()  # name -> qty (порядок первого появления сохраняется)
    name_buf: List[str] = []  # копим строки названия, в т.ч. между страницами

    in_totals = False

    for page in doc:
        lines = split_lines_from_page(page)
        if not lines:
            continue

        for line in lines:
            if is_noise(line):
                continue

            # остановка по блоку итогов
            if is_project_total_only(line) or is_totals_block(line):
                in_totals = True
                name_buf.clear()
                continue
            if in_totals:
                continue

            # пропуск шапки
            if is_header_line(line):
                name_buf.clear()
                continue

            # ключ: строка деталей (мм + ₽ + размер)
            if looks_like_detail_line(line):
                qty = extract_qty_from_detail_line(line)
                if qty is None:
                    # если вдруг не распознали — сбросим буфер и пойдём дальше
                    name_buf.clear()
                    continue

                inline_prefix = prefix_name_from_detail_line(line)

                # Название = накопленное + inline-префикс (если есть)
                parts = []
                if name_buf:
                    parts.append(" ".join(name_buf))
                if inline_prefix:
                    # если inline_prefix уже входит в name_buf, не дублируем
                    if not parts or inline_prefix not in parts[-1]:
                        parts.append(inline_prefix)

                name = clean_name(" ".join(parts))
                name_buf.clear()

                if not name:
                    continue

                # страховка от мусора
                low = name.lower()
                if "стоимость проекта" in low or "развертка стены" in low:
                    continue

                if name in ordered:
                    ordered[name] += qty
                else:
                    ordered[name] = qty

                continue

            # иначе — часть названия (переносы, в т.ч. на стыке страниц)
            # Важно: НЕ сбрасываем name_buf на конце страницы — это и фиксит стык страниц
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
# Simple UI
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
        rows = extract_items(pdf_bytes)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Не удалось распарсить PDF: {e}")

    if not rows:
        raise HTTPException(status_code=422, detail="Не удалось найти позиции (мм + ₽) в PDF.")

    csv_bytes = make_csv_cp1251(rows)
    return Response(
        content=csv_bytes,
        media_type="text/csv; charset=windows-1251",
        headers={"Content-Disposition": 'attachment; filename="items.csv"'},
    )
