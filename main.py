from fastapi import FastAPI, File, Form, UploadFile, Request
from fastapi.responses import HTMLResponse, Response
from fastapi.templating import Jinja2Templates

from parser import build_csv_from_pdf

app = FastAPI()
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/convert")
async def convert(
    file: UploadFile = File(...),
    delimiter: str = Form(";"),
    use_catalog: str = Form("1"),
):
    pdf_bytes = await file.read()

    csv_text = build_csv_from_pdf(
        pdf_bytes=pdf_bytes,
        items_xlsx_path="data/items.xlsx",
        delimiter=delimiter if delimiter != "\\t" else "\t",
        use_catalog=(use_catalog == "1"),
    )

    filename = (file.filename or "result.pdf").rsplit(".", 1)[0] + ".csv"
    return Response(
        content=csv_text.encode("utf-8-sig"),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
