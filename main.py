from fastapi import FastAPI, UploadFile, File
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
import io
from parser import build_csv_from_pdf

app = FastAPI()
templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/convert")
async def convert(pdf: UploadFile = File(...)):
    pdf_bytes = await pdf.read()
    csv_text = build_csv_from_pdf(
        pdf_bytes=pdf_bytes,
        items_xlsx_path="data/items.xlsx",
        delimiter=";"
    )
    # Для корректного открытия в Excel на Windows (русские буквы)
    csv_bytes = csv_text.encode("utf-8-sig")
    return StreamingResponse(
        io.BytesIO(csv_bytes),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="result.csv"'}
    )
