from fastapi import FastAPI, UploadFile, File, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import db_manager
from utils import import_kaspi_pdf
import os
from fastapi.middleware.cors import CORSMiddleware
from db_manager import fetch_transactions

app = FastAPI(title="RichPapa Backend", version="0.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class SelectedTransaction(BaseModel):
    date: str
    total_amount: float
    category: Optional[str] = None
    extra_details: Optional[str] = None
    raw_transaction_ids: List[int]


@app.get("/")
def root():
    return {"isActive": "True"}


@app.post("/upload-pdf")
async def upload_pdf(file: UploadFile = File(...)):
    try:
        if not file.filename.endswith(".pdf"):
            raise HTTPException(status_code=400, detail="Можно загружать только PDF-файлы")

        temp_path = f"temp_{file.filename}"
        with open(temp_path, "wb") as f:
            f.write(await file.read())

        result = import_kaspi_pdf(temp_path)
        os.remove(temp_path)

        return {"status": "ok", **result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/add_selected")
def add_selected(selected: SelectedTransaction):
    try:
        result = db_manager.add_selected_transaction(selected.dict())
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/transactions/{year}/{month}")
def get_transactions(year: int, month: int):
    try:
        data = fetch_transactions(year, month)
        return {"count": len(data), "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/selected/{year}/{month}")
def get_selected(year: int, month: int):
    try:
        data = db_manager.fetch_selected_transactions(year, month)
        return {"count": len(data), "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
