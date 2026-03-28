import hashlib
import io
import json
import csv
import os
from datetime import datetime, timezone
from uuid import uuid4  

from fastapi import UploadFile, HTTPException

BRONZE_DIR = "bronze"
INGEST_LOG = os.path.join(BRONZE_DIR, "ingestions.jsonl")

MAX_ROWS_TO_VALIDATE = 200  # para APIs: validación rápida (ajustable)
REJECT_ZERO_QUANTITY = True  # puedes dejarlo en False si tu negocio lo permite
MAX_MISMATCH_EXAMPLES = 5

REQUIRED_COLUMNS = {
    "InvoiceNo", 
    "StockCode", 
    "Description", 
    "Quantity", 
    "InvoiceDate", 
    "UnitPrice", 
    "CustomerID", 
    "Country"
}

def _extract_csv_columns(content: bytes) -> set[str]:
    """ 
    Lee SOlo el header del CS:
    Asume UTF-8; si el dataset es diferente se ajusta.
    """

    try:
        text= content.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="File encoding not supported. Please upload a UTF-8 encoded CSV.")
    
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise HTTPException(status_code=400, detail="CSV file is missing header row.")
    

    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="CSV must be UTF-8 encoded")

    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise HTTPException(status_code=400, detail="CSV has no header row")

    cols = {c.strip() for c in reader.fieldnames if c is not None}
    missing = sorted(REQUIRED_COLUMNS - cols)
    if missing:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "CSV schema validation failed",
                "missing_columns": missing,
                "required_columns": sorted(REQUIRED_COLUMNS),
            },
        )

    profile = _validate_sample_rows_and_profile(text)

    log_record = {
    # ...
    "columns": sorted(cols),
    "profile": profile,
    }

    # Normaliza los nombres de columna (quita espacios) 
    return {c.strip() for c in reader.fieldnames if c is not None}


def _already_ingested(sha256: str) -> bool:
    if not os.path.exists(INGEST_LOG):
        return False
    
    with open(INGEST_LOG, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            if record.get("sha256") == sha256:
                return True
    return False


async def ingest_csv_file(file: UploadFile) -> dict:
    os.makedirs(BRONZE_DIR, exist_ok=True)

    content= await file.read()

    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="CSV must be UTF-8 encoded")

    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise HTTPException(status_code=400, detail="CSV has no header row")

    if not content:
        raise HTTPException(status_code=400, detail="File is empty.")
        
    cols = {c.strip() for c in reader.fieldnames if c is not None}
    missing = sorted(REQUIRED_COLUMNS - cols)
    if missing:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "CSV schema validation failed",
                "missing_columns": missing,
                "required_columns": sorted(REQUIRED_COLUMNS),
            },
        )

    profile = _validate_sample_rows_and_profile(text)
    
    sha256 = hashlib.sha256(content).hexdigest()

    #Deduplication by hash (simple and effective for learning purposes)
    if _already_ingested(sha256):
        raise HTTPException(status_code=409, detail="File has already been ingested(duplicate)")
    
    ingestion_id = str(uuid4())
    stored_path = os.path.join(BRONZE_DIR, f"{ingestion_id}_{file.filename}")

    with open(stored_path, "wb") as out:
        out.write(content)
    
    log_record = {
        "ingestion_id": ingestion_id,
        "filename": file.filename,
        "bytes": len(content),
        "sha256": sha256,
        "stored_path": stored_path,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "columns": sorted(cols),
        "profile": profile
    }

    with open(INGEST_LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps(log_record) + "\n")

    return log_record


def _is_return_by_invoice(invoice_no: str) -> bool:
    return str(invoice_no).strip().upper().startswith("C")

def _validate_sample_rows_and_profile(text: str) -> dict:

    reader = csv.DictReader(io.StringIO(text))

    checked = 0
    return_by_qty = 0
    return_by_invoice = 0
    mismatches = 0
    mismatch_examples: list[dict] = []

    for row in reader:
        checked += 1

        invoice_no = str(row.get("InvoiceNo", "")).strip()

        # quantity
        try:
            q = int(str(row["Quantity"]).strip())
        except Exception:
            raise HTTPException(status_code=400, detail=f"Invalid Quantity at row {checked}")

        if REJECT_ZERO_QUANTITY and q == 0:
            raise HTTPException(status_code=400, detail=f"Quantity cannot be 0 (row {checked})")

        
        is_return_qty = q < 0
        is_return_inv = _is_return_by_invoice(invoice_no)

        if is_return_qty:
            return_by_qty += 1
        if is_return_inv:
            return_by_invoice += 1

        if is_return_qty != is_return_inv:
            mismatches += 1
            if len(mismatch_examples) < MAX_MISMATCH_EXAMPLES:
                mismatch_examples.append(
                    {
                        "row": checked,
                        "InvoiceNo": invoice_no,
                        "Quantity": q,
                        "return_by_quantity": is_return_qty,
                        "return_by_invoice_prefix": is_return_inv,
                    }
                )


        # unit price
        try:
            price = float(str(row["UnitPrice"]).strip())
        except Exception:
            raise HTTPException(status_code=400, detail=f"Invalid UnitPrice at row {checked}")

        if price < 0:
            raise HTTPException(status_code=400, detail=f"UnitPrice cannot be negative (row {checked})")

        if checked >= MAX_ROWS_TO_VALIDATE:
            break

    return {
        "rows_validated": checked,
        "return_rows_by_quantity": return_by_qty,
        "return_rows_by_invoice_prefix": return_by_invoice,
        "mismatch_rows": mismatches,
        "mismatch_examples": mismatch_examples,
        "warnings": (
            []
            if mismatches == 0
            else ["Returns mismatch detected between Quantity sign and InvoiceNo prefix 'C'"]
        ),        
    }

    log_record = {
    # ...
    "columns": sorted(cols),
    "profile": profile,
    }

def list_ingestions(
        limit: int = 100,
        offset: int = 0,
        only_warnings: bool = False,
        filename_contains: str | None = None,
        sha256: str | None = None, 
        newest_first: bool = True,         
    ) -> dict:
    if limit < 1 or limit > 1000:
        raise HTTPException(status_code=400, detail="Limit must be between 1 and 1000")
    if offset < 0 or offset > 1_000_000:
        raise HTTPException(status_code=400, detail="Offset must be between 0 and 1000000")
        
    if sha256 is not None and not sha256.strip():
        raise HTTPException(status_code=400, detail="SHA256 filter cannot be empty")
    
    if filename_contains is not None and not filename_contains.strip():
        raise HTTPException(status_code=400, detail="Filename filter cannot be empty")

    if not os.path.exists(INGEST_LOG):
        return {"items": [], "total_matched": 0}
    
    matched: list[dict] = []

    with open(INGEST_LOG, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            rec = json.loads(line)

            #filtros
            if sha256 is not None and rec.get("sha256") != sha256:
                continue

            if filename_contains is not None:
                fn = str(rec.get("filename", ""))
                if filename_contains.lower() not in fn.lower():
                    continue

            if only_warnings:
                prof = rec.get("profile",{}) if isinstance(rec.get("profile"), dict) else {}
                mismatch_rows = prof.get("mismatch_rows",0)
                warnings = prof.get("warnings",[]) 
                has_warnings =(isinstance(mismatch_rows,int) and len(mismatch_rows) > 0) or (
                    isinstance(warnings,list) and len(warnings)> 0)
                if not has_warnings:
                    continue
            matched.append(rec)

    #orden
    if newest_first:
        matched.reverse()
    
    total_matched = len(matched)
    items = matched[offset:offset+limit]

    return {"items":items, "total_matched": total_matched}

def get_ingestion_by_id(ingestion_id:str) -> dict:
    ingestion_id= ingestion_id.strip()
    if not ingestion_id:
        raise HTTPException(status_code=400, detail="Ingestion ID cannot be empty")
    
    if not os.path.exists(INGEST_LOG):
        raise HTTPException(status_code=404, detail="Ingestion not found")
    
    with open(INGEST_LOG, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            if rec.get("ingestion_id") == ingestion_id:
                return rec
            
    raise HTTPException(status_code=404, detail="Ingestion not found")