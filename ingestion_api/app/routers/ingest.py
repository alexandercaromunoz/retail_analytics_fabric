from fastapi import APIRouter, UploadFile, File, HTTPException
from app.services.ingest_service import ingest_csv_file, list_ingestions, get_ingestion_by_id

router = APIRouter(prefix="/ingestion", tags=["ingestion"])

@router.post("/upload-file")
async def upload_file(file: UploadFile = File(...)):
    # 1) Validate file type (for example, only allow CSV files)
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are supported.")
    
    return await ingest_csv_file(file)

@router.get("/ingestions")
def get_ingestions(limit: int = 100,
                   offset: int = 0,
                   newest_first: bool = True,
                   only_warnings: bool = False,
                   filename_contains:str | None = None,
                   sha256:str |None = None,
    ):
    
    result = list_ingestions(
        limit=limit, 
        offset=offset,
        newest_first=newest_first,
        only_warnings=only_warnings,
        filename_contains=filename_contains, 
        sha256=sha256,
    )

    items = result["items"]
    total_matched = result["total_matched"]

    count= len(items)
    next_offset = offset + count
    has_more = next_offset < total_matched
    prev_offset = max(offset - limit, 0) if offset > 0 else None

    return {
        "items": items,
        "count": len(items),
        "total_matched": total_matched,
        "offset": offset,
        "limit": limit,
        "newest_first": newest_first,
        "has_more":has_more,
        "next_offset": next_offset if has_more else None,
        "prev_offset": prev_offset,
        "filters" :{
            "only_warnings": only_warnings,
            "filename_contains": filename_contains,
            "sha256": sha256,
        },
    } 

@router.get("/ingestions/{ingestion_id}")
def get_ingestion(ingestion_id: str):
    return get_ingestion_by_id(ingestion_id)