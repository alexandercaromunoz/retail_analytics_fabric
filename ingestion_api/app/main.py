from fastapi import FastAPI
from app.routers import health, ingest

app = FastAPI(title="Retail Analytics Ingestion API", version="0.1.0")

app.include_router(health.router)
app.include_router(ingest.router)

