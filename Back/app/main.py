# app/main.py
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from .routers import health, upload, search, config_runtime
from fastapi.middleware.cors import CORSMiddleware
from .core.logger import logger 
from app.services.search.index_search import get_index_cached


app = FastAPI(
    title="Plagiarism", 
    default_response_class=ORJSONResponse
)

@app.on_event("startup")
def warmup_index():
    try:
        idx = get_index_cached()
        logger.info(f"[warmup] index loaded at startup, docs={len(idx.get('docs_meta') or {})}")
    except FileNotFoundError:
        logger.warning("[warmup] index.json not found, build later")
    except Exception as e:
        logger.error(f"[warmup] failed to load index at startup: {e}")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],       # ← можно указать конкретные домены позже
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
logger.info("=== main_ops started ===")  
app.include_router(health.router)
app.include_router(upload.router)
app.include_router(search.router)
app.include_router(config_runtime.router)
