# app/main_search.py
from fastapi import FastAPI
from fastapi.logger import logger
from fastapi.responses import ORJSONResponse
from fastapi.middleware.cors import CORSMiddleware

from .routers import search, config_runtime, health
from .services.search_native import native_load_index
from .core.memlog import log_mem

app = FastAPI(
    title="Plagiarism Search API",
    default_response_class=ORJSONResponse,
)

@app.on_event("startup")
def warmup_index():
    log_mem("startup: before native_load_index")
    try:
        native_load_index()
        logger.info("[warmup] native C++ index loaded at startup")
    except FileNotFoundError:
        logger.warning("[warmup] native index not found, build later")
    except Exception as e:
        logger.error("[warmup] failed to load native index at startup: %s", e)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(search.router)
app.include_router(config_runtime.router)
app.include_router(health.router)
