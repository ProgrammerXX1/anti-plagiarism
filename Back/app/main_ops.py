# app/main_ops.py
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from fastapi.middleware.cors import CORSMiddleware
from .core.logger import logger 
from .routers import search, config_runtime, health, experiment
from .services.search_native import native_load_index
from .core.memlog import log_mem
from .routers import upload, config_runtime, health  # upload = твой "Operations" router

app = FastAPI(
    title="Plagiarism Operations API",
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
    allow_origins=["*"],       # ← можно указать конкретные домены позже
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
logger.info("=== main_ops started ===") 
# тяжёлые штуки: OCR, загрузка, билд индекса, corpus list/text


app.include_router(experiment.router)
# app.include_router(upload.router)
# app.include_router(search.router)
# app.include_router(config_runtime.router)
# app.include_router(health.router)


# опционально health
# try:
#     from .routers import health
#     app.include_router(health.router)
# except ImportError:
#     pass
