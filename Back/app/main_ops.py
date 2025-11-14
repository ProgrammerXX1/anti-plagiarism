# app/main_ops.py
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from fastapi.middleware.cors import CORSMiddleware

from .routers import upload, config_runtime  # upload = твой "Operations" router

app = FastAPI(
    title="Plagiarism Operations API",
    default_response_class=ORJSONResponse,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],       # ← можно указать конкретные домены позже
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# тяжёлые штуки: OCR, загрузка, билд индекса, corpus list/text
app.include_router(upload.router)
app.include_router(config_runtime.router)

# опционально health
try:
    from .routers import health
    app.include_router(health.router)
except ImportError:
    pass
