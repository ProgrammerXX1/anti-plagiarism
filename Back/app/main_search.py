# app/main_search.py
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from fastapi.middleware.cors import CORSMiddleware

from .routers import search, config_runtime  # health можно тоже воткнуть, если есть

app = FastAPI(
    title="Plagiarism Search API",
    default_response_class=ORJSONResponse,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],       # ← можно указать конкретные домены позже
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# только лёгкие роуты
app.include_router(search.router)
app.include_router(config_runtime.router)

# опционально health
try:
    from .routers import health
    app.include_router(health.router)
except ImportError:
    pass
