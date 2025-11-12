from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from .routers import health, upload, search
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="Checker FastAPI", 
    default_response_class=ORJSONResponse
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],       # ← можно указать конкретные домены позже
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(upload.router)
app.include_router(search.router)
