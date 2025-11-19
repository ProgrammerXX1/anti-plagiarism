# app/main.py
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from .routers import health, upload, search, config_runtime
from fastapi.middleware.cors import CORSMiddleware
from .core.logger import logger 

app = FastAPI(
    title="Plagiarism", 
    default_response_class=ORJSONResponse
)

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