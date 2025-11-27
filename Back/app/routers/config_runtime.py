from fastapi import APIRouter
from pydantic import BaseModel, Field

from ..core.config import IndexConfig
from ..core.runtime_cfg import (
    get_runtime_cfg,
    update_ocr_cfg,
    update_index_cfg,
    RuntimeConfig,
)


router = APIRouter(prefix="/api/config", tags=["Config"])


class OCRUpdate(BaseModel):
    lang: str | None = Field(None, min_length=2)
    workers: int | None = Field(None, ge=1, le=128)


@router.get("", response_model=RuntimeConfig)
def get_config():
    """
    Полный runtime-конфиг (OCR + Index).
    """
    return get_runtime_cfg()


@router.patch("/ocr", response_model=RuntimeConfig)
def patch_ocr(body: OCRUpdate):
    """
    Частично обновить OCR-конфиг (язык/поточность).
    """
    cfg = update_ocr_cfg(lang=body.lang, workers=body.workers)
    return cfg


@router.put("/index", response_model=RuntimeConfig)
def put_index(body: IndexConfig):
    """
    Полностью задать IndexConfig (все пороги/веса/лимиты для C++-индекса k=9).
    Новый конфиг начнёт использоваться при следующем /build.
    """
    cfg = update_index_cfg(body.model_dump())
    return cfg
