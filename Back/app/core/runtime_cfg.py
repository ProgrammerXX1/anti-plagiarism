# app/core/runtime_cfg.py
from pydantic import BaseModel, Field
from .config import (
    IndexConfig,
    DEFAULT_CFG_OBJ,
    OCR_LANG_DEFAULT,
    OCR_WORKERS_DEFAULT,
    ensure_index_cfg,
)


class OCRRuntimeCfg(BaseModel):
    lang: str = Field(default=OCR_LANG_DEFAULT, min_length=2)
    workers: int = Field(default=OCR_WORKERS_DEFAULT, ge=1, le=128)


class RuntimeConfig(BaseModel):
    """
    Живой конфиг сервиса, который можно крутить через API.
    """
    ocr: OCRRuntimeCfg = OCRRuntimeCfg()
    index: IndexConfig = DEFAULT_CFG_OBJ


_RUNTIME_CFG = RuntimeConfig()


def get_runtime_cfg() -> RuntimeConfig:
    """
    Текущий runtime-конфиг (без копирования, но он иммутабелен как модель).
    """
    return _RUNTIME_CFG


def set_runtime_cfg(new_cfg: RuntimeConfig) -> RuntimeConfig:
    """
    Полная замена runtime-конфига.
    Прогоняем index через ensure_index_cfg для валидации.
    """
    global _RUNTIME_CFG
    fixed_index = IndexConfig(**ensure_index_cfg(new_cfg.index.model_dump()))
    _RUNTIME_CFG = RuntimeConfig(ocr=new_cfg.ocr, index=fixed_index)
    return _RUNTIME_CFG


def update_ocr_cfg(lang: str | None = None, workers: int | None = None) -> RuntimeConfig:
    """
    Частичное обновление OCR-конфига.
    """
    global _RUNTIME_CFG
    data = _RUNTIME_CFG.model_dump()
    if lang is not None:
        data["ocr"]["lang"] = lang
    if workers is not None:
        data["ocr"]["workers"] = workers
    _RUNTIME_CFG = RuntimeConfig(**data)
    return _RUNTIME_CFG


def update_index_cfg(index_cfg_dict: dict) -> RuntimeConfig:
    """
    Обновить index-конфиг (целиком). Валидация через IndexConfig + ensure_index_cfg.
    """
    global _RUNTIME_CFG
    fixed = ensure_index_cfg(index_cfg_dict)
    idx = IndexConfig(**fixed)
    _RUNTIME_CFG = RuntimeConfig(ocr=_RUNTIME_CFG.ocr, index=idx)
    return _RUNTIME_CFG
