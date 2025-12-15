from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class SourceItem(BaseModel):
    """
    Агрегированный источник совпадения
    """

    id: str
    source_id: str
    module_id: str
    name: str
    url: str
    author: Optional[str] = None
    index_date: datetime


class MatchSourceItem(BaseModel):
    """
    Конкретное совпадение в тексте
    """

    id: str
    source_id: str
    offset: int = Field(..., description="Смещение в тексте")
    limit: int = Field(..., description="Длина фрагмента")
    type: str = Field(..., description="Тип совпадения")


class ModuleItem(BaseModel):
    """
    Модуль анализа
    """

    id: str
    module_name: str


class IndexResultResponse(BaseModel):
    """
    RESULT-ответ для GET /result
    (результат проверки / анализа)
    """

    document_id: str = Field(..., description="ID документа во внешней системе")
    status: str = Field(..., description="pending | processing | completed | failed")
    processed_at: datetime = Field(..., description="Время формирования результата")

    plagiarism_percentage: float = Field(0.0)
    selfcite_percentage: float = Field(0.0)
    legal_percentage: float = Field(0.0)
    unknown_percentage: float = Field(0.0)

    sources: List[SourceItem] = Field(default_factory=list)
    matchsources: List[MatchSourceItem] = Field(default_factory=list)
    modules: List[ModuleItem] = Field(default_factory=list)
