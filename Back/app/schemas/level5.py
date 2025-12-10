# app/schemas/level5.py
from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class Level5ReindexRequest(BaseModel):
    """
    Параметры пересборки монолитного индекса (уровень 5).
    """
    incremental: bool = Field(
        default=True,
        description="Инкрементальная сборка по существующему index.json"
    )
    max_new_docs_per_build: Optional[int] = Field(
        default=None,
        description="Лимит новых документов за один прогон (0/None = без лимита)"
    )
    rebuild_corpus: bool = True 
    rebuild_bm25: bool = Field(
        default=True,
        description="Пересобирать BM25-индекс"
    )
    rebuild_main_index: bool = Field(
        default=True,
        description="Пересобирать MinHash/inverted index.json"
    )
    export_native: bool = Field(
        default=True,
        description="Перегенерировать index_native.bin + docids + meta для C++"
    )


class Level5ReindexResponse(BaseModel):
    ok: bool
    message: str
    stats: Dict[str, Any] = Field(default_factory=dict)


class Level5DocDetails(BaseModel):
    doc_id: str
    title: Optional[str] = None
    author: Optional[str] = None
    max_score: float
    originality_pct: float
    decision: str
    details: Dict[str, Any]  # raw details из native_search


class Level5SearchRequest(BaseModel):
    query: str = Field(..., description="Текст запроса для монолитного поиска")
    bm25_top: int = Field(300, ge=1, le=5000)
    shingle_top: int = Field(50, ge=1, le=2000)
    final_top: int = Field(10, ge=1, le=200)


class Level5SearchResponse(BaseModel):
    hits_total: int
    docs_found: int
    documents: List[Level5DocDetails]
