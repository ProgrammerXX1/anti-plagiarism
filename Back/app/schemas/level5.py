from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class Level5ReindexBaseRequest(BaseModel):
    shard_id: int = 0
    rebuild_corpus: bool = True
    rebuild_index: bool = True
    load_after: bool = True


class Level5BaseInfo(BaseModel):
    shard_id: int
    path: str
    has_index: bool
    docs: int
    size_bytes: int


class Level5ReindexRequest(BaseModel):
    """
    Параметры пересборки индекса уровня 5.

    Старые поля оставлены для совместимости схем, но фактически не используются:
      - incremental
      - max_new_docs_per_build
      - rebuild_bm25
    """
    incremental: bool = Field(
        default=True,
        description="Не используется, оставлено для совместимости",
    )
    max_new_docs_per_build: Optional[int] = Field(
        default=None,
        description="Не используется, оставлено для совместимости",
    )
    rebuild_corpus: bool = True
    rebuild_bm25: bool = Field(
        default=True,
        description="Не используется, оставлено для совместимости",
    )
    rebuild_main_index: bool = Field(
        default=True,
        description="Пересобирать C++ индекс (index_native.bin)",
    )
    export_native: bool = Field(
        default=True,
        description="Загрузить индекс в C++ после сборки",
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
    details: Dict[str, Any]


class Level5SearchResponse(BaseModel):
    hits_total: int
    docs_found: int
    documents: List[Level5DocDetails]
