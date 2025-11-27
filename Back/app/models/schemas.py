from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional


class UploadResp(BaseModel):
    doc_id: str
    bytes: int


class BuildReq(BaseModel):
    """
    Зарезервировано под возможную конфигурацию /api/build.
    Сейчас /api/build использует runtime IndexConfig и не принимает body.
    """
    cfg: Optional[Dict[str, Any]] = None


class BuildResp(BaseModel):
    """
    Актуальный формат ответа билда под C++-индекс.
    Можно использовать как response_model для /api/build, если захочешь.
    """
    index_dir: str
    index_native_meta: str

    corpus_lines: int
    corpus_docs: int

    indexed_docs_before: int
    unindexed_docs_before: int

    indexed_docs_after: int
    unindexed_docs_after: int
    delta_indexed: int


class SearchReq(BaseModel):
    query: str = Field(..., min_length=3)
    top: int = Field(5, ge=1, le=50)


class Frag(BaseModel):
    start: int
    end: int
    text: str


class HitDetails(BaseModel):
    """
    Детали под k=9-шинглы и simhash.
    J9, C9 — основная метрика плагиата.
    """
    J9: float
    C9: float

    # оценка по simhash (например, количество совпавших бит или distance)
    hamming_simhash: int

    # сколько сырых кандидатов пришло из C++ (до фильтрации/merge)
    cand_hits: int

    matching_fragments: List[Frag]


class Hit(BaseModel):
    doc_id: str
    title: str | None = None
    author: str | None = None

    max_score: float
    originality_pct: float
    decision: str  # например: "plagiarized" / "partial" / "original"

    details: HitDetails


class SearchResp(BaseModel):
    hits_total: int          # сколько кандидатов вообще нашли
    docs_found: int          # сколько уникальных doc_id
    documents: List[Hit]
