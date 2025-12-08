# app/api/routes/search.py
from __future__ import annotations

from typing import List

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import UPLOAD_DIR
from app.db.session import get_db
from app.models.document import Document
from app.models.segment import Segment
from app.services.helpers.file_extract import extract_text_from_file_bytes, norm_for_local
from app.services.search_service import (
    SearchHit,
    run_cpp_search,
)


router = APIRouter(tags=["Search"])


class SearchHitModel(BaseModel):
    doc_id: int
    score: float
    shard_id: int
    segment_id: int


class SearchResponse(BaseModel):
    query_doc_id: int
    shard_id: int
    hits: List[SearchHitModel]


@router.get("/search/by-doc/{doc_id}", response_model=SearchResponse)
async def search_by_doc(
    doc_id: int,
    db: AsyncSession = Depends(get_db),
) -> SearchResponse:
    """
    Поиск похожих документов по уже загруженному doc_id.

    1) Берём документ из БД.
    2) Читаем его файл из UPLOAD_DIR.
    3) Делаем extract_text + norm_for_local.
    4) Выбираем сегменты по shard_id.
    5) Передаём всё в C++ search_core.
    """
    doc: Document | None = await db.get(Document, doc_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")

    if not doc.external_id:
        raise HTTPException(status_code=400, detail="Document has no external_id")

    if doc.status not in {"etl_ok", "indexed"}:
        # Можно сделать мягче (просто предупреждение), но так честнее
        raise HTTPException(
            status_code=409,
            detail=f"Document status is '{doc.status}', expected 'etl_ok' or 'indexed'",
        )

    # читаем оригинальный файл
    file_path = UPLOAD_DIR / doc.external_id
    if not file_path.exists():
        raise HTTPException(
            status_code=500,
            detail=f"File not found on server: {str(file_path)}",
        )

    try:
        raw_bytes = file_path.read_bytes()
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Cannot read file: {e}",
        )

    # ETL on-the-fly (можно в будущем кешировать)
    try:
        raw_text = extract_text_from_file_bytes(raw_bytes, filename=str(file_path))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"extract_text failed: {e}",
        )

    norm_text = norm_for_local(raw_text)

    # выбираем все сегменты shard_id
    result = await db.execute(
        select(Segment).where(
            Segment.shard_id == doc.shard_id,
        )
    )
    segments: list[Segment] = list(result.scalars())

    if not segments:
        # теоретически можем искать только по L0, но его у нас пока нет
        # поэтому честно говорим, что индекс пустой
        return SearchResponse(
            query_doc_id=doc.id,
            shard_id=doc.shard_id,
            hits=[],
        )

    # реальный C++ поиск (внутри сам выберет базовый сегмент)
    hits: list[SearchHit] = run_cpp_search(
        norm_text=norm_text,
        shard_id=doc.shard_id,
        segments=segments,
        top_k=10,
    )

    # конвертация в Pydantic
    hits_model = [
        SearchHitModel(
            doc_id=h.doc_id,
            score=h.score,
            shard_id=h.shard_id,
            segment_id=h.segment_id,
        )
        for h in hits
    ]

    return SearchResponse(
        query_doc_id=doc.id,
        shard_id=doc.shard_id,
        hits=hits_model,
    )
