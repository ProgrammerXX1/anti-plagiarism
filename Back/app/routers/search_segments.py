# app/api/routes/search_segments.py
from __future__ import annotations

import json
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.models.document import Document
from app.models.segment import Segment
from app.core.config import CORPUS_JSONL

router = APIRouter(tags=["Search"])


class SearchHit(BaseModel):
    doc_id: int
    score: float
    segment_id: Optional[int]


class SearchResponse(BaseModel):
    query_doc_id: int
    status: str          # статус самого документа (uploaded / etl_ok / indexed / ...)
    hits: List[SearchHit]


def _load_norm_text_from_corpus(doc_id: int, corpus_path: Path) -> str:
    """
    Временный простой способ достать нормализованный текст из corpus.jsonl.
    При реальном объёме нужно будет делать отдельный corpus-индекс или
    хранить нормализованный текст в БД.
    """
    if not corpus_path.exists():
        raise FileNotFoundError(f"corpus file not found: {corpus_path}")

    last_text: Optional[str] = None
    with corpus_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if rec.get("doc_id") == doc_id:
                last_text = rec.get("text")

    if last_text is None:
        raise LookupError(f"doc_id={doc_id} not found in corpus.jsonl")

    return last_text


@router.get("/search/{doc_id}", response_model=SearchResponse)
async def search_by_document(
    doc_id: int,
    db: AsyncSession = Depends(get_db),
) -> SearchResponse:
    # 1. Достаём документ
    result = await db.execute(
        select(Document).where(Document.id == doc_id)
    )
    doc: Optional[Document] = result.scalars().first()

    if doc is None:
        raise HTTPException(status_code=404, detail="Document not found")

    # Если документ ещё не индексирован — возвращаем статус и пустой список.
    if doc.status != "indexed":
        return SearchResponse(
            query_doc_id=doc.id,
            status=doc.status,
            hits=[],
        )

    # 2. Выбираем нужные сегменты по shard_id (на текущем этапе — все готовые)
    seg_result = await db.execute(
        select(Segment).where(
            Segment.shard_id == doc.shard_id,
            Segment.status == "ready",
        )
    )
    segments: List[Segment] = list(seg_result.scalars())

    if not segments:
        # теоретически не должно быть, раз статус indexed,
        # но на всякий случай отдаём пустой результат.
        return SearchResponse(
            query_doc_id=doc.id,
            status=doc.status,
            hits=[],
        )

    # 3. Поднимаем нормализованный текст из corpus.jsonl
    try:
        norm_text = _load_norm_text_from_corpus(doc.id, CORPUS_JSONL)
    except (FileNotFoundError, LookupError) as e:
        # текст не нашли — формально ошибка данных
        raise HTTPException(status_code=500, detail=str(e))

    # 4. TODO: здесь должен быть вызов C++ search_core
    #
    # Псевдо-интерфейс:
    # hits_raw = search_core.search_document(
    #     norm_text=norm_text,
    #     segments=[
    #         {
    #             "id": s.id,
    #             "path": s.path,        # типа "shard_0/segment_1_dummy"
    #             "level": s.level,
    #         }
    #         for s in segments
    #     ],
    #     top_k=20,
    # )
    #
    # где hits_raw ~ [
    #   {"doc_id": 123, "score": 0.87, "segment_id": 1},
    #   ...
    # ]
    #
    # Пока сделаем stub, чтобы фронт мог интегрироваться:
    # отдадим сам документ с score=1.0 (имитация perfect self-match).

    # Реальный код поиска вставишь сюда.
    hits = [
        SearchHit(
            doc_id=doc.id,
            score=1.0,
            segment_id=doc.segment_id or segments[0].id,
        )
    ]

    return SearchResponse(
        query_doc_id=doc.id,
        status=doc.status,
        hits=hits,
    )
