# app/api/routes/status.py
from datetime import datetime
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.models.document import Document
from app.models.segment import Segment

router = APIRouter(tags=["Document status"])


class DocumentStatusResponse(BaseModel):
    id: int
    status: str
    shard_id: int
    segment_id: Optional[int]

    created_at: datetime
    updated_at: datetime
    last_checked_at: Optional[datetime]

    segment_level: Optional[int] = None
    segment_status: Optional[str] = None
    segment_path: Optional[str] = None


# ───────────────────────────────────────────────────────────────
# Статус одного документа
# ───────────────────────────────────────────────────────────────

@router.get("/status/{doc_id}", response_model=DocumentStatusResponse)
async def get_document_status(
    doc_id: int,
    db: AsyncSession = Depends(get_db),
) -> DocumentStatusResponse:
    result = await db.execute(
        select(Document).where(Document.id == doc_id)
    )
    doc: Optional[Document] = result.scalars().first()

    if doc is None:
        raise HTTPException(status_code=404, detail="Document not found")

    seg_level = None
    seg_status = None
    seg_path = None

    if doc.segment_id is not None:
        seg_result = await db.execute(
            select(Segment).where(Segment.id == doc.segment_id)
        )
        segment: Optional[Segment] = seg_result.scalars().first()
        if segment:
            seg_level = segment.level
            seg_status = segment.status
            seg_path = segment.path

    return DocumentStatusResponse(
        id=doc.id,
        status=doc.status,
        shard_id=doc.shard_id,
        segment_id=doc.segment_id,
        created_at=doc.created_at,
        updated_at=doc.updated_at,
        last_checked_at=doc.last_checked_at,
        segment_level=seg_level,
        segment_status=seg_status,
        segment_path=seg_path,
    )


# ───────────────────────────────────────────────────────────────
# Список документов (все/по шардy) с пагинацией
# ───────────────────────────────────────────────────────────────

@router.get("/status", response_model=List[DocumentStatusResponse])
async def list_document_statuses(
    shard_id: Optional[int] = Query(None, description="Фильтр по shard_id"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
) -> List[DocumentStatusResponse]:
    stmt = select(Document)

    if shard_id is not None:
        stmt = stmt.where(Document.shard_id == shard_id)

    stmt = stmt.order_by(Document.id.desc()).limit(limit).offset(offset)

    result = await db.execute(stmt)
    docs: List[Document] = list(result.scalars())

    if not docs:
        return []

    # Собираем все segment_id и грузим сегменты одним запросом
    segment_ids = {d.segment_id for d in docs if d.segment_id is not None}
    segments_by_id: dict[int, Segment] = {}

    if segment_ids:
        seg_result = await db.execute(
            select(Segment).where(Segment.id.in_(segment_ids))
        )
        segments = list(seg_result.scalars())
        segments_by_id = {s.id: s for s in segments}

    out: List[DocumentStatusResponse] = []

    for doc in docs:
        seg = segments_by_id.get(doc.segment_id) if doc.segment_id else None

        out.append(
            DocumentStatusResponse(
                id=doc.id,
                status=doc.status,
                shard_id=doc.shard_id,
                segment_id=doc.segment_id,
                created_at=doc.created_at,
                updated_at=doc.updated_at,
                last_checked_at=doc.last_checked_at,
                segment_level=seg.level if seg else None,
                segment_status=seg.status if seg else None,
                segment_path=seg.path if seg else None,
            )
        )

    return out
