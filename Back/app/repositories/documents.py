# app/repositories/documents.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update

from app.models.document import Document
from app.core.settings_index import calc_shard_id_from_meta


async def create_document(
    db: AsyncSession,
    *,
    title: Optional[str],
    student_name: Optional[str],
    university: Optional[str],
    faculty: Optional[str],
    group_name: Optional[str],
    external_id: Optional[str] = None,
) -> Document:
    now = datetime.now(timezone.utc)
    shard_id = calc_shard_id_from_meta(
        university=university,
        faculty=faculty,
        group_name=group_name,
    )

    doc = Document(
        external_id=external_id,
        shard_id=shard_id,
        status="uploaded",
        created_at=now,
        updated_at=now,
        title=title,
        student_name=student_name,
        university=university,
        faculty=faculty,
        group_name=group_name,
    )
    db.add(doc)
    await db.flush()  # чтобы появился doc.id
    return doc


async def set_document_status(
    db: AsyncSession,
    doc_id: int,
    *,
    status: str,
    segment_id: Optional[int] = None,
    simhash_hi: Optional[int] = None,
    simhash_lo: Optional[int] = None,
) -> None:
    now = datetime.now(timezone.utc)

    stmt = (
        update(Document)
        .where(Document.id == doc_id)
        .values(
            status=status,
            segment_id=segment_id,
            simhash_hi=simhash_hi,
            simhash_lo=simhash_lo,
            updated_at=now,
        )
    )
    await db.execute(stmt)


async def get_document(db: AsyncSession, doc_id: int) -> Optional[Document]:
    res = await db.execute(
        select(Document).where(Document.id == doc_id)
    )
    return res.scalar_one_or_none()
