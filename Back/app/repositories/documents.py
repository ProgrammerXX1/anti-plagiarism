# app/repositories/documents.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update

from app.models.document import Document
from app.core.config import N_SHARDS
from app.core.settings_index import calc_shard_id_from_meta


def calc_shard_id_from_org(organization_id: int) -> int:
    """
    Единое распределение для случаев, когда нет meta (university/faculty/group_name).
    """
    try:
        n = int(N_SHARDS or 0)
    except Exception:
        n = 0
    if n <= 1:
        return 0
    return int(organization_id) % n


async def list_unsegmented_docs_for_shard(
    db: AsyncSession,
    *,
    organization_id: int,
    shard_id: int,
    limit: int,
) -> list[Document]:
    res = await db.execute(
        select(Document)
        .where(
            Document.organization_id == organization_id,
            Document.shard_id == shard_id,
            Document.segment_id.is_(None),
            Document.status == "uploaded",
        )
        .order_by(Document.id)
        .limit(limit)
    )
    return list(res.scalars().all())


async def create_document(
    db: AsyncSession,
    *,
    organization_id: int,
    title: Optional[str],
    student_name: Optional[str],
    university: Optional[str],
    faculty: Optional[str],
    group_name: Optional[str],
    external_id: Optional[str] = None,
    shard_id: Optional[int] = None,
) -> Document:
    now = datetime.now(timezone.utc)

    if shard_id is None:
        # старое поведение — если есть meta
        if any([university, faculty, group_name]):
            shard_id = calc_shard_id_from_meta(
                university=university,
                faculty=faculty,
                group_name=group_name,
            )
        else:
            shard_id = calc_shard_id_from_org(organization_id)

    doc = Document(
        organization_id=organization_id,
        external_id=external_id,
        shard_id=int(shard_id),
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
    await db.flush()
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
    res = await db.execute(select(Document).where(Document.id == doc_id))
    return res.scalar_one_or_none()
