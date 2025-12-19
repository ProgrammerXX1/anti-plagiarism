# app/repositories/segments.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Sequence

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.segment import Segment


async def create_segment(
    db: AsyncSession,
    *,
    organization_id: int,
    shard_id: int,
    level: int,
    path: str,
    doc_count: int,
    shingle_count: int,
    size_bytes: int,
    status: str = "ready",
) -> Segment:
    now = datetime.now(timezone.utc)
    seg = Segment(
        organization_id=int(organization_id),
        shard_id=int(shard_id),
        level=int(level),
        status=status,
        path=path,
        doc_count=int(doc_count),
        shingle_count=int(shingle_count),
        size_bytes=int(size_bytes),
        created_at=now,
        last_compacted_at=None,
        last_access_at=None,
    )
    db.add(seg)
    await db.flush()
    return seg


async def mark_segment_retired(db: AsyncSession, segment_id: int) -> None:
    stmt = (
        update(Segment)
        .where(Segment.id == int(segment_id))
        .values(status="retired")
    )
    await db.execute(stmt)


async def touch_segment_access(db: AsyncSession, segment_id: int) -> None:
    now = datetime.now(timezone.utc)
    stmt = (
        update(Segment)
        .where(Segment.id == int(segment_id))
        .values(last_access_at=now)
    )
    await db.execute(stmt)


async def list_ready_segments_for_scope(
    db: AsyncSession,
    *,
    organization_id: int,
    shard_id: int,
    levels: Sequence[int] = (1, 2, 3, 4),
) -> Sequence[Segment]:
    res = await db.execute(
        select(Segment)
        .where(
            Segment.organization_id == int(organization_id),
            Segment.shard_id == int(shard_id),
            Segment.status == "ready",
            Segment.level.in_([int(x) for x in levels]),
        )
        .order_by(Segment.level, Segment.id)
    )
    return res.scalars().all()
