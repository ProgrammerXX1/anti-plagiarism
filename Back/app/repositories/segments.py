# app/repositories/segments.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, Sequence

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update

from app.models.segment import Segment


async def create_segment(
    db: AsyncSession,
    *,
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
        shard_id=shard_id,
        level=level,
        status=status,
        path=path,
        doc_count=doc_count,
        shingle_count=shingle_count,
        size_bytes=size_bytes,
        created_at=now,
        last_compacted_at=None,
        last_access_at=None,
    )
    db.add(seg)
    await db.flush()
    return seg


async def mark_segment_retired(
    db: AsyncSession,
    segment_id: int,
) -> None:
    stmt = (
        update(Segment)
        .where(Segment.id == segment_id)
        .values(status="retired")
    )
    await db.execute(stmt)


async def touch_segment_access(
    db: AsyncSession,
    segment_id: int,
) -> None:
    now = datetime.now(timezone.utc)
    stmt = (
        update(Segment)
        .where(Segment.id == segment_id)
        .values(last_access_at=now)
    )
    await db.execute(stmt)


async def list_ready_segments_for_shard(
    db: AsyncSession,
    shard_id: int,
) -> Sequence[Segment]:
    res = await db.execute(
        select(Segment)
        .where(
            Segment.shard_id == shard_id,
            Segment.status == "ready",
        )
        .order_by(Segment.level, Segment.id)
    )
    return res.scalars().all()
