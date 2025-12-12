from __future__ import annotations

from typing import Any, Dict, List
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import INDEX_DIR
from app.models.segment import Segment
from app.services.levels_0_4.native_segments import seg_search_many


async def search_levels_1_4(
    db: AsyncSession,
    *,
    shard_id: int,
    query: str,
    top_k: int = 10,
) -> Dict[str, Any]:
    res = await db.execute(
        select(Segment)
        .where(
            Segment.shard_id == shard_id,
            Segment.status == "ready",
            Segment.level.in_([1, 2, 3, 4]),
        )
        .order_by(Segment.level.desc(), Segment.id.desc())
    )
    segs: List[Segment] = list(res.scalars())

    index_dirs = [str(INDEX_DIR / s.path) for s in segs if s.path]
    return seg_search_many(query=query, top_k=top_k, index_dirs=index_dirs)
