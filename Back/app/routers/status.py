from typing import List
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.models.document import Document
from app.models.segment import Segment

from app.core.config import (
    ETL_BATCH_SIZE,
    DOCS_PER_L1_SEGMENT,
    MAX_AUTO_LEVEL,
    SEGMENTS_PER_L2_COMPACT,
    SEGMENTS_PER_L3_COMPACT,
    SEGMENTS_PER_L4_COMPACT,
)

router = APIRouter(tags=["Admin-levels"])


# ───────────────────────── schemas ───────────────────────── #

class LevelSegmentItem(BaseModel):
    segment_id: int
    shard_id: int
    level: int
    status: str
    doc_count: int
    size_bytes: int
    path: str


class LevelsConfigResponse(BaseModel):
    etl_batch_size: int
    docs_per_l1_segment: int
    max_auto_level: int
    segments_per_l2_compact: int
    segments_per_l3_compact: int
    segments_per_l4_compact: int


class LevelsStatusResponse(BaseModel):
    level0_docs: int
    level1_segments: List[LevelSegmentItem]
    level2_segments: List[LevelSegmentItem]
    level3_segments: List[LevelSegmentItem]
    level4_segments: List[LevelSegmentItem]


class LevelsFullResponse(BaseModel):
    config: LevelsConfigResponse
    status: LevelsStatusResponse


# ───────────────────────── endpoints ───────────────────────── #

@router.get("/levels/config", response_model=LevelsConfigResponse)
async def get_levels_config() -> LevelsConfigResponse:
    """
    Показывает ТЕКУЩИЕ конфиги, которые реально используются воркером (env).
    """
    return LevelsConfigResponse(
        etl_batch_size=ETL_BATCH_SIZE,
        docs_per_l1_segment=DOCS_PER_L1_SEGMENT,
        max_auto_level=MAX_AUTO_LEVEL,
        segments_per_l2_compact=SEGMENTS_PER_L2_COMPACT,
        segments_per_l3_compact=SEGMENTS_PER_L3_COMPACT,
        segments_per_l4_compact=SEGMENTS_PER_L4_COMPACT,
    )


@router.get("/levels/status", response_model=LevelsStatusResponse)
async def get_levels_status(
    db: AsyncSession = Depends(get_db),
) -> LevelsStatusResponse:
    """
    Текущий статус уровней в БД.
    """

    # 0 уровень — ещё не индексированы
    level0_stmt = select(func.count(Document.id)).where(
        Document.status.in_(["uploaded", "etl_ok"]),
        Document.segment_id.is_(None),
    )
    level0_count = (await db.execute(level0_stmt)).scalar_one()

    async def load_level(level: int):
        stmt = (
            select(Segment)
            .where(
                Segment.level == level,
                Segment.status == "ready",
            )
            .order_by(Segment.id)
        )
        return list((await db.execute(stmt)).scalars())

    l1_segments = await load_level(1)
    l2_segments = await load_level(2)
    l3_segments = await load_level(3)
    l4_segments = await load_level(4)

    def to_item(s: Segment) -> LevelSegmentItem:
        return LevelSegmentItem(
            segment_id=s.id,
            shard_id=s.shard_id,
            level=s.level,
            status=s.status,
            doc_count=s.doc_count,
            size_bytes=s.size_bytes or 0,
            path=s.path or "",
        )

    return LevelsStatusResponse(
        level0_docs=level0_count,
        level1_segments=[to_item(s) for s in l1_segments],
        level2_segments=[to_item(s) for s in l2_segments],
        level3_segments=[to_item(s) for s in l3_segments],
        level4_segments=[to_item(s) for s in l4_segments],
    )
