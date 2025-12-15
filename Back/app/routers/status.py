from typing import List, Dict, Optional
import json
from pathlib import Path

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.models.document import Document
from app.models.segment import Segment
from app.models.segment_doc import SegmentDoc

from app.core.config import (
    ETL_BATCH_SIZE,
    DOCS_PER_L1_SEGMENT,
    MAX_AUTO_LEVEL,
    SEGMENTS_PER_L2_COMPACT,
    SEGMENTS_PER_L3_COMPACT,
    SEGMENTS_PER_L4_COMPACT,
    L5_SHARDS_DIR,
)
from app.schemas.level5 import Level5BaseInfo

router = APIRouter(tags=["Admin-levels"])


class LevelSegmentItem(BaseModel):
    segment_id: int
    organization_id: int
    shard_id: int
    level: int
    status: str
    doc_count: int
    size_bytes: int
    path: str
    document_ids: List[int]


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

    level5_waiting_docs: int
    level5_indexed_docs: int
    level5_bases: List[Level5BaseInfo]


def _iter_l5_doc_ids() -> set[int]:
    ids: set[int] = set()
    if not L5_SHARDS_DIR.exists():
        return ids

    for shard_dir in L5_SHARDS_DIR.glob("shard_*"):
        if not shard_dir.is_dir():
            continue
        idx_dir = shard_dir / "current"
        if not idx_dir.is_dir():
            continue

        docids_path: Path = idx_dir / "index_native_docids.json"
        if not docids_path.exists():
            continue

        try:
            data = json.loads(docids_path.read_text(encoding="utf-8"))
            if isinstance(data, list):
                for v in data:
                    try:
                        ids.add(int(v))
                    except (TypeError, ValueError):
                        continue
        except Exception:
            continue

    return ids


def _iter_l5_bases() -> List[Level5BaseInfo]:
    bases: List[Level5BaseInfo] = []
    if not L5_SHARDS_DIR.exists():
        return bases

    for shard_dir in sorted(L5_SHARDS_DIR.glob("shard_*")):
        if not shard_dir.is_dir():
            continue

        try:
            shard_id = int(shard_dir.name.split("_", 1)[1])
        except (IndexError, ValueError):
            continue

        idx_dir = shard_dir / "current"
        docids_path = idx_dir / "index_native_docids.json"
        bin_path = idx_dir / "index_native.bin"
        meta_path = idx_dir / "index_native_meta.json"

        has_index = bin_path.exists() and docids_path.exists()

        docs = 0
        if docids_path.exists():
            try:
                data = json.loads(docids_path.read_text(encoding="utf-8"))
                if isinstance(data, list):
                    docs = len(data)
            except Exception:
                docs = 0

        size = 0
        for p in (bin_path, docids_path, meta_path):
            if p.exists():
                try:
                    size += p.stat().st_size
                except OSError:
                    continue

        bases.append(
            Level5BaseInfo(
                shard_id=shard_id,
                path=str(idx_dir),
                has_index=has_index,
                docs=docs,
                size_bytes=size,
            )
        )

    return bases


@router.get("/levels/config", response_model=LevelsConfigResponse)
async def get_levels_config() -> LevelsConfigResponse:
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
    organization_id: Optional[int] = Query(
        None,
        description="If provided: filter L0-L4 by organization_id. If omitted: show all orgs.",
    ),
) -> LevelsStatusResponse:
    # L0
    level0_conds = [
        Document.status.in_(["uploaded", "etl_ok"]),
        Document.segment_id.is_(None),
    ]
    if organization_id is not None:
        level0_conds.append(Document.organization_id == organization_id)

    level0_stmt = select(func.count(Document.id)).where(*level0_conds)
    level0_count = (await db.execute(level0_stmt)).scalar_one()

    async def load_level(level: int) -> List[Segment]:
        seg_conds = [
            Segment.level == level,
            Segment.status == "ready",
        ]
        if organization_id is not None:
            seg_conds.append(Segment.organization_id == organization_id)

        stmt = select(Segment).where(*seg_conds).order_by(Segment.id)
        return list((await db.execute(stmt)).scalars())

    l1_segments = await load_level(1)
    l2_segments = await load_level(2)
    l3_segments = await load_level(3)
    l4_segments = await load_level(4)

    all_segs = l1_segments + l2_segments + l3_segments + l4_segments
    seg_ids = [int(s.id) for s in all_segs]

    docs_by_seg: Dict[int, List[int]] = {}
    if seg_ids:
        sd_conds = [SegmentDoc.segment_id.in_(seg_ids)]
        # фильтровать по org в segment_docs имеет смысл только если org задан
        if organization_id is not None:
            sd_conds.append(SegmentDoc.organization_id == organization_id)

        sd_stmt = (
            select(SegmentDoc.segment_id, SegmentDoc.document_id)
            .where(*sd_conds)
            .order_by(SegmentDoc.segment_id, SegmentDoc.document_id)
        )
        rows = (await db.execute(sd_stmt)).all()
        for sid, did in rows:
            docs_by_seg.setdefault(int(sid), []).append(int(did))

    def to_item(s: Segment) -> LevelSegmentItem:
        org = int(s.organization_id or 0)  # FIX: old rows may have NULL
        return LevelSegmentItem(
            segment_id=int(s.id),
            organization_id=org,
            shard_id=int(s.shard_id or 0),
            level=int(s.level or 0),
            status=s.status or "",
            doc_count=int(s.doc_count or 0),
            size_bytes=int(s.size_bytes or 0),
            path=s.path or "",
            document_ids=docs_by_seg.get(int(s.id), []),
        )

    # L5
    level5_bases = _iter_l5_bases()
    indexed_ids = _iter_l5_doc_ids()
    level5_indexed = len(indexed_ids)

    if indexed_ids:
        l5_wait_stmt = select(func.count(Document.id)).where(
            Document.status == "l5_uploaded",
            ~Document.id.in_(indexed_ids),
        )
    else:
        l5_wait_stmt = select(func.count(Document.id)).where(Document.status == "l5_uploaded")
    level5_waiting = (await db.execute(l5_wait_stmt)).scalar_one()

    return LevelsStatusResponse(
        level0_docs=int(level0_count),
        level1_segments=[to_item(s) for s in l1_segments],
        level2_segments=[to_item(s) for s in l2_segments],
        level3_segments=[to_item(s) for s in l3_segments],
        level4_segments=[to_item(s) for s in l4_segments],
        level5_waiting_docs=int(level5_waiting),
        level5_indexed_docs=int(level5_indexed),
        level5_bases=level5_bases,
    )
