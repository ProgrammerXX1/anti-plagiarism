from __future__ import annotations

from typing import Any, Dict, List, Optional

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.segment import Segment
from app.models.segment_doc import SegmentDoc


async def segments_overview(
    db: AsyncSession,
    *,
    organization_id: int,
    shard_id: Optional[int] = None,
    levels: Optional[List[int]] = None,
    include_merged: bool = False,
    limit_segments: int = 500,
) -> Dict[str, Any]:
    """
    Возвращает по уровням список сегментов и doc_id внутри каждого сегмента.

    - organization_id обязателен
    - shard_id опционален (если None -> по всем shard в org)
    - levels опционален (если None -> [1,2,3,4])
    - include_merged: показывать ли merged/error и т.п.
    """

    if levels is None:
        levels = [1, 2, 3, 4]

    seg_conds = [
        Segment.organization_id == organization_id,
        Segment.level.in_(levels),
    ]
    if shard_id is not None:
        seg_conds.append(Segment.shard_id == shard_id)

    if not include_merged:
        seg_conds.append(Segment.status == "ready")

    segs_res = await db.execute(
        select(Segment)
        .where(*seg_conds)
        .order_by(Segment.level.asc(), Segment.id.asc())
        .limit(limit_segments)
    )
    segs: List[Segment] = list(segs_res.scalars())

    if not segs:
        return {
            "organization_id": organization_id,
            "shard_id": shard_id,
            "levels": {str(lv): [] for lv in levels},
            "segments_count": 0,
        }

    seg_ids = [s.id for s in segs]

    # Достаём document_id по segment_id
    sd_res = await db.execute(
        select(SegmentDoc.segment_id, SegmentDoc.document_id)
        .where(
            SegmentDoc.organization_id == organization_id,
            SegmentDoc.segment_id.in_(seg_ids),
        )
        .order_by(SegmentDoc.segment_id.asc(), SegmentDoc.document_id.asc())
    )
    rows = sd_res.all()

    docs_by_seg: Dict[int, List[int]] = {}
    for seg_id, doc_id in rows:
        docs_by_seg.setdefault(int(seg_id), []).append(int(doc_id))

    out_levels: Dict[str, List[Dict[str, Any]]] = {str(lv): [] for lv in levels}

    for s in segs:
        out_levels[str(int(s.level))].append(
            {
                "segment_id": int(s.id),
                "level": int(s.level),
                "status": s.status,
                "shard_id": int(s.shard_id),
                "path": s.path,
                "doc_count": int(s.doc_count),
                "size_bytes": int(s.size_bytes),
                "document_ids": docs_by_seg.get(int(s.id), []),
            }
        )

    return {
        "organization_id": organization_id,
        "shard_id": shard_id,
        "levels": out_levels,
        "segments_count": len(segs),
    }
