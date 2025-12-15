from __future__ import annotations

from fastapi import APIRouter, Depends, Body, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.services.levels_0_4.search_service import search_levels_1_4

router = APIRouter(prefix="/api/levels0_4", tags=["levels0_4"])


@router.post("/search", summary="Search across L1-L4 segments for a shard")
async def search(
    text: str = Body(..., media_type="text/plain"),

    organization_id: int = Query(..., description="Organization id"),
    shard_id: int = Query(..., description="Shard id"),

    top_k: int = Query(10, ge=1, le=100),
    include_matches: bool = Query(True),
    max_matches_per_doc: int | None = Query(None, ge=0, le=5000),

    # если false — запрос уже нормализован C++-совместимо, C++ не должен его менять
    normalize_query: bool = Query(True),

    db: AsyncSession = Depends(get_db),
):
    return await search_levels_1_4(
        db,
        organization_id=organization_id,
        shard_id=shard_id,
        query=text,
        top_k=top_k,
        include_matches=include_matches,
        max_matches_per_doc=max_matches_per_doc,
        normalize_query=normalize_query,
    )
