from __future__ import annotations

from fastapi import APIRouter, Depends, Body, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.services.levels_0_4.search_service import search_levels_1_4

router = APIRouter(prefix="/api/levels0_4", tags=["levels0_4"])


@router.post(
    "/search",
    summary="Search across L1-L4 segments for a shard",
)
async def search(
    text: str = Body(..., media_type="text/plain"),
    shard_id: int = Query(..., description="Shard id"),
    top_k: int = Query(10, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    return await search_levels_1_4(
        db,
        shard_id=shard_id,
        query=text,
        top_k=top_k,
    )
