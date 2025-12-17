from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import UPLOAD_DIR, N_SHARDS
from app.core.logger import logger
from app.db.session import get_db
from app.models.document import Document
from app.services.levels_0_4.search_service import search_levels_1_4

router = APIRouter(prefix="/api/prod/v1", tags=["prod_v1"])


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _validate_org_id(organization_id: int) -> None:
    if organization_id is None or int(organization_id) <= 0:
        raise HTTPException(status_code=422, detail="organization_id must be a positive integer")


def compute_shard_id(organization_id: int) -> int:
    try:
        n = int(N_SHARDS or 0)
    except Exception:
        n = 0
    if n <= 1:
        return 0
    return int(organization_id) % n


def _cleanup_search_result(search: Dict[str, Any]) -> Dict[str, Any]:
    """
    External API:
      - remove internal fields
      - expose J/C (j9/c9)
    """
    hits = search.get("hits") or []
    cleaned = []

    for h in hits:
        cleaned.append(
            {
                "doc_id": str(h.get("doc_id", "")),
                "score": float(h.get("score", 0.0)),
                "J": float(h.get("j9", 0.0)),
                "C": float(h.get("c9", 0.0)),
                "cand_hits": int(h.get("cand_hits", 0) or 0),
                "segment_level": int(h.get("segment_level", 0) or 0),
                "match_spans": h.get("match_spans") or [],
            }
        )

    return {"count": int(len(cleaned)), "hits": cleaned}


class ProdV1IngestRequest(BaseModel):
    document_id: str
    title: str
    author: Optional[str] = None
    document_type: Optional[str] = None
    created_at: Optional[str] = None

    text: str
    file_name: str = "document.txt"
    enable_ocr: bool = False

    organization_id: int

    do_index: bool = False
    do_search: bool = True

    # single knob (user choice)
    index_normalize: bool = True

    # search-side normalization toggle (still exposed, but beware mixed-mode indices)
    normalize_query: bool = True


class ProdV1IngestResponse(BaseModel):
    document_id: str
    status: str
    processed_at: str

    organization_id: int

    indexed: bool
    search: Optional[Dict[str, Any]] = None


@router.post("/ingest", response_model=ProdV1IngestResponse)
async def prod_v1_ingest(
    req: ProdV1IngestRequest,
    db: AsyncSession = Depends(get_db),
) -> ProdV1IngestResponse:
    _validate_org_id(req.organization_id)

    if not req.text.strip():
        raise HTTPException(status_code=400, detail="Empty text")

    if not req.do_index and not req.do_search:
        raise HTTPException(status_code=400, detail="Nothing to do")

    now = utcnow()
    shard_id = compute_shard_id(req.organization_id)

    # SEARCH-ONLY: do not store anything
    if req.do_search and not req.do_index:
        raw = await search_levels_1_4(
            db,
            organization_id=req.organization_id,
            shard_id=shard_id,
            query=req.text,
            normalize_query=req.normalize_query,
        )
        search_result = _cleanup_search_result(raw)

        logger.info(
            "[prod_v1] search-only doc=%s org=%s shard=%s",
            req.document_id, req.organization_id, shard_id
        )

        return ProdV1IngestResponse(
            document_id=req.document_id,
            status="checked",
            processed_at=now.isoformat(),
            organization_id=req.organization_id,
            indexed=False,
            search=search_result,
        )

    # INDEX (and optionally search): store file + Document(uploaded)
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

    external_id = (
        f"org_{req.organization_id}_"
        f"doc_{req.document_id}_"
        f"normindex{int(req.index_normalize)}_"
        f"{int(now.timestamp())}_{uuid.uuid4().hex}.txt"
    )

    (UPLOAD_DIR / external_id).write_text(req.text, encoding="utf-8")

    meta = {
        "organization_id": req.organization_id,
        "document_id": req.document_id,
        "title": req.title,
        "author": req.author,
        "document_type": req.document_type,
        "source_created_at": req.created_at,
        "file_name": req.file_name,
        "enable_ocr": req.enable_ocr,
        "index_normalize": bool(req.index_normalize),
        "saved_at": now.isoformat(),
    }
    (UPLOAD_DIR / f"{external_id}.meta.json").write_text(
        json.dumps(meta, ensure_ascii=False),
        encoding="utf-8",
    )

    doc = Document(
        external_id=external_id,
        organization_id=req.organization_id,
        shard_id=shard_id,
        status="uploaded",
        created_at=now,
        updated_at=now,
        title=req.title,
        student_name=req.author,
    )
    db.add(doc)
    await db.commit()
    await db.refresh(doc)

    search_result = None
    if req.do_search:
        raw = await search_levels_1_4(
            db,
            organization_id=req.organization_id,
            shard_id=shard_id,
            query=req.text,
            normalize_query=req.normalize_query,
        )
        search_result = _cleanup_search_result(raw)

        doc.last_checked_at = now
        await db.commit()

    logger.info(
        "[prod_v1] indexed doc=%s internal=%s org=%s shard=%s index_normalize=%s",
        req.document_id, doc.id, req.organization_id, shard_id, int(req.index_normalize)
    )

    return ProdV1IngestResponse(
        document_id=req.document_id,
        status=("indexed_and_checked" if req.do_search else "queued_for_index"),
        processed_at=now.isoformat(),
        organization_id=req.organization_id,
        indexed=True,
        search=search_result,
    )
