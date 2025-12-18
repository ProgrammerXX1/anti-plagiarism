from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import anyio
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import UPLOAD_DIR, N_SHARDS
from app.core.logger import logger
from app.db.session import get_db
from app.models.document import Document
from app.services.levels_0_4.search_service import search_levels_1_4

router = APIRouter(prefix="/app", tags=["Worker-Prod"])


# ─────────────────────────────────────────────
# helpers
# ─────────────────────────────────────────────

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _validate_org_id(organization_id: int) -> None:
    if organization_id is None or int(organization_id) <= 0:
        raise HTTPException(
            status_code=422,
            detail="organization_id must be a positive integer",
        )


def compute_shard_id(organization_id: int) -> int:
    try:
        n = int(N_SHARDS or 0)
    except Exception:
        n = 0
    if n <= 1:
        return 0
    return int(organization_id) % n


def _cleanup_search_result(search: Dict[str, Any]) -> Dict[str, Any]:
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


def _write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def _write_json_atomic(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


# ─────────────────────────────────────────────
# API models
# ─────────────────────────────────────────────

class ProdV1IngestRequest(BaseModel):
    document_id: str = Field(..., min_length=1)
    title: str = Field(..., min_length=1)
    author: Optional[str] = None
    created_at: Optional[str] = None

    text: str = Field(..., min_length=1)
    file_name: str = "document.txt"
    enable_ocr: bool = False

    organization_id: int

    do_index: bool = False
    do_search: bool = True


class ProdV1IngestResponse(BaseModel):
    document_id: str
    status: str
    processed_at: str

    organization_id: int
    shard_id: int

    indexed: bool
    search: Optional[Dict[str, Any]] = None


# ─────────────────────────────────────────────
# Endpoint
# ─────────────────────────────────────────────

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

    # ── SEARCH ONLY ────────────────────────────
    if req.do_search and not req.do_index:
        raw = await search_levels_1_4(
            db,
            organization_id=req.organization_id,
            shard_id=shard_id,
            query=req.text,           # уже нормализован
            normalize_query=False,    # ВАЖНО: никогда не нормализуем
        )
        search_result = _cleanup_search_result(raw)

        logger.info(
            "[prod_v1] search-only ext_doc=%s org=%s shard=%s",
            req.document_id,
            req.organization_id,
            shard_id,
        )

        return ProdV1IngestResponse(
            document_id=req.document_id,
            status="checked",
            processed_at=now.isoformat(),
            organization_id=req.organization_id,
            shard_id=shard_id,
            indexed=False,
            search=search_result,
        )

    # ── INDEX ─────────────────────────────────
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

    external_id = (
        f"org_{req.organization_id}_"
        f"doc_{req.document_id}_"
        f"{int(now.timestamp())}_{uuid.uuid4().hex}.txt"
    )

    file_path = UPLOAD_DIR / external_id
    meta_path = UPLOAD_DIR / f"{external_id}.meta.json"

    # IMPORTANT CONTRACT:
    # - text IS ALREADY normalized
    # - internal normalizer is NEVER applied
    meta: Dict[str, Any] = {
        "organization_id": int(req.organization_id),
        "document_id": str(req.document_id),
        "title": req.title,
        "author": req.author,
        "source_created_at": req.created_at,
        "file_name": req.file_name,
        "enable_ocr": bool(req.enable_ocr),
        "saved_at": now.isoformat(),
        "text_is_normalized": True,
        "index_normalize": False,
    }

    try:
        await anyio.to_thread.run_sync(_write_text_atomic, file_path, req.text)
        await anyio.to_thread.run_sync(_write_json_atomic, meta_path, meta)
    except Exception as e:
        # best-effort cleanup
        for p in (file_path, meta_path):
            try:
                if p.exists():
                    p.unlink()
            except Exception:
                pass
        raise HTTPException(status_code=500, detail=f"Failed to persist upload: {e}")

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
    try:
        await db.commit()
        await db.refresh(doc)
    except Exception as e:
        for p in (file_path, meta_path):
            try:
                if p.exists():
                    p.unlink()
            except Exception:
                pass
        raise HTTPException(status_code=500, detail=f"DB commit failed: {e}")

    search_result = None
    if req.do_search:
        raw = await search_levels_1_4(
            db,
            organization_id=req.organization_id,
            shard_id=shard_id,
            query=req.text,
            normalize_query=False,  # строго false
        )
        search_result = _cleanup_search_result(raw)

        if hasattr(doc, "last_checked_at"):
            doc.last_checked_at = now
            await db.commit()

    logger.info(
        "[prod_v1] indexed ext_doc=%s internal_id=%s org=%s shard=%s",
        req.document_id,
        getattr(doc, "id", None),
        req.organization_id,
        shard_id,
    )

    return ProdV1IngestResponse(
        document_id=req.document_id,
        status=("indexed_and_checked" if req.do_search else "queued_for_index"),
        processed_at=now.isoformat(),
        organization_id=req.organization_id,
        shard_id=shard_id,
        indexed=True,
        search=search_result,
    )
