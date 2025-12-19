from __future__ import annotations

from functools import partial
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import anyio
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import UPLOAD_DIR, N_SHARDS
from app.core.logger import logger
from app.db.session import get_db
from app.models.document import Document
from app.models.plagiarism_report import PlagiarismReportSource, PlagiarismReportMatch
from app.repositories.plagiarism_reports import upsert_report, replace_report_children
from app.services.levels_0_4.native_segments import seg_excerpt_for_span
from app.services.levels_0_4.search_service import search_levels_1_4

router = APIRouter(prefix="/app", tags=["Worker-Prod"])

K_SHINGLE = 9


# ───────────────────────────────────────────────
# Models (backend contract)
# ───────────────────────────────────────────────

class SourceItem(BaseModel):
    id: str                       # internal doc_id (string)
    source_id: str                # external source_id (best-effort), may equal id
    module_id: str = "plagiarism"
    name: str = ""
    url: Optional[str] = None
    author: Optional[str] = None
    index_date: str


class MatchSourceItem(BaseModel):
    id: str
    source_id: str                # internal doc_id (string) — MUST match SourceItem.id for joins
    q_offset: int
    q_limit: int
    s_offset: int
    s_limit: int
    type: str = "1"


class ModuleItem(BaseModel):
    id: str
    module_name: str


class BackendContractResponse(BaseModel):
    document_id: str
    status: str
    processed_at: str

    plagiarism_percentage: float
    selfcite_percentage: float
    legal_percentage: float
    unknown_percentage: float

    sources: List[SourceItem]
    matchsources: List[MatchSourceItem]
    modules: List[ModuleItem]


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


# ───────────────────────────────────────────────
# Helpers
# ───────────────────────────────────────────────

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


def _load_upload_meta_by_external_id(external_id: str) -> Dict[str, Any]:
    p = UPLOAD_DIR / f"{external_id}.meta.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _load_source_text_best_effort(doc: Optional[Document]) -> str:
    """
    Best-effort loader. For now: txt only.
    If you need docx/pdf support, reuse extract_text_from_file_bytes as in search_service.
    """
    if not doc or not getattr(doc, "external_id", None):
        return ""
    p = UPLOAD_DIR / doc.external_id
    if not p.exists():
        return ""
    try:
        raw = p.read_bytes()
        return raw.decode("utf-8", errors="ignore")
    except Exception:
        return ""


def _cleanup_search_hits_for_contract(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    hits = raw.get("hits") or []
    out: List[Dict[str, Any]] = []
    for h in hits:
        if not isinstance(h, dict):
            continue
        out.append(
            {
                "doc_id": str(h.get("doc_id", "")).strip(),
                "C": float(h.get("C", 0.0) or 0.0),
                "match_spans": h.get("match_spans") or [],
            }
        )
    return out


async def _excerpt_offset_limit(
    *,
    text: str,
    sh_from: int,
    sh_to: int,
    normalize_text: bool,
) -> Tuple[int, int]:
    fn = partial(
        seg_excerpt_for_span,
        text=text,
        d_from=int(sh_from),
        d_to=int(sh_to),
        k_shingle=int(K_SHINGLE),
        max_chars=10_000,
        normalize_text=bool(normalize_text),
    )
    try:
        ex = await anyio.to_thread.run_sync(fn)
    except Exception:
        return 0, 0

    if not isinstance(ex, dict) or not ex.get("ok"):
        return 0, 0

    cf = int(ex.get("char_from", 0) or 0)
    ct = int(ex.get("char_to", 0) or 0)
    if ct < cf:
        ct = cf
    return cf, (ct - cf)


# ───────────────────────────────────────────────
# Contract builder + DB persist
# ───────────────────────────────────────────────

async def build_backend_contract_and_persist(
    db: AsyncSession,
    *,
    req: ProdV1IngestRequest,
    hits: List[Dict[str, Any]],
    processed_at: datetime,
    shard_id: int,
) -> BackendContractResponse:
    processed_at_str = processed_at.isoformat()

    # cache docs by internal doc_id
    doc_cache: Dict[str, Optional[Document]] = {}

    def get_doc_cached(did: str) -> Optional[Document]:
        if did in doc_cache:
            return doc_cache[did]
        try:
            doc_cache[did] = None
            if did.isdigit():
                # NOTE: db.get is async; cache fill happens outside. Caller must await separately.
                return None
        except Exception:
            return None
        return None

    # unique doc_ids
    uniq_doc_ids: List[str] = []
    seen: set[str] = set()
    for h in hits:
        did = str(h.get("doc_id", "")).strip()
        if did and did not in seen:
            seen.add(did)
            uniq_doc_ids.append(did)

    # fetch Document rows once
    for did in uniq_doc_ids:
        if did in doc_cache:
            continue
        doc_cache[did] = None
        if not did.isdigit():
            continue
        try:
            doc_cache[did] = await db.get(Document, int(did))
        except Exception:
            doc_cache[did] = None

    # build sources
    sources: List[SourceItem] = []
    for did in uniq_doc_ids:
        doc = doc_cache.get(did)
        meta = _load_upload_meta_by_external_id(doc.external_id) if doc and getattr(doc, "external_id", None) else {}

        external_source_id = str(meta.get("source_id") or did)

        sources.append(
            SourceItem(
                id=did,  # internal key
                source_id=external_source_id,
                module_id=str(meta.get("module_id") or "plagiarism"),
                name=str((getattr(doc, "title", None) or meta.get("title") or "")),
                url=meta.get("url"),
                author=(str(getattr(doc, "student_name", None) or meta.get("author") or "") or None),
                index_date=str(getattr(doc, "created_at", None).isoformat() if doc and getattr(doc, "created_at", None) else processed_at_str),
            )
        )

    # matchsources: 2 pairs
    matchsources: List[MatchSourceItem] = []

    # MUST match search normalize_query=False for offsets computed on req.text
    normalize_query_offsets = False

    match_id = 1
    for h in hits:
        did = str(h.get("doc_id", "")).strip()
        if not did:
            continue

        doc = doc_cache.get(did)
        source_text = _load_source_text_best_effort(doc)

        source_norm = False
        if doc and getattr(doc, "external_id", None):
            meta = _load_upload_meta_by_external_id(doc.external_id)
            source_norm = bool(meta.get("index_normalize", False))

        spans = h.get("match_spans") or []
        if not isinstance(spans, list):
            continue

        for sp in spans:
            if not isinstance(sp, dict):
                continue

            q_from = sp.get("q_from")
            q_to = sp.get("q_to")
            d_from = sp.get("d_from")
            d_to = sp.get("d_to")
            if q_from is None or q_to is None or d_from is None or d_to is None:
                continue

            q_offset, q_limit = await _excerpt_offset_limit(
                text=req.text,
                sh_from=int(q_from),
                sh_to=int(q_to),
                normalize_text=normalize_query_offsets,
            )
            if q_limit <= 0:
                continue

            s_offset = 0
            s_limit = 0
            if source_text:
                s_offset, s_limit = await _excerpt_offset_limit(
                    text=source_text,
                    sh_from=int(d_from),
                    sh_to=int(d_to),
                    normalize_text=source_norm,
                )

            # if source text isn't available, keep record with zeros (better than dropping)
            matchsources.append(
                MatchSourceItem(
                    id=str(match_id),
                    source_id=did,  # internal key
                    q_offset=int(q_offset),
                    q_limit=int(q_limit),
                    s_offset=int(s_offset),
                    s_limit=int(s_limit),
                    type="1",
                )
            )
            match_id += 1

    # percentages
    c_sum = 0.0
    for h in hits:
        c_sum += float(h.get("C", 0.0) or 0.0)
    c_sum = min(1.0, max(0.0, c_sum))

    plagiarism_pct = round(c_sum * 100.0, 2)
    unknown_pct = round(max(0.0, 100.0 - plagiarism_pct), 2)

    # ── persist to DB ───────────────────────────
    try:
        report = await upsert_report(
            db,
            organization_id=req.organization_id,
            shard_id=shard_id,
            document_id=req.document_id,
            status="completed",
            processed_at=processed_at,
            plagiarism_percentage=plagiarism_pct,
            selfcite_percentage=0.0,
            legal_percentage=0.0,
            unknown_percentage=unknown_pct,
            internal_doc_id=None,
        )

        db_sources: List[PlagiarismReportSource] = []
        for s in sources:
            db_sources.append(
                PlagiarismReportSource(
                    report_id=report.id,
                    # IMPORTANT: internal join key
                    source_id=s.id,  # internal doc_id string
                    internal_source_doc_id=int(s.id) if s.id.isdigit() else None,
                    module_id=s.module_id,
                    name=s.name,
                    url=s.url,
                    author=s.author,
                    index_date=processed_at,
                    # if you want external_source_id in DB, add a column; for now it's in API only
                )
            )

        db_matches: List[PlagiarismReportMatch] = []
        for m in matchsources:
            db_matches.append(
                PlagiarismReportMatch(
                    report_id=report.id,
                    source_id=m.source_id,  # internal doc_id string
                    q_offset=m.q_offset,
                    q_limit=m.q_limit,
                    s_offset=m.s_offset,
                    s_limit=m.s_limit,
                    type=m.type,
                )
            )

        await replace_report_children(db, report_id=report.id, sources=db_sources, matches=db_matches)
        await db.commit()
    except Exception:
        await db.rollback()
        raise

    return BackendContractResponse(
        document_id=req.document_id,
        status="completed",
        processed_at=processed_at_str,
        plagiarism_percentage=plagiarism_pct,
        selfcite_percentage=0.0,
        legal_percentage=0.0,
        unknown_percentage=unknown_pct,
        sources=sources,
        matchsources=matchsources,
        modules=[ModuleItem(id="1", module_name="Segment")],
    )


# ───────────────────────────────────────────────
# API
# ───────────────────────────────────────────────

@router.post("/ingest", response_model=BackendContractResponse)
async def prod_v1_ingest(
    req: ProdV1IngestRequest,
    db: AsyncSession = Depends(get_db),
) -> BackendContractResponse:
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
            query=req.text,
            normalize_query=False,
        )
        hits = _cleanup_search_hits_for_contract(raw)

        logger.info(
            "[prod_v1] search-only ext_doc=%s org=%s shard=%s hits=%s",
            req.document_id,
            req.organization_id,
            shard_id,
            len(hits),
        )

        return await build_backend_contract_and_persist(
            db,
            req=req,
            hits=hits,
            processed_at=now,
            shard_id=shard_id,
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

    meta: Dict[str, Any] = {
        "organization_id": int(req.organization_id),
        "document_id": str(req.document_id),
        "title": req.title,
        "author": req.author,
        "source_created_at": req.created_at,
        "file_name": req.file_name,
        "enable_ocr": bool(req.enable_ocr),
        "saved_at": now.isoformat(),
        "text_is_normalized": True,   # assumption (keep)
        "index_normalize": False,     # keep
        # optional meta fields (best-effort)
        "module_id": "plagiarism",
        "source_id": str(req.document_id),
        "url": None,
    }

    try:
        await anyio.to_thread.run_sync(lambda: _write_text_atomic(file_path, req.text))
        await anyio.to_thread.run_sync(lambda: _write_json_atomic(meta_path, meta))
    except Exception as e:
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

    # optional post-index search
    hits: List[Dict[str, Any]] = []
    if req.do_search:
        raw = await search_levels_1_4(
            db,
            organization_id=req.organization_id,
            shard_id=shard_id,
            query=req.text,
            normalize_query=False,
        )
        hits = _cleanup_search_hits_for_contract(raw)

        if hasattr(doc, "last_checked_at"):
            doc.last_checked_at = now
            await db.commit()

    logger.info(
        "[prod_v1] indexed ext_doc=%s internal_id=%s org=%s shard=%s hits=%s",
        req.document_id,
        getattr(doc, "id", None),
        req.organization_id,
        shard_id,
        len(hits),
    )

    return await build_backend_contract_and_persist(
        db,
        req=req,
        hits=hits,
        processed_at=now,
        shard_id=shard_id,
    )
