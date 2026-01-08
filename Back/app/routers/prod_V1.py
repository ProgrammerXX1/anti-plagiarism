from __future__ import annotations

from functools import partial
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from app.models.segment import Segment
import anyio
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func, or_, cast, String
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.helpers.file_extract import extract_text_from_file_bytes
from app.core.config import UPLOAD_DIR, N_SHARDS
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
class DocumentListItem(BaseModel):
    internal_doc_id: int
    external_id: Optional[str] = None
    organization_id: int
    shard_id: int
    status: str
    title: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class DocumentContentResponse(BaseModel):
    internal_doc_id: int
    external_id: Optional[str] = None
    organization_id: int
    shard_id: int
    status: str
    file_path: str
    meta: Dict[str, Any] = {}
    text: str
    truncated: bool = False
    total_chars: int = 0

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
    source_id: str                # internal doc_id (string) — joins to SourceItem.id
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

def _upload_file_path_best_effort(doc: Document) -> Optional[Path]:
    """
    Tries multiple common layouts:

    1) UPLOAD_DIR/{doc.id}.txt
    2) UPLOAD_DIR/{doc.id}
    3) UPLOAD_DIR/{external_id}.txt
    4) UPLOAD_DIR/{external_id}
    5) UPLOAD_DIR/{external_id}.* (pdf/docx/etc.)
    """
    if getattr(doc, "id", None) is None:
        return None

    p1 = UPLOAD_DIR / f"{int(doc.id)}.txt"
    if p1.exists():
        return p1

    p1b = UPLOAD_DIR / f"{int(doc.id)}"
    if p1b.exists():
        return p1b

    ext = getattr(doc, "external_id", None)
    if ext:
        ext_s = str(ext)

        p2 = UPLOAD_DIR / f"{ext_s}.txt"
        if p2.exists():
            return p2

        p3 = UPLOAD_DIR / ext_s
        if p3.exists():
            return p3

        matches = sorted(UPLOAD_DIR.glob(f"{ext_s}.*"))
        if matches:
            return matches[0]

    return None


def _read_text_from_path_best_effort(p: Path) -> str:
    raw = p.read_bytes()
    if p.suffix.lower() == ".txt":
        return raw.decode("utf-8", errors="ignore")
    return extract_text_from_file_bytes(raw, filename=str(p))



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


def _load_upload_meta_best_effort(doc: Optional[Document]) -> Dict[str, Any]:
    """
    New layout:  UPLOAD_DIR/{doc.id}.meta.json
    Legacy:      UPLOAD_DIR/{doc.external_id}.meta.json   (when external_id used to be a file-key)
    """
    if not doc:
        return {}

    candidates: List[Path] = []
    if getattr(doc, "id", None) is not None:
        candidates.append(UPLOAD_DIR / f"{int(doc.id)}.meta.json")
    if getattr(doc, "external_id", None):
        candidates.append(UPLOAD_DIR / f"{str(doc.external_id)}.meta.json")

    for p in candidates:
        if not p.exists():
            continue
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _load_source_text_best_effort(doc: Optional[Document]) -> str:
    """
    Symmetric with indexing:
      New layout: UPLOAD_DIR/{doc.id}.txt (utf-8)
      Legacy:     UPLOAD_DIR/{doc.external_id} (file-key, may be .txt/.pdf/.docx/...)
    """
    if not doc:
        return ""

    candidates: List[Path] = []
    if getattr(doc, "id", None) is not None:
        candidates.append(UPLOAD_DIR / f"{int(doc.id)}.txt")
    if getattr(doc, "external_id", None):
        candidates.append(UPLOAD_DIR / str(doc.external_id))

    for p in candidates:
        if not p.exists():
            continue
        try:
            raw = p.read_bytes()
            if p.suffix.lower() == ".txt":
                return raw.decode("utf-8", errors="ignore")
            return extract_text_from_file_bytes(raw, filename=str(p))
        except Exception:
            return ""
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

async def build_backend_contract(
    db: AsyncSession,
    *,
    req: ProdV1IngestRequest,
    hits: List[Dict[str, Any]],
    processed_at: datetime,
) -> BackendContractResponse:
    """
    PURE builder (no commits).
    DB reads are allowed (Document fetch), but no writes and no commit/rollback.
    """
    processed_at_str = processed_at.isoformat()

    uniq_doc_ids: List[str] = []
    seen: set[str] = set()
    for h in hits:
        did = str(h.get("doc_id", "")).strip()
        if did and did not in seen:
            seen.add(did)
            uniq_doc_ids.append(did)

    doc_cache: Dict[str, Optional[Document]] = {}
    for did in uniq_doc_ids:
        doc_cache[did] = None
        if not did.isdigit():
            continue
        try:
            doc_cache[did] = await db.get(Document, int(did))
        except Exception:
            doc_cache[did] = None

    sources: List[SourceItem] = []
    for did in uniq_doc_ids:
        doc = doc_cache.get(did)
        meta = _load_upload_meta_best_effort(doc)

        external_source_id = str(meta.get("source_id") or did)

        if doc and getattr(doc, "created_at", None):
            idx_date = doc.created_at.isoformat()
        else:
            idx_date = processed_at_str

        sources.append(
            SourceItem(
                id=did,
                source_id=external_source_id,
                module_id=str(meta.get("module_id") or "plagiarism"),
                name=str((getattr(doc, "title", None) or meta.get("title") or "")),
                url=meta.get("url"),
                author=(str(getattr(doc, "student_name", None) or meta.get("author") or "") or None),
                index_date=str(idx_date),
            )
        )

    matchsources: List[MatchSourceItem] = []

    normalize_query_offsets = False
    match_id = 1

    for h in hits:
        did = str(h.get("doc_id", "")).strip()
        if not did:
            continue

        doc = doc_cache.get(did)
        source_text = _load_source_text_best_effort(doc)

        source_norm = False

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

            matchsources.append(
                MatchSourceItem(
                    id=str(match_id),
                    source_id=did,
                    q_offset=int(q_offset),
                    q_limit=int(q_limit),
                    s_offset=int(s_offset),
                    s_limit=int(s_limit),
                    type="1",
                )
            )
            match_id += 1

    c_sum = 0.0
    for h in hits:
        c_sum += float(h.get("C", 0.0) or 0.0)
    c_sum = min(1.0, max(0.0, c_sum))

    plagiarism_pct = round(c_sum * 100.0, 2)
    unknown_pct = round(max(0.0, 100.0 - plagiarism_pct), 2)

    return BackendContractResponse(
        document_id=req.document_id,  # <-- внешний id из запроса
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


async def persist_report(
    db: AsyncSession,
    *,
    req: ProdV1IngestRequest,
    shard_id: int,
    processed_at: datetime,
    contract: BackendContractResponse,
    internal_doc_id: Optional[int] = None,
    doc_cache: Optional[Dict[str, Optional[Document]]] = None,
) -> None:
    """
    DB writer.
    Caller controls commit/rollback scope.
    """
    report = await upsert_report(
        db,
        organization_id=req.organization_id,
        shard_id=shard_id,
        document_id=req.document_id,  # <-- внешний id
        status=contract.status,
        processed_at=processed_at,
        plagiarism_percentage=contract.plagiarism_percentage,
        selfcite_percentage=contract.selfcite_percentage,
        legal_percentage=contract.legal_percentage,
        unknown_percentage=contract.unknown_percentage,
        internal_doc_id=internal_doc_id,
    )

    db_sources: List[PlagiarismReportSource] = []
    for s in contract.sources:
        doc_dt = None
        if doc_cache is not None:
            d = doc_cache.get(s.id)
            if d and getattr(d, "created_at", None):
                doc_dt = d.created_at

        db_sources.append(
            PlagiarismReportSource(
                report_id=report.id,
                source_id=s.id,
                internal_source_doc_id=int(s.id) if s.id.isdigit() else None,
                module_id=s.module_id,
                name=s.name,
                url=s.url,
                author=s.author,
                index_date=(doc_dt or processed_at),
            )
        )

    db_matches: List[PlagiarismReportMatch] = []
    for m in contract.matchsources:
        db_matches.append(
            PlagiarismReportMatch(
                report_id=report.id,
                source_id=m.source_id,
                q_offset=m.q_offset,
                q_limit=m.q_limit,
                s_offset=m.s_offset,
                s_limit=m.s_limit,
                type=m.type,
            )
        )

    await replace_report_children(db, report_id=report.id, sources=db_sources, matches=db_matches)


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

        uniq: List[str] = []
        seen: set[str] = set()
        for h in hits:
            did = str(h.get("doc_id", "")).strip()
            if did and did not in seen:
                seen.add(did)
                uniq.append(did)

        doc_cache: Dict[str, Optional[Document]] = {}
        for did in uniq:
            doc_cache[did] = None
            if did.isdigit():
                try:
                    doc_cache[did] = await db.get(Document, int(did))
                except Exception:
                    doc_cache[did] = None

        contract = await build_backend_contract(db, req=req, hits=hits, processed_at=now)

        try:
            await persist_report(
                db,
                req=req,
                shard_id=shard_id,
                processed_at=now,
                contract=contract,
                internal_doc_id=None,
                doc_cache=doc_cache,
            )
            await db.commit()
        except Exception:
            await db.rollback()
            raise

        return contract

    # ── INDEX ─────────────────────────────────
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

    # 1) create DB row first (store external document_id in DB)
    doc = Document(
        external_id=req.document_id,  # <-- внешний id от другого сервиса
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
        await db.flush()  # получаем doc.id без commit
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"DB flush failed: {e}")

    # 2) write files by internal doc.id (safe unique filenames)
    file_path = UPLOAD_DIR / f"{int(doc.id)}.txt"
    meta_path = UPLOAD_DIR / f"{int(doc.id)}.meta.json"

    meta: Dict[str, Any] = {
        "organization_id": int(req.organization_id),
        "internal_doc_id": int(doc.id),
        "document_id": str(req.document_id),
        "title": req.title,
        "author": req.author,
        "source_created_at": req.created_at,
        "file_name": req.file_name,
        "enable_ocr": bool(req.enable_ocr),
        "saved_at": now.isoformat(),

        "text_is_normalized": True,
        "index_normalize": False,

        "module_id": "plagiarism",
        "source_id": str(req.document_id),
        "url": None,
    }

    try:
        await anyio.to_thread.run_sync(lambda: _write_text_atomic(file_path, req.text))
        await anyio.to_thread.run_sync(lambda: _write_json_atomic(meta_path, meta))
    except Exception as e:
        await db.rollback()
        for p in (file_path, meta_path):
            try:
                if p.exists():
                    p.unlink()
            except Exception:
                pass
        raise HTTPException(status_code=500, detail=f"Failed to persist upload: {e}")

    # 3) optional search
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

    uniq: List[str] = []
    seen: set[str] = set()
    for h in hits:
        did = str(h.get("doc_id", "")).strip()
        if did and did not in seen:
            seen.add(did)
            uniq.append(did)

    doc_cache: Dict[str, Optional[Document]] = {}
    for did in uniq:
        doc_cache[did] = None
        if did.isdigit():
            try:
                doc_cache[did] = await db.get(Document, int(did))
            except Exception:
                doc_cache[did] = None

    contract = await build_backend_contract(db, req=req, hits=hits, processed_at=now)

    try:
        await persist_report(
            db,
            req=req,
            shard_id=shard_id,
            processed_at=now,
            contract=contract,
            internal_doc_id=int(doc.id),
            doc_cache=doc_cache,
        )
        await db.commit()
        await db.refresh(doc)
    except Exception:
        await db.rollback()
        for p in (file_path, meta_path):
            try:
                if p.exists():
                    p.unlink()
            except Exception:
                pass
        raise

    return contract

@router.get("/documents", response_model=List[DocumentListItem])
async def list_documents(
    organization_id: int = Query(..., ge=1),
    q: Optional[str] = Query(None, description="search by title or external_id"),
    status: Optional[str] = Query(None, description="filter by document status"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
) -> List[DocumentListItem]:
    _validate_org_id(organization_id)
    shard_id = compute_shard_id(organization_id)

    stmt = (
        select(Document)
        .where(
            Document.organization_id == organization_id,
            Document.shard_id == shard_id,
        )
        .order_by(Document.id.desc())
        .limit(limit)
        .offset(offset)
    )

    if status:
        stmt = stmt.where(Document.status == status)

    if q:
        q_like = f"%{q.strip()}%"
        stmt = stmt.where(
            or_(
                Document.title.ilike(q_like),
                func.cast(Document.external_id, str).ilike(q_like),  # best-effort
            )
        )

    res = await db.execute(stmt)
    docs: List[Document] = list(res.scalars())

    out: List[DocumentListItem] = []
    for d in docs:
        out.append(
            DocumentListItem(
                internal_doc_id=int(d.id),
                external_id=(str(d.external_id) if getattr(d, "external_id", None) is not None else None),
                organization_id=int(d.organization_id or 0),
                shard_id=int(d.shard_id or 0),
                status=str(d.status or ""),
                title=(str(d.title) if getattr(d, "title", None) is not None else None),
                created_at=(d.created_at.isoformat() if getattr(d, "created_at", None) else None),
                updated_at=(d.updated_at.isoformat() if getattr(d, "updated_at", None) else None),
            )
        )
    return out


# ───────────────────────────────────────────────
# API: get document content by internal id
# ───────────────────────────────────────────────

@router.get("/documents/{internal_doc_id}/content", response_model=DocumentContentResponse)
async def get_document_content(
    internal_doc_id: int,
    organization_id: int = Query(..., ge=1),
    max_chars: int = Query(200_000, ge=1, le=5_000_000),
    include_meta: bool = Query(True),
    db: AsyncSession = Depends(get_db),
) -> DocumentContentResponse:
    _validate_org_id(organization_id)

    doc = await db.get(Document, int(internal_doc_id))
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")

    # tenant guard (minimal)
    if int(getattr(doc, "organization_id", 0) or 0) != int(organization_id):
        raise HTTPException(status_code=404, detail="Document not found")

    p = _upload_file_path_best_effort(doc)
    if not p or not p.exists():
        raise HTTPException(status_code=404, detail="Uploaded file not found")

    try:
        text = await anyio.to_thread.run_sync(lambda: _read_text_from_path_best_effort(p))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read/extract text: {e}")

    total_chars = len(text)
    truncated = False
    if total_chars > max_chars:
        text = text[:max_chars]
        truncated = True

    meta: Dict[str, Any] = {}
    if include_meta:
        meta = _load_upload_meta_best_effort(doc)

    return DocumentContentResponse(
        internal_doc_id=int(doc.id),
        external_id=(str(doc.external_id) if getattr(doc, "external_id", None) is not None else None),
        organization_id=int(doc.organization_id or 0),
        shard_id=int(doc.shard_id or 0),
        status=str(doc.status or ""),
        file_path=str(p),
        meta=meta,
        text=text,
        truncated=truncated,
        total_chars=total_chars,
    )


# ───────────────────────────────────────────────
# API: get document content by external id (document_id from other service)
# ───────────────────────────────────────────────

@router.get("/documents/by-external/{external_id}/content", response_model=DocumentContentResponse)
async def get_document_content_by_external_id(
    external_id: str,
    organization_id: int = Query(..., ge=1),
    max_chars: int = Query(200_000, ge=1, le=5_000_000),
    include_meta: bool = Query(True),
    db: AsyncSession = Depends(get_db),
) -> DocumentContentResponse:
    _validate_org_id(organization_id)
    shard_id = compute_shard_id(organization_id)

    stmt = (
        select(Document)
        .where(
            Document.organization_id == organization_id,
            Document.shard_id == shard_id,
            Document.external_id == external_id,
        )
        .order_by(Document.id.desc())
        .limit(1)
    )
    res = await db.execute(stmt)
    doc = res.scalars().first()
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")

    p = _upload_file_path_best_effort(doc)
    if not p or not p.exists():
        raise HTTPException(status_code=404, detail="Uploaded file not found")

    try:
        text = await anyio.to_thread.run_sync(lambda: _read_text_from_path_best_effort(p))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read/extract text: {e}")

    total_chars = len(text)
    truncated = False
    if total_chars > max_chars:
        text = text[:max_chars]
        truncated = True

    meta: Dict[str, Any] = {}
    if include_meta:
        meta = _load_upload_meta_best_effort(doc)

    return DocumentContentResponse(
        internal_doc_id=int(doc.id),
        external_id=(str(doc.external_id) if getattr(doc, "external_id", None) is not None else None),
        organization_id=int(doc.organization_id or 0),
        shard_id=int(doc.shard_id or 0),
        status=str(doc.status or ""),
        file_path=str(p),
        meta=meta,
        text=text,
        truncated=truncated,
        total_chars=total_chars,
    )

class GlobalSearchRequest(BaseModel):
    text: str = Field(..., min_length=1)
    top_k: int = Field(20, ge=1, le=200)
    normalize_query: bool = False

class GlobalSearchResponse(BaseModel):
    hits: List[Dict[str, Any]]
 
# Add/replace your global search response to include document + file info and preview
# Required imports (top of module)
from sqlalchemy import select, func
from app.models.segment import Segment

# ───────────────────────────────────────────────
# Models
# ───────────────────────────────────────────────

class GlobalSearchHit(BaseModel):
    doc_id: str                    # internal Document.id as string
    C: float
    match_spans: List[Dict[str, Any]] = []

    # enrichment
    organization_id: Optional[int] = None
    shard_id: Optional[int] = None
    external_id: Optional[str] = None
    title: Optional[str] = None
    status: Optional[str] = None
    file_path: Optional[str] = None
    meta_path: Optional[str] = None
    preview: Optional[str] = None  # small text snippet from file
    preview_truncated: bool = False


class GlobalSearchResponse(BaseModel):
    hits: List[GlobalSearchHit]


class GlobalSearchRequest(BaseModel):
    text: str = Field(..., min_length=1)
    top_k: int = Field(20, ge=1, le=200)
    normalize_query: bool = False

    # enrichment knobs
    include_preview: bool = True
    preview_chars: int = Field(600, ge=0, le=20_000)


# ───────────────────────────────────────────────
# Helpers (reuse your existing best-effort readers)
# ───────────────────────────────────────────────

def _best_effort_file_paths(doc: Document) -> tuple[Optional[Path], Optional[Path]]:
    # main text file path (same logic as in your module)
    candidates: List[Path] = []
    if getattr(doc, "id", None) is not None:
        candidates.append(UPLOAD_DIR / f"{int(doc.id)}.txt")
        candidates.append(UPLOAD_DIR / f"{int(doc.id)}")

    if getattr(doc, "external_id", None):
        ext = str(doc.external_id)
        candidates.append(UPLOAD_DIR / f"{ext}.txt")
        candidates.append(UPLOAD_DIR / ext)
        # any ext.*
        matches = sorted(UPLOAD_DIR.glob(f"{ext}.*"))
        candidates.extend(matches)

    file_path = next((p for p in candidates if p.exists()), None)

    # meta path best-effort
    meta_candidates: List[Path] = []
    if getattr(doc, "id", None) is not None:
        meta_candidates.append(UPLOAD_DIR / f"{int(doc.id)}.meta.json")
    if getattr(doc, "external_id", None):
        meta_candidates.append(UPLOAD_DIR / f"{str(doc.external_id)}.meta.json")
    meta_path = next((p for p in meta_candidates if p.exists()), None)

    return file_path, meta_path


async def _load_preview_for_doc(*, doc: Document, max_chars: int) -> tuple[Optional[str], bool, Optional[str]]:
    """
    Returns (preview_text, truncated, file_path_str)
    """
    if max_chars <= 0:
        return None, False, None

    fp, _ = _best_effort_file_paths(doc)
    if not fp:
        return None, False, None

    def _read() -> str:
        raw = fp.read_bytes()
        if fp.suffix.lower() == ".txt":
            return raw.decode("utf-8", errors="ignore")
        return extract_text_from_file_bytes(raw, filename=str(fp))

    try:
        text = await anyio.to_thread.run_sync(_read)
    except Exception:
        return None, False, str(fp)

    if len(text) > max_chars:
        return text[:max_chars], True, str(fp)
    return text, False, str(fp)


# ───────────────────────────────────────────────
# API: global search with file info
# ───────────────────────────────────────────────

@router.post("/search-global", response_model=GlobalSearchResponse)
async def search_global(
    req: GlobalSearchRequest,
    db: AsyncSession = Depends(get_db),
) -> GlobalSearchResponse:
    # 1) scopes where ready segments exist
    rows = await db.execute(
        select(Segment.organization_id, Segment.shard_id)
        .where(Segment.status == "ready")
        .distinct()
    )
    scopes = [(int(o), int(s)) for (o, s) in rows.fetchall() if o is not None and s is not None]
    if not scopes:
        return GlobalSearchResponse(hits=[])

    # 2) search each scope
    all_hits: List[Dict[str, Any]] = []
    for org_id, shard_id in scopes:
        raw = await search_levels_1_4(
            db,
            organization_id=org_id,
            shard_id=shard_id,
            query=req.text,
            normalize_query=bool(req.normalize_query),
        )
        cleaned = _cleanup_search_hits_for_contract(raw)
        # annotate scope in each hit (so we can show org/shard)
        for h in cleaned:
            h["_org_id"] = org_id
            h["_shard_id"] = shard_id
        all_hits.extend(cleaned)

    # 3) merge best by doc_id (keep highest C, but also keep its scope)
    best: Dict[str, Dict[str, Any]] = {}
    for h in all_hits:
        did = str(h.get("doc_id", "")).strip()
        if not did:
            continue
        c = float(h.get("C", 0.0) or 0.0)
        prev = best.get(did)
        if prev is None or c > float(prev.get("C", 0.0) or 0.0):
            best[did] = h

    merged = sorted(best.values(), key=lambda x: float(x.get("C", 0.0) or 0.0), reverse=True)
    merged = merged[: int(req.top_k)]

    # 4) enrich with Document row + file paths + preview
    # bulk fetch documents by internal ids
    doc_ids: List[int] = [int(h["doc_id"]) for h in merged if str(h.get("doc_id", "")).isdigit()]
    docs_by_id: Dict[int, Document] = {}
    if doc_ids:
        res_docs = await db.execute(select(Document).where(Document.id.in_(doc_ids)))
        for d in res_docs.scalars():
            docs_by_id[int(d.id)] = d

    out_hits: List[GlobalSearchHit] = []
    for h in merged:
        did_s = str(h.get("doc_id", "")).strip()
        d: Optional[Document] = docs_by_id.get(int(did_s)) if did_s.isdigit() else None

        file_path_str: Optional[str] = None
        meta_path_str: Optional[str] = None
        preview: Optional[str] = None
        preview_tr = False

        if d is not None:
            fp, mp = _best_effort_file_paths(d)
            file_path_str = str(fp) if fp else None
            meta_path_str = str(mp) if mp else None

            if req.include_preview and req.preview_chars > 0:
                preview, preview_tr, _ = await _load_preview_for_doc(doc=d, max_chars=int(req.preview_chars))

        out_hits.append(
            GlobalSearchHit(
                doc_id=did_s,
                C=float(h.get("C", 0.0) or 0.0),
                match_spans=h.get("match_spans") or [],
                organization_id=int(h.get("_org_id")) if h.get("_org_id") is not None else None,
                shard_id=int(h.get("_shard_id")) if h.get("_shard_id") is not None else None,
                external_id=(str(getattr(d, "external_id")) if d is not None and getattr(d, "external_id", None) is not None else None),
                title=(str(getattr(d, "title")) if d is not None and getattr(d, "title", None) is not None else None),
                status=(str(getattr(d, "status")) if d is not None and getattr(d, "status", None) is not None else None),
                file_path=file_path_str,
                meta_path=meta_path_str,
                preview=preview,
                preview_truncated=bool(preview_tr),
            )
        )

    return GlobalSearchResponse(hits=out_hits)
