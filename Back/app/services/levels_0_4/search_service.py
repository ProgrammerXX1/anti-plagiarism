# app/services/levels_0_4/search_service.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import INDEX_DIR, UPLOAD_DIR
from app.models.segment import Segment
from app.models.document import Document
from app.services.levels_0_4.native_segments import seg_search_many, seg_excerpt_for_span
from app.services.helpers.file_extract import extract_text_from_file_bytes

K_SHINGLE = 9
MIN_SPAN_SHINGLES = 6
MAX_SPANS_PER_HIT = 3


def _load_upload_meta(external_id: str) -> Dict[str, Any]:
    p = UPLOAD_DIR / f"{external_id}.meta.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _spans_to_match_spans(spans: Any) -> List[Dict[str, int]]:
    """
    C++ returns spans: [{q_from,q_to,d_from,d_to,length,(delta)}, ...]
    External API should NOT expose delta.
    """
    if not isinstance(spans, list):
        return []

    out: List[Dict[str, int]] = []
    for s in spans:
        if not isinstance(s, dict):
            continue
        try:
            length = int(s.get("length", 0))
            if length < MIN_SPAN_SHINGLES:
                continue
            out.append(
                {
                    "q_from": int(s["q_from"]),
                    "q_to": int(s["q_to"]),
                    "d_from": int(s["d_from"]),
                    "d_to": int(s["d_to"]),
                    "length": length,
                }
            )
        except Exception:
            continue

    out.sort(key=lambda x: int(x.get("length", 0)), reverse=True)
    return out[:MAX_SPANS_PER_HIT]


async def _load_doc_text_and_index_norm(db: AsyncSession, doc_id: int) -> Tuple[Optional[str], bool]:
    """
    Returns (raw_text, index_normalize).
    PROD: router stores text already normalized; index_normalize expected False.
    """
    doc = await db.get(Document, doc_id)
    if not doc or not doc.external_id:
        return None, False

    file_path = UPLOAD_DIR / doc.external_id
    if not file_path.exists():
        return None, False

    meta = _load_upload_meta(doc.external_id)
    index_normalize = bool(meta.get("index_normalize", False))

    try:
        raw = file_path.read_bytes()
        if file_path.suffix.lower() == ".txt":
            return raw.decode("utf-8", errors="ignore"), index_normalize
        return extract_text_from_file_bytes(raw, filename=str(file_path)), index_normalize
    except Exception:
        return None, index_normalize


async def search_levels_1_4(
    db: AsyncSession,
    *,
    organization_id: int,
    shard_id: int,
    query: str,
    top_k: int = 20,
    include_user_view: bool = True,
    excerpt_max_chars: int = 800,
    normalize_query: bool = False,  # PROD: never normalize; keep arg for compatibility
) -> Dict[str, Any]:
    """
    Search in ready segments level 1..4.

    PROD CONTRACT:
      - query is already normalized -> normalize_query must be False
    """
    # Hard safety: prod forbids query normalization
    normalize_query = False

    res = await db.execute(
        select(Segment)
        .where(
            Segment.organization_id == organization_id,
            Segment.shard_id == shard_id,
            Segment.status == "ready",
            Segment.level.in_([1, 2, 3, 4]),
        )
        .order_by(Segment.level.desc(), Segment.id.desc())
    )
    segs: List[Segment] = list(res.scalars())

    index_dirs: List[str] = []
    by_dir: Dict[str, Dict[str, Any]] = {}

    for s in segs:
        if not s.path:
            continue
        d = str(INDEX_DIR / s.path)
        index_dirs.append(d)
        by_dir[d] = {
            "segment_id": int(s.id),
            "segment_level": int(s.level),
            "segment_path": s.path,
            "organization_id": int(s.organization_id or 0),
        }

    data = seg_search_many(
        query=query,
        top_k=top_k,
        index_dirs=index_dirs,
        include_matches=False,
        max_matches_per_doc=None,
        normalize_query=normalize_query,  # always False
    )

    hits = data.get("hits") or []

    # Attach segment meta by index_dir + convert spans
    for h in hits:
        if not isinstance(h, dict):
            continue
        d = h.get("index_dir")
        meta = by_dir.get(d)
        if not meta and d:
            try:
                dd = str(Path(d))
                meta = by_dir.get(dd)
            except Exception:
                meta = None
        if meta:
            h.update(meta)

        h["match_spans"] = _spans_to_match_spans(h.get("spans"))

    if include_user_view:
        doc_cache: Dict[int, Tuple[Optional[str], bool]] = {}

        for h in hits:
            if not isinstance(h, dict):
                continue
            doc_id_str = h.get("doc_id")
            try:
                doc_id_int = int(doc_id_str)
            except Exception:
                doc_id_int = None

            spans = h.get("match_spans") or []
            if not doc_id_int or not spans:
                h["user_view"] = {"summary": "", "spans": []}
                continue

            if doc_id_int not in doc_cache:
                doc_cache[doc_id_int] = await _load_doc_text_and_index_norm(db, doc_id_int)

            raw_text, index_normalize = doc_cache[doc_id_int]
            if not raw_text:
                h["user_view"] = {"summary": "Текст документа недоступен", "spans": []}
                continue

            normalize_text = bool(index_normalize)

            uv_spans = []
            total_sh = 0

            for sp in spans:
                d_from = int(sp["d_from"])
                d_to = int(sp["d_to"])
                length = int(sp.get("length", 0))
                total_sh += length

                ex = seg_excerpt_for_span(
                    text=raw_text,
                    d_from=d_from,
                    d_to=d_to,
                    k_shingle=K_SHINGLE,
                    max_chars=excerpt_max_chars,
                    normalize_text=normalize_text,
                )

                uv_spans.append(
                    {
                        "doc_shingle_from": d_from,
                        "doc_shingle_to": d_to,
                        "shingles": length,
                        "excerpt": ex.get("excerpt", ""),
                        "doc_char_from": ex.get("char_from", 0),
                        "doc_char_to": ex.get("char_to", 0),
                        "tok_from": ex.get("tok_from", None),
                        "tok_to": ex.get("tok_to", None),
                        "ok": bool(ex.get("ok", False)),
                        "normalize_text_applied": bool(normalize_text),
                    }
                )

            h["user_view"] = {
                "summary": f"Найдено {len(spans)} фрагм., совпало {total_sh} шинглов",
                "spans": uv_spans,
            }

    # Keep debug fields from native on top-level
    data["count"] = int(len(hits))
    return data
