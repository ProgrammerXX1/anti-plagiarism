from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

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


def _build_match_spans(q_pos: List[int], d_pos: List[int]) -> List[Dict[str, int]]:
    if not q_pos or not d_pos or len(q_pos) != len(d_pos):
        return []

    spans: List[Dict[str, int]] = []
    q_start = int(q_pos[0])
    d_start = int(d_pos[0])
    q_prev = int(q_pos[0])
    d_prev = int(d_pos[0])

    for i in range(1, len(q_pos)):
        q = int(q_pos[i])
        d = int(d_pos[i])

        if q == q_prev + 1 and d == d_prev + 1:
            q_prev = q
            d_prev = d
            continue

        spans.append(
            {
                "q_from": q_start,
                "q_to": q_prev,
                "d_from": d_start,
                "d_to": d_prev,
                "length": (q_prev - q_start + 1),
            }
        )
        q_start = q
        d_start = d
        q_prev = q
        d_prev = d

    spans.append(
        {
            "q_from": q_start,
            "q_to": q_prev,
            "d_from": d_start,
            "d_to": d_prev,
            "length": (q_prev - q_start + 1),
        }
    )
    return spans


async def _load_doc_text(db: AsyncSession, doc_id: int) -> Optional[str]:
    doc = await db.get(Document, doc_id)
    if not doc or not doc.external_id:
        return None

    file_path = UPLOAD_DIR / doc.external_id
    if not file_path.exists():
        return None

    try:
        raw = file_path.read_bytes()
        return extract_text_from_file_bytes(raw, filename=str(file_path))
    except Exception:
        return None


async def search_levels_1_4(
    db: AsyncSession,
    *,
    shard_id: int,
    query: str,
    top_k: int = 10,
    include_matches: bool = True,
    max_matches_per_doc: Optional[int] = None,
    include_spans: bool = True,
    include_user_view: bool = True,
    keep_raw_matches: bool = False,
    excerpt_max_chars: int = 800,
) -> Dict[str, Any]:
    res = await db.execute(
        select(Segment)
        .where(
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
        by_dir[d] = {"segment_id": s.id, "segment_level": s.level, "segment_path": s.path}

    data = seg_search_many(
        query=query,
        top_k=top_k,
        index_dirs=index_dirs,
        include_matches=include_matches,
        max_matches_per_doc=max_matches_per_doc,
    )

    hits = data.get("hits") or []

    # enrich + spans
    for h in hits:
        d = h.get("index_dir")
        meta = by_dir.get(d)
        if not meta:
            try:
                dd = str(Path(d))
                meta = by_dir.get(dd)
            except Exception:
                meta = None
        if meta:
            h.update(meta)

        if include_spans and include_matches:
            m = h.get("matches")
            if isinstance(m, dict):
                q_pos = m.get("q_pos") or []
                d_pos = m.get("d_pos") or []
                spans = _build_match_spans(q_pos, d_pos)

                # filter + keep biggest spans only (for UX)
                spans = [sp for sp in spans if int(sp.get("length", 0)) >= MIN_SPAN_SHINGLES]
                spans.sort(key=lambda x: int(x.get("length", 0)), reverse=True)
                h["match_spans"] = spans[:MAX_SPANS_PER_HIT]
            else:
                h["match_spans"] = []

    # user_view via C++ excerpt (single normalization)
    if include_user_view and include_matches:
        doc_text_cache: Dict[int, Optional[str]] = {}

        for h in hits:
            doc_id_str = h.get("doc_id")
            try:
                doc_id_int = int(doc_id_str)
            except Exception:
                doc_id_int = None

            spans = h.get("match_spans") or []
            if not doc_id_int or not spans:
                h["user_view"] = {"summary": "", "spans": []}
                continue

            if doc_id_int not in doc_text_cache:
                doc_text_cache[doc_id_int] = await _load_doc_text(db, doc_id_int)

            raw_text = doc_text_cache[doc_id_int]
            if not raw_text:
                h["user_view"] = {"summary": "Текст документа недоступен", "spans": []}
                continue

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
                    }
                )

            h["user_view"] = {
                "summary": f"Найдено {len(spans)} фрагм., совпало {total_sh} шинглов",
                "spans": uv_spans,
            }

    if include_matches and not keep_raw_matches:
        for h in hits:
            h.pop("matches", None)

    return data
