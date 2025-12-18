from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import INDEX_DIR, UPLOAD_DIR
from app.models.segment import Segment
from app.models.document import Document
from app.services.levels_0_4.native_segments import (
    seg_search_many,
    seg_search_windowed_sources,
    seg_excerpt_for_span,
)
from app.services.helpers.file_extract import extract_text_from_file_bytes

K_SHINGLE = 9
MIN_SPAN_SHINGLES = 6
MAX_SPANS_PER_HIT = 3

# How many tokens we allow to "bridge" between adjacent covered intervals.
# With K=9, the theoretical "stitch gap" around source boundaries is up to K-1=8 tokens.
C_TOK_GAP_CLOSE = K_SHINGLE - 1  # 8


def _load_upload_meta(external_id: str) -> Dict[str, Any]:
    p = UPLOAD_DIR / f"{external_id}.meta.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _spans_to_match_spans(spans: Any) -> List[Dict[str, int]]:
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


def _apply_marginal_C_tokens_gapclose(hits: List[Dict[str, Any]]) -> None:
    """
    Makes per-hit C summable to ~1.0 for composite texts by:
      - converting qpos spans to TOKEN spans: tok_to = q_to + (K-1)
      - measuring coverage on token axis
      - closing small "stitch gaps" up to C_TOK_GAP_CLOSE tokens in UNION coverage

    Output per hit:
      - C_doc: original engine c9 (doc-level coverage by unique shingles)
      - C: marginal TOKEN coverage contribution (summable; aims to be close to 1.0)
      - C_total_tokens: total token length used for normalization (optional)
    """
    if not hits:
        return

    # Determine total token length from spans: max(tok_to) + 1
    max_tok = -1
    for h in hits:
        for sp in (h.get("match_spans") or []):
            try:
                q_from = int(sp.get("q_from", -1))
                q_to = int(sp.get("q_to", -1))
            except Exception:
                continue
            if q_from < 0 or q_to < 0 or q_to < q_from:
                continue
            tok_to = q_to + (K_SHINGLE - 1)
            if tok_to > max_tok:
                max_tok = tok_to

    total_tokens = max_tok + 1
    if total_tokens <= 0:
        for h in hits:
            h["C_doc"] = float(h.get("c9", 0.0) or 0.0)
            h["C"] = 0.0
        return

    # covered token positions; gap-closing is handled during union accounting, but marginal needs a consistent rule.
    # We'll use a boolean cover array for exact marginal contributions, and additionally "gap close" after the fact
    # by treating small uncovered runs <= gap as covered in the final union baseline.
    covered = [False] * total_tokens

    # Step 1: mark raw covered positions per hit to compute marginal contributions
    for h in hits:
        h["C_doc"] = float(h.get("c9", 0.0) or 0.0)
        new_cov = 0

        for sp in (h.get("match_spans") or []):
            try:
                q_from = int(sp["q_from"])
                q_to = int(sp["q_to"])
            except Exception:
                continue
            if q_to < q_from:
                continue

            tok_from = q_from
            tok_to = q_to + (K_SHINGLE - 1)

            if tok_from < 0:
                tok_from = 0
            if tok_to < tok_from:
                tok_to = tok_from
            if tok_from >= total_tokens:
                continue
            if tok_to >= total_tokens:
                tok_to = total_tokens - 1

            for i in range(tok_from, tok_to + 1):
                if not covered[i]:
                    covered[i] = True
                    new_cov += 1

        # provisional marginal (without gap-closing)
        h["C"] = new_cov / total_tokens

    # Step 2: apply gap-closing to make union closer to 1.0 for stitch gaps.
    # We need to adjust per-hit marginals so their sum equals the gap-closed union.
    # We'll do that by computing additional "virtual covered" tokens from closing gaps,
    # and attributing them to the nearest hit boundary (previous hit) deterministically.
    gap = int(C_TOK_GAP_CLOSE)
    if gap <= 0:
        return

    # Rebuild "owner" array: for each token, which hit first covered it (in hit order).
    owner = [-1] * total_tokens
    for idx, h in enumerate(hits):
        for sp in (h.get("match_spans") or []):
            try:
                q_from = int(sp["q_from"])
                q_to = int(sp["q_to"])
            except Exception:
                continue
            if q_to < q_from:
                continue
            tok_from = max(0, q_from)
            tok_to = q_to + (K_SHINGLE - 1)
            if tok_to >= total_tokens:
                tok_to = total_tokens - 1
            for i in range(tok_from, tok_to + 1):
                if owner[i] == -1:
                    owner[i] = idx

    # Close gaps: find uncovered runs whose length <= gap and assign them to the previous non-empty owner.
    i = 0
    added = [0] * len(hits)
    last_owner = -1
    while i < total_tokens:
        if owner[i] != -1:
            last_owner = owner[i]
            i += 1
            continue

        j = i
        while j < total_tokens and owner[j] == -1:
            j += 1
        run_len = j - i

        if 0 < run_len <= gap and last_owner != -1:
            # attribute these "stitch" tokens to last_owner
            added[last_owner] += run_len
            # also fill owner for completeness
            for t in range(i, j):
                owner[t] = last_owner

        i = j

    if any(added):
        for idx, add_n in enumerate(added):
            if add_n > 0:
                hits[idx]["C"] = float(hits[idx].get("C", 0.0)) + (add_n / total_tokens)

    # Optional: expose total_tokens used for normalization
    for h in hits:
        h["C_total_tokens"] = total_tokens


async def _load_doc_text_and_index_norm(db: AsyncSession, doc_id: int) -> Tuple[Optional[str], bool]:
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


async def _collect_index_dirs(
    db: AsyncSession,
    *,
    organization_id: int,
    shard_id: int,
) -> Tuple[List[str], Dict[str, Dict[str, Any]]]:
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

    return index_dirs, by_dir


async def search_sources_windowed_1_4(
    db: AsyncSession,
    *,
    organization_id: int,
    shard_id: int,
    query: str,
    top_k: int = 20,
    win_tokens: int = 120,
    stride_tokens: int = 60,
) -> Dict[str, Any]:
    """
    Returns:
      {"C": float, "segments":[{"source_doc_id","q_tok_from","q_tok_to"}, ...]}
    """
    index_dirs, _by_dir = await _collect_index_dirs(db, organization_id=organization_id, shard_id=shard_id)

    data = seg_search_windowed_sources(
        query=query,
        top_k=top_k,
        index_dirs=index_dirs,
        normalize_query=False,
        win_tokens=win_tokens,
        stride_tokens=stride_tokens,
    )

    segs = data.get("segments")
    if not isinstance(segs, list):
        segs = []

    out_segs: List[Dict[str, Any]] = []
    for s in segs:
        if not isinstance(s, dict):
            continue
        out_segs.append(
            {
                "source_doc_id": str(s.get("source_doc_id", "")),
                "q_tok_from": int(s.get("q_tok_from", 0) or 0),
                "q_tok_to": int(s.get("q_tok_to", 0) or 0),
            }
        )

    c_val = data.get("C", 0.0)
    if not isinstance(c_val, (int, float)):
        c_val = 0.0

    return {"C": float(c_val), "segments": out_segs}


async def search_levels_1_4(
    db: AsyncSession,
    *,
    organization_id: int,
    shard_id: int,
    query: str,
    top_k: int = 20,
    include_user_view: bool = True,
    excerpt_max_chars: int = 800,
    normalize_query: bool = False,
) -> Dict[str, Any]:
    """
    Flat search: returns {"count","hits":[...]}.

    IMPORTANT:
      - Per-hit C is rewritten to be a *summable* marginal TOKEN coverage contribution.
      - Original doc-level engine coverage remains as C_doc.
    """
    normalize_query = False  # prod safety

    index_dirs, by_dir = await _collect_index_dirs(db, organization_id=organization_id, shard_id=shard_id)

    data = seg_search_many(
        query=query,
        top_k=top_k,
        index_dirs=index_dirs,
        include_matches=False,
        max_matches_per_doc=None,
        normalize_query=normalize_query,
    )

    hits = data.get("hits") or []
    if not isinstance(hits, list):
        hits = []
        data["hits"] = hits

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

    # NEW: rewrite C to token-marginal + stitch-gap closing
    _apply_marginal_C_tokens_gapclose([h for h in hits if isinstance(h, dict)])

    if include_user_view:
        doc_cache: Dict[int, Tuple[Optional[str], bool]] = {}

        for h in hits:
            if not isinstance(h, dict):
                continue
            try:
                doc_id_int = int(h.get("doc_id"))
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

    data["count"] = int(len([h for h in hits if isinstance(h, dict)]))
    return data
