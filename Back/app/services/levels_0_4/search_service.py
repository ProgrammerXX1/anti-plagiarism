from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import INDEX_DIR, UPLOAD_DIR
from app.models.segment import Segment
from app.models.document import Document
from app.services.levels_0_4.native_segments import seg_search_many
from app.services.helpers.file_extract import extract_text_from_file_bytes

K_SHINGLE = 9


def _build_match_spans(q_pos: List[int], d_pos: List[int]) -> List[Dict[str, int]]:
    if not q_pos or not d_pos or len(q_pos) != len(d_pos):
        return []

    spans: List[Dict[str, int]] = []
    q_start = q_pos[0]
    d_start = d_pos[0]
    q_prev = q_pos[0]
    d_prev = d_pos[0]

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


def _normalize_for_shingles_simple_py(text: str) -> str:
    """
    Python-side approx of C++ normalize_for_shingles_simple.
    IMPORTANT: for exact char offsets you'd need to share the same normalization logic.
    For user-readable excerpts this is sufficient.
    """
    # минимально: lower + collapse spaces
    t = text.lower()
    t = " ".join(t.split())
    return t


def _tokenize_simple(norm_text: str) -> List[str]:
    # простой токенайзер; если хочешь 1-в-1, лучше вынести из C++ в shared lib
    return norm_text.split()


def _excerpt_from_tokens(tokens: List[str], tok_from: int, tok_to: int, max_tokens: int = 80) -> str:
    if not tokens:
        return ""
    tok_from = max(0, min(tok_from, len(tokens)))
    tok_to = max(0, min(tok_to, len(tokens) - 1))
    if tok_from > tok_to:
        return ""

    length = tok_to - tok_from + 1
    if length > max_tokens:
        tok_to = tok_from + max_tokens - 1

    piece = tokens[tok_from : tok_to + 1]
    return " ".join(piece)


async def _load_doc_text(db: AsyncSession, doc_id: int) -> Optional[str]:
    """
    Берёт Document.external_id -> UPLOAD_DIR -> extract_text_from_file_bytes.
    """
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
    include_user_view: bool = True,     # <- главное: читабельные фрагменты
    keep_raw_matches: bool = False,     # <- по умолчанию скрываем мусор для UI
    excerpt_max_tokens: int = 80,       # сколько токенов показывать
) -> Dict[str, Any]:
    # 1) сегменты
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

    # 2) нативный поиск
    data = seg_search_many(
        query=query,
        top_k=top_k,
        index_dirs=index_dirs,
        include_matches=include_matches,
        max_matches_per_doc=max_matches_per_doc,
    )

    hits = data.get("hits") or []

    # 3) enrich segment meta + spans
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
                h["match_spans"] = _build_match_spans(q_pos, d_pos)
            else:
                h["match_spans"] = []

    # 4) user_view: excerpts (дорого, но читаемо)
    if include_user_view and include_matches:
        # Чтобы не читать/извлекать один и тот же doc много раз
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

            norm = _normalize_for_shingles_simple_py(raw_text)
            tokens = _tokenize_simple(norm)

            uv_spans = []
            for sp in spans:
                d_from = int(sp["d_from"])
                d_to = int(sp["d_to"])

                # перевод шингл-оффсетов в токен-оффсеты
                tok_from = d_from
                tok_to = d_to + (K_SHINGLE - 1)

                excerpt = _excerpt_from_tokens(tokens, tok_from, tok_to, max_tokens=excerpt_max_tokens)

                uv_spans.append(
                    {
                        "doc_token_from": tok_from,
                        "doc_token_to": tok_to,
                        "doc_shingle_from": d_from,
                        "doc_shingle_to": d_to,
                        "shingles": int(sp["length"]),
                        "excerpt": excerpt,
                    }
                )

            # короткое описание
            total_sh = sum(int(sp.get("length", 0)) for sp in spans)
            h["user_view"] = {
                "summary": f"Найдено {len(spans)} фрагм., совпало {total_sh} шинглов",
                "spans": uv_spans,
            }

    # 5) скрыть сырой мусор, если надо
    if include_matches and not keep_raw_matches:
        for h in hits:
            h.pop("matches", None)
            # match_spans можно оставить (это уже норм)

    return data
