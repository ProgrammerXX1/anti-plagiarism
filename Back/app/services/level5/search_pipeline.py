# app/services/level5/search_pipeline.py
from typing import Dict, Any, List, Set, Tuple

from ..level5.bm25_index import bm25_candidates
from .search_native import native_search


def search_bm25_native(
    qtext: str,
    *,
    bm25_top: int = 300,
    shingle_top: int = 50,
    final_top: int = 10,
) -> Dict[str, Any]:
    """
    Лёгкий пайплайн без реранка:

    1) BM25 → список doc_id-кандидатов.
    2) C++-поиск по шинглам (native_search) с ограничением по allowed_doc_ids.
    3) Обрезаем до final_top и возвращаем.
    """
    q = (qtext or "").strip()
    if not q:
        return {"hits_total": 0, "docs_found": 0, "documents": []}

    # 1) BM25 — кандидаты (Python, но это относительно дешёвая часть)
    try:
        bm25_list: List[Tuple[str, float]] = bm25_candidates(q, top_docs=bm25_top)
    except FileNotFoundError:
        # если BM25 ещё не построен — просто пробуем поиск по всем документам
        bm25_list = []

    allowed: Set[str] | None = None
    if bm25_list:
        allowed = {did for did, _ in bm25_list}

    # 2) Чисто C++-поиск
    se_res = native_search(q, top=shingle_top, allowed_doc_ids=allowed)

    docs = se_res.get("documents") or []
    if not docs:
        return se_res

    # 3) Никакого реранка — просто обрежем до final_top
    docs = docs[:final_top]
    se_res["documents"] = docs
    se_res["docs_found"] = len(docs)
    # hits_total оставляем как есть (количество кандидатов до обрезки)
    return se_res
