# app/services/search/search_pipeline.py
from typing import Dict, Any, List, Set, Tuple

from ..indexing.bm25_index import bm25_candidates
from .index_search import search_cached   # останется только для debug/резерва
from .postsearch_semantic import rerank_search_result
from ..search_native import native_search


def search_bm25_shingles_rerank(
    qtext: str,
    *,
    bm25_top: int = 300,
    shingle_top: int = 50,
    final_top: int = 10,
    use_rerank: bool = True,
    backend: str = "native",  # "native" | "python"
) -> Dict[str, Any]:
    q = (qtext or "").strip()
    if not q:
        return {"hits_total": 0, "docs_found": 0, "documents": []}

    # 1) BM25 — кандидаты
    try:
        bm25_list: List[Tuple[str, float]] = bm25_candidates(q, top_docs=bm25_top)
    except FileNotFoundError:
        bm25_list = []

    allowed: Set[str] | None = None
    if bm25_list:
        allowed = {did for did, _ in bm25_list}

    # 2) Шингловый поиск
    shingle_res = native_search(q, top=shingle_top, allowed_doc_ids=allowed)

    docs = shingle_res.get("documents") or []
    if not docs:
        return shingle_res

    # 3) Семантический rerank
    if not use_rerank:
        docs = docs[:final_top]
        shingle_res["documents"] = docs
        shingle_res["docs_found"] = len(docs)
        return shingle_res

    shingle_res = rerank_search_result(q, shingle_res)
    docs = (shingle_res.get("documents") or [])[:final_top]
    shingle_res["documents"] = docs
    shingle_res["docs_found"] = len(docs)
    return shingle_res
