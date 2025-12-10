# app/services/level5/search_pipeline.py
from typing import Dict, Any

from .search_native import native_search


def search_bm25_native(
    qtext: str,
    *,
    bm25_top: int = 300,     # оставлены в сигнатуре для совместимости
    shingle_top: int = 50,
    final_top: int = 10,
) -> Dict[str, Any]:
    """
    Упрощённый пайплайн L5:

    РАНЬШЕ:
      1) BM25 (Python) → doc_id-кандидаты.
      2) C++-поиск по шинглам (native_search) с allowed_doc_ids.
      3) Обрезка до final_top.

    СЕЙЧАС:
      1) Только C++-поиск по шинглам, без BM25.
         bm25_top/shingle_top/final_top используются только как числовые лимиты.
    """
    q = (qtext or "").strip()
    if not q:
        return {"hits_total": 0, "docs_found": 0, "documents": []}

    # ищем сразу final_top, можно использовать shingle_top если хочешь больше кандидатов
    top = max(1, int(final_top or 10))

    se_res = native_search(q, top=top, allowed_doc_ids=None)

    docs = se_res.get("documents") or []
    if not docs:
        return se_res

    docs = docs[:top]
    se_res["documents"] = docs
    se_res["docs_found"] = len(docs)
    se_res["hits_total"] = len(docs)

    return se_res
