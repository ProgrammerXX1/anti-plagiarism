# app/services/search/postsearch_semantic.py

from typing import Dict, Any, List, Tuple
from ...core.config import (
    USE_SEMANTIC_RERANK,
    SEMANTIC_ALPHA,
    SEMANTIC_TOP_K,
    SEMANTIC_FRAG_PER_DOC,
)
from ...core.logger import logger
from .semantic_client import semantic_client



def _collect_passages_for_rerank(
    docs: List[Dict[str, Any]],
    max_docs: int,
    max_frag_per_doc: int,
) -> Tuple[List[str], List[str]]:
    passages: List[str] = []
    doc_ids: List[str] = []

    for h in docs[:max_docs]:
        did = h.get("doc_id")
        det = h.get("details") or {}
        frags = det.get("matching_fragments") or []
        for frag in frags[:max_frag_per_doc]:
            txt = (frag.get("text") or "").strip()
            if not txt:
                continue
            passages.append(txt)
            doc_ids.append(did)

    return passages, doc_ids


def rerank_search_result(query_text: str, result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Постобработка результата поиска:
    - исходное ранжирование по шинглам;
    - поверх него — BGE-reranker (semantic_score);
    - финальный скор: alpha * lex + (1 - alpha) * semantic_score.

    Эмбеддинги здесь не используются.
    """
    if not USE_SEMANTIC_RERANK:
        return result

    docs: List[Dict[str, Any]] = result.get("documents") or []
    if len(docs) <= 1:
        return result

    # исходный порядок по шинглам
    for i, h in enumerate(docs, start=1):
        h["lex_rank"] = i

    # --- реранкер по фрагментам ---
    passages, doc_ids = _collect_passages_for_rerank(
        docs,
        max_docs=SEMANTIC_TOP_K,
        max_frag_per_doc=SEMANTIC_FRAG_PER_DOC,
    )

    per_doc_rerank: Dict[str, float] = {}
    if passages:
        logger.info(
            f"[semantic-rerank] start: docs={len(docs)}, "
            f"passages={len(passages)}, alpha={SEMANTIC_ALPHA}"
        )
        scores = semantic_client.rerank(query_text, passages)
        if scores and len(scores) == len(passages):
            for did, sc in zip(doc_ids, scores):
                if did is None:
                    continue
                cur = per_doc_rerank.get(did)
                if cur is None or sc > cur:
                    per_doc_rerank[did] = sc
        else:
            logger.error(
                f"[semantic-rerank] invalid scores: got={len(scores)}, "
                f"expected={len(passages)}"
            )

    alpha = float(SEMANTIC_ALPHA)

    # проставляем скор реранкера и финальный скор
    for h in docs:
        did = h.get("doc_id")
        det = h.get("details") or {}

        sem_rerank = float(per_doc_rerank.get(did, 0.0)) if USE_SEMANTIC_RERANK else 0.0
        det["semantic_score"] = sem_rerank
        # embed_score убран полностью
        h["details"] = det

        lex = float(h.get("max_score", 0.0))
        final = alpha * lex + (1.0 - alpha) * sem_rerank
        h["semantic_final_score"] = round(final, 6)

    # сортируем по финальному скору
    docs.sort(
        key=lambda x: x.get("semantic_final_score", x.get("max_score", 0.0)),
        reverse=True,
    )

    # новый ранг + флаг "сомнительный"
    for i, h in enumerate(docs, start=1):
        h["semantic_rank"] = i
        lex = float(h.get("max_score", 0.0))
        sem = float(h.get("details", {}).get("semantic_score", 0.0))
        h["semantic_suspect"] = bool(lex >= 0.7 and sem < 0.3)

    result["documents"] = docs
    logger.info("[semantic-rerank] done")
    return result
