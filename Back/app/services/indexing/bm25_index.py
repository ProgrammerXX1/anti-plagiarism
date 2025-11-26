# app/services/indexing/bm25_index.py

import json
import gzip
import math
import pickle
from collections import Counter, defaultdict
from typing import Dict, Any, List, Tuple

from ...core.config import (
    CORPUS_JSONL,
    BM25_INDEX,
    BM25_K1,
    BM25_B,
    BM25_MAX_DOCS,
)
from ...core.logger import logger
from ..helpers.normalizer import (
    normalize_for_shingles,
    normalize_nfkc_lower,
    clean_spaces_punct,
    simple_tokens,
)
# Структура индекса в памяти:
# {
#   "N": int,
#   "avgdl": float,
#   "doc_len": {doc_id: int},
#   "df": {term: int},
#   "postings": {term: [(doc_id, tf), ...]}
# }

_BM25_CACHE: Dict[str, Any] | None = None


def _normalize_text(text: str) -> List[str]:
    """Тот же пайплайн, что и для шинглов, но до токенов."""
    norm = normalize_for_shingles(text or "")
    return simple_tokens(norm)


def build_bm25_index() -> Dict[str, Any]:
    """
    Полный rebuild BM25-индекса по corpus.jsonl.
    Храним компактную структуру, сериализуем через pickle+gzip.
    """
    if not CORPUS_JSONL.exists():
        raise FileNotFoundError(f"corpus.jsonl not found: {CORPUS_JSONL}")

    logger.info(f"[bm25_build] start: corpus={CORPUS_JSONL}")

    postings: Dict[str, Dict[str, int]] = defaultdict(dict)  # term -> {doc_id: tf}
    doc_len: Dict[str, int] = {}
    N = 0

    with open(CORPUS_JSONL, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                doc = json.loads(line)
            except json.JSONDecodeError:
                logger.warning(f"[bm25_build] skip bad json at line={line_no}")
                continue

            did = doc.get("doc_id")
            text = doc.get("text") or ""
            if not did:
                # резервный doc_id, как в index_build
                import hashlib
                did = f"doc_{hashlib.sha256(text.encode('utf-8')).hexdigest()[:8]}"

            tokens = _normalize_text(text)
            if not tokens:
                continue

            N += 1
            doc_len[did] = len(tokens)
            tf = Counter(tokens)
            for term, c in tf.items():
                postings[term][did] = c

            if N % 100 == 0:
                logger.info(f"[bm25_build] docs={N}, line={line_no}")

    if N == 0:
        raise RuntimeError("[bm25_build] no documents indexed (N=0)")

    total_len = sum(doc_len.values())
    avgdl = total_len / float(N)

    df: Dict[str, int] = {t: len(docs) for t, docs in postings.items()}

    # Для экономии памяти postings переводим в список пар
    postings_list: Dict[str, List[Tuple[str, int]]] = {
        t: list(d.items()) for t, d in postings.items()
    }

    index: Dict[str, Any] = {
        "N": N,
        "avgdl": avgdl,
        "doc_len": doc_len,
        "df": df,
        "postings": postings_list,
    }

    BM25_INDEX.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(BM25_INDEX, "wb") as fo:
        pickle.dump(index, fo, protocol=pickle.HIGHEST_PROTOCOL)

    logger.info(
        f"[bm25_build] done: N={N}, avgdl={avgdl:.2f}, terms={len(postings_list)}, "
        f"path={BM25_INDEX}"
    )
    return index


def load_bm25_index() -> Dict[str, Any]:
    if not BM25_INDEX.exists():
        raise FileNotFoundError(f"BM25 index not found: {BM25_INDEX}")
    with gzip.open(BM25_INDEX, "rb") as f:
        idx = pickle.load(f)
    # простая валидация
    for key in ("N", "avgdl", "doc_len", "df", "postings"):
        if key not in idx:
            raise ValueError(f"invalid BM25 index: missing {key}")
    return idx


def load_bm25_index_cached() -> Dict[str, Any]:
    global _BM25_CACHE
    if _BM25_CACHE is not None:
        return _BM25_CACHE
    _BM25_CACHE = load_bm25_index()
    return _BM25_CACHE


def clear_bm25_cache() -> None:
    global _BM25_CACHE
    _BM25_CACHE = None


def _bm25_idf(N: int, df: int) -> float:
    # классический idf от BM25
    return math.log((N - df + 0.5) / (df + 0.5) + 1.0)


def bm25_candidates(qtext: str, top_docs: int | None = None) -> List[Tuple[str, float]]:
    """
    Вернёт top_docs кандидатов (doc_id, score) по BM25.
    Это первый лёгкий слой, поверх которого потом крутятся шинглы.
    """
    idx = load_bm25_index_cached()
    N: int = int(idx["N"])
    avgdl: float = float(idx["avgdl"])
    doc_len: Dict[str, int] = idx["doc_len"]
    df: Dict[str, int] = idx["df"]
    postings: Dict[str, List[Tuple[str, int]]] = idx["postings"]

    if top_docs is None:
        top_docs = BM25_MAX_DOCS

    tokens = _normalize_text(qtext)
    if not tokens:
        return []

    uniq_terms = set(tokens)
    scores: Dict[str, float] = defaultdict(float)
    k1 = BM25_K1
    b = BM25_B

    for term in uniq_terms:
        plist = postings.get(term)
        if not plist:
            continue
        df_t = df.get(term, 0)
        if df_t <= 0:
            continue
        idf = _bm25_idf(N, df_t)
        for did, tf in plist:
            dl = doc_len.get(did)
            if not dl:
                continue
            num = tf * (k1 + 1.0)
            den = tf + k1 * (1.0 - b + b * dl / avgdl)
            scores[did] += idf * (num / den)

    if not scores:
        return []

    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    return ranked[:top_docs]
