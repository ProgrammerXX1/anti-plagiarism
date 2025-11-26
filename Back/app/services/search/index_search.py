# app/services/search/index_search.py

import json
import gzip
from collections import Counter, defaultdict
from typing import Dict, Any, List, Tuple, Set, Optional

from ...core.config import INDEX_JSON, ensure_index_cfg, DEFAULT_CFG
from ..helpers.normalizer import normalize_for_shingles, normalize_nfkc_lower, clean_spaces_punct
from ..helpers.shingles import build_shingles_multi
from ..helpers.simhash import simhash128, hamming_hex128
from ..helpers.minhash import minhash_signature_from_set_fast, get_lsh_candidates

# ── helpers ────────────────────────────────────────────────────────────────────


def _jc(inter: int, S_size: int, T_size: int) -> Tuple[float, float]:
    """Jaccard и Containment по числу пересечений и размерам множеств."""
    if S_size <= 0:
        return 0.0, 0.0
    union = S_size + T_size - inter
    if union <= 0:
        union = 1
    return inter / union, inter / S_size


def _merge(intervals: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
    """Слияние пересекающихся интервалов [s, e)."""
    if not intervals:
        return []
    intervals.sort()
    out = [intervals[0]]
    for s, e in intervals[1:]:
        ls, le = out[-1]
        if s <= le:
            out[-1] = (ls, max(le, e))
        else:
            out.append((s, e))
    return out


def _pos_map(sh_list: List[int]) -> Dict[int, List[int]]:
    """Карта: hash шингла → список позиций в последовательности шинглов."""
    pos: Dict[int, List[int]] = defaultdict(list)
    for i, h in enumerate(sh_list):
        pos[h].append(i)
    return pos


def _validate_index(idx: Dict[str, Any]) -> None:
    """Базовая валидация структуры индекса."""
    req = ["version", "config", "docs_meta", "inverted_doc", "lsh"]
    if not all(k in idx for k in req):
        raise ValueError("invalid index: missing required keys")

    inv = idx["inverted_doc"]
    if not isinstance(inv, dict) or not all(k in inv for k in ("k5", "k9", "k13")):
        # k5 по-прежнему должен быть в индексе, даже если не используем в поиске
        raise ValueError("invalid inverted_doc structure")


def _get_intersections_lazy(
    cand_ids: Set[str],
    qS: Set[int],
    inv: Dict[str, List[str]],
) -> Dict[str, int]:
    """
    |Q ∩ D| только по выбранным кандидатам.
    qS — множество шинглов запроса (hash),
    inv — inverted index: hash -> [doc_id,...].
    """
    inter: Dict[str, int] = {}
    if not qS or not cand_ids:
        return inter

    for sh in qS:
        lst = inv.get(str(sh))
        if not lst:
            continue
        for did in lst:
            if did in cand_ids:
                inter[did] = inter.get(did, 0) + 1

    return inter


# ── core search ────────────────────────────────────────────────────────────────


def search(
    index: Dict[str, Any],
    qtext: str,
    top: int = 5,
    allowed_doc_ids: Optional[Set[str]] = None,
) -> Dict[str, Any]:
    """
    Основной поиск по предсобранному индексу.

    index          — уже загруженный индекс (dict),
    qtext          — текст запроса,
    top            — сколько документов вернуть,
    allowed_doc_ids — если задано, ограничиваемся этим множеством doc_id
                      (например, результат BM25-кандидатов).
    """
    _validate_index(index)
    cfg = ensure_index_cfg(index.get("config") or DEFAULT_CFG)

    inv = index["inverted_doc"]
    inv9, inv13 = inv["k9"], inv["k13"]

    lsh = index["lsh"]
    bands = lsh["bands"]
    A = lsh["A"]
    B = lsh["B"]

    minhash_cfg = cfg["minhash"]
    K = int(minhash_cfg["K"])
    rows = int(minhash_cfg["rows"])
    hbits = int(minhash_cfg.get("hamming_bonus_bits", 0))
    use_lsh = bool(minhash_cfg.get("use_lsh", False))
    use_minhash_est = bool(minhash_cfg.get("use_minhash_est", False))

    weights = cfg["weights"]
    alpha = float(weights["alpha"])
    w13 = float(weights["w13"])
    w9 = float(weights["w9"])

    simhash_bonus = float(cfg.get("simhash_bonus", 0.0))
    fetch_per_k = int(cfg.get("fetch_per_k_doc", 256))

    thr = cfg.get("thresholds", {})
    plag_thr = float(thr.get("plag_thr", 0.85))
    partial_thr = float(thr.get("partial_thr", 0.45))

    max_cands_doc = int(cfg.get("max_cands_doc", 5000))
    frag_for_top = int(cfg.get("fragments_for_top", min(16, top)))

    # ── нормализация запроса ──────────────────────────────────────────────────
    qnorm = normalize_for_shingles(qtext or "")
    qtoks = qnorm.split()
    if len(qtoks) < cfg["w_min_query"]:
        return {"hits_total": 0, "docs_found": 0, "documents": []}

    # шинглы за один проход + pos-карты (только k9/k13)
    sh = build_shingles_multi(qtoks, [9, 13])
    S9_list = sh.get(9, [])
    S13_list = sh.get(13, [])

    S9: Set[int] = set(S9_list)
    S13: Set[int] = set(S13_list)

    qpos9 = _pos_map(S9_list)
    qpos13 = _pos_map(S13_list)

    cand: Counter[str] = Counter()

    # ── инвертированный индекс по 9/13 ────────────────────────────────────────
    if S9_list:
        for shv in S9_list[:fetch_per_k]:
            lst = inv9.get(str(shv))
            if lst:
                cand.update(lst)

    if S13_list:
        for shv in S13_list[:fetch_per_k]:
            lst = inv13.get(str(shv))
            if lst:
                cand.update(lst)

    # ── MinHash LSH ───────────────────────────────────────────────────────────
    base_for_mh = S9 if S9 else S13
    if use_lsh and base_for_mh:
        sig_q = minhash_signature_from_set_fast(base_for_mh, A, B)
        lsh_cands = get_lsh_candidates(sig_q, bands, rows)
        if lsh_cands:
            cand.update(lsh_cands)
    else:
        sig_q = None  # LSH не используем

    if not cand:
        return {"hits_total": 0, "docs_found": 0, "documents": []}

    # ── ограничение кандидатов ────────────────────────────────────────────────
    if len(cand) > max_cands_doc:
        cand = Counter(dict(cand.most_common(max_cands_doc)))

    cand_set: Set[str] = set(cand.keys())

    # если есть внешнее множество разрешённых doc_id (например, из BM25) — режем
    if allowed_doc_ids is not None:
        cand_set &= allowed_doc_ids
        if not cand_set:
            return {"hits_total": 0, "docs_found": 0, "documents": []}
        cand = Counter({did: cand[did] for did in cand_set})

    # ── пересечения только по кандидатам ──────────────────────────────────────
    inter9_map = _get_intersections_lazy(cand_set, S9, inv9)
    inter13_map = _get_intersections_lazy(cand_set, S13, inv13)

    qS9 = max(0, len(qtoks) - 9 + 1)
    qS13 = max(0, len(qtoks) - 13 + 1)

    q_sim = simhash128(qtoks)

    # минимальные пересечения для раннего отсева
    min_inter9 = 1 if qS9 <= 8 else 2
    min_inter13 = 1

    mh_all = index.get("minhash_sig") or {}
    use_mh_est_runtime = bool(mh_all) and use_minhash_est and sig_q is not None

    docs_meta = index["docs_meta"]

    # ── скоринг кандидатов ────────────────────────────────────────────────────
    scored: List[Dict[str, Any]] = []
    for did, cnt in cand.items():
        meta = docs_meta.get(did)
        if not meta:
            continue

        tlen = int(meta.get("tok_len", 0))
        if tlen < cfg["w_min_doc"]:
            continue

        T9 = max(0, tlen - 9 + 1)
        T13 = max(0, tlen - 13 + 1)

        inter9 = inter9_map.get(did, 0)
        inter13 = inter13_map.get(did, 0)

        # ранний отсев
        if inter9 < min_inter9 and inter13 < min_inter13:
            continue

        J9, C9 = _jc(inter9, qS9, T9)
        J13, C13 = _jc(inter13, qS13, T13)

        s13 = w13 * (alpha * J13 + (1.0 - alpha) * C13)
        s9 = w9 * (alpha * J9 + (1.0 - alpha) * C9)
        score = max(s13, s9)

        # SimHash-бонус только для уже подозрительных кандидатов
        use_simhash = simhash_bonus > 0.0
        if use_simhash and score >= partial_thr:
            dsim = hamming_hex128(q_sim, meta["simhash128"])
            if dsim <= hbits:
                score += simhash_bonus
        else:
            dsim = 128

        # оценка сходства по MinHash (если сигнатуры есть)
        mh_est = 0.0
        if use_mh_est_runtime:
            mh_sig = mh_all.get(did)
            if mh_sig:
                eq = 0
                for a, b in zip(sig_q, mh_sig):
                    if a == b:
                        eq += 1
                mh_est = round(eq / K, 3)

        scored.append(
            {
                "doc_id": did,
                "score": round(score, 6),
                "J9": round(J9, 6),
                "C9": round(C9, 6),
                "J13": round(J13, 6),
                "C13": round(C13, 6),
                "cand_hits": int(cnt),
                "hamming_simhash": int(dsim),
                "minhash_sim_est": mh_est,
                "matching_fragments": [],
            }
        )

    if not scored:
        return {"hits_total": 0, "docs_found": 0, "documents": []}

    scored.sort(key=lambda x: x["score"], reverse=True)
    kept = scored[:top]

    # ── фрагменты: только для top_k_for_fragments (по 9/13) ───────────────────
    top_for_fr = kept[:frag_for_top]
    for h in top_for_fr:
        did = h["doc_id"]
        hits: List[Tuple[int, int]] = []

        # совпавшие 13-граммы
        for shv, starts in qpos13.items():
            lst = inv13.get(str(shv))
            if lst and did in lst:
                for s in starts:
                    hits.append((s, s + 13))

        # совпавшие 9-граммы
        for shv, starts in qpos9.items():
            lst = inv9.get(str(shv))
            if lst and did in lst:
                for s in starts:
                    hits.append((s, s + 9))

        merged = _merge(hits)
        h["matching_fragments"] = [
            {"start": s, "end": e, "text": " ".join(qtoks[s:e])}
            for s, e in merged
        ]

    def decide(s: float) -> str:
        if s >= plag_thr:
            return "plagiarism"
        if s >= partial_thr:
            return "partial"
        return "original"

    docs: List[Dict[str, Any]] = []
    for h in kept:
        did = h["doc_id"]
        meta = docs_meta.get(did, {})
        score = h["score"]
        docs.append(
            {
                "doc_id": did,
                "title": meta.get("title"),
                "author": meta.get("author"),
                "max_score": score,
                "originality_pct": round(
                    (1.0 - min(max(score, 0.0), 1.0)) * 100.0,
                    1,
                ),
                "decision": decide(score),
                "details": h,
            }
        )

    return {
        "hits_total": len(scored),
        "docs_found": len(docs),
        "documents": docs,
    }


# ── cache/load ────────────────────────────────────────────────────────────────

_INDEX_CACHE: Optional[Dict[str, Any]] = None


def load_index() -> Dict[str, Any]:
    """
    Одна точка чтения index.json с диска.
    Используется только внутри кеширующих обёрток.
    """
    if not INDEX_JSON.exists():
        raise FileNotFoundError(f"index not found: {INDEX_JSON}")

    if str(INDEX_JSON).endswith(".gz"):
        with gzip.open(INDEX_JSON, "rt", encoding="utf-8") as f:
            idx = json.load(f)
    else:
        with open(INDEX_JSON, "r", encoding="utf-8") as f:
            idx = json.load(f)

    _validate_index(idx)
    # нормализуем конфиг индекса (добьём дефолты, проверим K%rows)
    idx["config"] = ensure_index_cfg(idx.get("config") or DEFAULT_CFG)
    return idx


def get_index_cached() -> Dict[str, Any]:
    """
    Единая точка входа: всегда работаем через кэш.
    При первом вызове читает индекс с диска, дальше только из RAM.
    """
    global _INDEX_CACHE
    if _INDEX_CACHE is None:
        _INDEX_CACHE = load_index()
    return _INDEX_CACHE


def load_index_cached() -> Dict[str, Any]:
    """
    Alias для старого имени, чтобы не ломать существующие импорты.
    """
    return get_index_cached()


def clear_index_cache() -> None:
    """Сброс кэша (например, после rebuild индекса)."""
    global _INDEX_CACHE
    _INDEX_CACHE = None


def search_cached(
    qtext: str,
    top: int = 5,
    allowed_doc_ids: Optional[Set[str]] = None,
) -> Dict[str, Any]:
    """
    Удобный хелпер: берёт индекс из кэша и запускает search.
    """
    idx = get_index_cached()
    return search(idx, qtext, top=top, allowed_doc_ids=allowed_doc_ids)
