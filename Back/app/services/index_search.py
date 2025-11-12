# app/services/index_search.py
import json, gzip
from typing import Dict, Any, List, Tuple
from ..core.config import INDEX_JSON, DEFAULT_CFG
from .normalizer import normalize_nfkc_lower, clean_spaces_punct
from .shingles import build_shingles_multi
from .simhash import simhash128, hamming_hex128
from .minhash import minhash_signature_from_set_fast, get_lsh_candidates

# ── helpers ────────────────────────────────────────────────────────────────────
def _jc(inter: int, S_size: int, T_size: int) -> Tuple[float, float]:
    if S_size <= 0: return 0.0, 0.0
    union = S_size + T_size - inter
    if union <= 0: union = 1
    return inter / union, inter / S_size  # (Jaccard, Containment)

def _merge(intervals: List[Tuple[int,int]]) -> List[Tuple[int,int]]:
    if not intervals: return []
    intervals.sort()
    out=[intervals[0]]
    for s,e in intervals[1:]:
        ls,le=out[-1]
        if s<=le: out[-1]=(ls,max(le,e))
        else: out.append((s,e))
    return out

def _pos_map(sh_list: List[int]) -> Dict[int, List[int]]:
    from collections import defaultdict
    pos = defaultdict(list)
    for i,h in enumerate(sh_list): pos[h].append(i)
    return pos

def _validate_index(idx: Dict[str, Any]) -> None:
    req = ["version","config","docs_meta","inverted_doc","lsh"]
    if not all(k in idx for k in req): raise ValueError("invalid index")
    inv=idx["inverted_doc"]
    if not isinstance(inv,dict) or not all(k in inv for k in ("k5","k9","k13")):
        raise ValueError("invalid inverted_doc")
    K = int(idx["config"]["minhash"]["K"])
    rows = int(idx["config"]["minhash"]["rows"])
    if K <= 0 or rows <= 0 or K % rows != 0:
        raise ValueError(f"Invalid MinHash/LSH config: K={K}, rows={rows}")

def _get_intersections_lazy(cand_ids: set[str], qS: set[int], inv: dict) -> dict[str, int]:
    """|Q ∩ D| только по выбранным кандидатам."""
    inter: dict[str,int] = {}
    if not qS or not cand_ids: return inter
    for sh in qS:
        lst = inv.get(str(sh))
        if not lst: 
            continue
        for did in lst:
            if did in cand_ids:
                inter[did] = inter.get(did, 0) + 1
    return inter

# ── core search ────────────────────────────────────────────────────────────────
def search(index: Dict[str, Any], qtext: str, top: int = 5) -> Dict[str, Any]:
    _validate_index(index)
    cfg=index["config"]; inv=index["inverted_doc"]
    inv5,inv9,inv13=inv["k5"],inv["k9"],inv["k13"]
    bands=index["lsh"]["bands"]
    A=index["lsh"]["A"]; B=index["lsh"]["B"]
    K=cfg["minhash"]["K"]; rows=cfg["minhash"]["rows"]; alpha=cfg["weights"]["alpha"]
    w13=cfg["weights"]["w13"]; w9=cfg["weights"]["w9"]; w5=cfg["weights"]["w5"]
    simhash_bonus=float(cfg.get("simhash_bonus",0.0)); hbits=int(cfg["minhash"]["hamming_bonus_bits"])
    fetch_per_k=int(cfg.get("fetch_per_k_doc", 256)); fetch_per_k5=int(cfg.get("fetch_per_k5_doc",512))
    thr=cfg.get("thresholds",{}); plag_thr=float(thr.get("plag_thr",0.85)); partial_thr=float(thr.get("partial_thr",0.45))
    max_cands_doc=int(cfg.get("max_cands_doc", 5000))
    frag_for_top=int(cfg.get("fragments_for_top", min(16, top)))

    qnorm = clean_spaces_punct(normalize_nfkc_lower(qtext))
    qtoks = qnorm.split()
    if len(qtoks) < cfg["w_min_query"]:
        return {"hits_total":0,"docs_found":0,"documents":[]}

    # шинглы за один проход + pos-карты
    sh = build_shingles_multi(qtoks, [5, 9, 13])
    S5_list, S9_list, S13_list = sh[5], sh[9], sh[13]
    S5, S9, S13 = set(S5_list), set(S9_list), set(S13_list)
    qpos5 = _pos_map(S5_list)
    qpos9 = _pos_map(S9_list)
    qpos13 = _pos_map(S13_list)

    from collections import Counter
    cand=Counter()

    # инвертированный по 9/13
    for shv in S9_list[:fetch_per_k]:
        lst=inv9.get(str(shv))
        if lst: cand.update(lst)
    for shv in S13_list[:fetch_per_k]:
        lst=inv13.get(str(shv))
        if lst: cand.update(lst)

    # MinHash LSH
    base_for_mh = S9 if S9 else S5
    if base_for_mh:
        sig_q = minhash_signature_from_set_fast(base_for_mh, A, B)
        lsh_cands = get_lsh_candidates(sig_q, bands, rows)
        if lsh_cands:
            cand.update(lsh_cands)
    else:
        sig_q = [0xffffffff]*K

    # k5 как fallback
    if not cand or (len(qtoks)<30 and len(cand)<3) or (not S13):
        for shv in S5_list[:min(fetch_per_k5, len(S5_list))]:
            lst=inv5.get(str(shv))
            if lst: cand.update(lst)

    if not cand:
        return {"hits_total":0,"docs_found":0,"documents":[]}

    # ограничение кандидатов
    if len(cand) > max_cands_doc:
        cand = Counter(dict(cand.most_common(max_cands_doc)))
    cand_set = set(cand.keys())

    # пересечения только по кандидатам
    inter9_map  = _get_intersections_lazy(cand_set, S9,  inv9)
    inter13_map = _get_intersections_lazy(cand_set, S13, inv13)
    inter5_map  = _get_intersections_lazy(cand_set, S5,  inv5)

    qS9=max(0,len(qtoks)-9+1); qS13=max(0,len(qtoks)-13+1); qS5=max(0,len(qtoks)-5+1)
    q_sim=simhash128(qtoks)

    # минимальные пересечения для раннего отсева
    min_inter9 = 1 if qS9 <= 8 else 2
    min_inter13 = 1

    mh_all = index.get("minhash_sig") or {}

    scored=[]
    for did,cnt in cand.items():
        meta=index["docs_meta"].get(did)
        if not meta: continue
        tlen=meta["tok_len"]
        if tlen < cfg["w_min_doc"]: continue

        T9=max(0,tlen-9+1); T13=max(0,tlen-13+1); T5=max(0,tlen-5+1)
        inter9 = inter9_map.get(did, 0)
        inter13= inter13_map.get(did, 0)
        inter5 = inter5_map.get(did, 0)

        # ранний отсев
        if inter9 < min_inter9 and inter13 < min_inter13 and inter5 == 0:
            continue

        J9,C9=_jc(inter9,qS9,T9); J13,C13=_jc(inter13,qS13,T13); J5,C5=_jc(inter5,qS5,T5)
        s13=w13*(alpha*J13+(1-alpha)*C13)
        s9 =w9 *(alpha*J9 +(1-alpha)*C9)
        s5 =w5 *(alpha*J5 +(1-alpha)*C5)
        score=max(s13,s9,s5)

        if score >= partial_thr and simhash_bonus > 0.0:
            dsim = hamming_hex128(q_sim, meta["simhash128"])
            if dsim <= hbits: score += simhash_bonus
        else:
            dsim = 128

        # оценка сходства по MinHash (дёшево)
        mh_sig = mh_all.get(did)
        mh_est = 0.0
        if mh_sig:
            eq = 0
            for a,b in zip(sig_q, mh_sig):
                if a == b: eq += 1
            mh_est = round(eq / K, 3)

        scored.append({
            "doc_id": did,
            "score": round(score,6),
            "J9": round(J9,6), "C9": round(C9,6),
            "J13": round(J13,6), "C13": round(C13,6),
            "J5": round(J5,6), "C5": round(C5,6),
            "cand_hits": int(cnt),
            "hamming_simhash": int(dsim),
            "minhash_sim_est": mh_est,
            "matching_fragments": []  # заполнится для топ-N
        })

    if not scored:
        return {"hits_total":0,"docs_found":0,"documents":[]}

    scored.sort(key=lambda x: x["score"], reverse=True)
    kept=scored[:top]

    # фрагменты: только для top_k_for_fragments
    top_for_fr = kept[:frag_for_top]
    for h in top_for_fr:
        did=h["doc_id"]
        hits=[]
        # собираем интервалы по совпавшим шинглам
        for shv,starts in qpos13.items():
            lst = inv13.get(str(shv))
            if lst and did in lst:
                for s in starts: hits.append((s,s+13))
        for shv,starts in qpos9.items():
            lst = inv9.get(str(shv))
            if lst and did in lst:
                for s in starts: hits.append((s,s+9))
        for shv,starts in qpos5.items():
            lst = inv5.get(str(shv))
            if lst and did in lst:
                for s in starts: hits.append((s,s+5))
        merged=_merge(hits)
        h["matching_fragments"]=[{"start":s,"end":e,"text":" ".join(qtoks[s:e])} for s,e in merged]

    def decide(s: float) -> str:
        if s>=plag_thr: return "plagiarism"
        if s>=partial_thr: return "partial"
        return "original"

    docs=[]
    for h in kept:
        docs.append({
            "doc_id": h["doc_id"],
            "max_score": h["score"],
            "originality_pct": round((1.0 - min(max(h["score"],0.0),1.0))*100.0,1),
            "decision": decide(h["score"]),
            "details": h
        })
    return {"hits_total": len(scored), "docs_found": len(docs), "documents": docs}

# ── cache/load ────────────────────────────────────────────────────────────────
_INDEX_CACHE = None

def load_index() -> dict:
    if not INDEX_JSON.exists():
        raise FileNotFoundError(f"index not found: {INDEX_JSON}")
    if str(INDEX_JSON).endswith(".gz"):
        with gzip.open(INDEX_JSON, "rb") as f:
            idx = json.loads(f.read().decode("utf-8"))
    else:
        with open(INDEX_JSON, "r", encoding="utf-8") as f:
            idx = json.load(f)
    _validate_index(idx)
    return idx

def load_index_cached():
    global _INDEX_CACHE
    if _INDEX_CACHE is not None:
        return _INDEX_CACHE
    _INDEX_CACHE = load_index()
    return _INDEX_CACHE

def clear_index_cache():
    global _INDEX_CACHE
    _INDEX_CACHE = None

def get_index_cached() -> dict:
    global _INDEX_CACHE
    if _INDEX_CACHE is None:
        _INDEX_CACHE = load_index()
    return _INDEX_CACHE

def search_cached(qtext: str, top: int = 5) -> dict:
    idx = get_index_cached()
    return search(idx, qtext, top=top)
