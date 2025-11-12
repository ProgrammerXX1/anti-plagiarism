# app/services/index_build.py
# Быстрый и детерминированный билдер индекса документов.
# Совместим с текущим DEFAULT_CFG и остальным кодом.

import json, gzip, hashlib
from pathlib import Path
from typing import Dict, Any, List, Set, Tuple

from ..core.config import CORPUS_JSONL, INDEX_JSON, MANIFEST_JSON, DEFAULT_CFG
from ..services.normalizer import clean_spaces_punct, normalize_nfkc_lower
from ..services.shingles import build_shingles_multi
from ..services.simhash import simhash128
from ..services.minhash import make_AB, minhash_signature_from_set_fast

# -------------------- JSON writer --------------------
try:
    import orjson as _json
    def _dumps(obj) -> bytes: return _json.dumps(obj)
except Exception:
    def _dumps(obj) -> bytes: return json.dumps(obj, ensure_ascii=False).encode("utf-8")

# -------------------- helpers --------------------
def _validate_cfg(cfg: Dict[str, Any]) -> Tuple[int, int]:
    K = int(cfg["minhash"]["K"])
    rows = int(cfg["minhash"]["rows"])
    if K <= 0 or rows <= 0 or K % rows != 0:
        raise ValueError(f"Invalid MinHash/LSH config: K={K}, rows={rows}")
    if int(cfg.get("w_min_doc", 1)) < 1:
        raise ValueError("w_min_doc must be >= 1")
    return K, rows

def _hash_chunk_u32_be(chunk: List[int]) -> str:
    bb = b"".join(int(x).to_bytes(4, "big") for x in chunk)
    return hashlib.sha1(bb).digest()[:8].hex()

def _safe_sig_list(sig, K: int) -> List[int] | None:
    if sig is None:
        return None
    if not isinstance(sig, list):
        try:
            sig = sig.tolist()  # numpy.ndarray -> list
        except Exception:
            return None
    if len(sig) != K:
        return None
    # убедимся, что это именно int32/uint32 диапазон
    try:
        return [int(x) & 0xffffffff for x in sig]
    except Exception:
        return None

# -------------------- main --------------------
def build_index_json(cfg: Dict[str, Any] | None = None) -> Dict[str, Any]:
    cfg = cfg or DEFAULT_CFG
    K, rows = _validate_cfg(cfg)
    A, B = make_AB(K, seed=cfg["minhash"]["seed"])
    bands_cnt = K // rows

    if not CORPUS_JSONL.exists():
        raise FileNotFoundError(f"corpus.jsonl not found: {CORPUS_JSONL}")

    docs_meta: Dict[str, Dict[str, Any]] = {}
    inv_sets = {"k5": {}, "k9": {}, "k13": {}}  # str(hash) -> set(doc_id)
    lsh_bands = [{"band_no": i, "buckets": {}} for i in range(bands_cnt)]
    minhash_sig: Dict[str, List[int]] = {}

    progress_every = int(cfg.get("logging", {}).get("progress_every", 0)) or 0
    n_docs = 0
    kept_docs = 0

    with open(CORPUS_JSONL, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            n_docs += 1
            doc = json.loads(line)
            did = doc.get("doc_id")
            raw_text = doc.get("text", "")

            # нормализация
            toks = clean_spaces_punct(normalize_nfkc_lower(raw_text)).split()
            if len(toks) < cfg["w_min_doc"]:
                continue

            # шинглы за один проход
            sh = build_shingles_multi(toks, [5, 9, 13]) or {}
            S5: Set[int] = set(sh.get(5, []))
            S9: Set[int] = set(sh.get(9, []))
            S13: Set[int] = set(sh.get(13, []))

            if not S5 and not S9 and not S13:
                continue

            # инвертированный индекс (set — без лишнего dedup позже)
            for k, S in ((5, S5), (9, S9), (13, S13)):
                idx = inv_sets[f"k{k}"]
                for h in S:
                    key = str(h)
                    bucket = idx.get(key)
                    if bucket is None:
                        idx[key] = {did}
                    else:
                        bucket.add(did)

            # MinHash + LSH
            base = S9 if S9 else S5
            if not base:
                # нет k9 и k5 после нормализации — пропускаем
                continue

            sig = _safe_sig_list(minhash_signature_from_set_fast(base, A, B), K)
            if sig is None:
                # деградация: пропустить документ, чтобы не ломать индекс
                continue

            minhash_sig[did] = sig
            # распределение по LSH-бэндам
            for b in range(bands_cnt):
                st, en = b * rows, (b + 1) * rows
                key = _hash_chunk_u32_be(sig[st:en])
                bucket = lsh_bands[b]["buckets"].get(key)
                if bucket is None:
                    lsh_bands[b]["buckets"][key] = [did]
                else:
                    bucket.append(did)

            # метаданные
            docs_meta[did] = {
                "tok_len": len(toks),
                "simhash128": simhash128(toks),
            }

            kept_docs += 1
            if progress_every and kept_docs % progress_every == 0:
                # минимальный лог в stdout (если нужен)
                print(f"[index_build] kept={kept_docs} / seen={n_docs}")

    # финализация: set -> отсортированный list
    inv = {
        name: {k: sorted(v) for k, v in d.items()}
        for name, d in inv_sets.items()
    }
    for b in lsh_bands:
        b["buckets"] = {k: sorted(v) for k, v in b["buckets"].items()}

    index = {
        "version": "plagio-doc-index-1",
        "config": cfg,
        "docs_meta": docs_meta,
        "inverted_doc": inv,
        "lsh": {"bands": lsh_bands, "A": A, "B": B},
        "minhash_sig": minhash_sig,
    }

    # запись индекса
    INDEX_JSON.parent.mkdir(parents=True, exist_ok=True)
    payload = _dumps(index)
    if str(INDEX_JSON).endswith(".gz"):
        with gzip.open(INDEX_JSON, "wb") as fo:
            fo.write(payload)
    else:
        with open(INDEX_JSON, "wb") as fo:
            fo.write(payload)

    # манифест
    with open(MANIFEST_JSON, "w", encoding="utf-8") as fo:
        json.dump(
            {
                "version": "plagio-doc-index-1",
                "counts": {"docs": len(docs_meta)},
                "cfg": cfg,
            },
            fo,
            ensure_ascii=False,
        )

    return index
