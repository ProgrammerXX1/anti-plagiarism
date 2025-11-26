# app/services/indexing/index_build.py
import json
import gzip
import hashlib
import gc
from pathlib import Path
from typing import Dict, Any, List, Set

from ...core.logger import logger
from ...core.config import (
    CORPUS_JSONL,
    INDEX_JSON,
    MANIFEST_JSON,
    DEFAULT_CFG,
    ensure_index_cfg,
    MAX_NEW_DOCS_PER_BUILD,
)
from ..helpers.normalizer import clean_spaces_punct, normalize_nfkc_lower
from ..helpers.shingles import build_shingles_multi
from ..helpers.simhash import simhash128
from ..helpers.minhash import make_AB, minhash_signature_from_set_fast

# -------------------- helpers --------------------


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
    try:
        # убедимся, что это именно int32/uint32 диапазон
        return [int(x) & 0xFFFFFFFF for x in sig]
    except Exception:
        return None


# -------------------- main --------------------

def build_index_json(
    cfg: Dict[str, Any] | None = None,
    incremental: bool = False,
    max_new_docs_per_build: int | None = None,
) -> Dict[str, Any]:
    """
    Собирает MinHash/LSH индекс по corpus.jsonl.

    k5 шинглы отключены: строим инвертированный индекс только по k9/k13.
    MinHash/LSH базируется на S9, если пусто — на S13.

    Если minhash.use_lsh=False и minhash.use_minhash_est=False —
    MinHash/LSH полностью отключены и не занимают память.

    max_new_docs_per_build:
        - None или <=0  → без лимита (индексируем все неиндексированные документы);
        - >0            → ограничить количество НОВЫХ документов за один прогон.
    """
    # raw_cfg может содержать extra-поля, например logging
    raw_cfg = cfg or DEFAULT_CFG

    # валидация/нормализация ядра конфига (IndexConfig)
    cfg_core = ensure_index_cfg(raw_cfg)

    # runtime-опции логирования/лимитов — берём из raw_cfg.logging, если есть
    logging_cfg = dict(raw_cfg.get("logging") or {})

    # прогресс-лог
    progress_every = int(logging_cfg.get("progress_every", 0)) or 0
    log_every = progress_every or 100

    # лимит новых документов: приоритет параметра функции над конфигом
    if max_new_docs_per_build is not None:
        _max_new_docs = int(max_new_docs_per_build)
    else:
        _max_new_docs = int(logging_cfg.get("max_new_docs_per_build", 0) or 0)
    if _max_new_docs <= 0:
        _max_new_docs = 0  # 0 = без лимита

    cfg = cfg_core
    minhash_cfg = cfg["minhash"]
    K = int(minhash_cfg["K"])
    rows = int(minhash_cfg["rows"])

    use_lsh = bool(minhash_cfg.get("use_lsh", False))
    use_minhash_est = bool(minhash_cfg.get("use_minhash_est", False))

    # хранить сигнатуры есть смысл только если мы их используем в деталях
    store_sig_cfg = bool(minhash_cfg.get("store_sig", False))
    store_sig = store_sig_cfg and use_minhash_est

    # Если ни LSH, ни MinHash-оценка не нужны — MinHash вообще не считаем
    if use_lsh or use_minhash_est:
        A, B = make_AB(K, seed=minhash_cfg["seed"])
        bands_cnt = K // rows
        lsh_bands: List[Dict[str, Any]] = [
            {"band_no": i, "buckets": {}} for i in range(bands_cnt)
        ]
        minhash_sig: Dict[str, List[int]] = {}
    else:
        A, B = None, None
        bands_cnt = 0
        lsh_bands = []
        minhash_sig = {}

    if not CORPUS_JSONL.exists():
        raise FileNotFoundError(f"corpus.jsonl not found: {CORPUS_JSONL}")

    # --- базовые структуры ---
    docs_meta: Dict[str, Dict[str, Any]] = {}
    # k5 оставляем только для совместимости структуры, но не заполняем
    inv_sets: Dict[str, Dict[str, set[str]]] = {"k5": {}, "k9": {}, "k13": {}}

    existing_ids: set[str] = set()

    # --- если incremental=True, пробуем подхватить старый индекс ---
    if incremental and INDEX_JSON.exists():
        if str(INDEX_JSON).endswith(".gz"):
            with gzip.open(INDEX_JSON, "rt", encoding="utf-8") as f:
                idx_old = json.load(f)
        else:
            with open(INDEX_JSON, "r", encoding="utf-8") as f:
                idx_old = json.load(f)

        old_cfg = ensure_index_cfg(idx_old.get("config") or {})
        old_m = old_cfg["minhash"]

        # если конфиг MinHash/LSH поменялся — инкрементальный режим запрещаем
        if (
            int(old_m["K"]) != K
            or int(old_m["rows"]) != rows
            or bool(old_m.get("use_lsh", False)) != use_lsh
            or bool(old_m.get("use_minhash_est", False)) != use_minhash_est
        ):
            raise ValueError("minhash/LSH config changed, cannot use incremental build")

        docs_meta = idx_old.get("docs_meta") or {}
        existing_ids = set(docs_meta.keys())

        old_inv = idx_old.get("inverted_doc") or {}
        for name in ("k5", "k9", "k13"):
            d = old_inv.get(name) or {}
            inv_sets[name] = {k: set(v) for k, v in d.items()}

        old_lsh = idx_old.get("lsh") or {}
        # если LSH ещё используется — подхватываем старые бэнды
        if use_lsh:
            lsh_bands = old_lsh.get("bands") or lsh_bands
        else:
            lsh_bands = []

        if store_sig and use_minhash_est:
            minhash_sig = idx_old.get("minhash_sig") or {}
        else:
            minhash_sig = {}

        del idx_old
        gc.collect()

        logger.info(
            f"[index_build] incremental mode: loaded existing index with "
            f"{len(docs_meta)} docs (use_lsh={use_lsh}, use_minhash_est={use_minhash_est})"
        )
    else:
        logger.info(
            f"[index_build] full rebuild mode (use_lsh={use_lsh}, "
            f"use_minhash_est={use_minhash_est})"
        )

    # --- оценка total для логов ---
    total_est = 0
    with open(CORPUS_JSONL, "r", encoding="utf-8") as f:
        for _ in f:
            total_est += 1

    logger.info(
        f"[index_build] start: corpus={CORPUS_JSONL}, total_est={total_est}, "
        f"incremental={incremental}, store_sig={store_sig}, "
        f"max_new_docs_per_build={_max_new_docs or 'no-limit'}"
    )

    n_docs = 0
    kept_new = 0  # сколько новых документов добавили в этом прогоне

    # --- основной проход по корпусу (стримом, по строкам) ---
    with open(CORPUS_JSONL, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            n_docs += 1
            doc = json.loads(line)
            did = doc.get("doc_id")
            raw_text = doc.get("text", "") or ""
            title = doc.get("title")
            author = doc.get("author")

            if not did:
                did = f"doc_{hashlib.sha256(raw_text.encode('utf-8')).hexdigest()[:8]}"

            if did in existing_ids:
                continue

            # нормализация
            toks = clean_spaces_punct(normalize_nfkc_lower(raw_text)).split()
            if len(toks) < cfg["w_min_doc"]:
                continue

            # шинглы только k9 и k13
            sh = build_shingles_multi(toks, [9, 13]) or {}
            S9: Set[int] = set(sh.get(9, []))
            S13: Set[int] = set(sh.get(13, []))

            if not S9 and not S13:
                continue

            # inverted index (set) — только k9/k13
            for k, S in ((9, S9), (13, S13)):
                idx_map = inv_sets[f"k{k}"]
                for h in S:
                    key = str(h)
                    bucket = idx_map.get(key)
                    if bucket is None:
                        idx_map[key] = {did}
                    else:
                        bucket.add(did)

            # MinHash + LSH: только если включены
            if use_lsh or use_minhash_est:
                base = S9 if S9 else S13
                if base:
                    sig_raw = minhash_signature_from_set_fast(base, A, B)
                    sig = _safe_sig_list(sig_raw, K)
                    if sig is not None:
                        if store_sig and use_minhash_est:
                            minhash_sig[did] = sig

                        if use_lsh:
                            for b in range(bands_cnt):
                                st, en = b * rows, (b + 1) * rows
                                key = _hash_chunk_u32_be(sig[st:en])
                                buckets = lsh_bands[b]["buckets"]
                                bucket = buckets.get(key)
                                if bucket is None:
                                    buckets[key] = [did]
                                else:
                                    bucket.append(did)

            # метаданные
            meta: Dict[str, Any] = {
                "tok_len": len(toks),
                "simhash128": simhash128(toks),
            }
            if title is not None:
                meta["title"] = title
            if author is not None:
                meta["author"] = author
            docs_meta[did] = meta
            existing_ids.add(did)

            kept_new += 1

            # лимит новых документов за один прогон
            if _max_new_docs and kept_new >= _max_new_docs:
                pct = (n_docs * 100.0 / total_est) if total_est else 0.0
                logger.info(
                    f"[index_build] reached max_new_docs_per_build={_max_new_docs}, "
                    f"seen={n_docs}/{total_est} ({pct:.1f}%), stopping early"
                )
                gc.collect()
                break

            if kept_new % log_every == 0:
                pct = (n_docs * 100.0 / total_est) if total_est else 0.0
                logger.info(
                    f"[index_build] new_docs={kept_new}, seen={n_docs}/{total_est} "
                    f"({pct:.1f}%)"
                )
                gc.collect()

    # --- финализация: set -> sorted list (inverted index) ---
    inv: Dict[str, Dict[str, List[str]]] = {}
    for name, d in inv_sets.items():
        inv[name] = {k: sorted(v) for k, v in d.items()}

    inv_sets.clear()
    del inv_sets
    gc.collect()

    # отсортировать buckets в LSH (если он вообще есть)
    if use_lsh:
        for b in lsh_bands:
            b["buckets"] = {k: sorted(v) for k, v in b["buckets"].items()}
    else:
        # оставляем структуру пустой, чтобы search не падал
        lsh_bands = []

    # собираем итоговый индекс
    index: Dict[str, Any] = {
        "version": "plagio-doc-index-1",
        "config": cfg,  # уже нормализованный IndexConfig
        "docs_meta": docs_meta,
        "inverted_doc": inv,
        "lsh": {"bands": lsh_bands, "A": A, "B": B},
    }
    if store_sig and use_minhash_est:
        index["minhash_sig"] = minhash_sig

    INDEX_JSON.parent.mkdir(parents=True, exist_ok=True)

    if str(INDEX_JSON).endswith(".gz"):
        with gzip.open(INDEX_JSON, "wt", encoding="utf-8") as fo:
            json.dump(index, fo, ensure_ascii=False)
    else:
        with open(INDEX_JSON, "w", encoding="utf-8") as fo:
            json.dump(index, fo, ensure_ascii=False)

    # манифест
    with open(MANIFEST_JSON, "w", encoding="utf-8") as fo:
        json.dump(
            {
                "version": "plagio-doc-index-1",
                "counts": {
                    "docs": len(docs_meta),
                    "new_docs": kept_new,
                    "total_est": total_est,
                },
                "cfg": cfg,
            },
            fo,
            ensure_ascii=False,
        )

    logger.info(
        f"[index_build] done: total_docs={len(docs_meta)}, new_docs={kept_new}, "
        f"incremental={incremental}, index_path={INDEX_JSON}, "
        f"use_lsh={use_lsh}, use_minhash_est={use_minhash_est}, store_sig={store_sig}, "
        f"max_new_docs_per_build={_max_new_docs or 'no-limit'}"
    )

    return index



