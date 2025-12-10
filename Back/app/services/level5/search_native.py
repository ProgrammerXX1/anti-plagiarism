# app/services/level5/search_native.py
from __future__ import annotations

import ctypes
from ctypes import c_char_p, c_int, c_double
from pathlib import Path
from typing import Dict, Any, List, Set

import orjson

from app.core.logger import logger
from app.core.config import INDEX_DIR
from app.core.memlog import log_mem

# ── пути к .so ──────────────────────────────────────────────────────

LIB_PATHS = [
    "/usr/local/lib/libsearchcore.so",
    str(Path(__file__).resolve().parents[2] / "build" / "libsearchcore.so"),
]


class SeHit(ctypes.Structure):
    _fields_ = [
        ("doc_id_int", c_int),
        ("score", c_double),
        ("j9", c_double),
        ("c9", c_double),
        ("j13", c_double),
        ("c13", c_double),
        ("cand_hits", c_int),
    ]


class SeSearchResult(ctypes.Structure):
    _fields_ = [("count", c_int)]


def _load_lib():
    last_err = None
    for p in LIB_PATHS:
        try:
            lib = ctypes.CDLL(p)
            logger.info("[search-native] loaded libsearchcore.so from %s", p)
            return lib
        except OSError as e:
            last_err = e
    raise RuntimeError(f"Cannot load libsearchcore.so: {last_err}")


_lib = _load_lib()

_lib.se_load_index.argtypes = [c_char_p]
_lib.se_load_index.restype = c_int

_lib.se_search_text.argtypes = [
    c_char_p,                    # text_utf8 (сырой UTF-8, нормализуем в C++)
    c_int,                       # top_k
    ctypes.POINTER(SeHit),       # out_hits
    c_int,                       # max_hits
]
_lib.se_search_text.restype = SeSearchResult

SE_MAX_HITS = 4096

# ── текущее местоположение индекса + кэши ───────────────────────────

CURRENT_INDEX_DIR: Path = INDEX_DIR

_doc_ids: List[str] | None = None
_meta_cache: Dict[str, Any] | None = None


def _load_doc_ids() -> List[str]:
    global _doc_ids
    if _doc_ids is not None:
        return _doc_ids

    path = CURRENT_INDEX_DIR / "index_native_docids.json"
    with open(path, "r", encoding="utf-8") as f:
        _doc_ids = orjson.loads(f.read())
    logger.info(
        "[search-native] loaded doc_ids: %d from %s",
        len(_doc_ids),
        path,
    )
    return _doc_ids


def _load_meta_cfg() -> Dict[str, Any]:
    global _meta_cache
    if _meta_cache is not None:
        return _meta_cache

    path = CURRENT_INDEX_DIR / "index_native_meta.json"
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                _meta_cache = orjson.loads(f.read())
                return _meta_cache
        except Exception as e:
            logger.error("[search-native] meta broken (%s): %s", path, e)

    # если меты нет, дефолты, но обязательно docs_meta = {}
    _meta_cache = {
        "docs_meta": {},
        "config": {},
    }
    return _meta_cache


# ── API для FastAPI / пайплайна ─────────────────────────────────────

def native_load_index(index_dir: Path) -> None:
    """
    Загружает C++ индекс из директории index_dir.
    Одновременно обновляет CURRENT_INDEX_DIR и сбрасывает кэши.
    """
    global CURRENT_INDEX_DIR, _doc_ids, _meta_cache

    index_dir = Path(index_dir)
    CURRENT_INDEX_DIR = index_dir
    _doc_ids = None
    _meta_cache = None

    logger.info("[search-native] trying to load index from %s", index_dir)
    for name in [
        "index_native.bin",
        "index_native_docids.json",
        "index_native_meta.json",
        "index_config.json",
    ]:
        path = index_dir / name
        exists = path.exists()
        size = path.stat().st_size if exists else None
        logger.info(
            "[search-native] check %s exists=%s size=%s",
            path,
            exists,
            size,
        )

    log_mem("[search-native] before se_load_index")
    rc = _lib.se_load_index(str(index_dir).encode("utf-8"))
    if rc != 0:
        raise RuntimeError(f"se_load_index failed: rc={rc}")

    _load_doc_ids()
    _load_meta_cfg()
    log_mem("[search-native] after se_load_index + meta/doc_ids")
    logger.info("[search-native] C++ index loaded successfully")


def native_search(
    q: str,
    top: int = 10,
    allowed_doc_ids: Set[str] | None = None,
) -> Dict[str, Any]:
    """
    Чистый C++-поиск.

    1) Передаём сырой текст запроса в C++.
    2) Вызываем se_search_text.
    3) Фильтруем по allowed_doc_ids (если передано).
    4) Обогащаем метаданными из docs_meta + thresholds из config.
    """
    q = (q or "").strip()
    if not q:
        return {"hits_total": 0, "docs_found": 0, "documents": []}

    q_bytes = q.encode("utf-8", "ignore")

    doc_ids = _load_doc_ids()
    meta_cfg = _load_meta_cfg()
    docs_meta = meta_cfg.get("docs_meta") or {}
    cfg = meta_cfg.get("config") or {}

    thresholds = cfg.get("thresholds") or {}
    plag_thr = float(thresholds.get("plag_thr", 0.7))
    partial_thr = float(thresholds.get("partial_thr", 0.3))

    hits = (SeHit * SE_MAX_HITS)()

    log_mem("[search-native] before se_search_text")
    res = _lib.se_search_text(
        q_bytes,
        c_int(top),
        hits,
        c_int(SE_MAX_HITS),
    )
    log_mem("[search-native] after se_search_text")

    if res.count <= 0:
        return {"hits_total": 0, "docs_found": 0, "documents": []}

    docs: List[Dict[str, Any]] = []
    allow = allowed_doc_ids

    for i in range(int(res.count)):
        h = hits[i]
        di = int(h.doc_id_int)
        if di < 0 or di >= len(doc_ids):
            continue

        did = doc_ids[di]
        if allow is not None and did not in allow:
            continue

        meta = docs_meta.get(did, {})

        score = float(h.score)
        if score >= plag_thr:
            decision = "plagiarism"
        elif score >= partial_thr:
            decision = "partial"
        else:
            decision = "original"

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
                "decision": decision,
                "details": {
                    "doc_id": did,
                    "score": score,
                    "J9": float(h.j9),
                    "C9": float(h.c9),
                    "J13": float(h.j13),
                    "C13": float(h.c13),
                    "cand_hits": int(h.cand_hits),
                    "matching_fragments": [],
                },
            }
        )

    docs.sort(key=lambda x: x["max_score"], reverse=True)
    docs = docs[:top]

    return {
        "hits_total": len(docs),
        "docs_found": len(docs),
        "documents": docs,
    }
