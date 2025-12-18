# app/services/levels_0_4/native_segments.py
from __future__ import annotations

import ctypes
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.core.logger import logger

SO_PATH = os.getenv("PLAGIO_SEGMENTS_SO", "/usr/local/lib/libplagio_segments.so")
if not os.path.exists(SO_PATH):
    alt = "/usr/local/lib/libplagio_segments.so"
    if os.path.exists(alt):
        SO_PATH = alt

_lib: Optional[ctypes.CDLL] = None
_load_err: Optional[str] = None

try:
    _lib = ctypes.CDLL(SO_PATH)
except Exception as e:
    _lib = None
    _load_err = f"failed to load {SO_PATH}: {e}"
    logger.error("[native_segments] %s", _load_err)

if _lib is not None:
    _lib.seg_search_many_json_v3.argtypes = [
        ctypes.c_char_p,                 # query_utf8
        ctypes.c_int,                    # top_k
        ctypes.POINTER(ctypes.c_char_p), # index_dirs
        ctypes.c_int,                    # n_dirs
        ctypes.c_int,                    # normalize_query (0/1)
        ctypes.c_int,                    # include_matches (0/1)
    ]
    _lib.seg_search_many_json_v3.restype = ctypes.c_void_p

    _lib.seg_search_many_json_v2.argtypes = [
        ctypes.c_char_p,                 # query_utf8
        ctypes.c_int,                    # top_k
        ctypes.POINTER(ctypes.c_char_p), # index_dirs
        ctypes.c_int,                    # n_dirs
        ctypes.c_int,                    # normalize_query (0/1)
    ]
    _lib.seg_search_many_json_v2.restype = ctypes.c_void_p

    _lib.seg_excerpt_for_span_json_v2.argtypes = [
        ctypes.c_char_p, # text_utf8
        ctypes.c_int,    # d_from
        ctypes.c_int,    # d_to
        ctypes.c_int,    # k_shingle
        ctypes.c_int,    # max_chars
        ctypes.c_int,    # normalize_text (0/1)
    ]
    _lib.seg_excerpt_for_span_json_v2.restype = ctypes.c_void_p

    _lib.seg_normalize_json_v1.argtypes = [ctypes.c_char_p]
    _lib.seg_normalize_json_v1.restype = ctypes.c_void_p

    _lib.seg_free.argtypes = [ctypes.c_void_p]
    _lib.seg_free.restype = None


def _safe_json_loads(b: bytes) -> Dict[str, Any]:
    """
    Do NOT use errors='ignore' (it can corrupt JSON).
    Return structured error for visibility.
    """
    if not b:
        return {"count": 0, "hits": [], "error": "empty_native_response"}

    try:
        s = b.decode("utf-8")  # strict
    except UnicodeDecodeError as e:
        return {
            "count": 0,
            "hits": [],
            "error": "bad_utf8",
            "detail": str(e),
            "raw_len": len(b),
        }

    try:
        obj = json.loads(s)
    except Exception as e:
        return {
            "count": 0,
            "hits": [],
            "error": "bad_json",
            "detail": str(e),
            "raw_len": len(b),
        }

    if isinstance(obj, dict):
        # Ensure minimal keys exist
        obj.setdefault("hits", [])
        obj.setdefault("count", int(len(obj.get("hits") or [])))
        return obj

    return {"count": 0, "hits": [], "error": "bad_json_root"}


def _ensure_loaded() -> bool:
    return _lib is not None


def seg_search_many(
    *,
    query: str,
    top_k: int,
    index_dirs: List[str],
    include_matches: bool = False,
    max_matches_per_doc: Optional[int] = None,
    normalize_query: bool = False,  # PROD: always false (query already normalized)
) -> Dict[str, Any]:
    if not query or top_k <= 0 or not index_dirs:
        return {"count": 0, "hits": []}

    if not _ensure_loaded():
        return {"count": 0, "hits": [], "error": "native_not_loaded", "detail": (_load_err or "")}

    # validate dirs
    dirs: List[bytes] = []
    for d in index_dirs:
        p = Path(d)
        if p.exists() and p.is_dir():
            dirs.append(str(p).encode("utf-8"))

    if not dirs:
        return {"count": 0, "hits": []}

    arr = (ctypes.c_char_p * len(dirs))(*dirs)

    ptr = _lib.seg_search_many_json_v3(
        query.encode("utf-8"),
        int(top_k),
        arr,
        int(len(dirs)),
        1 if normalize_query else 0,
        1 if include_matches else 0,
    )
    if not ptr:
        return {"count": 0, "hits": []}

    try:
        raw = ctypes.cast(ptr, ctypes.c_char_p).value or b""
        data = _safe_json_loads(raw)

        hits = data.get("hits") or []
        if not isinstance(hits, list):
            data["hits"] = []
            data["count"] = 0
            data["error"] = data.get("error") or "bad_hits"
            return data

        # strip matches if not requested (keep all top-level debug keys!)
        if not include_matches:
            for h in hits:
                if isinstance(h, dict):
                    h.pop("matches", None)

        # optional python-side cap (only when include_matches True)
        if include_matches and max_matches_per_doc is not None and max_matches_per_doc >= 0:
            for h in hits:
                if not isinstance(h, dict):
                    continue
                m = h.get("matches")
                if not isinstance(m, dict):
                    continue
                q_pos = m.get("q_pos") or []
                d_pos = m.get("d_pos") or []
                hh = m.get("h") or []
                if not (isinstance(q_pos, list) and isinstance(d_pos, list) and isinstance(hh, list)):
                    continue
                n = min(len(q_pos), len(d_pos), len(hh), int(max_matches_per_doc))
                m["q_pos"] = q_pos[:n]
                m["d_pos"] = d_pos[:n]
                m["h"] = hh[:n]

        # normalize count
        data["count"] = int(len(hits))
        return data
    finally:
        _lib.seg_free(ptr)


def seg_normalize_text(text: str) -> Dict[str, Any]:
    if not _ensure_loaded():
        return {"ok": False, "text": "", "error": "native_not_loaded", "detail": (_load_err or "")}
    if not text:
        return {"ok": False, "text": ""}

    ptr = _lib.seg_normalize_json_v1(text.encode("utf-8"))
    if not ptr:
        return {"ok": False, "text": ""}

    try:
        raw = ctypes.cast(ptr, ctypes.c_char_p).value or b""
        return _safe_json_loads(raw)
    finally:
        _lib.seg_free(ptr)


def seg_excerpt_for_span(
    *,
    text: str,
    d_from: int,
    d_to: int,
    k_shingle: int = 9,
    max_chars: int = 800,
    normalize_text: bool = False,  # PROD: text already normalized -> false
) -> Dict[str, Any]:
    if not text:
        return {"ok": False, "excerpt": ""}

    if not _ensure_loaded():
        return {"ok": False, "excerpt": "", "error": "native_not_loaded", "detail": (_load_err or "")}

    ptr = _lib.seg_excerpt_for_span_json_v2(
        text.encode("utf-8"),
        int(d_from),
        int(d_to),
        int(k_shingle),
        int(max_chars),
        1 if normalize_text else 0,
    )
    if not ptr:
        return {"ok": False, "excerpt": ""}

    try:
        raw = ctypes.cast(ptr, ctypes.c_char_p).value or b""
        return _safe_json_loads(raw)
    finally:
        _lib.seg_free(ptr)
