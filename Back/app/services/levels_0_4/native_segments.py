from __future__ import annotations

import ctypes
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

SO_PATH = os.getenv("PLAGIO_SEGMENTS_SO", "/usr/local/lib/libplagio_segments.so")
if not os.path.exists(SO_PATH):
    alt = "/usr/local/lib/libplagio_segments.so"
    if os.path.exists(alt):
        SO_PATH = alt

_lib = ctypes.CDLL(SO_PATH)

# v3 search (preferred)
_lib.seg_search_many_json_v3.argtypes = [
    ctypes.c_char_p,                 # query_utf8
    ctypes.c_int,                    # top_k
    ctypes.POINTER(ctypes.c_char_p), # index_dirs
    ctypes.c_int,                    # n_dirs
    ctypes.c_int,                    # normalize_query (0/1)
    ctypes.c_int,                    # include_matches (0/1)
]
_lib.seg_search_many_json_v3.restype = ctypes.c_void_p

# fallback v2 (optional, but keep defined if you want)
_lib.seg_search_many_json_v2.argtypes = [
    ctypes.c_char_p,
    ctypes.c_int,
    ctypes.POINTER(ctypes.c_char_p),
    ctypes.c_int,
    ctypes.c_int,
]
_lib.seg_search_many_json_v2.restype = ctypes.c_void_p

# v2 excerpt
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
    try:
        return json.loads(b.decode("utf-8", errors="ignore"))
    except Exception:
        return {"count": 0, "hits": [], "error": "bad_json"}


def seg_search_many(
    *,
    query: str,
    top_k: int,
    index_dirs: List[str],
    include_matches: bool = False,
    max_matches_per_doc: Optional[int] = None,  # kept for compatibility; used only if include_matches=True
    normalize_query: bool = True,
) -> Dict[str, Any]:
    if not query or top_k <= 0 or not index_dirs:
        return {"count": 0, "hits": []}

    dirs: List[bytes] = []
    for d in index_dirs:
        p = Path(d)
        if p.exists() and p.is_dir():
            dirs.append(str(p).encode("utf-8"))

    if not dirs:
        return {"count": 0, "hits": []}

    arr = (ctypes.c_char_p * len(dirs))(*dirs)

    ptr = _lib.seg_search_many_json_v3(
        query.encode("utf-8", errors="ignore"),
        int(top_k),
        arr,
        int(len(dirs)),
        1 if normalize_query else 0,
        1 if include_matches else 0,
    )
    if not ptr:
        return {"count": 0, "hits": []}

    try:
        raw = ctypes.cast(ptr, ctypes.c_char_p).value
        if not raw:
            return {"count": 0, "hits": []}

        data = _safe_json_loads(raw)
        hits = data.get("hits") or []

        if not include_matches:
            # v3 already doesn't include "matches", but keep safe:
            for h in hits:
                h.pop("matches", None)
            return data

        # If matches requested, optionally hard-limit arrays on Python side
        if max_matches_per_doc is not None and max_matches_per_doc >= 0:
            for h in hits:
                m = h.get("matches")
                if not isinstance(m, dict):
                    continue
                q_pos = m.get("q_pos") or []
                d_pos = m.get("d_pos") or []
                hh = m.get("h") or []
                n = min(len(q_pos), len(d_pos), len(hh), int(max_matches_per_doc))
                m["q_pos"] = q_pos[:n]
                m["d_pos"] = d_pos[:n]
                m["h"] = hh[:n]

        return data
    finally:
        _lib.seg_free(ptr)


def seg_normalize_text(text: str) -> dict:
    ptr = _lib.seg_normalize_json_v1(text.encode("utf-8", errors="ignore"))
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
    normalize_text: bool = True,
) -> Dict[str, Any]:
    if not text:
        return {"ok": False, "excerpt": ""}

    ptr = _lib.seg_excerpt_for_span_json_v2(
        text.encode("utf-8", errors="ignore"),
        int(d_from),
        int(d_to),
        int(k_shingle),
        int(max_chars),
        1 if normalize_text else 0,
    )
    if not ptr:
        return {"ok": False, "excerpt": ""}

    try:
        raw = ctypes.cast(ptr, ctypes.c_char_p).value
        if not raw:
            return {"ok": False, "excerpt": ""}
        return _safe_json_loads(raw)
    finally:
        _lib.seg_free(ptr)
