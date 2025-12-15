# app/services/levels_0_4/native_segments.py
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

_lib.seg_search_many_json.argtypes = [
    ctypes.c_char_p,
    ctypes.c_int,
    ctypes.POINTER(ctypes.c_char_p),
    ctypes.c_int,
]
_lib.seg_search_many_json.restype = ctypes.c_void_p  # malloc ptr

_lib.seg_free.argtypes = [ctypes.c_void_p]
_lib.seg_free.restype = None


def _safe_json_loads(b: bytes) -> Dict[str, Any]:
    try:
        return json.loads(b.decode("utf-8", errors="ignore"))
    except Exception:
        return {"count": 0, "hits": []}


def seg_search_many(
    *,
    query: str,
    top_k: int,
    index_dirs: List[str],
    include_matches: bool = True,
    max_matches_per_doc: Optional[int] = None,  # python-side trim (C++ already has its own limit)
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
    ptr = _lib.seg_search_many_json(query.encode("utf-8"), int(top_k), arr, int(len(dirs)))
    if not ptr:
        return {"count": 0, "hits": []}

    try:
        raw = ctypes.cast(ptr, ctypes.c_char_p).value
        if not raw:
            return {"count": 0, "hits": []}

        data = _safe_json_loads(raw)

        hits = data.get("hits") or []
        if not include_matches:
            for h in hits:
                h.pop("matches", None)
            return data

        if max_matches_per_doc is not None and max_matches_per_doc >= 0:
            # trim arrays consistently
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
