from __future__ import annotations

import ctypes
import json
import os
from pathlib import Path
from typing import Any, List, Dict

SO_PATH = os.getenv("PLAGIO_SEGMENTS_SO", "/usr/local/lib/libplagio_segments.so")

if not os.path.exists(SO_PATH):
    # если кто-то всё же указал /runtime/... а volume пустой
    alt = "/usr/local/lib/libplagio_segments.so"
    if os.path.exists(alt):
        SO_PATH = alt

_lib = ctypes.CDLL(SO_PATH)



# char* seg_search_many_json(const char* query_utf8, int top_k, const char** index_dirs_utf8, int n_dirs)
_lib.seg_search_many_json.argtypes = [
    ctypes.c_char_p,
    ctypes.c_int,
    ctypes.POINTER(ctypes.c_char_p),
    ctypes.c_int,
]
_lib.seg_search_many_json.restype = ctypes.c_void_p  # malloc ptr

_lib.seg_free.argtypes = [ctypes.c_void_p]
_lib.seg_free.restype = None


def seg_search_many(query: str, top_k: int, index_dirs: List[str]) -> Dict[str, Any]:
    if not query or top_k <= 0 or not index_dirs:
        return {"count": 0, "hits": []}

    # нормализуем и фильтруем реально существующие директории
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
        s = ctypes.cast(ptr, ctypes.c_char_p).value
        if not s:
            return {"count": 0, "hits": []}
        return json.loads(s.decode("utf-8", errors="ignore"))
    finally:
        _lib.seg_free(ptr)
