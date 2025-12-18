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

# resolved function pointers
_search_v3 = None
_windowed_v1 = None
_excerpt = None
_normalize = None
_free = None

if _lib is not None:
    # search v3
    try:
        _search_v3 = _lib.seg_search_many_json_v3
        _search_v3.argtypes = [
            ctypes.c_char_p,                 # query_utf8
            ctypes.c_int,                    # top_k
            ctypes.POINTER(ctypes.c_char_p), # index_dirs
            ctypes.c_int,                    # n_dirs
            ctypes.c_int,                    # normalize_query (0/1)
            ctypes.c_int,                    # include_matches (0/1)
        ]
        _search_v3.restype = ctypes.c_void_p
    except Exception as e:
        _search_v3 = None
        logger.error("[native_segments] missing seg_search_many_json_v3: %s", e)

    # windowed v1 (optional until .so rebuilt)
    try:
        _windowed_v1 = _lib.seg_search_windowed_json_v1
        _windowed_v1.argtypes = [
            ctypes.c_char_p,                 # query_utf8
            ctypes.c_int,                    # top_k
            ctypes.POINTER(ctypes.c_char_p), # index_dirs
            ctypes.c_int,                    # n_dirs
            ctypes.c_int,                    # normalize_query (0/1)
            ctypes.c_int,                    # include_matches (0/1)
            ctypes.c_int,                    # win_tokens
            ctypes.c_int,                    # stride_tokens
        ]
        _windowed_v1.restype = ctypes.c_void_p
    except Exception:
        _windowed_v1 = None

    # excerpt: prefer v2, fallback to v1
    try:
        _excerpt = _lib.seg_excerpt_for_span_json_v2
        _excerpt.argtypes = [
            ctypes.c_char_p, # text_utf8
            ctypes.c_int,    # d_from
            ctypes.c_int,    # d_to
            ctypes.c_int,    # k_shingle
            ctypes.c_int,    # max_chars
            ctypes.c_int,    # normalize_text (0/1)
        ]
        _excerpt.restype = ctypes.c_void_p
    except Exception:
        try:
            _excerpt = _lib.seg_excerpt_for_span_json
            _excerpt.argtypes = [
                ctypes.c_char_p, # text_utf8
                ctypes.c_int,    # d_from
                ctypes.c_int,    # d_to
                ctypes.c_int,    # k_shingle
                ctypes.c_int,    # max_chars
            ]
            _excerpt.restype = ctypes.c_void_p
            logger.warning("[native_segments] seg_excerpt_for_span_json_v2 missing; fallback to v1")
        except Exception as e:
            _excerpt = None
            logger.error("[native_segments] missing excerpt symbols: %s", e)

    # normalize
    try:
        _normalize = _lib.seg_normalize_json_v1
        _normalize.argtypes = [ctypes.c_char_p]
        _normalize.restype = ctypes.c_void_p
    except Exception:
        _normalize = None

    # free (required)
    try:
        _free = _lib.seg_free
        _free.argtypes = [ctypes.c_void_p]
        _free.restype = None
    except Exception as e:
        _free = None
        logger.error("[native_segments] missing seg_free: %s", e)


def _safe_json_loads(b: bytes) -> Dict[str, Any]:
    if not b:
        return {"error": "empty_native_response"}

    try:
        s = b.decode("utf-8")  # strict
    except UnicodeDecodeError as e:
        return {"error": "bad_utf8", "detail": str(e), "raw_len": len(b)}

    try:
        obj = json.loads(s)
    except Exception as e:
        return {"error": "bad_json", "detail": str(e), "raw_len": len(b)}

    if isinstance(obj, dict):
        # Only normalize hits/count if this is a "hits" response
        if "hits" in obj:
            if not isinstance(obj.get("hits"), list):
                obj["hits"] = []
            if "count" not in obj:
                obj["count"] = int(len(obj.get("hits") or []))
        return obj

    return {"error": "bad_json_root"}


def _ensure_loaded() -> bool:
    return _lib is not None and _search_v3 is not None and _free is not None


def seg_search_many(
    *,
    query: str,
    top_k: int,
    index_dirs: List[str],
    include_matches: bool = False,
    max_matches_per_doc: Optional[int] = None,
    normalize_query: bool = False,
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

    ptr = _search_v3(
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

        hits = data.get("hits") if isinstance(data, dict) else None
        if hits is None or not isinstance(hits, list):
            return {"count": 0, "hits": [], "error": data.get("error", "bad_hits")} if isinstance(data, dict) else {"count": 0, "hits": []}

        if not include_matches:
            for h in hits:
                if isinstance(h, dict):
                    h.pop("matches", None)

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

        data["count"] = int(len(hits))
        return data
    finally:
        _free(ptr)


def seg_search_windowed_sources(
    *,
    query: str,
    top_k: int,
    index_dirs: List[str],
    normalize_query: bool = False,
    win_tokens: int = 120,
    stride_tokens: int = 60,
) -> Dict[str, Any]:
    """
    Returns dict like:
      {"segments":[{"source_doc_id","q_tok_from","q_tok_to"}, ...], "C": float}
    """
    if not query or top_k <= 0 or not index_dirs:
        return {"segments": [], "C": 0.0}

    if not _ensure_loaded() or _windowed_v1 is None:
        return {"segments": [], "C": 0.0, "error": "native_windowed_not_available", "detail": (_load_err or "")}

    dirs: List[bytes] = []
    for d in index_dirs:
        p = Path(d)
        if p.exists() and p.is_dir():
            dirs.append(str(p).encode("utf-8"))
    if not dirs:
        return {"segments": [], "C": 0.0}

    arr = (ctypes.c_char_p * len(dirs))(*dirs)

    ptr = _windowed_v1(
        query.encode("utf-8"),
        int(top_k),
        arr,
        int(len(dirs)),
        1 if normalize_query else 0,
        0,  # include_matches=0 for segments mode
        int(win_tokens),
        int(stride_tokens),
    )
    if not ptr:
        return {"segments": [], "C": 0.0}

    try:
        raw = ctypes.cast(ptr, ctypes.c_char_p).value or b""
        data = _safe_json_loads(raw)
        if not isinstance(data, dict):
            return {"segments": [], "C": 0.0}
        # normalize shape
        segs = data.get("segments")
        if not isinstance(segs, list):
            data["segments"] = []
        cval = data.get("C")
        if not isinstance(cval, (int, float)):
            data["C"] = 0.0
        else:
            data["C"] = float(cval)
        return data
    finally:
        _free(ptr)


def seg_normalize_text(text: str) -> Dict[str, Any]:
    if not _ensure_loaded() or _normalize is None:
        return {"ok": False, "text": "", "error": "native_not_loaded", "detail": (_load_err or "")}
    if not text:
        return {"ok": False, "text": ""}

    ptr = _normalize(text.encode("utf-8"))
    if not ptr:
        return {"ok": False, "text": ""}

    try:
        raw = ctypes.cast(ptr, ctypes.c_char_p).value or b""
        return _safe_json_loads(raw)
    finally:
        _free(ptr)


def seg_excerpt_for_span(
    *,
    text: str,
    d_from: int,
    d_to: int,
    k_shingle: int = 9,
    max_chars: int = 800,
    normalize_text: bool = False,
) -> Dict[str, Any]:
    if not text:
        return {"ok": False, "excerpt": ""}

    if not _ensure_loaded() or _excerpt is None:
        return {"ok": False, "excerpt": "", "error": "native_not_loaded", "detail": (_load_err or "")}

    argc = len(getattr(_excerpt, "argtypes", []) or [])
    if argc == 6:
        ptr = _excerpt(
            text.encode("utf-8"),
            int(d_from),
            int(d_to),
            int(k_shingle),
            int(max_chars),
            1 if normalize_text else 0,
        )
    else:
        ptr = _excerpt(
            text.encode("utf-8"),
            int(d_from),
            int(d_to),
            int(k_shingle),
            int(max_chars),
        )

    if not ptr:
        return {"ok": False, "excerpt": ""}

    try:
        raw = ctypes.cast(ptr, ctypes.c_char_p).value or b""
        return _safe_json_loads(raw)
    finally:
        _free(ptr)
