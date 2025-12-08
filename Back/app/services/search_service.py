# app/services/search_service.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List

import ctypes

from app.core.config import INDEX_DIR
from app.models.segment import Segment


# ───────────────────────────────────────────────────────────────
# Модель хита (то, что возвращаем наружу)
# ───────────────────────────────────────────────────────────────

@dataclass
class SearchHit:
    doc_id: int          # пока: внутренний doc_id_int из C++
    score: float
    shard_id: int
    segment_id: int


# ───────────────────────────────────────────────────────────────
# Хелперы по сегментам (как у тебя было)
# ───────────────────────────────────────────────────────────────

def resolve_segment_dir(segment: Segment) -> Path:
    """
    Превращаем segment.path (типа 'shard_0/segment_1_dummy')
    в реальный путь на диске.
    Пока C++ ядро работает с одним глобальным индексом (INDEX_DIR),
    но эта функция останется, когда пойдём в multi-segment/mmap.
    """
    return INDEX_DIR / segment.path


def select_segments_for_search(
    segments: List[Segment],
    max_levels: int | None = None,
) -> List[Segment]:
    """
    Стратегия выбора сегментов для поиска.

    Сейчас: берём все level>=1, status='ready', опционально режем по max_levels.
    Потом сюда можно добавить:
      - приоритет L0/L1
      - LRU по last_access_at
      - лимит по суммарному объёму индекса.
    """
    ready = [s for s in segments if s.status == "ready"]

    if max_levels is not None:
        lvl_cut: List[Segment] = []
        seen_levels: set[int] = set()
        for s in sorted(ready, key=lambda x: x.level):
            if len(seen_levels) >= max_levels and s.level not in seen_levels:
                continue
            lvl_cut.append(s)
            seen_levels.add(s.level)
        ready = lvl_cut

    return ready


# ───────────────────────────────────────────────────────────────
# ctypes-обёртки над C++ (ABI из search_core.h)
# ───────────────────────────────────────────────────────────────

class SeHit(ctypes.Structure):
    _fields_ = [
        ("doc_id_int", ctypes.c_int),
        ("score", ctypes.c_double),
        ("j9", ctypes.c_double),
        ("c9", ctypes.c_double),
        ("j13", ctypes.c_double),
        ("c13", ctypes.c_double),
        ("cand_hits", ctypes.c_int),
    ]


class SeSearchResult(ctypes.Structure):
    _fields_ = [
        ("count", ctypes.c_int),
    ]


LIB_PATH = Path("/usr/local/lib/libsearchcore.so")
if not LIB_PATH.exists():
    raise RuntimeError(f"C++ search lib not found at {LIB_PATH}")

_lib = ctypes.CDLL(str(LIB_PATH))

_lib.se_load_index.argtypes = [ctypes.c_char_p]
_lib.se_load_index.restype = ctypes.c_int

_lib.se_search_text.argtypes = [
    ctypes.c_char_p,                 # text_utf8
    ctypes.c_int,                    # top_k
    ctypes.POINTER(SeHit),           # out_hits
    ctypes.c_int,                    # max_hits
]
_lib.se_search_text.restype = SeSearchResult


# ───────────────────────────────────────────────────────────────
# Загрузка индекса
# ───────────────────────────────────────────────────────────────

_INDEX_LOADED = False


def _ensure_index_loaded() -> None:
    """
    Загружаем ОДИН глобальный индекс из INDEX_DIR.
    C++ внутри использует std::call_once, поэтому путь по факту
    зафиксируется один раз на процесс.
    """
    global _INDEX_LOADED
    if _INDEX_LOADED:
        return

    index_dir = str(INDEX_DIR)
    rc = _lib.se_load_index(index_dir.encode("utf-8"))
    if rc != 0:
        raise RuntimeError(f"se_load_index failed for dir={index_dir}, rc={rc}")

    _INDEX_LOADED = True


# ───────────────────────────────────────────────────────────────
# Основная функция поиска
# ───────────────────────────────────────────────────────────────

def run_cpp_search(
    norm_text: str,
    shard_id: int,
    segments: List[Segment],
    top_k: int = 10,
) -> List[SearchHit]:
    """
    Реальный поиск через C++ search_core.

    Важно:
      - сейчас C++ ядро загружает ОДИН глобальный индекс из INDEX_DIR
        (index_native.bin + index_native_docids.json).
      - параметр `segments` пока используется только чтобы проставить
        segment_id в ответ (берём первый ready-сегмент, если есть).

    norm_text — уже нормализованный текст (norm_for_local),
    C++ внутри ещё раз прогонит normalize_for_shingles_simple,
    но это не ломает консистентность (просто «двойная чистка»).
    """
    if not norm_text.strip():
        return []

    _ensure_index_loaded()

    if not segments:
        # индекса нет/сегментов нет — честно возвращаем пусто
        return []

    segment = segments[0]  # пока один глобальный индекс → мапим на первый сегмент

    MAX_HITS = max(64, top_k)
    buf = (SeHit * MAX_HITS)()

    res = _lib.se_search_text(
        norm_text.encode("utf-8"),
        int(top_k),
        buf,
        MAX_HITS,
    )

    hits: List[SearchHit] = []

    for i in range(res.count):
        h = buf[i]
        hits.append(
            SearchHit(
                doc_id=int(h.doc_id_int),   # сейчас это внутренний doc_id_int из индекса
                score=float(h.score),
                shard_id=shard_id,
                segment_id=segment.id,
            )
        )

    return hits
