# app/services/level5/index_native_export.py
import json
import gzip
import struct
from pathlib import Path
from typing import Dict, Any, List, Tuple

from ...core.config import INDEX_DIR, INDEX_JSON
from ...core.logger import logger

MAGIC = b"PLAG"
VERSION = 1

NATIVE_BIN = INDEX_DIR / "index_native.bin"
DOCIDS_JSON = INDEX_DIR / "index_native_docids.json"
META_JSON = INDEX_DIR / "index_native_meta.json"


def _split_simhash128(hex_str: str) -> Tuple[int, int]:
    """
    simhash128 хранится как 32-символьная hex-строка.
    Разбиваем на hi/lo по 64 бита.
    """
    if not hex_str:
        return 0, 0
    x = int(hex_str, 16) & ((1 << 128) - 1)
    lo = x & ((1 << 64) - 1)
    hi = (x >> 64) & ((1 << 64) - 1)
    return hi, lo


def load_index() -> Dict[str, Any]:
    """
    Грузим тяжёлый index.json / index.json.gz, который сделал build_index_json().
    Ожидаем как минимум ключи: docs_meta, inverted_doc, config.
    """
    if not INDEX_JSON.exists():
        raise FileNotFoundError(f"index.json not found: {INDEX_JSON}")

    # поддержка как обычного JSON, так и gzip-версии
    if INDEX_JSON.suffix == ".gz":
        with gzip.open(INDEX_JSON, "rt", encoding="utf-8") as f:
            idx = json.load(f)
    else:
        with open(INDEX_JSON, "r", encoding="utf-8") as f:
            idx = json.load(f)

    # лёгкая валидация
    for key in ("docs_meta", "inverted_doc", "config"):
        if key not in idx:
            raise ValueError(f"invalid index.json: missing key '{key}'")

    return idx


def export_native_index() -> None:
    """
    Генерит:
      - index_native.bin          (для C++)
      - index_native_docids.json  (для Python и C++)
      - index_native_meta.json    (облегчённая docs_meta + config для runtime)
    на основе текущего index.json.
    """
    if not INDEX_JSON.exists():
        raise FileNotFoundError(f"index.json not found: {INDEX_JSON}")

    logger.info(f"[native-export] loading index from {INDEX_JSON}")
    # грузим тяжёлый index.json — это оффлайн-этап
    idx = load_index()  # уже провалиденный и с config

    docs_meta: Dict[str, Any] = idx["docs_meta"]
    inv: Dict[str, Dict[str, List[str]]] = idx["inverted_doc"]

    inv9 = inv.get("k9") or {}
    inv13 = inv.get("k13") or {}

    # фиксируем порядок doc_id_int: просто порядок doc_ids в отдельном массиве
    doc_ids: List[str] = list(docs_meta.keys())
    N_docs = len(doc_ids)

    # мапа doc_id -> int
    doc2int: Dict[str, int] = {d: i for i, d in enumerate(doc_ids)}

    # собираем docs_meta для C++
    docs_bin_meta: List[Tuple[int, int, int]] = []
    for did in doc_ids:
        meta = docs_meta.get(did) or {}
        tok_len = int(meta.get("tok_len", 0))
        simh = meta.get("simhash128") or "0" * 32
        hi, lo = _split_simhash128(simh)
        docs_bin_meta.append((tok_len, hi, lo))

    # postings k9/k13: "плоский" список (hash, doc_id_int)
    post9: List[Tuple[int, int]] = []
    for h_str, dlist in inv9.items():
        try:
            h = int(h_str)
        except ValueError:
            continue
        for did in dlist:
            di = doc2int.get(did)
            if di is None:
                continue
            post9.append((h, di))

    post13: List[Tuple[int, int]] = []
    for h_str, dlist in inv13.items():
        try:
            h = int(h_str)
        except ValueError:
            continue
        for did in dlist:
            di = doc2int.get(did)
            if di is None:
                continue
            post13.append((h, di))

    logger.info(
        "[native-export] docs=%d, post9=%d, post13=%d",
        N_docs,
        len(post9),
        len(post13),
    )

    # пишем binary
    NATIVE_BIN.parent.mkdir(parents=True, exist_ok=True)

    with open(NATIVE_BIN, "wb") as f:
        # заголовок
        f.write(MAGIC)
        f.write(struct.pack("<I", VERSION))   # u32

        f.write(struct.pack("<I", N_docs))           # u32
        f.write(struct.pack("<Q", len(post9)))       # u64
        f.write(struct.pack("<Q", len(post13)))      # u64

        # docs_meta
        for tok_len, hi, lo in docs_bin_meta:
            f.write(struct.pack("<IQQ", tok_len, hi, lo))  # u32, u64, u64

        # postings k9
        for h, di in post9:
            f.write(struct.pack("<Q", h))                  # hash u64
            f.write(struct.pack("<I", di))                 # doc_id_int u32

        # postings k13
        for h, di in post13:
            f.write(struct.pack("<Q", h))
            f.write(struct.pack("<I", di))

    # пишем docids.json
    with open(DOCIDS_JSON, "w", encoding="utf-8") as f:
        json.dump(doc_ids, f, ensure_ascii=False)

    # пишем облегчённую мету + конфиг для runtime (без inverted_doc)
    meta_out = {
        "docs_meta": docs_meta,
        "config": idx.get("config") or {},
    }
    with open(META_JSON, "w", encoding="utf-8") as f:
        json.dump(meta_out, f, ensure_ascii=False)

    logger.info(
        "[native-export] written %s, %s and %s",
        NATIVE_BIN,
        DOCIDS_JSON,
        META_JSON,
    )
