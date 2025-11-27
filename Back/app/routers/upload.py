import hashlib
import os
import json
import zipfile
from typing import Iterator, Dict, Any, List, Tuple, Set

import orjson
import subprocess

from pathlib import Path

from fastapi import APIRouter, UploadFile, File, HTTPException, Query

from ..core.runtime_cfg import get_runtime_cfg
from ..core.memlog import log_mem

from ..core.config import (
    CORPUS_JSONL,
    INDEX_DIR,
    UPLOAD_DIR,
)
from ..core.io_utils import file_lock
from ..core.logger import logger

from ..services.converters.pdf_heavy import extract_and_normalize_pdf
from ..services.converters.pdf_convert import smart_pdf_to_docx
from ..services.converters.docx_utils import extract_docx_text
from ..services.helpers.normalizer import simple_tokens

router = APIRouter(prefix="/api", tags=["Operations"])

# C++ индексы лежат прямо в INDEX_DIR
INDEX_NATIVE_META = INDEX_DIR / "index_native_meta.json"
DOCIDS_NATIVE = INDEX_DIR / "index_native_docids.json"


# ---------- helpers ----------
def _iter_jsonl(path) -> Iterator[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if "doc_id" in obj and "text" in obj:
                    obj["_line_no"] = i
                    yield obj
            except json.JSONDecodeError:
                continue


def _chunked(seq, size: int):
    """Разбивает последовательность на куски по size."""
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def _bytes_to_text_utf8(raw: bytes) -> str:
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("utf-8", "ignore")


def _build_native_index() -> None:
    """
    Строит C++-индекс через бинарь `index_builder`.

    Ожидаемый CLI:
        index_builder <corpus_jsonl> <index_dir>

    Где:
      - corpus_jsonl = CORPUS_JSONL
      - index_dir    = директория, в которой лежат index_native.* файлы
    """
    index_dir = INDEX_DIR
    index_dir.mkdir(parents=True, exist_ok=True)

    log_mem(
        f"[index_build] native: before index_builder, corpus={CORPUS_JSONL}, index_dir={index_dir}"
    )
    logger.info(
        "[index_build] NATIVE start: corpus_jsonl=%s, index_dir=%s",
        CORPUS_JSONL,
        index_dir,
    )

    try:
        subprocess.run(
            [
                "index_builder",
                str(CORPUS_JSONL),
                str(index_dir),
            ],
            check=True,
        )
    except FileNotFoundError as e:
        logger.error("[index_build] native index_builder not found in PATH: %s", e)
        raise HTTPException(500, "native index_builder binary not found")
    except subprocess.CalledProcessError as e:
        logger.error("[index_build] native index_builder failed: %s", e)
        raise HTTPException(500, f"native index_builder failed: {e}")

    log_mem("[index_build] native: after index_builder")
    logger.info("[index_build] NATIVE done")


def _guess_ext(fname: str) -> str:
    fname = (fname or "").lower()
    if fname.endswith((".txt", ".log", ".md")):
        return "txt"
    if fname.endswith(".docx"):
        return "docx"
    if fname.endswith(".pdf"):
        return "pdf"
    return ""


def _collect_corpus_stats() -> Tuple[int, Set[str]]:
    """Подсчёт строк и множества doc_id в corpus.jsonl."""
    lines = 0
    ids: Set[str] = set()
    if not CORPUS_JSONL.exists():
        return 0, set()

    with open(CORPUS_JSONL, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            lines += 1
            try:
                obj = json.loads(s)
            except Exception:
                continue
            did = obj.get("doc_id")
            if did:
                ids.add(did)
    return lines, ids


def _collect_index_ids_safe() -> Set[str]:
    """
    doc_id из:
      1) index_native_meta.json (если есть и что-то в нём есть),
      2) fallback: index_native_docids.json (массива doc_id).
    Если ничего нет — пустое множество.
    """
    ids: Set[str] = set()

    # --- 1) пробуем meta (docs_meta / docs) ---
    if INDEX_NATIVE_META.exists():
        try:
            with open(INDEX_NATIVE_META, "r", encoding="utf-8") as f:
                meta = json.load(f)
        except Exception as e:
            logger.error("[_collect_index_ids_safe] failed to read meta: %s", e)
            meta = {}

        docs_meta = meta.get("docs_meta")
        if isinstance(docs_meta, dict):
            ids.update(str(did) for did in docs_meta.keys())

        docs = meta.get("docs")
        if isinstance(docs, list):
            for d in docs:
                if isinstance(d, dict):
                    did = d.get("doc_id")
                    if isinstance(did, str):
                        ids.add(did)

        if ids:
            logger.info(
                "[_collect_index_ids_safe] got %d ids from index_native_meta.json",
                len(ids),
            )
            return ids

    # --- 2) fallback: index_native_docids.json ---
    if DOCIDS_NATIVE.exists():
        try:
            with open(DOCIDS_NATIVE, "r", encoding="utf-8") as f:
                lst = json.load(f)
            for did in lst:
                if isinstance(did, str):
                    ids.add(did)
            logger.info(
                "[_collect_index_ids_safe] got %d ids from index_native_docids.json",
                len(ids),
            )
        except Exception as e:
            logger.error("[_collect_index_ids_safe] failed to read docids: %s", e)

    return ids


def _ingest_single(
    raw: bytes,
    fname: str,
    normalize: bool,
    save_docx: bool,
    ocr: bool,
    ocr_mode: str,
    title: str | None = None,
    author: str | None = None,
    *,
    file_idx: int | None = None,
    total_files: int | None = None,
) -> Dict[str, Any]:
    """
    Общий пайплайн для одного файла.

    Нормализацию для шинглов сейчас делаем ТОЛЬКО в C++-части
    (index_builder / search_core), поэтому здесь normalize принудительно
    выключен.
    """
    # Python-нормализация полностью выключена — всё делает C++
    normalize = False

    prefix = ""
    if file_idx is not None and total_files is not None:
        prefix = f"[{file_idx}/{total_files}] "

    log_mem(f"{prefix}ingest_single: start fname={fname}")
    rt_cfg = get_runtime_cfg()
    ocr_lang = rt_cfg.ocr.lang
    ocr_workers = rt_cfg.ocr.workers

    ctype_guess = _guess_ext(fname)

    logger.info(
        "%s[ingest_single] start fname=%s, ext=%s, bytes=%d, normalize=%s, save_docx=%s, ocr=%s, ocr_mode=%s",
        prefix,
        fname,
        ctype_guess,
        len(raw),
        normalize,
        save_docx,
        ocr,
        ocr_mode,
    )

    # 1) извлечение текста
    if ctype_guess == "txt":
        text = _bytes_to_text_utf8(raw)

    elif ctype_guess == "docx":
        try:
            text = extract_docx_text(raw)
        except RuntimeError as e:
            logger.error("%s[ingest_single] docx extract failed: %s", prefix, e)
            raise HTTPException(500, str(e))

    elif ctype_guess == "pdf":
        # heavy PDF extract (OCR / layout), НЕ шингловая нормализация
        try:
            text = extract_and_normalize_pdf(
                raw,
                try_ocr=ocr,
                dpi=200,
                psm=3,
                return_debug=False,
                ocr_mode=ocr_mode,
                lang=ocr_lang,
                ocr_workers=ocr_workers,
            )
        except Exception as e:
            logger.error("%s[ingest_single] PDF extract failed: %s", prefix, e)
            raise HTTPException(500, f"PDF extract failed: {e}")

        # сохранить DOCX-версию
        if save_docx:
            try:
                docx_bytes = smart_pdf_to_docx(
                    raw,
                    try_ocr=ocr,
                    ocr_mode=ocr_mode,
                    force_ocr=False,
                    lang=ocr_lang,
                    ocr_workers=ocr_workers,
                )
                UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
                base = hashlib.sha256(raw).hexdigest()[:12]
                out_path = UPLOAD_DIR / f"{base}.docx"
                out_path.write_bytes(docx_bytes)
            except Exception as e:
                logger.error("%sdocx conversion failed for %s: %s", prefix, fname, e)

    else:
        logger.error("%s[ingest_single] unsupported extension fname=%s", prefix, fname)
        raise HTTPException(400, f"unsupported file extension: {fname or 'unknown'}")

    if not text.strip():
        logger.warning("%s[ingest_single] empty text after extraction fname=%s", prefix, fname)
        raise HTTPException(400, "empty text after extraction")

    if not title:
        base_name = Path(fname).name if fname else ""
        if "." in base_name:
            title = base_name.rsplit(".", 1)[0]
        else:
            title = base_name or ""

    if author is None:
        author = ""

    # 3) запись в корпус (сырые тексты, нормализация будет в C++)
    CORPUS_JSONL.parent.mkdir(parents=True, exist_ok=True)
    doc_id = f"doc_{hashlib.sha256(text.encode('utf-8')).hexdigest()[:8]}"

    lock_path = CORPUS_JSONL.with_suffix(".lock")
    with file_lock(lock_path):
        with open(CORPUS_JSONL, "ab") as f:
            f.write(
                orjson.dumps(
                    {
                        "doc_id": doc_id,
                        "text": text,
                        "title": title,
                        "author": author,
                    }
                )
                + b"\n"
            )

    # токены считаем по сырым (для статистики)
    toks = simple_tokens(text)

    logger.info(
        "%s[ingest_single] done doc_id=%s, filename=%s, title=%s, bytes=%d, tokens=%d",
        prefix,
        doc_id,
        fname,
        title,
        len(raw),
        len(toks),
    )
    log_mem(f"{prefix}ingest_single: done doc_id={doc_id}")

    return {
        "doc_id": doc_id,
        "filename": fname,
        "title": title,
        "author": author,
        "bytes": len(raw),
        "tokens": len(toks),
    }


# ---------- routes ----------

@router.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    normalize: bool = Query(
        False,
        description="(зарезервировано) Python-нормализация отключена, используется C++",
    ),
    save_docx: bool = Query(True),
    ocr: bool = Query(True, description="Включить OCR, если PDF пустой"),
    ocr_mode: str = Query(
        "speed",
        pattern="^(speed|balanced|quality)$",
        description="OCR режим: speed / balanced / quality",
    ),
    title: str | None = Query(
        None,
        description="Название документа (если не указано — берётся из имени файла)",
    ),
    author: str | None = Query(
        None,
        description="Автор документа (если не указано — пустая строка)",
    ),
):
    fname = (file.filename or "").lower()
    raw = await file.read()

    logger.info(
        "[upload_file] start fname=%s, bytes=%d, normalize=%s, save_docx=%s, ocr=%s, ocr_mode=%s",
        fname,
        len(raw),
        normalize,
        save_docx,
        ocr,
        ocr_mode,
    )
    log_mem("upload_file: before _ingest_single")

    result = _ingest_single(
        raw=raw,
        fname=fname,
        normalize=normalize,
        save_docx=save_docx,
        ocr=ocr,
        ocr_mode=ocr_mode,
        title=title,
        author=author,
    )

    logger.info(
        "[upload_file] done doc_id=%s, tokens=%d",
        result.get("doc_id"),
        result.get("tokens"),
    )
    log_mem(f"upload_file: after _ingest_single doc_id={result.get('doc_id')}")

    return result


# ---------- массовая загрузка из ZIP ----------

@router.post("/upload/zip_admin")
async def upload_zip_admin(
    file: UploadFile = File(...),
    normalize: bool = Query(
        False,
        description="(зарезервировано) Python-нормализация отключена, используется C++",
    ),
    save_docx: bool = Query(True),
    ocr: bool = Query(True, description="Включить OCR для PDF внутри архива"),
    ocr_mode: str = Query(
        "speed",
        pattern="^(speed|balanced|quality)$",
        description="OCR режим: speed / balanced / quality",
    ),
    batch_size: int = Query(
        100,
        ge=1,
        le=1000,
        description="Сколько файлов обрабатывать в одном батче внутри архива",
    ),
):
    """
    ZIP загрузка:
      ✓ Батчи
      ✓ Параллельное CPU/OCR
      ✓ Красивые логи:  051/200  | OK      | book.docx   | doc_ab12ce
    """
    import asyncio
    import gc as _gc
    from starlette.concurrency import run_in_threadpool

    # ---------- открыть ZIP ----------
    try:
        file.file.seek(0)
        zf = zipfile.ZipFile(file.file)
    except Exception as e:
        logger.error("[upload_zip_admin] not a valid ZIP: %s", e)
        raise HTTPException(400, f"not a valid ZIP: {e}")

    # ---------- собрать валидные файлы ----------
    infos: List[zipfile.ZipInfo] = []
    for info in zf.infolist():
        name = info.filename.rsplit("/", 1)[-1]
        if not name or info.is_dir() or name.startswith("__MACOSX"):
            continue
        infos.append(info)

    total = len(infos)
    if total == 0:
        logger.info(
            "[upload_zip_admin] empty archive=%s (no valid files)", file.filename
        )
        return {
            "archive_name": file.filename or "archive.zip",
            "total_files": 0,
            "processed": 0,
            "items": [],
        }

    logger.info(
        "[upload_zip_admin] START archive=%s, files=%d, batch_size=%d normalize=%s save_docx=%s ocr=%s mode=%s",
        file.filename,
        total,
        batch_size,
        normalize,
        save_docx,
        ocr,
        ocr_mode,
    )
    log_mem("upload_zip_admin: start")

    def _short(name: str, maxlen: int = 30) -> str:
        """Обрезка длинных имён для логов."""
        return name if len(name) <= maxlen else name[:27] + "..."

    items: List[Dict[str, Any]] = []
    processed = 0
    idx = 0

    # ---------- обрабатываем батчами ----------
    for batch in _chunked(infos, batch_size):
        tasks = []
        meta: List[Tuple[int, str]] = []  # номер:имя

        # --- подготовка задач ---
        for info in batch:
            idx += 1
            name = info.filename.rsplit("/", 1)[-1]

            try:
                raw = zf.read(info)
            except Exception as e:
                pct = idx * 100.0 / total
                logger.error(
                    "[upload_zip_admin] %03d/%03d (%.1f%%) | ERROR   | %-30s | %s",
                    idx,
                    total,
                    pct,
                    _short(name),
                    f"read-failed: {e}",
                )
                items.append({"filename": name, "status": "error", "error": str(e)})
                continue

            meta.append((idx, name))
            tasks.append(
                run_in_threadpool(
                    _ingest_single,
                    raw,
                    name,
                    normalize,
                    save_docx,
                    ocr,
                    ocr_mode,
                    None,   # title
                    None,   # author
                    file_idx=idx,
                    total_files=total,
                )
            )

        if not tasks:
            _gc.collect()
            continue

        # --- Параллельно запускаем задачи ---
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # --- Разбираем результаты ---
        for (n, name), res in zip(meta, results):
            pct = n * 100.0 / total

            if isinstance(res, HTTPException):
                items.append(
                    {"filename": name, "status": "skipped", "error": res.detail}
                )
                logger.warning(
                    "[upload_zip_admin] %03d/%03d (%.1f%%) | SKIPPED | %-30s | %s",
                    n,
                    total,
                    pct,
                    _short(name),
                    res.detail,
                )
                continue

            elif isinstance(res, Exception):
                items.append(
                    {"filename": name, "status": "error", "error": str(res)}
                )
                logger.error(
                    "[upload_zip_admin] %03d/%03d (%.1f%%) | ERROR   | %-30s | %s",
                    n,
                    total,
                    pct,
                    _short(name),
                    res,
                )
                continue

            # ---------- SUCCESS ----------
            processed += 1
            obj = dict(res)
            obj.setdefault("status", "ok")
            items.append(obj)

            logger.info(
                "[upload_zip_admin] %03d/%03d (%.1f%%) | OK      | %-30s | %s",
                n,
                total,
                pct,
                _short(name),
                obj.get("doc_id"),
            )

        # ---- GC после батча ----
        _gc.collect()
        log_mem(f"upload_zip_admin: after batch {processed}/{total}")
        logger.info(
            "[upload_zip_admin] batch done processed=%d/%d", processed, total
        )

    # ---------- FINISH ----------
    logger.info(
        "[upload_zip_admin] DONE archive=%s total=%d processed=%d",
        file.filename,
        total,
        processed,
    )
    log_mem("upload_zip_admin: done")

    return {
        "archive_name": file.filename or "archive.zip",
        "total_files": total,
        "processed": processed,
        "items": items,
    }


@router.post("/build")
def build_index_native():
    """
    Строим ТОЛЬКО C++-индекс (index_native.*) поверх corpus.jsonl.
    Python-индексов не касаемся.
    """
    logger.info("[index_build_native] === /api/build called (native only) ===")
    log_mem("index_build_native: STAGE 0 before stats")

    # ── STAGE 1: статы ДО ────────────────────────────────────────────
    corpus_lines_before, corpus_ids = _collect_corpus_stats()
    index_ids_before = _collect_index_ids_safe()
    unindexed_before = corpus_ids - index_ids_before

    logger.info(
        "[index_build_native] STAGE 1 BEFORE\n"
        "  corpus.jsonl: lines=%d, doc_ids=%d\n"
        "  indexed_docs(native)=%d\n"
        "  unindexed_docs(native)=%d",
        corpus_lines_before,
        len(corpus_ids),
        len(index_ids_before),
        len(unindexed_before),
    )
    log_mem("index_build_native: STAGE 1 BEFORE")

    # ── STAGE 2: C++ index_builder ───────────────────────────────────
    log_mem("[index_build_native] STAGE 2 before index_builder")
    _build_native_index()
    log_mem("[index_build_native] STAGE 2 after index_builder")

    # ── STAGE 3: статы ПОСЛЕ ────────────────────────────────────────
    index_ids_after = _collect_index_ids_safe()
    unindexed_after = corpus_ids - index_ids_after
    delta_indexed = len(index_ids_after) - len(index_ids_before)

    logger.info(
        "[index_build_native] STAGE 3 AFTER\n"
        "  corpus.jsonl: lines=%d, doc_ids=%d\n"
        "  indexed_docs(native)=%d\n"
        "  unindexed_docs(native)=%d\n"
        "  delta_indexed=%d",
        corpus_lines_before,
        len(corpus_ids),
        len(index_ids_after),
        len(unindexed_after),
        delta_indexed,
    )
    log_mem("index_build_native: STAGE 3 AFTER")

    return {
        "index_dir": str(INDEX_DIR),
        "index_native_meta": str(INDEX_NATIVE_META),
        "stats_before": {
            "corpus_lines": corpus_lines_before,
            "corpus_docs": len(corpus_ids),
            "indexed_docs": len(index_ids_before),
            "unindexed_docs": len(unindexed_before),
        },
        "stats_after": {
            "corpus_lines": corpus_lines_before,
            "corpus_docs": len(corpus_ids),
            "indexed_docs": len(index_ids_after),
            "unindexed_docs": len(unindexed_after),
            "delta_indexed": delta_indexed,
        },
    }


@router.delete(
    "/corpus/cleanup",
    summary="Удалить полностью базу или только неиндексированные документы",
)
def corpus_cleanup(
    delete_all: bool = Query(
        False,
        description="true — удалить ВСЮ базу и C++-индекс; false — удалить только неиндексированные",
    ),
):
    # ===================== ВЕСЬ CORPUS + INDEX =====================
    if delete_all:
        log_mem("corpus_cleanup: delete_all")

        removed: List[str] = []
        for p in (
            CORPUS_JSONL,
            INDEX_NATIVE_META,
            INDEX_DIR / "index_native.bin",
            INDEX_DIR / "index_native_docids.json",
        ):
            if p.exists():
                try:
                    os.remove(p)
                    removed.append(str(p))
                except OSError as e:
                    raise HTTPException(500, f"cannot remove {p.name}: {e}")

        logger.info("[corpus_cleanup] delete_all removed=%s", removed)
        log_mem("corpus_cleanup: delete_all done")
        return {
            "status": "ok",
            "mode": "all",
            "removed": removed,
        }

    # ===================== ТОЛЬКО НЕИНДЕКСИРОВАННЫЕ =====================
    log_mem("corpus_cleanup: unindexed_only")

    corpus_lines_before, corpus_ids = _collect_corpus_stats()
    index_ids = _collect_index_ids_safe()
    unindexed = corpus_ids - index_ids

    if not CORPUS_JSONL.exists() or not corpus_ids:
        logger.info("[corpus_cleanup] nothing to clean: corpus empty")
        return {
            "status": "ok",
            "mode": "unindexed",
            "corpus_lines_before": corpus_lines_before,
            "corpus_docs_before": len(corpus_ids),
            "indexed_docs": len(index_ids),
            "unindexed_docs": len(unindexed),
            "corpus_lines_after": 0,
            "corpus_docs_after": 0,
        }

    if not unindexed:
        logger.info(
            "[corpus_cleanup] nothing to remove: all docs are indexed (corpus_docs=%d)",
            len(corpus_ids),
        )
        return {
            "status": "ok",
            "mode": "unindexed",
            "corpus_lines_before": corpus_lines_before,
            "corpus_docs_before": len(corpus_ids),
            "indexed_docs": len(index_ids),
            "unindexed_docs": 0,
            "corpus_lines_after": corpus_lines_before,
            "corpus_docs_after": len(corpus_ids),
        }

    # переписываем corpus.jsonl, оставляя только doc_id, которые есть в index_ids
    lock_path = CORPUS_JSONL.with_suffix(".lock")
    kept_lines = 0
    kept_docs: Set[str] = set()

    tmp_path = CORPUS_JSONL.with_suffix(".tmp")

    with file_lock(lock_path):
        with open(CORPUS_JSONL, "r", encoding="utf-8") as src, open(
            tmp_path, "w", encoding="utf-8"
        ) as dst:
            for line in src:
                s = line.strip()
                if not s:
                    continue
                try:
                    obj = json.loads(s)
                except Exception:
                    continue
                did = obj.get("doc_id")
                if isinstance(did, str) and did in index_ids:
                    dst.write(line)
                    kept_lines += 1
                    kept_docs.add(did)

        os.replace(tmp_path, CORPUS_JSONL)

    logger.info(
        "[corpus_cleanup] unindexed_only: removed=%d, kept=%d",
        len(unindexed),
        kept_lines,
    )
    log_mem("corpus_cleanup: unindexed_only done")

    return {
        "status": "ok",
        "mode": "unindexed",
        "corpus_lines_before": corpus_lines_before,
        "corpus_docs_before": len(corpus_ids),
        "indexed_docs": len(index_ids),
        "unindexed_docs": len(unindexed),
        "corpus_lines_after": kept_lines,
        "corpus_docs_after": len(kept_docs),
    }
