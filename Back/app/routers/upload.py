import hashlib, io, os, json, re, zipfile
from collections import deque
from typing import Iterator, Dict, Any, List
from ..core.runtime_cfg import get_runtime_cfg

import orjson
from fastapi import APIRouter, UploadFile, File, HTTPException, Query
from fastapi.responses import Response
from pathlib import Path

from ..core.config import (
    CORPUS_JSONL,
    INDEX_JSON,
    UPLOAD_DIR,
    OCR_LANG_DEFAULT,
    OCR_WORKERS_DEFAULT,
)
from ..core.io_utils import file_lock
from ..core.logger import logger

from ..services.index_build import build_index_json
from ..services.index_search import clear_index_cache, load_index_cached
from ..services.normalizer import normalize_for_shingles, simple_tokens
from ..services.pdf_heavy import extract_and_normalize_pdf
from ..services.pdf_convert import smart_pdf_to_docx
from ..services.docx_utils import extract_docx_text

router = APIRouter(prefix="/api", tags=["Operations"])

# --- optional parsers ---
try:
    from docx import Document
except Exception:
    Document = None
# ------------------------


# ---------- helpers ----------

def _bytes_to_text_utf8(raw: bytes) -> str:
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("utf-8", "ignore")

def _docx_to_text(raw: bytes) -> str:
    if Document is None:
        raise HTTPException(500, "python-docx not installed")
    f = io.BytesIO(raw)
    doc = Document(f)
    parts = [p.text for p in doc.paragraphs if p.text]
    for tbl in doc.tables:
        for row in tbl.rows:
            parts.append(" ".join(c.text for c in row.cells if c.text))
    return "\n".join(parts)

def _guess_ext(fname: str) -> str:
    fname = (fname or "").lower()
    if fname.endswith((".txt", ".log", ".md")):
        return "txt"
    if fname.endswith(".docx"):
        return "docx"
    if fname.endswith(".pdf"):
        return "pdf"
    return ""

def _ingest_single(
    raw: bytes,
    fname: str,
    normalize: bool,
    save_docx: bool,
    ocr: bool,
    ocr_mode: str,
    title: str | None = None,
    author: str | None = None,
) -> Dict[str, Any]:
    """
    Общий пайплайн для одного файла.
    Возвращает dict с doc_id, токенами и пр.
    """
    rt_cfg = get_runtime_cfg()
    ocr_lang = rt_cfg.ocr.lang
    ocr_workers = rt_cfg.ocr.workers

    ctype_guess = _guess_ext(fname)
    text: str

    # 1) извлечение текста
    if ctype_guess == "txt":
        text = _bytes_to_text_utf8(raw)

    elif ctype_guess == "docx":
        try:
            text = extract_docx_text(raw)
        except RuntimeError as e:
            raise HTTPException(500, str(e))

    elif ctype_guess == "pdf":
        # heavy PDF normalize for indexing
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
                logger.error(f"docx conversion failed for {fname}: {e}")

    else:
        raise HTTPException(400, f"unsupported file extension: {fname or 'unknown'}")

    # 2) нормализация
    if normalize:
        text = normalize_for_shingles(text)

    if not text.strip():
        raise HTTPException(400, "empty text after extraction")

    if not title:
        base_name = Path(fname).name if fname else ""
        if "." in base_name:
            title = base_name.rsplit(".", 1)[0]
        else:
            title = base_name or ""

    if author is None:
        author = ""

    # 3) запись в корпус
    CORPUS_JSONL.parent.mkdir(parents=True, exist_ok=True)
    doc_id = f"doc_{hashlib.sha256(text.encode('utf-8')).hexdigest()[:8]}"

    with file_lock(CORPUS_JSONL.with_suffix(".lock")):
        with open(CORPUS_JSONL, "ab") as f:
            f.write(orjson.dumps(
                {
                    "doc_id": doc_id,
                    "text": text,
                    "title": title,
                    "author": author,
                }        
            ) + b"\n")

    toks = simple_tokens(text)

    return {
        "doc_id": doc_id,
        "filename": fname,
        "title": title,
        "author": author,
        "bytes": len(raw),
        "tokens": len(toks),
    }


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

# ---------- routes ----------
@router.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    normalize: bool = Query(True),
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
    return result


# ---------- массовая загрузка из ZIP ----------

@router.post("/upload/zip")
async def upload_zip(
    file: UploadFile = File(...),
    normalize: bool = Query(True),
    save_docx: bool = Query(True),
    ocr: bool = Query(True, description="Включить OCR для PDF внутри архива"),
    ocr_mode: str = Query(
        "speed",
        pattern="^(speed|balanced|quality)$",
        description="OCR режим: speed / balanced / quality",
    ),
):
    """
    Массовая загрузка из zip-архива.
    Внутри поддерживает те же типы, что /upload: .txt, .docx, .pdf.
    Остальные файлы пропускаются с ошибкой в items.
    """
    raw_zip = await file.read()

    try:
        zf = zipfile.ZipFile(io.BytesIO(raw_zip))
    except Exception as e:
        raise HTTPException(400, f"not a valid ZIP: {e}")

    items: List[Dict[str, Any]] = []
    total = 0
    processed = 0

    for info in zf.infolist():
        name = info.filename.rsplit("/", 1)[-1]
        # пропускаем директории и системный мусор
        if info.is_dir():
            continue
        name = info.filename.rsplit("/", 1)[-1]
        if not name:  # странные записи
            continue
        if name.startswith("__MACOSX"):
            continue

        total += 1
        try:
            raw = zf.read(info)
        except Exception as e:
            items.append(
                {
                    "filename": name,
                    "error": f"cannot read from zip: {e}",
                }
            )
            continue

        try:
            res = _ingest_single(
                raw=raw,
                fname=name,
                normalize=normalize,
                save_docx=save_docx,
                ocr=ocr,
                ocr_mode=ocr_mode,
                title=None,   # возьмётся из имени файла
                author=None,  # пустой
            )
            processed += 1
            items.append(res)
        except HTTPException as e:
            items.append(
                {
                    "filename": name,
                    "status": "skipped",
                    "error": e.detail,
                }
            )
        except Exception as e:
            items.append(
                {
                    "filename": name,
                    "status": "error",
                    "error": str(e),
                }
            )

    return {
        "archive_name": file.filename or "archive.zip",
        "total_files": total,
        "processed": processed,
        "items": items,
    }

# ---------- index ops ----------

@router.post("/build")
def build_index():
    rt_cfg = get_runtime_cfg()
    try:
        idx = build_index_json(cfg=rt_cfg.index.model_dump())
    except FileNotFoundError:
        raise HTTPException(400, "corpus.jsonl not found")
    clear_index_cache()
    _ = load_index_cached()
    return {
        "index_path": str(INDEX_JSON),
        "docs": len(idx["docs_meta"]),
        "k5": len(idx["inverted_doc"]["k5"]),
        "k9": len(idx["inverted_doc"]["k9"]),
        "k13": len(idx["inverted_doc"]["k13"]),
    }

@router.delete("/reset")
def reset_all():
    removed = []
    for p in (CORPUS_JSONL, INDEX_JSON):
        if p.exists():
            try:
                os.remove(p)
                removed.append(str(p))
            except OSError as e:
                raise HTTPException(500, f"cannot remove {p.name}: {e}")
    clear_index_cache()
    return {"status": "ok", "removed": removed}

# ---------- точечное удаление из корпуса по doc_id ----------

@router.delete("/corpus/delete")
def corpus_delete(
    doc_id: List[str] = Query(..., description="doc_id можно передать несколько раз"),
):
    """
    Точечное удаление документов из corpus.jsonl по doc_id.
    Другие записи не трогаем.
    Пример:
      DELETE /api/corpus/delete?doc_id=doc_123&doc_id=doc_456
    """
    if not CORPUS_JSONL.exists():
        raise HTTPException(404, "corpus.jsonl not found")

    to_remove = set(doc_id)
    removed = 0

    lock_path = CORPUS_JSONL.with_suffix(".lock")
    tmp_path = CORPUS_JSONL.with_suffix(".tmp")

    with file_lock(lock_path):
        with open(CORPUS_JSONL, "rb") as src, open(tmp_path, "wb") as dst:
            for line in src:
                line_stripped = line.strip()
                if not line_stripped:
                    continue
                try:
                    obj = orjson.loads(line_stripped)
                except Exception:
                    # если вдруг битая строка — прокидываем как есть
                    dst.write(line)
                    continue

                if obj.get("doc_id") in to_remove:
                    removed += 1
                    continue

                # оставляем как есть
                dst.write(line)

        os.replace(tmp_path, CORPUS_JSONL)

    return {
        "removed": removed,
        "requested": list(to_remove),
    }
