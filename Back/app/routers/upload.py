# app/routers/upload.py
import hashlib, io, os, json, re, zipfile
from collections import deque
from typing import Iterator, Dict, Any, List

import orjson
from fastapi import APIRouter, UploadFile, File, HTTPException, Query
from fastapi.responses import Response

from ..core.config import CORPUS_JSONL, INDEX_JSON, UPLOAD_DIR
from ..core.io_utils import file_lock
from ..services.index_build import build_index_json
from ..services.index_search import clear_index_cache, load_index_cached
from ..services.normalizer import normalize_for_shingles, simple_tokens
from ..services.pdf_heavy import extract_and_normalize_pdf
from ..services.pdf_convert import smart_pdf_to_docx
from ..core.logger import logger  # вместо from venv import logger

# --- optional parsers ---
try:
    from docx import Document
except Exception:
    Document = None
# ------------------------

router = APIRouter(prefix="/api", tags=["Operations"])

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
) -> Dict[str, Any]:
    """
    Общий пайплайн для одного файла.
    Возвращает dict с doc_id, токенами и пр.
    """
    ctype_guess = _guess_ext(fname)
    text: str

    # 1) извлечение текста
    if ctype_guess == "txt":
        text = _bytes_to_text_utf8(raw)

    elif ctype_guess == "docx":
        text = _docx_to_text(raw)

    elif ctype_guess == "pdf":
        # heavy PDF normalize for indexing
        try:
            text = extract_and_normalize_pdf(
                raw,
                try_ocr=ocr,
                lang="kaz+rus+eng",
                ocr_workers=16,
                dpi=200,
                psm=3,
                return_debug=False,
            )
        except Exception as e:
            raise HTTPException(500, f"PDF extract failed: {e}")

        # сохранить DOCX-версию
        if save_docx:
            try:
                docx_bytes = smart_pdf_to_docx(
                    raw,
                    try_ocr=ocr,
                    lang="kaz+rus+eng",
                    ocr_workers=16,
                    ocr_mode=ocr_mode,
                    # здесь OCR не форсим, просто «умный» режим
                    force_ocr=False,
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

    # 3) запись в корпус
    CORPUS_JSONL.parent.mkdir(parents=True, exist_ok=True)
    doc_id = f"doc_{hashlib.sha256(text.encode('utf-8')).hexdigest()[:8]}"

    with file_lock(CORPUS_JSONL.with_suffix(".lock")):
        with open(CORPUS_JSONL, "ab") as f:
            f.write(orjson.dumps({"doc_id": doc_id, "text": text}) + b"\n")

    toks = simple_tokens(text)

    return {
        "doc_id": doc_id,
        "filename": fname,
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
            )
            processed += 1
            items.append(res)
        except HTTPException as e:
            # бизнес-ошибка (unsupported, пустой и т.п.)
            items.append(
                {
                    "filename": name,
                    "status": "skipped",
                    "error": e.detail,
                }
            )
        except Exception as e:
            # защитный fallback
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
    try:
        idx = build_index_json()
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

# ---------- corpus list ----------

@router.get("/corpus/list")
def corpus_list(
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=1000),
    reverse: bool = Query(True),
    preview: bool = Query(True),
    max_preview_chars: int = Query(160, ge=0, le=2000),
):
    if not CORPUS_JSONL.exists():
        return {"total": 0, "offset": offset, "limit": limit, "items": []}

    total = 0
    if reverse:
        buf: deque = deque(maxlen=offset + limit)
        for obj in _iter_jsonl(CORPUS_JSONL):
            buf.append(obj)
            total += 1
        selected = list(buf)[max(0, len(buf) - (offset + limit)) :]
        selected = (
            selected[-limit:]
            if offset == 0
            else selected[:-offset]
            if offset < len(selected)
            else []
        )
        selected.reverse()
    else:
        selected: List[Dict[str, Any]] = []
        for obj in _iter_jsonl(CORPUS_JSONL):
            if total >= offset and len(selected) < limit:
                selected.append(obj)
            total += 1

    items = []
    for obj in selected:
        text = obj.get("text", "")
        toks = simple_tokens(text)
        rec = {
            "doc_id": obj["doc_id"],
            "line_no": obj.get("_line_no"),
            "chars": len(text),
            "tokens": len(toks),
        }
        if preview and max_preview_chars > 0:
            p = text[:max_preview_chars].replace("\n", " ")
            if len(text) > max_preview_chars:
                p += "…"
            rec["preview"] = p
        items.append(rec)

    return {"total": total, "offset": offset, "limit": limit, "items": items}

# ---------- отдельная конвертация PDF→DOCX ----------

@router.post("/convert/pdf-to-docx")
async def convert_pdf_to_docx(
    file: UploadFile = File(...),
    ocr: bool = Query(True, description="Включить OCR (получить DOCX с распознанным текстом)"),
    ocr_mode: str = Query(
        "speed",
        pattern="^(speed|balanced|quality)$",
        description="Режим OCR: speed / balanced / quality",
    ),
):
    raw = await file.read()
    try:
        docx = smart_pdf_to_docx(
            raw,
            try_ocr=ocr,
            lang="kaz+rus+eng",
            ocr_workers=16,
            ocr_mode=ocr_mode,
            force_ocr=ocr,
        )
    except Exception as e:
        raise HTTPException(500, f"convert failed: {e}")
    return Response(
        content=docx,
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        headers={
            "Content-Disposition": (
                f'attachment; filename="{(file.filename or "file").rsplit(".",1)[0]}.docx"'
            )
        },
    )
@router.get("/corpus/text")
def corpus_text(
    doc_id: str = Query(..., description="Идентификатор документа из corpus.jsonl"),
    as_plain: bool = Query(
        False,
        description="Если true — вернуть только сырой текст (text/plain), без JSON-обёртки"
    ),
):
    """
    Вернуть полный текст документа из corpus.jsonl по doc_id.
    Это как раз тот текст, который пошёл в индексацию (после normalize, если он был включён при upload).
    """
    if not CORPUS_JSONL.exists():
        raise HTTPException(404, "corpus.jsonl not found")

    found: Dict[str, Any] | None = None
    line_no: int | None = None

    for obj in _iter_jsonl(CORPUS_JSONL):
        if obj.get("doc_id") == doc_id:
            found = obj
            line_no = obj.get("_line_no")
            break

    if not found:
        raise HTTPException(404, f"doc_id not found in corpus: {doc_id}")

    text = found.get("text", "") or ""
    if as_plain:
        # голый текст, удобно для просмотра результата OCR
        return Response(
            content=text,
            media_type="text/plain; charset=utf-8"
        )

    toks = simple_tokens(text)
    return {
        "doc_id": doc_id,
        "line_no": line_no,
        "chars": len(text),
        "tokens": len(toks),
        "text": text,
    }
