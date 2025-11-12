# app/routers/upload.py
import hashlib, io, os, json, re
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
    if fname.endswith((".txt", ".log", ".md")): return "txt"
    if fname.endswith(".docx"): return "docx"
    if fname.endswith(".pdf"): return "pdf"
    return ""

# ---------- routes ----------

@router.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    normalize: bool = Query(True),
    save_docx: bool = Query(True)
):
    ctype = (file.content_type or "").lower()
    fname = (file.filename or "").lower()
    raw = await file.read()

    # 1) извлечь текст
    if ctype.startswith("text/") or _guess_ext(fname) == "txt":
        text = _bytes_to_text_utf8(raw)

    elif (
        ctype in (
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "application/msword",
        ) or _guess_ext(fname) == "docx"
    ):
        text = _docx_to_text(raw)

    elif ctype == "application/pdf" or _guess_ext(fname) == "pdf":
        # heavy PDF normalize for indexing
        try:
            text = extract_and_normalize_pdf(raw)
        except Exception as e:
            raise HTTPException(500, f"PDF extract failed: {e}")

        # сохранить DOCX-версию
        if save_docx:
            try:
                docx_bytes = smart_pdf_to_docx(raw, try_ocr=True)
                UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
                base = hashlib.sha256(raw).hexdigest()[:12]
                out_path = UPLOAD_DIR / f"{base}.docx"
                out_path.write_bytes(docx_bytes)
            except Exception:
                pass

    else:
        raise HTTPException(400, f"unsupported content-type: {ctype or fname or 'unknown'}")

    # 2) нормализация для индекса
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
    return {"doc_id": doc_id, "bytes": len(raw), "tokens": len(toks)}

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

# ---------- corpus list ----------

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
        selected = list(buf)[max(0, len(buf) - (offset + limit)):]
        selected = selected[-limit:] if offset == 0 else selected[:-offset] if offset < len(selected) else []
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

# ---------- optional: отдельная конвертация ----------

@router.post("/convert/pdf-to-docx")
async def convert_pdf_to_docx(file: UploadFile = File(...)):
    raw = await file.read()
    try:
        docx = smart_pdf_to_docx(raw, try_ocr=True)
    except Exception as e:
        raise HTTPException(500, f"convert failed: {e}")
    return Response(
        content=docx,
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        headers={"Content-Disposition": f'attachment; filename="{(file.filename or "file").rsplit(".",1)[0]}.docx"'}
    )
