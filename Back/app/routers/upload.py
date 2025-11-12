# app/routers/upload.py
import hashlib, io, os, json, re
from collections import deque
from typing import Iterator, Dict, Any, List

import orjson
from fastapi import APIRouter, UploadFile, File, HTTPException, Query

from ..services.normalizer import normalize_nfkc_lower, clean_spaces_punct, simple_tokens
from ..services.index_build import build_index_json
from ..core.config import CORPUS_JSONL, INDEX_JSON
from ..services.index_search import clear_index_cache, load_index_cached

# --- optional parsers ---
try:
    from docx import Document  # python-docx
except Exception:
    Document = None

try:
    from pypdf import PdfReader  # pypdf
except Exception:
    PdfReader = None

try:
    from pdfminer.high_level import extract_text as pdfminer_extract_text  # pdfminer.six
except Exception:
    pdfminer_extract_text = None
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

# Unicode classes: [^\W\d_] == letters with UNICODE flag
# Unicode classes: [^\W\d_] == letters with UNICODE flag
_LET = r"[^\W\d_]"
_ZW = "[\u200B-\u200F\u202A-\u202E\u2060\uFEFF]"
_SOFT_HYPHEN = "\u00AD"
_NBSP = "\u00A0"

def _repair_pdf_text(raw_txt: str) -> str:
    t = raw_txt.replace("\r\n", "\n").replace("\r", "\n")

    # 0) невидимые символы и NBSP
    t = re.sub(_ZW, "", t)
    t = t.replace(_SOFT_HYPHEN, "")     # мягкий перенос убрать
    t = t.replace(_NBSP, " ")

    # 1) склейка переносов со знаком (включая en-dash, non-breaking hyphen)
    t = re.sub(fr"({_LET}+)[\--–]\s*\n\s*({_LET}+)", r"\1\2", t, flags=re.UNICODE)

    # 2) одинарные переносы внутри абзаца -> пробел
    t = re.sub(r"([^\n])\n(?!\n)", r"\1 ", t)

    # 3) редкие разрывы внутри слова без дефиса: "исто рии"
    # Склеиваем короткие куски 1–3 символа по обе стороны пробела.
    for _ in range(3):
        t2 = re.sub(fr"\b({_LET}{{1,3}})\s({_LET}{{1,3}})\b", r"\1\2", t, flags=re.UNICODE)
        if t2 == t:
            break
        t = t2

    # 4) множественные пробелы
    t = re.sub(r"[ \t]{2,}", " ", t)

    return t.strip()


from pdfminer.high_level import extract_text as pdfminer_extract_text
from pdfminer.layout import LAParams

def _pdf_to_text(raw: bytes) -> str:
    # 1) предпочтительно pdfminer: обычно лучше для кириллицы и переносов
    if pdfminer_extract_text is not None:
        try:
            laparams = LAParams(
                line_margin=0.12,   # склеивать строки одного абзаца
                word_margin=0.08,   # не рвать слова
                char_margin=2.0,    # терпим межсимвольные зазоры
                detect_vertical=False,
                all_texts=False,
            )
            txt = pdfminer_extract_text(io.BytesIO(raw), laparams=laparams) or ""
            if txt.strip():
                return txt
        except Exception:
            pass

    # 2) fallback: pypdf
    if PdfReader is not None:
        try:
            reader = PdfReader(io.BytesIO(raw))
            parts = []
            for page in reader.pages:
                txt = page.extract_text() or ""
                if txt:
                    parts.append(txt)
            if parts:
                return "\n".join(parts)
        except Exception:
            pass

    raise HTTPException(500, "no PDF parser available (install pdfminer.six or pypdf)")
def _guess_ext(fname: str) -> str:
    fname = (fname or "").lower()
    if fname.endswith((".txt", ".log", ".md")): return "txt"
    if fname.endswith(".docx"): return "docx"
    if fname.endswith(".pdf"): return "pdf"
    return ""

# ---------- routes ----------

@router.post("/upload")

async def upload_file(file: UploadFile = File(...), normalize: bool = Query(True)):
    ctype = (file.content_type or "").lower()
    fname = (file.filename or "").lower()
    raw = await file.read()

    # 1) извлечь текст
    if ctype.startswith("text/") or _guess_ext(fname) == "txt":
        text = _bytes_to_text_utf8(raw)

    elif (
        ctype in (
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "application/msword",  # некоторые клиенты так шлют и .docx
        ) or _guess_ext(fname) == "docx"
    ):
        text = _docx_to_text(raw)

    elif ctype == "application/pdf" or _guess_ext(fname) == "pdf":
        base_text = _pdf_to_text(raw)
        text = _repair_pdf_text(base_text)

    else:
        raise HTTPException(400, f"unsupported content-type: {ctype or fname or 'unknown'}")

    # 2) нормализация для индекса
    if normalize:
        text = clean_spaces_punct(normalize_nfkc_lower(text))

    if not text.strip():
        raise HTTPException(400, "empty text after extraction")

    # 3) запись в корпус
    CORPUS_JSONL.parent.mkdir(parents=True, exist_ok=True)
    doc_hash_src = text[:2048].encode("utf-8", "ignore")
    doc_id = f"doc_{hashlib.sha1(doc_hash_src).hexdigest()[:8]}"
    with open(CORPUS_JSONL, "ab") as f:
        f.write(orjson.dumps({"doc_id": doc_id, "text": text}) + b"\n")

    # lightweight метрики
    toks = simple_tokens(text)
    return {"doc_id": doc_id, "bytes": len(raw), "tokens": len(toks)}

@router.post("/build")
def build_index():
    try:
        idx = build_index_json()
    except FileNotFoundError:
        raise HTTPException(400, "corpus.jsonl not found")
    # сброс и прогрев кэша индекса
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
    if CORPUS_JSONL.exists():
        try:
            os.remove(CORPUS_JSONL); removed.append(str(CORPUS_JSONL))
        except OSError as e:
            raise HTTPException(500, f"cannot remove corpus: {e}")
    if INDEX_JSON.exists():
        try:
            os.remove(INDEX_JSON); removed.append(str(INDEX_JSON))
        except OSError as e:
            raise HTTPException(500, f"cannot remove index: {e}")
    clear_index_cache()
    return {"status": "ok", "removed": removed}

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
    reverse: bool = Query(True, description="Последние сверху"),
    preview: bool = Query(True, description="Добавить краткий текстовый превью"),
    max_preview_chars: int = Query(160, ge=0, le=2000),
):
    if not CORPUS_JSONL.exists():
        return {"total": 0, "offset": offset, "limit": limit, "items": []}

    total = 0
    if reverse:
        buf: deque = deque(maxlen=offset + limit)
        for obj in _iter_jsonl(CORPUS_JSONL):
            buf.append(obj); total += 1
        selected = list(buf)[max(0, len(buf) - (offset + limit)) :]
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
            p = text[: max_preview_chars].replace("\n", " ")
            if len(text) > max_preview_chars:
                p += "…"
            rec["preview"] = p
        items.append(rec)

    return {"total": total, "offset": offset, "limit": limit, "items": items}
