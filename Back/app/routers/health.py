from Desktop.Plagiarism.Back.app.routers.upload import _iter_jsonl
from ..core.config import CORPUS_JSONL, INDEX_JSON, MANIFEST_JSON
from collections import deque
from typing import Dict, Any, List
from ..core.runtime_cfg import get_runtime_cfg
from fastapi import APIRouter, UploadFile, File, HTTPException, Query
from fastapi.responses import Response
from ..services.normalizer import simple_tokens
from ..services.pdf_convert import smart_pdf_to_docx



router = APIRouter(prefix="/api", tags=["Base"])

@router.get("/health")
def health():
    return {
        "corpus_exists": CORPUS_JSONL.exists(),
        "index_exists": INDEX_JSON.exists(),
        "manifest_exists": MANIFEST_JSON.exists(),
        "index_path": str(INDEX_JSON)
    }

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

    rt_cfg = get_runtime_cfg()
    ocr_lang = rt_cfg.ocr.lang
    ocr_workers = rt_cfg.ocr.workers

    try:
        docx = smart_pdf_to_docx(
            raw,
            try_ocr=ocr,
            ocr_mode=ocr_mode,
            force_ocr=ocr,
            lang=ocr_lang,
            ocr_workers=ocr_workers,
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
