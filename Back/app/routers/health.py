from ..routers.upload import _iter_jsonl
from ..core.config import CORPUS_JSONL, INDEX_JSON, MANIFEST_JSON
from collections import deque
from typing import Dict, Any, List
from ..core.runtime_cfg import get_runtime_cfg
from fastapi import APIRouter, UploadFile, File, HTTPException, Query, logger
from fastapi.responses import Response
from ..services.helpers.normalizer import simple_tokens
from ..services.converters.pdf_convert import smart_pdf_to_docx
from ..services.search.index_search import load_index, load_index_cached


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
    doc_id: str | None = Query(
        None,
        description="Идентификатор документа из corpus.jsonl. Если задан и title, приоритет у doc_id.",
    ),
    title: str | None = Query(
        None,
        description="Поиск по названию (подстрока, регистр не учитывается), если doc_id не указан",
    ),
    as_plain: bool = Query(
        False,
        description="Если true — вернуть только сырой текст (text/plain), без JSON-обёртки",
    ),
):
    """
    Вернуть полный текст документа из corpus.jsonl.

    Поддерживаются два режима:
    - по doc_id (точное совпадение)
    - по title (подстрока, регистр не учитывается), если doc_id не указан
    """
    if not CORPUS_JSONL.exists():
        raise HTTPException(404, "corpus.jsonl not found")

    if not doc_id and not title:
        raise HTTPException(400, "either doc_id or title must be provided")

    found: Dict[str, Any] | None = None
    line_no: int | None = None

    # --- поиск по doc_id (приоритетный режим) ---
    if doc_id:
        for obj in _iter_jsonl(CORPUS_JSONL):
            if obj.get("doc_id") == doc_id:
                found = obj
                line_no = obj.get("_line_no")
                break

        if not found:
            raise HTTPException(404, f"doc_id not found in corpus: {doc_id}")

    # --- поиск по title (подстрока, case-insensitive) ---
    else:
        q = (title or "").strip().lower()
        if not q:
            raise HTTPException(400, "title must be non-empty if doc_id is not provided")

        for obj in _iter_jsonl(CORPUS_JSONL):
            t = (obj.get("title") or "").strip().lower()
            if q and q in t:
                found = obj
                line_no = obj.get("_line_no")
                break

        if not found:
            raise HTTPException(404, f"title not found in corpus (query='{title}')")

        # для консистентности отдаём фактический doc_id найденного документа
        doc_id = found.get("doc_id")

    # --- формирование ответа ---
    text = found.get("text", "") or ""
    title_val = found.get("title")
    author = found.get("author")

    if as_plain:
        return Response(
            content=text,
            media_type="text/plain; charset=utf-8",
        )

    toks = simple_tokens(text)
    return {
        "doc_id": doc_id,
        "title": title_val,
        "author": author,
        "line_no": line_no,
        "chars": len(text),
        "tokens": len(toks),
        "text": text,
    }


# ---------- corpus list ----------

@router.get("/corpus/list")
def corpus_list(
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
):
    """
    Список документов из corpus.jsonl с пагинацией.
    Плюс:
      - index: сколько документов сейчас проиндексировано (по docs_meta из index.json)
      - k5/k9/k13: сколько уникальных шинглов сейчас в индексе
    """
    if not CORPUS_JSONL.exists():
        raise HTTPException(404, "corpus.jsonl not found")

    # собираем с пагинацией
    items: List[Dict[str, Any]] = []
    total = 0

    for obj in _iter_jsonl(CORPUS_JSONL):
        total += 1
        if total <= offset:
            continue
        if len(items) >= limit:
            continue  # не прерываем, чтобы total был корректный

        text = obj.get("text", "") or ""
        toks = simple_tokens(text)
        preview = text[:300].replace("\n", " ").strip()

        items.append(
            {
                "doc_id": obj.get("doc_id"),
                "title": obj.get("title", ""),
                "author": obj.get("author", ""),
                "line_no": obj.get("_line_no", total),
                "chars": len(text),
                "tokens": len(toks),
                "preview": preview + ("…" if len(text) > 10 else ""),
            }
        )

    # статы индекса ─ читаем СВЕЖИЙ index.json, без кэша
    indexed = 0
    k5 = k9 = k13 = 0

    try:
        idx = load_index()  # важно: не load_index_cached()
        docs_meta = idx.get("docs_meta") or {}
        indexed = len(docs_meta)

        inv = idx.get("inverted_doc") or {}
        inv5 = inv.get("k5") or {}
        inv9 = inv.get("k9") or {}
        inv13 = inv.get("k13") or {}

        k5 = len(inv5)
        k9 = len(inv9)
        k13 = len(inv13)

    except FileNotFoundError:
        # индекса ещё нет — всё по нулям
        indexed = 0
        k5 = k9 = k13 = 0
    except Exception as e:
        logger.error(f"[corpus_list] cannot load index: {e}")
        indexed = 0
        k5 = k9 = k13 = 0

    return {
        "total": total,      # всего документов в corpus.jsonl
        "index": indexed,    # сколько документов есть в index.json (docs_meta)
        "k5": k5,            # уникальные шинглы k=5
        "k9": k9,            # уникальные шинглы k=9
        "k13": k13,          # уникальные шинглы k=13
        "offset": offset,
        "limit": limit,
        "items": items,
    }


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
