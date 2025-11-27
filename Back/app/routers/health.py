import json
from typing import Dict, Any, List

from fastapi import APIRouter, HTTPException, Query, UploadFile, File
from fastapi.responses import Response

from ..routers.upload import _iter_jsonl
from ..core.config import CORPUS_JSONL, INDEX_JSON, MANIFEST_JSON
from ..core.runtime_cfg import get_runtime_cfg
from ..core.logger import logger
from ..services.helpers.normalizer import simple_tokens
from ..services.converters.pdf_convert import smart_pdf_to_docx

router = APIRouter(prefix="/api", tags=["Base"])

# meta-файл C++-индекса рядом с index.json (используем только как директорию)
INDEX_NATIVE_META = INDEX_JSON.parent / "index_native_meta.json"


# ---------- helpers ----------

def _load_index_stats() -> tuple[int, int]:
    """
    Возвращает (indexed_docs, k9).

    Читает ТОЛЬКО C++ meta: index_native_meta.json.
    Ожидаемый формат (упрощённо):
    {
      "docs_meta": {
        "doc_...": {...},
        ...
      },
      "stats": {
        "k9": 123456,
        ...
      }
    }

    Если файла нет или формат неожиданный — вернёт (0, 0).
    """
    if not INDEX_NATIVE_META.exists():
        return 0, 0

    try:
        with open(INDEX_NATIVE_META, "r", encoding="utf-8") as f:
            meta = json.load(f)
    except Exception as e:
        logger.error("[_load_index_stats] failed to read INDEX_NATIVE_META: %s", e)
        return 0, 0

    # docs / docs_meta могут быть либо списком, либо dict — учитываем оба варианта
    docs = meta.get("docs") or meta.get("docs_meta") or []
    indexed = 0
    if isinstance(docs, list):
        indexed = sum(1 for d in docs if isinstance(d, dict) and d.get("doc_id"))
    elif isinstance(docs, dict):
        indexed = sum(1 for _ in docs.keys())

    # k9 из stats или из корня meta
    stats = meta.get("stats") or {}
    k9_raw = (
        stats.get("k9")
        or stats.get("k9_unique")
        or meta.get("k9")
        or meta.get("k9_unique")
        or 0
    )
    try:
        k9 = int(k9_raw)
    except (TypeError, ValueError):
        k9 = 0

    return indexed, k9


# ---------- health ----------

@router.get("/health")
def health():
    """
    Простейший health-check по файлам корпуса/индекса.
    """
    return {
        "corpus_exists": CORPUS_JSONL.exists(),
        "index_native_meta_exists": INDEX_NATIVE_META.exists(),
        "manifest_exists": MANIFEST_JSON.exists(),
        "index_native_meta_path": str(INDEX_NATIVE_META),
    }


# ---------- corpus text ----------

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
      - index: сколько документов сейчас проиндексировано в C++-индексе
      - k9: сколько уникальных шинглов k=9 (по данным C++)
    """
    if not CORPUS_JSONL.exists():
        raise HTTPException(404, "corpus.jsonl not found")

    items: List[Dict[str, Any]] = []
    total = 0

    for obj in _iter_jsonl(CORPUS_JSONL):
        total += 1
        if total <= offset:
            continue
        if len(items) >= limit:
            # не прерываем цикл, чтобы total был корректный
            continue

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

    indexed, k9 = _load_index_stats()

    return {
        "total": total,      # всего документов в corpus.jsonl
        "index": indexed,    # сколько документов есть в C++-индексе
        "k9": k9,            # уникальные шинглы k=9 (по данным C++)
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
                f'attachment; filename="{(file.filename or "file").rsplit(".", 1)[0]}.docx"'
            )
        },
    )
