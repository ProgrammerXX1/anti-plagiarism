# app/routers/search.py
import time
from ..core.config import CORPUS_JSONL
from ..services.search.index_search import get_index_cached
from fastapi import APIRouter, HTTPException, UploadFile, File, Query, Body
import orjson
from ..core.config import (
    CORPUS_JSONL,
)

from ..models.schemas import SearchReq
from ..services.converters.docx_utils import extract_docx_text
from ..services.search.search_pipeline import search_bm25_shingles_rerank

router = APIRouter(prefix="/api", tags=["Search"])


def _search_core(qtext: str, top: int, use_rerank: bool = True):
    q = (qtext or "").strip()
    if not q:
        raise HTTPException(400, "empty query")

    res = search_bm25_shingles_rerank(
        qtext=q,
        bm25_top=300,
        shingle_top=max(top, 50),
        final_top=top,
        use_rerank=use_rerank,
        backend="native",   # жёстко: только C++
    )
    return res


@router.post("/search")
def search_endpoint(req: SearchReq):
    return _search_core(req.query, top=req.top, use_rerank=True)


@router.post("/search-raw")
async def search_raw(
    body: str = Body(..., media_type="text/plain"),
    top: int = Query(5, ge=1, le=50),
    use_rerank: bool = Query(True),
):
    text = (body or "").strip()
    if not text:
        raise HTTPException(400, "empty body")
    return _search_core(text, top=top, use_rerank=use_rerank)


@router.post("/search-file")
async def search_file(
    file: UploadFile = File(...),
    top: int = Query(5, ge=1, le=50),
    use_rerank: bool = Query(True),
):
    ctype = (file.content_type or "").lower()
    fname = (file.filename or "").lower()
    raw = await file.read()

    if ctype.startswith("text/") or fname.endswith((".txt", ".log", ".md")):
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            text = raw.decode("utf-8", "ignore")
    elif (
        ctype
        == "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        or fname.endswith(".docx")
    ):
        try:
            text = extract_docx_text(raw)
        except RuntimeError as e:
            raise HTTPException(500, str(e))
    elif ctype == "application/msword" or fname.endswith(".doc"):
        raise HTTPException(
            415, "legacy .doc is not supported; convert to .docx or .txt"
        )
    else:
        raise HTTPException(
            400, f"unsupported file type: {ctype or fname or 'unknown'}"
        )

    text = (text or "").strip()
    if not text:
        raise HTTPException(400, "empty file after extraction")

    return _search_core(text, top=top, use_rerank=use_rerank)

@router.get(
    "/corpus/stats",
    summary="Статистика корпуса и индекса",
)
def corpus_stats():
    if not CORPUS_JSONL.exists():
        raise HTTPException(404, "corpus.jsonl not found")

    corpus_ids: set[str] = set()
    total_lines = 0

    with open(CORPUS_JSONL, "rb") as src:
        for line in src:
            line = line.strip()
            if not line:
                continue
            total_lines += 1
            try:
                obj = orjson.loads(line)
            except Exception:
                continue
            did = obj.get("doc_id")
            if did:
                corpus_ids.add(did)

    # ВРЕМЕННО: ничего не знаем про индекс
    index_loaded = True  # если native_load_index всегда вызывается на старте
    index_docs = None    # или 0, пока не прикрутишь метаданные из C++

    return {
        "corpus_total_lines": total_lines,
        "corpus_total_docs": len(corpus_ids),
        "index_loaded": index_loaded,
        "index_docs": index_docs,
        "corpus_indexed_docs": None,
        "corpus_unindexed_docs": None,
        "index_orphan_docs": None,
    }
