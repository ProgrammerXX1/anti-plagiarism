# app/routers/search.py

from fastapi import APIRouter, HTTPException, UploadFile, File, Query, Body
import orjson
from typing import Any, Dict, List, Set

from ..core.config import CORPUS_JSONL
from ..models.schemas import SearchReq
from ..services.converters.docx_utils import extract_docx_text
from ..services.search.search_pipeline import search_bm25_native

router = APIRouter(prefix="/api", tags=["Search"])


# ───────────────────────────────────────────────────────────────
# Вспомогательные функции для контекстов
# ───────────────────────────────────────────────────────────────

def _load_texts_for_docs(doc_ids: Set[str]) -> Dict[str, str]:
    """
    Одним проходом по corpus.jsonl забираем text для нужных doc_id.
    Предполагаем формат строк: {"doc_id": "...", "text": "...", ...}
    """
    if not doc_ids:
        return {}

    result: Dict[str, str] = {}
    if not CORPUS_JSONL.exists():
        return result

    with open(CORPUS_JSONL, "rb") as src:
        for line in src:
            line = line.strip()
            if not line:
                continue
            try:
                obj = orjson.loads(line)
            except Exception:
                continue

            did = obj.get("doc_id")
            if not did or did not in doc_ids:
                continue

            text = obj.get("text") or ""
            result[did] = text

            if len(result) == len(doc_ids):
                break

    return result


def _build_fragments(
    text: str,
    query: str,
    max_fragments: int = 3,
    window: int = 200,
) -> List[Dict[str, Any]]:
    """
    Простые сниппеты: ищем в тексте вхождения запроса (lower-case),
    вокруг каждого берём окно ±window/2 символов.
    Если совпадений нет — возвращаем первый фрагмент текста.
    """
    text = text or ""
    if not text:
        return []

    q = (query or "").strip()
    if not q:
        return []

    text_l = text.lower()
    q_l = q.lower()

    frags: List[Dict[str, Any]] = []

    pos = text_l.find(q_l)
    seen = 0

    while pos != -1 and seen < max_fragments:
        start = max(0, pos - window // 2)
        end = min(len(text), pos + len(q_l) + window // 2)
        snippet = text[start:end].replace("\n", " ").replace("\r", " ")

        frags.append(
            {
                "offset": pos,
                "length": len(q),
                "snippet": snippet,
            }
        )

        seen += 1
        pos = text_l.find(q_l, pos + len(q_l))

    if not frags:
        # fallback: просто начало текста
        snippet = text[:window].replace("\n", " ").replace("\r", " ")
        frags.append(
            {
                "offset": 0,
                "length": 0,
                "snippet": snippet,
            }
        )

    return frags


def _attach_contexts(result: Dict[str, Any], query: str) -> Dict[str, Any]:
    """
    Обогащаем результат поиска контекстами:
      documents[*].details.matching_fragments = [...сниппеты...]
    """
    docs = result.get("documents") or []
    doc_ids: Set[str] = {
        d.get("doc_id") for d in docs if isinstance(d, dict) and d.get("doc_id")
    }
    texts_by_id = _load_texts_for_docs(doc_ids)

    for d in docs:
        did = d.get("doc_id")
        if not did:
            continue
        text = texts_by_id.get(did)
        if text is None:
            continue

        details = d.setdefault("details", {})
        details["matching_fragments"] = _build_fragments(
            text=text,
            query=query,
            max_fragments=3,
            window=200,
        )

    return result


# ───────────────────────────────────────────────────────────────
# Основной поиск
# ───────────────────────────────────────────────────────────────

def _search_core(qtext: str, top: int):
    """
    Общий хелпер: BM25 → C++-поиск по шинглам (без реранка),
    + подстановка контекстов из корпуса.
    """
    q = (qtext or "").strip()
    if not q:
        raise HTTPException(400, "empty query")

    res = search_bm25_native(
        qtext=q,
        bm25_top=300,
        shingle_top=max(top, 50),
        final_top=top,
    )

    # Добавляем контексты (сниппеты) по найденным doc_id
    res = _attach_contexts(res, query=q)
    return res


@router.post("/search")
def search_endpoint(req: SearchReq):
    """
    Основной поиск по JSON-запросу.
    Тяжёлые операции: BM25 (Python) + C++-движок шинглов.
    """
    return _search_core(req.query, top=req.top)


@router.post("/search-raw")
async def search_raw(
    body: str = Body(..., media_type="text/plain"),
    top: int = Query(5, ge=1, le=50),
    use_rerank: bool = Query(True, description="Параметр зарезервирован, сейчас игнорируется"),
):
    """
    Поиск по "сырым" текстовым запросам (text/plain).
    Параметр use_rerank оставлен для совместимости, но не используется.
    """
    text = (body or "").strip()
    if not text:
        raise HTTPException(400, "empty body")

    return _search_core(text, top=top)


@router.post("/search-file")
async def search_file(
    file: UploadFile = File(...),
    top: int = Query(5, ge=1, le=50),
    use_rerank: bool = Query(True, description="Параметр зарезервирован, сейчас игнорируется"),
):
    """
    Поиск по загруженному файлу:
    - .txt/.log/.md и text/* → как UTF-8 текст;
    - .docx → через extract_docx_text;
    - .doc → не поддерживается (нужно конвертнуть в .docx/.txt).
    """
    ctype = (file.content_type or "").lower()
    fname = (file.filename or "").lower()
    raw = await file.read()

    # text/*
    if ctype.startswith("text/") or fname.endswith((".txt", ".log", ".md")):
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            text = raw.decode("utf-8", "ignore")

    # .docx
    elif (
        ctype
        == "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        or fname.endswith(".docx")
    ):
        try:
            text = extract_docx_text(raw)
        except RuntimeError as e:
            raise HTTPException(500, str(e))

    # legacy .doc
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

    return _search_core(text, top=top)
