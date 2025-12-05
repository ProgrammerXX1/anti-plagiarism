# app/routers/check_cross.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from fastapi import APIRouter, HTTPException, UploadFile, File, Query
from pydantic import BaseModel

from ..core.logger import logger
from ..services.helpers.file_extract import (
    extract_text_from_file_bytes,
    norm_for_local,
)
from ..services.originality_utils import make_mini_doc, score_pair, MiniDoc

router = APIRouter(prefix="/api/check/cross", tags=["Originality – Cross"])


# ───────────────────────────────────────────────────────────────
# In-memory сессии кросс-проверки
# ───────────────────────────────────────────────────────────────

@dataclass
class CrossSessionDoc:
    idx: int
    text: str
    filename: Optional[str]
    mini: MiniDoc


_CROSS_SESSIONS: Dict[str, List[CrossSessionDoc]] = {}


# ───────────────────────────────────────────────────────────────
# 4a) Инициализация по текстам
# ───────────────────────────────────────────────────────────────

class CrossInitReq(BaseModel):
    session_id: str
    texts: List[str]


class CrossInitResp(BaseModel):
    session_id: str
    docs_count: int


@router.post("/init", response_model=CrossInitResp)
def cross_init(payload: CrossInitReq):
    if not payload.texts:
        raise HTTPException(400, "texts is empty")

    docs: List[CrossSessionDoc] = []
    for i, t in enumerate(payload.texts):
        norm = norm_for_local(t)
        md = make_mini_doc(norm)
        docs.append(
            CrossSessionDoc(
                idx=i,
                text=t,
                filename=None,
                mini=md,
            )
        )

    _CROSS_SESSIONS[payload.session_id] = docs

    logger.info(
        "[cross_init] session_id=%s docs=%d",
        payload.session_id,
        len(docs),
    )

    return CrossInitResp(
        session_id=payload.session_id,
        docs_count=len(docs),
    )


# ───────────────────────────────────────────────────────────────
# 4b) Проверка ОДНОГО текста по сессии (A vs все B)
# ───────────────────────────────────────────────────────────────

class CrossCheckReq(BaseModel):
    session_id: str
    text: str
    save_to_session: bool = False


class CrossDocHitOut(BaseModel):
    idx: int

    score: float
    j: float

    c: float
    c_ab: float
    c_ba: float

    inter: int
    q_shingles: int
    d_shingles: int

    spans: List[Dict[str, int]]  # offset_a / limit_a / offset_b / limit_b


class CrossCheckResp(BaseModel):
    session_id: str
    hits: List[CrossDocHitOut]
    saved_as_index_doc: bool


@router.post("/check", response_model=CrossCheckResp)
def cross_check(payload: CrossCheckReq):
    """
    A = payload.text, B = документ из сессии.
    """
    docs = _CROSS_SESSIONS.get(payload.session_id)
    if docs is None:
        raise HTTPException(404, f"session_id={payload.session_id} not found")

    text_norm = norm_for_local(payload.text)
    if not text_norm.strip():
        raise HTTPException(400, "empty text after normalization")

    qdoc = make_mini_doc(text_norm)
    hits: List[CrossDocHitOut] = []

    for d in docs:
        metrics = score_pair(qdoc, d.mini)
        spans = metrics.get("spans", []) or []

        hits.append(
            CrossDocHitOut(
                idx=d.idx,
                score=float(metrics["score"]),
                j=float(metrics["j"]),
                c=float(metrics["c"]),
                c_ab=float(metrics.get("c_ab", metrics["c"])),
                c_ba=float(metrics.get("c_ba", 0.0)),
                inter=int(metrics["inter"]),
                q_shingles=int(metrics["q_shingles"]),
                d_shingles=int(metrics["d_shingles"]),
                spans=spans,
            )
        )

    hits.sort(key=lambda x: x.score, reverse=True)

    saved = False
    if payload.save_to_session:
        new_idx = max((d.idx for d in docs), default=-1) + 1
        docs.append(
            CrossSessionDoc(
                idx=new_idx,
                text=payload.text,
                filename=None,
                mini=qdoc,
            )
        )
        saved = True

    return CrossCheckResp(
        session_id=payload.session_id,
        hits=hits,
        saved_as_index_doc=saved,
    )


# ───────────────────────────────────────────────────────────────
# 4c) ПОЛНАЯ кросс-проверка внутри сессии (все студенты между собой)
# ───────────────────────────────────────────────────────────────

class CrossPairOut(BaseModel):
    idx_a: int
    idx_b: int

    score: float
    j: float

    c_ab: float
    c_ba: float

    inter: int
    q_shingles: int
    d_shingles: int

    spans: List[Dict[str, int]]  # offset_a / limit_a / offset_b / limit_b


class CrossMatrixResp(BaseModel):
    session_id: str
    pairs: List[CrossPairOut]


@router.get("/pairs", response_model=CrossMatrixResp)
def cross_pairs(
    session_id: str = Query(..., description="ID сессии перекрёстной проверки"),
):
    """
    Полная кросс-проверка внутри сессии:
      берём все документы в сессии и считаем пары (i, j) только для i < j.
    """
    docs = _CROSS_SESSIONS.get(session_id)
    if docs is None:
        raise HTTPException(404, f"session_id={session_id} not found")

    n = len(docs)
    if n < 2:
        return CrossMatrixResp(session_id=session_id, pairs=[])

    pairs: List[CrossPairOut] = []

    for i in range(n):
        for j in range(i + 1, n):
            di = docs[i]
            dj = docs[j]

            metrics = score_pair(di.mini, dj.mini)
            spans = metrics.get("spans", []) or []

            pairs.append(
                CrossPairOut(
                    idx_a=di.idx,
                    idx_b=dj.idx,
                    score=float(metrics["score"]),
                    j=float(metrics["j"]),
                    c_ab=float(metrics.get("c_ab", metrics["c"])),
                    c_ba=float(metrics.get("c_ba", 0.0)),
                    inter=int(metrics["inter"]),
                    q_shingles=int(metrics["q_shingles"]),
                    d_shingles=int(metrics["d_shingles"]),
                    spans=spans,
                )
            )

    pairs.sort(key=lambda p: p.score, reverse=True)

    logger.info(
        "[cross_pairs] session_id=%s docs=%d pairs=%d",
        session_id,
        n,
        len(pairs),
    )

    return CrossMatrixResp(
        session_id=session_id,
        pairs=pairs,
    )


# ───────────────────────────────────────────────────────────────
# 4a*) Инициализация по файлам
# ───────────────────────────────────────────────────────────────

class CrossInitFilesResp(BaseModel):
    session_id: str
    docs_count: int
    filenames: List[str]


@router.post("/init_files", response_model=CrossInitFilesResp)
async def cross_init_files(
    session_id: str = Query(..., description="ID сессии перекрёстной проверки"),
    files: List[UploadFile] = File(..., description="Набор файлов PDF/DOCX/TXT"),
    ocr: bool = Query(True, description="Включить OCR для PDF"),
    ocr_mode: str = Query(
        "speed",
        pattern="^(speed|balanced|quality)$",
        description="OCR режим: speed / balanced / quality",
    ),
    save_docx: bool = Query(
        False,
        description="Сохранять ли промежуточный DOCX в UPLOAD_DIR (как в /upload)",
    ),
):
    if not files:
        raise HTTPException(400, "files is empty")

    texts: List[str] = []
    filenames: List[str] = []

    for f in files:
        raw = await f.read()
        text, title = extract_text_from_file_bytes(
            raw,
            f.filename or "",
            save_docx=save_docx,
            ocr=ocr,
            ocr_mode=ocr_mode,
            log_prefix="cross",
        )
        texts.append(text)
        filenames.append(title or (f.filename or ""))

    docs: List[CrossSessionDoc] = []
    for i, (t, fn) in enumerate(zip(texts, filenames)):
        norm = norm_for_local(t)
        md = make_mini_doc(norm)
        docs.append(
            CrossSessionDoc(
                idx=i,
                text=t,
                filename=fn,
                mini=md,
            )
        )

    _CROSS_SESSIONS[session_id] = docs

    logger.info(
        "[cross_init_files] session_id=%s docs=%d",
        session_id,
        len(docs),
    )

    return CrossInitFilesResp(
        session_id=session_id,
        docs_count=len(docs),
        filenames=filenames,
    )


# ───────────────────────────────────────────────────────────────
# CRUD по сессиям
# ───────────────────────────────────────────────────────────────

class CrossSessionShort(BaseModel):
    session_id: str
    docs_count: int


class CrossSessionDocInfo(BaseModel):
    idx: int
    filename: Optional[str]
    text_preview: str


@router.get("/sessions", response_model=List[CrossSessionShort])
def list_cross_sessions():
    return [
        CrossSessionShort(session_id=sid, docs_count=len(docs))
        for sid, docs in _CROSS_SESSIONS.items()
    ]


@router.get("/sessions/{session_id}", response_model=List[CrossSessionDocInfo])
def get_cross_session(session_id: str):
    docs = _CROSS_SESSIONS.get(session_id)
    if docs is None:
        raise HTTPException(404, f"session_id={session_id} not found")

    res: List[CrossSessionDocInfo] = []
    for d in docs:
        preview = d.text[:200]
        res.append(
            CrossSessionDocInfo(
                idx=d.idx,
                filename=d.filename,
                text_preview=preview,
            )
        )
    return res


@router.delete("/sessions/{session_id}")
def delete_cross_session(session_id: str):
    docs = _CROSS_SESSIONS.pop(session_id, None)
    if docs is None:
        raise HTTPException(404, f"session_id={session_id} not found")

    logger.info(
        "[cross_delete_session] session_id=%s removed docs=%d",
        session_id,
        len(docs),
    )
    return {"session_id": session_id, "deleted_docs": len(docs)}
