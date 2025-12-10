# app/api/v1/level5.py
from __future__ import annotations
import time 

from app.core.logger import logger
from fastapi import APIRouter, HTTPException, status
from fastapi.concurrency import run_in_threadpool

from app.schemas.level5 import (
    Level5ReindexRequest,
    Level5ReindexResponse,
    Level5SearchRequest,
    Level5SearchResponse,
    Level5DocDetails,
)

from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import Depends
from app.db.session import get_db
from app.services.level5.l5_corpus import rebuild_l5_corpus

# Сервисы level-5
from app.services.level5.bm25_index import build_bm25_index
from app.services.level5.index_build import build_index_json
from app.services.level5.index_native_export import export_native_index
from app.services.level5.search_native import native_load_index
from app.services.level5.search_pipeline import search_bm25_native

router = APIRouter(
    prefix="/api/level5",
    tags=["Level - 5"],
)

# ───────────────────────── Индексация монолита (L5) ───────────────────────── #
@router.post("/reindex", response_model=Level5ReindexResponse)
async def level5_reindex(
    body: Level5ReindexRequest,
    db: AsyncSession = Depends(get_db),
) -> Level5ReindexResponse:
    stats: dict = {}
    t_total_start = time.monotonic()

    try:
        # 0) corpus.jsonl
        if body.rebuild_corpus:
            t0 = time.monotonic()
            corpus_stats = await rebuild_l5_corpus(db)
            t1 = time.monotonic()

            corpus_stats = dict(corpus_stats)
            corpus_stats["duration_sec"] = round(t1 - t0, 3)
            stats["corpus"] = corpus_stats

            logger.info(
                "[L5/reindex] step=corpus "
                "docs_total=%s corpus_docs=%s duration=%.3fs",
                corpus_stats.get("docs_total"),
                corpus_stats.get("corpus_docs"),
                corpus_stats["duration_sec"],
            )

            if corpus_stats.get("corpus_docs", 0) == 0:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="L5 corpus is empty, nothing to index",
                )

        # 1) BM25
        if body.rebuild_bm25:
            t0 = time.monotonic()
            bm25_idx = await run_in_threadpool(build_bm25_index)
            t1 = time.monotonic()

            bm25_stats = {
                "N": int(bm25_idx.get("N", 0)),
                "avgdl": float(bm25_idx.get("avgdl", 0.0)),
                "terms": len(bm25_idx.get("postings", {})),
                "duration_sec": round(t1 - t0, 3),
            }
            stats["bm25"] = bm25_stats

            logger.info(
                "[L5/reindex] step=bm25 N=%d terms=%d avgdl=%.2f duration=%.3fs",
                bm25_stats["N"],
                bm25_stats["terms"],
                bm25_stats["avgdl"],
                bm25_stats["duration_sec"],
            )

        # 2) index.json
        if body.rebuild_main_index:
            t0 = time.monotonic()
            idx = await run_in_threadpool(
                build_index_json,
                None,
                body.incremental,
                body.max_new_docs_per_build,
            )
            t1 = time.monotonic()

            docs_meta = idx.get("docs_meta") or {}
            counts = idx.get("inverted_doc") or {}

            index_stats = {
                "docs_total": len(docs_meta),
                "inv_terms_k5": len((counts.get("k5") or {})),
                "inv_terms_k9": len((counts.get("k9") or {})),
                "inv_terms_k13": len((counts.get("k13") or {})),
                "duration_sec": round(t1 - t0, 3),
                "incremental": bool(body.incremental),
                "max_new_docs_per_build": body.max_new_docs_per_build,
            }
            stats["index_json"] = index_stats

            logger.info(
                "[L5/reindex] step=index_json docs=%d k9_terms=%d k13_terms=%d "
                "incremental=%s max_new=%s duration=%.3fs",
                index_stats["docs_total"],
                index_stats["inv_terms_k9"],
                index_stats["inv_terms_k13"],
                index_stats["incremental"],
                index_stats["max_new_docs_per_build"],
                index_stats["duration_sec"],
            )

        # 3) native
        if body.export_native:
            t0 = time.monotonic()
            await run_in_threadpool(export_native_index)
            await run_in_threadpool(native_load_index)
            t1 = time.monotonic()

            native_stats = {
                "duration_sec": round(t1 - t0, 3),
            }
            stats["native"] = native_stats

            logger.info(
                "[L5/reindex] step=native_export+load duration=%.3fs",
                native_stats["duration_sec"],
            )

    except HTTPException:
        # уже нормальный ответ с кодом/деталью
        raise
    except Exception as e:
        import traceback
        logger.error(f"[L5/reindex] unexpected error: {e}\n{traceback.format_exc()}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Level5 reindex failed: {e}",
        )

    t_total_end = time.monotonic()
    total_sec = round(t_total_end - t_total_start, 3)

    # добавим общую сводку по времени
    stats.setdefault("meta", {})
    stats["meta"]["total_duration_sec"] = total_sec

    logger.info("[L5/reindex] finished total_duration=%.3fs", total_sec)

    return Level5ReindexResponse(
        ok=True,
        message="Level5 index rebuilt successfully",
        stats=stats,
    )


# ─────────────────────────── Поиск по монолиту (L5) ───────────────────────── #

@router.post(
    "/search",
    response_model=Level5SearchResponse,
    status_code=status.HTTP_200_OK,
)
async def level5_search(body: Level5SearchRequest) -> Level5SearchResponse:
    """
    Поиск по монолитному индексу уровня 5.

    Пайплайн:
      1) BM25 → кандидаты (по doc_id).
      2) C++-поиск по шинглам (native_search) с allowed_doc_ids.
      3) Обрезка до final_top без реранка.
    """
    if not body.query.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="query must be non-empty",
        )

    try:
        raw_res = await run_in_threadpool(
            search_bm25_native,
            body.query,
            bm25_top=body.bm25_top,
            shingle_top=body.shingle_top,
            final_top=body.final_top,
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Level5 search failed: {e}",
        )

    docs_raw = raw_res.get("documents") or []
    docs_norm = [
        Level5DocDetails(
            doc_id=str(d.get("doc_id")),
            title=d.get("title"),
            author=d.get("author"),
            max_score=float(d.get("max_score", 0.0)),
            originality_pct=float(d.get("originality_pct", 0.0)),
            decision=str(d.get("decision") or ""),
            details=d.get("details") or {},
        )
        for d in docs_raw
    ]

    return Level5SearchResponse(
        hits_total=int(raw_res.get("hits_total", len(docs_norm))),
        docs_found=len(docs_norm),
        documents=docs_norm,
    )


# ───────────────────── Ручная загрузка C++ индекса ───────────────────── #

@router.post(
    "/load-native",
    status_code=status.HTTP_200_OK,
)
async def level5_load_native_index() -> dict:
    """
    Явная загрузка index_native.bin в C++-ядро.

    Полезно:
      - после ручного копирования файлов;
      - если процесс перезапустился и нужно переинициализировать ядро.
    """
    try:
        await run_in_threadpool(native_load_index)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"native_load_index failed: {e}",
        )

    return {"ok": True, "message": "native index loaded"}
