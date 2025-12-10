# app/api/v1/level5.py
from __future__ import annotations

import json
import time
from typing import List

from fastapi import APIRouter, HTTPException, status, Depends
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logger import logger
from app.core.config import INDEX_DIR, L5_N_SHARDS, l5_index_dir_for_shard
from app.db.session import get_db
from app.schemas.level5 import (
    Level5BaseInfo,
    Level5ReindexRequest,
    Level5ReindexResponse,
    Level5SearchRequest,
    Level5SearchResponse,
    Level5DocDetails,
)
from app.services.level5.l5_corpus import rebuild_l5_corpus
from app.services.level5.l5_config import write_l5_index_config
from app.services.level5.native_cpp_build import build_native_index_cpp
from app.services.level5.search_native import native_load_index, native_search

router = APIRouter(
    prefix="/api/level5",
    tags=["Level - 5"],
)

@router.get("/bases", response_model=List[Level5BaseInfo])
async def list_level5_bases() -> List[Level5BaseInfo]:
    bases: List[Level5BaseInfo] = []

    for shard_id in range(L5_N_SHARDS):
        idx_dir = l5_index_dir_for_shard(shard_id)
        docids_path = idx_dir / "index_native_docids.json"
        bin_path = idx_dir / "index_native.bin"
        meta_path = idx_dir / "index_native_meta.json"

        has_index = bin_path.exists() and docids_path.exists()

        docs = 0
        if docids_path.exists():
            try:
                data = json.loads(docids_path.read_text(encoding="utf-8"))
                if isinstance(data, list):
                    docs = len(data)
            except Exception:
                docs = 0

        size = 0
        for p in (bin_path, docids_path, meta_path):
            if p.exists():
                size += p.stat().st_size

        bases.append(
            Level5BaseInfo(
                shard_id=shard_id,
                path=str(idx_dir),
                has_index=has_index,
                docs=docs,
                size_bytes=size,
            )
        )

    return bases
# ─────────────────────── Индексация L5 (офлайн / по кнопке) ─────────────────────── #


@router.post("/reindex", response_model=Level5ReindexResponse)
async def level5_reindex(
    body: Level5ReindexRequest,
    db: AsyncSession = Depends(get_db),
) -> Level5ReindexResponse:
    """
    Полное/частичное обновление индекса уровня 5.

    Пайплайн:
      0) rebuild_l5_corpus: собрать corpus.jsonl из документов со статусом 'l5_uploaded'.
      1) build_native_index_cpp: C++ index_builder → index_native.bin + docids + meta.
      2) write_l5_index_config: записать index_config.json для C++.
      3) native_load_index: загрузить индекс в C++-ядро.

    ВАЖНО:
      - Python-индексы (BM25, index.json) больше НЕ используются.
      - Поле rebuild_bm25 в запросе игнорируется (оставлено только для совместимости схемы).
    """
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
                "[L5/reindex] step=corpus docs_total=%s corpus_docs=%s duration=%.3fs",
                corpus_stats.get("docs_total"),
                corpus_stats.get("corpus_docs"),
                corpus_stats["duration_sec"],
            )

            if corpus_stats.get("corpus_docs", 0) == 0:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="L5 corpus is empty, nothing to index",
                )

        # 1) C++ native index (index_builder)
        if body.rebuild_main_index:
            t0 = time.monotonic()
            native_build_stats = await run_in_threadpool(build_native_index_cpp)
            t1 = time.monotonic()

            native_build_stats = dict(native_build_stats)
            native_build_stats.setdefault("duration_sec", round(t1 - t0, 3))
            stats["index_native_cpp"] = native_build_stats

            logger.info(
                "[L5/reindex] step=native_cpp rc=%s duration=%.3fs",
                native_build_stats.get("rc"),
                native_build_stats["duration_sec"],
            )

        # 2) index_config.json для C++
        #    (если cfg=None — берутся дефолты из Pydantic-конфига)
        write_l5_index_config(INDEX_DIR, None)

        # 3) загрузка индекса в C++
        if body.export_native:
            t0 = time.monotonic()
            await run_in_threadpool(native_load_index)
            t1 = time.monotonic()

            native_stats = {
                "duration_sec": round(t1 - t0, 3),
            }
            stats["native_load"] = native_stats

            logger.info(
                "[L5/reindex] step=native_load duration=%.3fs",
                native_stats["duration_sec"],
            )

    except HTTPException:
        raise
    except Exception as e:
        import traceback

        logger.error(
            "[L5/reindex] unexpected error: %s\n%s", e, traceback.format_exc()
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Level5 reindex failed: {e}",
        )

    t_total_end = time.monotonic()
    total_sec = round(t_total_end - t_total_start, 3)

    stats.setdefault("meta", {})
    stats["meta"]["total_duration_sec"] = total_sec

    logger.info("[L5/reindex] finished total_duration=%.3fs", total_sec)

    return Level5ReindexResponse(
        ok=True,
        message="Level5 index rebuilt successfully",
        stats=stats,
    )


# ─────────────────────────── Единственный публичный поиск L5 ─────────────────────────── #


@router.post(
    "/search",
    response_model=Level5SearchResponse,
    status_code=status.HTTP_200_OK,
)
async def level5_search(body: Level5SearchRequest) -> Level5SearchResponse:
    """
    Поиск по монолитному индексу уровня 5.

    Сейчас пайплайн максимально простой:
      1) Чистый C++-поиск по шинглам (native_search), без BM25 и реранка.

    Параметры:
      - body.query       — текст запроса.
      - body.shingle_top — сколько максимум хитов вернуть из C++ (используем как top_k).
      - body.final_top   — обрезка результата сверху (можно оставить < shingle_top).
    """
    q = (body.query or "").strip()
    if not q:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="query must be non-empty",
        )

    top_from_cpp = body.shingle_top
    if top_from_cpp <= 0:
        top_from_cpp = 10

    try:
        # Чистый C++-поиск, без allowed_doc_ids — поиск по всему L5-массиву
        raw_res = await run_in_threadpool(
            native_search,
            q,
            top_from_cpp,
            None,  # allowed_doc_ids
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Level5 search failed: {e}",
        )

    docs_raw = raw_res.get("documents") or []

    # Обрежем до final_top, если задано меньше, чем пришло из C++
    if body.final_top and body.final_top > 0 and len(docs_raw) > body.final_top:
        docs_raw = docs_raw[: body.final_top]

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


# ───────────────────── Внутренние админские ручки (можно скрыть из Swagger) ───────────────────── #


@router.post(
    "/load-native",
    status_code=status.HTTP_200_OK,
    include_in_schema=False,  # скрыть из Swagger, оставить только для админа
)
async def level5_load_native_index() -> dict:
    """
    Явная загрузка index_native.bin в C++-ядро.
    Можно дергать вручную, но не показываем во внешнем API.
    """
    try:
        await run_in_threadpool(native_load_index)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"native_load_index failed: {e}",
        )

    return {"ok": True, "message": "native index loaded"}


# Отладочный прямой поиск — тоже скрыт из Swagger
class Native5SearchRequest(BaseModel):
    text: str = Field(..., description="Текст для проверки (UTF-8)")
    top_k: int = Field(10, ge=1, le=100)


class Native5SearchHit(BaseModel):
    doc_id: str
    score: float
    j9: float
    c9: float
    cand_hits: int


class Native5SearchResponse(BaseModel):
    hits: List[Native5SearchHit]


@router.post(
    "/search-native",
    response_model=Native5SearchResponse,
    status_code=status.HTTP_200_OK,
    include_in_schema=False,  # скрыто из Swagger
)
async def native5_search(req: Native5SearchRequest) -> Native5SearchResponse:
    """
    Прямой C++ поиск по шинглам (отладочный, без обёртки Level5* моделей).
    """
    if not req.text.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="text must be non-empty",
        )

    try:
        raw_res = await run_in_threadpool(
            native_search,
            req.text,
            req.top_k,
            None,  # allowed_doc_ids
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"native5 search failed: {e}",
        )

    docs = raw_res.get("documents") or []
    hits: List[Native5SearchHit] = []

    for d in docs:
        details = d.get("details") or {}
        hits.append(
            Native5SearchHit(
                doc_id=str(d.get("doc_id")),
                score=float(d.get("max_score", 0.0)),
                j9=float(details.get("J9", 0.0)),
                c9=float(details.get("C9", 0.0)),
                cand_hits=int(details.get("cand_hits", 0)),
            )
        )

    return Native5SearchResponse(hits=hits)
