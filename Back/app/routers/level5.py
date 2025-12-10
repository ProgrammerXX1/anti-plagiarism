from __future__ import annotations

import json
import time
from pathlib import Path
from typing import List, Set

from fastapi import APIRouter, HTTPException, status, Depends, Body
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logger import logger
from app.core.config import INDEX_DIR, CORPUS_JSONL, L5_SHARDS_DIR
from app.db.session import get_db
from app.models.document import Document
from app.schemas.level5 import (
    Level5BaseInfo,
    Level5ReindexRequest,
    Level5ReindexResponse,
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

# Монолитный индекс уровня 5 живёт тут:
MONO_INDEX_DIR = INDEX_DIR / "current"


# ─────────────────────── helpers для L5 ─────────────────────── #


def _iter_l5_doc_ids() -> Set[int]:
    """
    Собираем уникальные doc_id из всех index_native_docids.json всех L5-баз.
    Формат doc_id в json — строки, конвертим в int.
    """
    ids: Set[int] = set()

    if not L5_SHARDS_DIR.exists():
        return ids

    for shard_dir in L5_SHARDS_DIR.glob("shard_*"):
        if not shard_dir.is_dir():
            continue

        idx_dir = shard_dir / "current"
        if not idx_dir.is_dir():
            continue

        docids_path = idx_dir / "index_native_docids.json"
        if not docids_path.exists():
            continue

        try:
            data = json.loads(docids_path.read_text(encoding="utf-8"))
            if isinstance(data, list):
                for v in data:
                    try:
                        ids.add(int(v))
                    except (TypeError, ValueError):
                        continue
        except Exception:
            continue

    return ids


def _alloc_new_l5_base_dir() -> tuple[Path, int]:
    """
    Выделяет новый каталог базы L5: L5_SHARDS_DIR / shard_<id>/current.
    id — автоинкремент по существующим shard_*.
    """
    L5_SHARDS_DIR.mkdir(parents=True, exist_ok=True)

    max_id = -1
    for shard_dir in L5_SHARDS_DIR.glob("shard_*"):
        if not shard_dir.is_dir():
            continue
        name = shard_dir.name  # shard_0000
        try:
            sid = int(name.split("_", 1)[1])
            if sid > max_id:
                max_id = sid
        except (IndexError, ValueError):
            continue

    new_id = max_id + 1 if max_id >= 0 else 0
    base_dir = L5_SHARDS_DIR / f"shard_{new_id:04d}" / "current"
    base_dir.mkdir(parents=True, exist_ok=True)
    return base_dir, new_id


# ─────────────────────── список баз L5 ─────────────────────── #


@router.get("/bases", response_model=List[Level5BaseInfo])
async def list_level5_bases() -> List[Level5BaseInfo]:
    """
    Список всех баз уровня 5.

    Каждая индексация создаёт отдельную базу:
      L5_SHARDS_DIR / shard_<id> / current / index_native*.*
    """
    bases: List[Level5BaseInfo] = []

    if not L5_SHARDS_DIR.exists():
        return bases

    for shard_dir in sorted(L5_SHARDS_DIR.glob("shard_*")):
        if not shard_dir.is_dir():
            continue

        try:
            shard_id = int(shard_dir.name.split("_", 1)[1])
        except (IndexError, ValueError):
            continue

        idx_dir = shard_dir / "current"
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
                try:
                    size += p.stat().st_size
                except OSError:
                    continue

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


# ─────────────────────── Индексация L5 ─────────────────────── #
# /reindex        — создаёт НОВУЮ отдельную базу только из ещё не проиндексированных L5-доков
# /reindex-merged — собирает ВСЕ L5-доки в один монолитный индекс в MONO_INDEX_DIR
# ────────────────────────────────────────────────────────────── #


@router.post("/reindex", response_model=Level5ReindexResponse)
async def level5_reindex(
    body: Level5ReindexRequest,
    db: AsyncSession = Depends(get_db),
) -> Level5ReindexResponse:
    """
    Инкрементальная индексация L5.

    Каждой индексации соответствует отдельная база:
      L5_SHARDS_DIR / shard_<id>/current

    В новую базу попадают только документы:
      - Document.status == 'l5_uploaded'
      - doc.id отсутствует во всех существующих L5-базах.
    """
    stats: dict = {}
    t_total_start = time.monotonic()

    # doc_id, которые уже присутствуют в L5-базах
    indexed_ids = _iter_l5_doc_ids()

    stmt = select(Document).where(Document.status == "l5_uploaded")
    if indexed_ids:
        stmt = stmt.where(~Document.id.in_(indexed_ids))

    result = await db.execute(stmt)
    docs: List[Document] = list(result.scalars())

    if not docs:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Нет новых документов со status='l5_uploaded' для индексации в L5",
        )

    # выделяем новую базу
    base_dir, shard_id = _alloc_new_l5_base_dir()
    corpus_path = base_dir / "corpus.jsonl"

    try:
        # 0) corpus.jsonl только по новым doc.id
        t0 = time.monotonic()
        corpus_stats = await rebuild_l5_corpus(
            db,
            only_doc_ids=[d.id for d in docs],
            corpus_path=corpus_path,
        )
        t1 = time.monotonic()

        corpus_stats = dict(corpus_stats)
        corpus_stats["duration_sec"] = round(t1 - t0, 3)
        corpus_stats["shard_id"] = shard_id
        corpus_stats["corpus_path"] = str(corpus_path)
        stats["corpus"] = corpus_stats

        logger.info(
            "[L5/reindex] shard=%s docs_total=%s corpus_docs=%s duration=%.3fs",
            shard_id,
            corpus_stats.get("docs_total"),
            corpus_stats.get("corpus_docs"),
            corpus_stats["duration_sec"],
        )

        if corpus_stats.get("corpus_docs", 0) == 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Новых документов для L5 корпуса нет (corpus_docs=0)",
            )

        # 1) C++ native index для этой базы
        t0 = time.monotonic()
        native_build_stats = await run_in_threadpool(
            build_native_index_cpp,
            base_dir,
            corpus_path,
        )
        t1 = time.monotonic()

        native_build_stats = dict(native_build_stats)
        native_build_stats.setdefault("duration_sec", round(t1 - t0, 3))
        native_build_stats["shard_id"] = shard_id
        native_build_stats["index_dir"] = str(base_dir)
        stats["index_native_cpp"] = native_build_stats

        logger.info(
            "[L5/reindex] shard=%s step=native_cpp rc=%s duration=%.3fs",
            shard_id,
            native_build_stats.get("rc"),
            native_build_stats["duration_sec"],
        )

        # 2) index_config.json для C++ (локальный конфиг под этот shard)
        write_l5_index_config(base_dir, None)

        # 3) по желанию — загрузить ЭТУ базу в C++ ядро
        if body.export_native:
            t0 = time.monotonic()
            await run_in_threadpool(native_load_index, base_dir)
            t1 = time.monotonic()

            native_stats = {
                "duration_sec": round(t1 - t0, 3),
                "index_dir": str(base_dir),
                "shard_id": shard_id,
            }
            stats["native_load"] = native_stats

            logger.info(
                "[L5/reindex] shard=%s step=native_load duration=%.3fs",
                shard_id,
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
            detail=f"Level5 incremental reindex failed: {e}",
        )

    t_total_end = time.monotonic()
    total_sec = round(t_total_end - t_total_start, 3)

    stats.setdefault("meta", {})
    stats["meta"]["total_duration_sec"] = total_sec

    logger.info("[L5/reindex] finished shard=%s total_duration=%.3fs", shard_id, total_sec)

    return Level5ReindexResponse(
        ok=True,
        message=f"Level5 incremental index built for shard {shard_id}",
        stats=stats,
    )


@router.post("/reindex-merged", response_model=Level5ReindexResponse)
async def level5_reindex_merged(
    body: Level5ReindexRequest,
    db: AsyncSession = Depends(get_db),
) -> Level5ReindexResponse:
    """
    Полная сборка МОНOLИТНОГО индекса L5 в каталоге MONO_INDEX_DIR
    (/runtime/index/current).

    Пайплайн:
      0) corpus.jsonl из всех документов со status='l5_uploaded'
      1) C++ index_builder → index_native.bin + docids + meta
      2) index_config.json
      3) native_load_index (по флагу export_native)

    Это та самая «кнопка сборки всего этого в 1».
    """
    stats: dict = {}
    t_total_start = time.monotonic()

    index_dir = MONO_INDEX_DIR

    try:
        # 0) corpus.jsonl (все l5_uploaded)
        need_corpus = body.rebuild_corpus or (not CORPUS_JSONL.exists())
        if need_corpus:
            t0 = time.monotonic()
            corpus_stats = await rebuild_l5_corpus(
                db,
                only_doc_ids=None,
                corpus_path=CORPUS_JSONL,
            )
            t1 = time.monotonic()

            corpus_stats = dict(corpus_stats)
            corpus_stats["duration_sec"] = round(t1 - t0, 3)
            stats["corpus"] = corpus_stats

            logger.info(
                "[L5/reindex-merged] step=corpus docs_total=%s corpus_docs=%s duration=%.3fs (forced=%s)",
                corpus_stats.get("docs_total"),
                corpus_stats.get("corpus_docs"),
                corpus_stats["duration_sec"],
                not body.rebuild_corpus and not CORPUS_JSONL.exists(),
            )

            if corpus_stats.get("corpus_docs", 0) == 0:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="L5 corpus is empty, nothing to index",
                )
        else:
            logger.info(
                "[L5/reindex-merged] reuse existing corpus.jsonl at %s (rebuild_corpus=%s)",
                CORPUS_JSONL,
                body.rebuild_corpus,
            )
            if not CORPUS_JSONL.exists():
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"L5 corpus.jsonl not found at {CORPUS_JSONL}",
                )

        # 1) C++ native index (монолит в MONO_INDEX_DIR)
        if body.rebuild_main_index:
            index_dir.mkdir(parents=True, exist_ok=True)

            t0 = time.monotonic()
            native_build_stats = await run_in_threadpool(
                build_native_index_cpp,
                index_dir,
                CORPUS_JSONL,
            )
            t1 = time.monotonic()

            native_build_stats = dict(native_build_stats)
            native_build_stats.setdefault("duration_sec", round(t1 - t0, 3))
            stats["index_native_cpp"] = native_build_stats

            logger.info(
                "[L5/reindex-merged] step=native_cpp rc=%s duration=%.3fs index_dir=%s",
                native_build_stats.get("rc"),
                native_build_stats["duration_sec"],
                index_dir,
            )

        # 2) index_config.json
        write_l5_index_config(index_dir, None)

        # 3) загрузка монолитного индекса в C++
        if body.export_native:
            t0 = time.monotonic()
            await run_in_threadpool(native_load_index, index_dir)
            t1 = time.monotonic()

            native_stats = {
                "duration_sec": round(t1 - t0, 3),
                "index_dir": str(index_dir),
            }
            stats["native_load"] = native_stats

            logger.info(
                "[L5/reindex-merged] step=native_load duration=%.3fs",
                native_stats["duration_sec"],
            )

    except HTTPException:
        raise
    except Exception as e:
        import traceback

        logger.error(
            "[L5/reindex-merged] unexpected error: %s\n%s", e, traceback.format_exc()
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Level5 merged reindex failed: {e}",
        )

    t_total_end = time.monotonic()
    total_sec = round(t_total_end - t_total_start, 3)

    stats.setdefault("meta", {})
    stats["meta"]["total_duration_sec"] = total_sec

    logger.info("[L5/reindex-merged] finished total_duration=%.3fs", total_sec)

    return Level5ReindexResponse(
        ok=True,
        message="Level5 merged index rebuilt successfully",
        stats=stats,
    )


# ─────────────────────────── Единственный публичный поиск L5 ─────────────────────────── #


@router.post(
    "/search",
    response_model=Level5SearchResponse,
    status_code=status.HTTP_200_OK,
)
async def level5_search(
    text: str = Body(
        ...,
        media_type="text/plain",
        description="Текст для проверки / поиска по L5",
    )
) -> Level5SearchResponse:
    """
    RAG-подобный поиск по L5:
    на вход — сырой текст (документ / запрос) как text/plain,
    на выход — топ совпадений из текущего загруженного C++ индекса.

    Важно: для монолитного поиска ожидается, что индекс собран в MONO_INDEX_DIR
    и загружен через native_load_index (warmup, /reindex-merged или /load-native).
    """
    q = (text or "").strip()
    if not q:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="text must be non-empty",
        )

    # Защита от ситуации, когда индекс ещё не собрали
    bin_path = MONO_INDEX_DIR / "index_native.bin"
    docids_path = MONO_INDEX_DIR / "index_native_docids.json"
    if not bin_path.exists() or not docids_path.exists():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "L5 монолитный индекс не найден в /runtime/index/current. "
                "Сначала вызови /api/level5/reindex-merged (rebuild_corpus=true, "
                "rebuild_main_index=true, export_native=true)."
            ),
        )

    top_from_cpp = 10  # фиксированный top_k

    try:
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


# ───────────────────── Админские ручки (скрыты из Swagger) ───────────────────── #


@router.post(
    "/load-native",
    status_code=status.HTTP_200_OK,
    include_in_schema=False,
)
async def level5_load_native_index() -> dict:
    """
    Ручная загрузка монолитного индекса из MONO_INDEX_DIR в C++ ядро.
    """
    bin_path = MONO_INDEX_DIR / "index_native.bin"
    if not bin_path.exists():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "L5 монолитный индекс ещё не собран. "
                "Сначала вызови /api/level5/reindex-merged."
            ),
        )

    try:
        await run_in_threadpool(native_load_index, MONO_INDEX_DIR)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"native_load_index failed: {e}",
        )

    return {"ok": True, "message": "native index loaded"}


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
    include_in_schema=False,
)
async def native5_search(req: Native5SearchRequest) -> Native5SearchResponse:
    """
    Отладочный прямой поиск по текущему загруженному C++ индексу.
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
            None,
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
