# app/api/routes/admin_levels.py
from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select, delete, func, text
from sqlalchemy.ext.asyncio import AsyncSession

# ДОБАВИЛ L5_SHARDS_DIR
from app.core.config import INDEX_DIR, UPLOAD_DIR, CORPUS_JSONL, L5_SHARDS_DIR
from app.db.session import get_db
from app.models.document import Document
from app.models.segment import Segment
from app.models.segment_doc import SegmentDoc

router = APIRouter(
    prefix="/api/admin",
    tags=["Admin levels"],
)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class LevelCleanupResult(BaseModel):
    level: int
    shard_id: Optional[int] = None
    deleted_segments: int
    affected_documents: int
    removed_paths: int
    removed_files: int


class NuclearCleanupResult(BaseModel):
    shard_id: Optional[int]
    deleted_documents: int
    deleted_segments: int
    deleted_segment_docs: int
    removed_index_files: int
    removed_upload_files: int
    corpus_truncated: bool


def _rm_tree(path: Path) -> int:
    """
    Удаляет директорию/файл рекурсивно.
    Возвращает примерное количество удалённых файлов/директорий.
    """
    if not path.exists():
        return 0

    count = 0

    def _on_rm(_func, _path, _excinfo):
        nonlocal count
        count += 1

    if path.is_dir():
        for _p in path.rglob("*"):
            count += 1
        shutil.rmtree(path, onerror=_on_rm)
    else:
        path.unlink(missing_ok=True)
        count += 1

    return count


# ───────────────────────────────────────────────────────────────
# ЯДЕРНАЯ КНОПКА: полный сброс всего (доки, сегменты, файлы, corpus, L5)
# ───────────────────────────────────────────────────────────────
@router.delete("/levels/nuke", response_model=NuclearCleanupResult)
async def nuke_all_levels(
    shard_id: Optional[int] = Query(
        None,
        description="Если задан — чистим только указанный shard_id; "
                    "если None — ПОЛНЫЙ сброс с RESTART IDENTITY",
    ),
    db: AsyncSession = Depends(get_db),
) -> NuclearCleanupResult:
    """
    Полный сброс системы индексации.

    Если shard_id is None:
      - TRUNCATE segment_docs, documents, segments RESTART IDENTITY CASCADE
      - чистим INDEX_DIR, L5_SHARDS_DIR, UPLOAD_DIR, corpus.jsonl

    Если shard_id задан:
      - удаляем только данные по этому шарду (без RESTART IDENTITY),
      - плюс L5-базу этого шарда (если есть).
    """

    # ─────────────────────────────────────────────
    # Ветка 1: полный Nuke (без shard_id) + RESTART IDENTITY
    # ─────────────────────────────────────────────
    if shard_id is None:
        # считаем, что было, до TRUNCATE
        docs_cnt = (
            await db.execute(select(func.count()).select_from(Document))
        ).scalar_one()
        seg_cnt = (
            await db.execute(select(func.count()).select_from(Segment))
        ).scalar_one()
        seg_docs_cnt = (
            await db.execute(select(func.count()).select_from(SegmentDoc))
        ).scalar_one()

        # TRUNCATE всё и сбросить sequence
        await db.execute(
            text(
                "TRUNCATE TABLE segment_docs, documents, segments "
                "RESTART IDENTITY CASCADE"
            )
        )
        await db.commit()

        deleted_documents = docs_cnt
        deleted_segments = seg_cnt
        deleted_segment_docs = seg_docs_cnt

    # ─────────────────────────────────────────────
    # Ветка 2: nuke только по shard_id (без сброса sequence)
    # ─────────────────────────────────────────────
    else:
        # documents по шарду
        doc_stmt = select(Document).where(Document.shard_id == shard_id)
        doc_result = await db.execute(doc_stmt)
        docs: List[Document] = list(doc_result.scalars())
        doc_ids = [d.id for d in docs]

        # segments по шарду
        seg_stmt = select(Segment).where(Segment.shard_id == shard_id)
        seg_result = await db.execute(seg_stmt)
        segments: List[Segment] = list(seg_result.scalars())
        seg_ids = [s.id for s in segments]

        # segment_docs по этим сегментам
        if seg_ids:
            sd_stmt = select(SegmentDoc).where(
                SegmentDoc.segment_id.in_(seg_ids)
            )
        else:
            sd_stmt = select(SegmentDoc).where(
                SegmentDoc.segment_id == -1  # ничего
            )

        sd_result = await db.execute(sd_stmt)
        seg_docs: List[SegmentDoc] = list(sd_result.scalars())

        deleted_segment_docs = len(seg_docs)
        deleted_segments = len(segments)
        deleted_documents = len(docs)

        # удаляем связи SegmentDoc
        if seg_ids:
            await db.execute(
                delete(SegmentDoc).where(
                    SegmentDoc.segment_id.in_(seg_ids)
                )
            )

        # удаляем сегменты
        if seg_ids:
            await db.execute(
                delete(Segment).where(Segment.id.in_(seg_ids))
            )

        # удаляем документы
        if doc_ids:
            await db.execute(
                delete(Document).where(Document.id.in_(doc_ids))
            )

        await db.commit()

    # ─────────────────────────────────────────────
    # Чистим файловую систему
    # ─────────────────────────────────────────────
    removed_index_files = 0

    try:
        # Основной индекс (L0–L4, старый L5)
        if INDEX_DIR.exists():
            removed_index_files += _rm_tree(INDEX_DIR)
            INDEX_DIR.mkdir(parents=True, exist_ok=True)

        # Базы уровня 5 (шарды)
        if shard_id is None:
            # Полный сброс: сносим весь L5_SHARDS_DIR
            if L5_SHARDS_DIR.exists():
                removed_index_files += _rm_tree(L5_SHARDS_DIR)
                L5_SHARDS_DIR.mkdir(parents=True, exist_ok=True)
        else:
            # Частичный nuke: чистим только один shard_XXXX
            # если структура L5_SHARDS_DIR/shard_0000/current
            shard_dir = None
            for p in L5_SHARDS_DIR.glob("shard_*"):
                # ожидаем имена вида shard_0000, shard_0001 и т.п.
                try:
                    sid = int(p.name.split("_", 1)[1])
                except (IndexError, ValueError):
                    continue
                if sid == shard_id:
                    shard_dir = p
                    break

            if shard_dir and shard_dir.exists():
                removed_index_files += _rm_tree(shard_dir)
                # корневую папку для L5 всё равно держим
                L5_SHARDS_DIR.mkdir(parents=True, exist_ok=True)

    except Exception:
        # не валимся из-за проблем с FS
        pass

    removed_upload_files = 0
    try:
        if UPLOAD_DIR.exists():
            removed_upload_files = _rm_tree(UPLOAD_DIR)
            UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    corpus_truncated = False
    try:
        if CORPUS_JSONL.exists():
            CORPUS_JSONL.unlink()
        corpus_truncated = True
    except Exception:
        corpus_truncated = False

    return NuclearCleanupResult(
        shard_id=shard_id,
        deleted_documents=deleted_documents,
        deleted_segments=deleted_segments,
        deleted_segment_docs=deleted_segment_docs,
        removed_index_files=removed_index_files,
        removed_upload_files=removed_upload_files,
        corpus_truncated=corpus_truncated,
    )
