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

# ДОБАВИЛ CORPUS_JSONL
from app.core.config import INDEX_DIR, UPLOAD_DIR, CORPUS_JSONL
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
# Удаление уровня 0: документы без сегмента, статусы uploaded/etl_ok
# ───────────────────────────────────────────────────────────────

# @router.delete("/levels/0", response_model=LevelCleanupResult)
# async def cleanup_level_zero(
#     shard_id: Optional[int] = Query(
#         None,
#         description="Если задан, чистим только указанный shard_id",
#     ),
#     db: AsyncSession = Depends(get_db),
# ) -> LevelCleanupResult:
#     """
#     Уровень 0:
#       - documents со status in {uploaded, etl_ok} и segment_id IS NULL
#       - удаляем файлы из UPLOAD_DIR по external_id
#       - удаляем сами документы
#     """
#     stmt = select(Document).where(
#         Document.segment_id.is_(None),
#         Document.status.in_(("uploaded", "etl_ok")),
#     )
#     if shard_id is not None:
#         stmt = stmt.where(Document.shard_id == shard_id)

#     result = await db.execute(stmt)
#     docs: List[Document] = list(result.scalars())

#     if not docs:
#         return LevelCleanupResult(
#             level=0,
#             shard_id=shard_id,
#             deleted_segments=0,
#             affected_documents=0,
#             removed_paths=0,
#             removed_files=0,
#         )

#     removed_files = 0

#     # сначала удаляем файлы
#     for doc in docs:
#         if not doc.external_id:
#             continue
#         fpath = UPLOAD_DIR / doc.external_id
#         if fpath.exists():
#             try:
#                 fpath.unlink()
#                 removed_files += 1
#             except Exception:
#                 # можно залогировать, но API не роняем
#                 pass

#     # потом удаляем документы из БД
#     for doc in docs:
#         await db.delete(doc)

#     await db.commit()

#     return LevelCleanupResult(
#         level=0,
#         shard_id=shard_id,
#         deleted_segments=0,
#         affected_documents=len(docs),
#         removed_paths=0,
#         removed_files=removed_files,
#     )


# ───────────────────────────────────────────────────────────────
# ЯДЕРНАЯ КНОПКА: полный сброс всего (доки, сегменты, файлы, corpus)
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
      - чистим INDEX_DIR, UPLOAD_DIR, corpus.jsonl

    Если shard_id задан:
      - удаляем только данные по этому шарду (без RESTART IDENTITY).
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
        if INDEX_DIR.exists():
            removed_index_files = _rm_tree(INDEX_DIR)
            INDEX_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
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


# ───────────────────────────────────────────────────────────────
# Удаление уровней 1–4: сегменты, index_native.* и сброс документов
# ───────────────────────────────────────────────────────────────

# @router.delete("/levels/{level}", response_model=LevelCleanupResult)
# async def cleanup_level(
#     level: int,
#     shard_id: Optional[int] = Query(
#         None,
#         description="Если задан, чистим только указанный shard_id",
#     ),
#     reset_docs_to: str = Query(
#         "etl_ok",
#         description="Во что перевести статус документов ('etl_ok' или 'uploaded')",
#     ),
#     db: AsyncSession = Depends(get_db),
# ) -> LevelCleanupResult:
#     """
#     Уровни 1–4:
#       - ищем Segment.level == level (и, опционально, shard_id)
#       - собираем SegmentDoc по этим сегментам
#       - сбрасываем у связанных документов segment_id и статус -> reset_docs_to
#       - удаляем SegmentDoc и Segment
#       - удаляем директории индексов на диске (INDEX_DIR / segment.path)
#     """
#     if level < 1 or level > 4:
#         raise HTTPException(
#             status_code=400,
#             detail="level должен быть от 1 до 4",
#         )

#     # 1) сегменты нужного уровня
#     stmt = select(Segment).where(Segment.level == level)
#     if shard_id is not None:
#         stmt = stmt.where(Segment.shard_id == shard_id)

#     seg_result = await db.execute(stmt)
#     segments: List[Segment] = list(seg_result.scalars())

#     if not segments:
#         return LevelCleanupResult(
#             level=level,
#             shard_id=shard_id,
#             deleted_segments=0,
#             affected_documents=0,
#             removed_paths=0,
#             removed_files=0,
#         )

#     seg_ids = [s.id for s in segments]
#     seg_paths = [s.path for s in segments if s.path]

#     # 2) связи segment_docs
#     sd_result = await db.execute(
#         select(SegmentDoc).where(SegmentDoc.segment_id.in_(seg_ids))
#     )
#     seg_docs: List[SegmentDoc] = list(sd_result.scalars())
#     doc_ids = {sd.document_id for sd in seg_docs}

#     # 3) обновляем документы
#     affected_docs = 0
#     if doc_ids:
#         doc_result = await db.execute(
#             select(Document).where(Document.id.in_(doc_ids))
#         )
#         docs: List[Document] = list(doc_result.scalars())
#         now = utcnow()
#         for d in docs:
#             d.segment_id = None
#             d.status = reset_docs_to
#             d.updated_at = now
#         affected_docs = len(docs)

#     # 4) удаляем SegmentDoc
#     if seg_ids:
#         await db.execute(
#             delete(SegmentDoc).where(SegmentDoc.segment_id.in_(seg_ids))
#         )

#     # 5) удаляем сами Segment
#     for s in segments:
#         await db.delete(s)

#     await db.commit()

#     # 6) чистим директории индексов на диске
#     removed_paths = 0
#     removed_files = 0

#     for rel in seg_paths:
#         dir_path = INDEX_DIR / rel
#         if dir_path.exists():
#             removed_paths += 1
#             try:
#                 removed_files += _rm_tree(dir_path)
#             except Exception:
#                 # можно залогировать, но API не роняем
#                 pass

#     return LevelCleanupResult(
#         level=level,
#         shard_id=shard_id,
#         deleted_segments=len(segments),
#         affected_documents=affected_docs,
#         removed_paths=removed_paths,
#         removed_files=removed_files,
#     )