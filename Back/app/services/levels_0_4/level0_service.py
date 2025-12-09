# app/services/levels0_4/level0_service.py
from __future__ import annotations

from typing import Optional, List

from sqlalchemy.ext.asyncio import AsyncSession

from app.repositories.documents import (
    create_document,
    list_unsegmented_docs_for_shard,
    set_document_status,
)
from app.repositories.index_tasks import enqueue_task
from app.core.logger import logger


async def upload_student_document(
    db: AsyncSession,
    *,
    title: Optional[str],
    student_name: Optional[str],
    university: Optional[str],
    faculty: Optional[str],
    group_name: Optional[str],
    external_id: Optional[str] = None,
) -> int:
    """
    Уровень 0:
      - создаём запись в documents со статусом 'uploaded'
      - возвращаем doc.id
    """
    doc = await create_document(
        db,
        title=title,
        student_name=student_name,
        university=university,
        faculty=faculty,
        group_name=group_name,
        external_id=external_id,
    )
    logger.info("[level0] uploaded document id=%s shard=%s", doc.id, doc.shard_id)
    return doc.id


async def enqueue_etl_for_unsegmented_docs(
    db: AsyncSession,
    *,
    shard_id: int,
    batch_limit: int = 100,
) -> int:
    """
    Уровень 0 → подготовка к 1 уровню:
      - забираем из documents все uploaded без segment_id для этого шарда
      - ставим задачи в очередь index_tasks (task_type='etl')
    """
    docs = await list_unsegmented_docs_for_shard(
        db,
        shard_id=shard_id,
        limit=batch_limit,
    )
    if not docs:
        logger.info("[level0] no unsegmented docs for shard=%s", shard_id)
        return 0

    cnt = 0
    for d in docs:
        await enqueue_task(
            db,
            task_type="etl",
            payload={
                "doc_id": d.id,
                "shard_id": d.shard_id,
            },
        )
        cnt += 1

    logger.info(
        "[level0] enqueued %d etl tasks for shard=%s (batch_limit=%d)",
        cnt,
        shard_id,
        batch_limit,
    )
    return cnt

async def mark_etl_ok(
    db: AsyncSession,
    doc_id: int,
) -> None:
    await set_document_status(
        db,
        doc_id=doc_id,
        status="etl_ok",
    )
