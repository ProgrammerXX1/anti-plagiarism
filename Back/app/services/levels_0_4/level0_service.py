# app/services/levels0_4/level0_service.py
from __future__ import annotations

from typing import Optional

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
    organization_id: int,
    title: Optional[str],
    student_name: Optional[str],
    university: Optional[str],
    faculty: Optional[str],
    group_name: Optional[str],
    external_id: Optional[str] = None,
) -> int:
    doc = await create_document(
        db,
        organization_id=organization_id,
        title=title,
        student_name=student_name,
        university=university,
        faculty=faculty,
        group_name=group_name,
        external_id=external_id,
    )
    logger.info(
        "[level0] uploaded document id=%s org=%s shard=%s",
        doc.id, doc.organization_id, doc.shard_id
    )
    return doc.id


async def enqueue_etl_for_unsegmented_docs(
    db: AsyncSession,
    *,
    organization_id: int,
    shard_id: int,
    batch_limit: int = 100,
) -> int:
    docs = await list_unsegmented_docs_for_shard(
        db,
        organization_id=organization_id,
        shard_id=shard_id,
        limit=batch_limit,
    )
    if not docs:
        logger.info("[level0] no unsegmented docs for org=%s shard=%s", organization_id, shard_id)
        return 0

    cnt = 0
    for d in docs:
        await enqueue_task(
            db,
            task_type="etl",
            payload={
                "doc_id": d.id,
                "organization_id": d.organization_id,
                "shard_id": d.shard_id,
            },
        )
        cnt += 1

    logger.info(
        "[level0] enqueued %d etl tasks for org=%s shard=%s (batch_limit=%d)",
        cnt, organization_id, shard_id, batch_limit
    )
    return cnt


async def mark_etl_ok(db: AsyncSession, doc_id: int) -> None:
    await set_document_status(db, doc_id=doc_id, status="etl_ok")
