# app/services/levels0_4/level0_service.py
from __future__ import annotations

from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app.repositories.documents import create_document, set_document_status
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
    logger.info("[level0] uploaded document id=%s org=%s shard=%s", doc.id, doc.organization_id, doc.shard_id)
    return int(doc.id)


async def mark_etl_ok(db: AsyncSession, doc_id: int) -> None:
    await set_document_status(db, doc_id=int(doc_id), status="etl_ok")
