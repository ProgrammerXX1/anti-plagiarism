# app/api/routes/upload.py
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.repositories.documents import create_document
from app.repositories.index_tasks import enqueue_task
from sqlalchemy import select, func

from app.db.session import get_db
from app.models.segment import Segment
from app.models.document import Document

# сюда же потом воткнёшь отправку задачи в очередь: from app.services.tasks import send_etl_task

router = APIRouter(prefix="/api", tags=["upload"])


@router.post("/upload")
async def upload_document(
    file: UploadFile = File(...),
    title: Optional[str] = Form(None),
    student_name: Optional[str] = Form(None),
    university: Optional[str] = Form(None),
    faculty: Optional[str] = Form(None),
    group_name: Optional[str] = Form(None),
    external_id: Optional[str] = Form(None),
    db: AsyncSession = Depends(get_db),
):
    if not file.filename:
        raise HTTPException(status_code=400, detail="empty filename")

    # пока только регистрируем метаданные
    doc = await create_document(
        db,
        title=title or file.filename,
        student_name=student_name,
        university=university,
        faculty=faculty,
        group_name=group_name,
        external_id=external_id,
    )
    doc = await create_document(...)
    await enqueue_task(
        db,
        task_type="etl_doc",
        payload={"doc_id": doc.id},
    )
    await db.commit()

    # TODO: послать задачу ETL(doc.id) в очередь
    # await send_etl_task(doc_id=doc.id, filename=file.filename, ...)

    return {
        "doc_id": doc.id,
        "status": doc.status,
        "shard_id": doc.shard_id,
    }



@router.get("/stats")
async def get_stats(db: AsyncSession = Depends(get_db)):
    total_docs = await db.scalar(select(func.count()).select_from(Document))
    total_segments = await db.scalar(select(func.count()).select_from(Segment))
    return {
        "total_docs": total_docs or 0,
        "total_segments": total_segments or 0,
    }