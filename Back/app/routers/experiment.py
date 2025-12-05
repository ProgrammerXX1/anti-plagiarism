# app/api/routes/upload.py
import os
import zlib
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import UPLOAD_DIR, N_SHARDS
from app.db.session import get_db
from app.models.document import Document

router = APIRouter(tags=["Upload"])


class UploadResponse(BaseModel):
    doc_id: int
    status: str
    shard_id: int
    external_id: str


def utcnow():
    return datetime.now(timezone.utc)


def compute_shard_id(
    university: Optional[str],
    faculty: Optional[str],
    group_name: Optional[str],
    n_shards: int,
) -> int:
    """
    Простой шардинг по (university, faculty, group_name).
    Если всё пустое — идём в shard 0.
    """
    key = "|".join([
        university or "",
        faculty or "",
        group_name or "",
    ])
    if not key.strip():
        return 0
    h = zlib.crc32(key.encode("utf-8"))
    return h % n_shards


@router.post("/upload", response_model=UploadResponse)
async def upload_document(
    file: UploadFile = File(...),

    title: Optional[str] = Form(None),
    student_name: Optional[str] = Form(None),
    university: Optional[str] = Form(None),
    faculty: Optional[str] = Form(None),
    group_name: Optional[str] = Form(None),

    db: AsyncSession = Depends(get_db),
) -> UploadResponse:
    """
    Загружаем файл, кладём его в UPLOAD_DIR,
    создаём запись в documents со статусом 'uploaded'.
    """
    # читаем содержимое файла
    try:
        content = await file.read()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Cannot read file: {e}")

    if not content:
        raise HTTPException(status_code=400, detail="Empty file")

    # считаем shard_id
    shard_id = compute_shard_id(university, faculty, group_name, N_SHARDS)

    # делаем external_id = "doc_<timestamp>_<pid>.<ext>"
    _, ext = os.path.splitext(file.filename or "")
    if not ext:
        ext = ".bin"

    ts = int(utcnow().timestamp())
    external_id = f"doc_{ts}_{os.getpid()}{ext}"

    # сохраняем файл на диск
    upload_path = UPLOAD_DIR / external_id
    try:
        upload_path.write_bytes(content)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cannot save file: {e}")

    now = utcnow()

    doc = Document(
        external_id=external_id,
        shard_id=shard_id,
        segment_id=None,
        status="uploaded",
        simhash_hi=None,
        simhash_lo=None,
        created_at=now,
        updated_at=now,
        last_checked_at=None,
        title=title,
        student_name=student_name,
        university=university,
        faculty=faculty,
        group_name=group_name,
    )

    db.add(doc)
    await db.commit()
    await db.refresh(doc)

    return UploadResponse(
        doc_id=doc.id,
        status=doc.status,
        shard_id=doc.shard_id,
        external_id=doc.external_id,
    )
