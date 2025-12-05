# app/api/routes/documents_upload.py
from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import UPLOAD_DIR
from app.db.session import get_db
from app.models.document import Document

router = APIRouter(prefix="/api/documents", tags=["Documents"])


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def compute_shard_id(
    university: Optional[str],
    faculty: Optional[str],
    group_name: Optional[str],
    total_shards: int = 1,  # пока один шард, потом вынесем в конфиг
) -> int:
    if total_shards <= 1:
        return 0

    base = "|".join([
        university or "",
        faculty or "",
        group_name or "",
    ])
    h = hashlib.sha256(base.encode("utf-8")).hexdigest()
    # берём последние 8 hex-символов → int → mod
    v = int(h[-8:], 16)
    return v % total_shards


@router.post("/upload")
async def upload_document(
    file: UploadFile = File(...),
    title: Optional[str] = Form(None),
    student_name: Optional[str] = Form(None),
    university: Optional[str] = Form(None),
    faculty: Optional[str] = Form(None),
    group_name: Optional[str] = Form(None),
    db: AsyncSession = Depends(get_db),
):
    """
    Загрузка работы студента:

    1) сохраняем файл в UPLOAD_DIR
    2) создаём Document со статусом 'uploaded'
    3) возвращаем id и статус
    """

    # проверка типа файла (при желании можно ужесточить)
    if not file.filename:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Пустое имя файла",
        )

    # генерируем external_id, чтобы не было конфликтов по именам
    # формат: <uuid4>__<original_name>
    safe_name = file.filename.replace("/", "_").replace("\\", "_")
    external_id = f"{uuid.uuid4().hex}__{safe_name}"

    dest_path: Path = UPLOAD_DIR / external_id

    # сохраняем файл на диск
    try:
        content = await file.read()
        dest_path.write_bytes(content)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка сохранения файла: {e}",
        )

    now = utcnow()

    # считаем shard_id (пока один шард, но функция уже готова для масштабирования)
    shard_id = compute_shard_id(university, faculty, group_name, total_shards=1)

    # создаём запись в documents
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

    return {
        "id": doc.id,
        "status": doc.status,
        "external_id": doc.external_id,
        "shard_id": doc.shard_id,
    }
