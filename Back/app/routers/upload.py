# app/api/routes/upload.py
import os
import zlib
from datetime import datetime, timezone
from typing import Optional, List
from io import BytesIO
import uuid
import zipfile

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

    # False → обычный конвейер 0–4 уровней
    # True  → только для L5 (giant index), worker L0–L4 такие документы не трогает
    for_level5: bool = Form(False),

    db: AsyncSession = Depends(get_db),
) -> UploadResponse:
    """
    Загружаем файл, кладём его в UPLOAD_DIR,
    создаём запись в documents.

    - Если for_level5 = False → status='uploaded' (pipeline L0–L4).
    - Если for_level5 = True  → status='l5_uploaded' (монолитный индекс L5).
    """
    try:
        content = await file.read()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Cannot read file: {e}")

    if not content:
        raise HTTPException(status_code=400, detail="Empty file")

    # считаем shard_id (для L5 можно тоже использовать, не мешает)
    shard_id = compute_shard_id(university, faculty, group_name, N_SHARDS)

    _, ext = os.path.splitext(file.filename or "")
    if not ext:
        ext = ".bin"

    ts = int(utcnow().timestamp())
    external_id = f"doc_{ts}_{os.getpid()}{ext}"

    upload_path = UPLOAD_DIR / external_id
    try:
        upload_path.write_bytes(content)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cannot save file: {e}")

    now = utcnow()

    status = "l5_uploaded" if for_level5 else "uploaded"

    doc = Document(
        external_id=external_id,
        shard_id=shard_id,
        segment_id=None,
        status=status,
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


class ZipUploadResponseItem(BaseModel):
    doc_id: int
    external_id: str


class ZipUploadResponse(BaseModel):
    count: int
    items: List[ZipUploadResponseItem]


@router.post("/upload-zip", response_model=ZipUploadResponse)
async def upload_zip_archive(
    file: UploadFile = File(...),

    # По умолчанию ZIP считаем «гигантским архивом» для L5
    for_level5: bool = Form(True),

    db: AsyncSession = Depends(get_db),
) -> ZipUploadResponse:
    """
    Принимает ZIP-архив.

    Каждый файл внутри:
      - сохраняется в UPLOAD_DIR
      - получает случайное external_id
      - создаётся Document

    - Если for_level5 = True  → status='l5_uploaded', shard_id=0, L5 монолит.
    - Если for_level5 = False → status='uploaded', shard_id=0, пойдёт в L0–L4.
    """
    if not file.filename.lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="Only .zip архивы разрешены")

    try:
        raw = await file.read()
        zip_bytes = BytesIO(raw)
        zf = zipfile.ZipFile(zip_bytes)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid zip archive: {e}")

    now = utcnow()
    created: List[ZipUploadResponseItem] = []

    status = "l5_uploaded" if for_level5 else "uploaded"

    for name in zf.namelist():
        # пропускаем директории и мусор
        if name.endswith("/") or name.startswith("__MACOSX"):
            continue

        try:
            content = zf.read(name)
        except Exception:
            continue

        if not content:
            continue

        _, ext = os.path.splitext(name)
        if not ext:
            ext = ".bin"

        external_id = f"zip_{uuid.uuid4().hex}{ext}"
        upload_path = UPLOAD_DIR / external_id

        try:
            upload_path.write_bytes(content)
        except Exception:
            continue

        doc = Document(
            external_id=external_id,
            shard_id=0,          # zip в любом случае кладём в shard 0
            segment_id=None,
            status=status,
            simhash_hi=None,
            simhash_lo=None,
            created_at=now,
            updated_at=now,
            last_checked_at=None,
            title=name,
            student_name=None,
            university=None,
            faculty=None,
            group_name=None,
        )

        db.add(doc)
        await db.flush()

        created.append(
            ZipUploadResponseItem(
                doc_id=doc.id,
                external_id=external_id,
            )
        )

    await db.commit()

    return ZipUploadResponse(
        count=len(created),
        items=created,
    )
