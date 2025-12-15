# app/api/routes/upload.py
import os
import uuid
from datetime import datetime, timezone
from typing import Optional
from fastapi import Body, Query
from fastapi import APIRouter, Form, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import UPLOAD_DIR, N_SHARDS
from app.db.session import get_db
from app.models.document import Document

router = APIRouter(tags=["Upload"])


def utcnow():
    return datetime.now(timezone.utc)


def _validate_org_id(organization_id: int) -> None:
    if organization_id is None or int(organization_id) <= 0:
        raise HTTPException(status_code=422, detail="organization_id must be a positive integer")


def _safe_ext(filename: str) -> str:
    _, ext = os.path.splitext(filename or "")
    if not ext or len(ext) > 16:
        return ".txt"
    return ext


def compute_shard_id_for_text(organization_id: int, n_shards: int) -> int:
    # текстовый upload без метаданных -> shard 0
    # если хочешь: можно шардинг по user/session/key
    return 0


class TextUploadResponse(BaseModel):
    doc_id: int
    status: str
    shard_id: int
    external_id: str
    organization_id: int
    title: str


@router.post("/upload-text", response_model=TextUploadResponse)
async def upload_text(
    text: str = Body(..., media_type="text/plain"),
    organization_id: int = Query(...),
    title: str = Query("text_upload"),
    for_level5: bool = Query(False),
    db: AsyncSession = Depends(get_db),
):
    _validate_org_id(organization_id)

    if text is None or not text.strip():
        raise HTTPException(status_code=400, detail="Empty text")

    now = utcnow()
    status = "l5_uploaded" if for_level5 else "uploaded"

    shard_id = compute_shard_id_for_text(organization_id, N_SHARDS)

    # store as UTF-8 txt file in uploads
    ext = _safe_ext(title)  # if user passed "a.txt" keep ext, else ".txt"
    external_id = f"org_{organization_id}_text_{int(now.timestamp())}_{uuid.uuid4().hex}{ext}"
    upload_path = UPLOAD_DIR / external_id

    try:
        upload_path.write_text(text, encoding="utf-8")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cannot save text: {e}")

    doc = Document(
        organization_id=organization_id,
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
        student_name=None,
        university=None,
        faculty=None,
        group_name=None,
    )

    db.add(doc)
    await db.commit()
    await db.refresh(doc)

    return TextUploadResponse(
        doc_id=doc.id,
        status=doc.status,
        shard_id=doc.shard_id,
        external_id=doc.external_id,
        organization_id=doc.organization_id,
        title=doc.title or title,
    )
