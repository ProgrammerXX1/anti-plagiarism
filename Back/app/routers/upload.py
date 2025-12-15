# app/api/upload_text.py
import os
import uuid
import json
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Body, Query
from pydantic import BaseModel
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
    # TODO: нормальный shard hash
    return 0


class TextUploadResponse(BaseModel):
    doc_id: int
    status: str
    shard_id: int
    external_id: str
    organization_id: int
    title: str
    index_normalize: bool


@router.post("/upload-text", response_model=TextUploadResponse)
async def upload_text(
    text: str = Body(..., media_type="text/plain"),
    organization_id: int = Query(...),
    title: str = Query("text_upload"),
    for_level5: bool = Query(False),

    # 🔥 ЕДИНСТВЕННЫЙ ФЛАГ
    index_normalize: bool = Query(
        True,
        description="Включить или выключить нормализацию при индексации"
    ),

    db: AsyncSession = Depends(get_db),
):
    _validate_org_id(organization_id)

    if not text or not text.strip():
        raise HTTPException(status_code=400, detail="Empty text")

    now = utcnow()
    status = "l5_uploaded" if for_level5 else "uploaded"
    shard_id = compute_shard_id_for_text(organization_id, N_SHARDS)

    ext = _safe_ext(title)

    external_id = (
        f"org_{organization_id}_"
        f"text_normindex{int(index_normalize)}_"
        f"{int(now.timestamp())}_{uuid.uuid4().hex}{ext}"
    )

    upload_path = UPLOAD_DIR / external_id

    try:
        upload_path.write_text(text, encoding="utf-8")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cannot save text: {e}")

    # sidecar meta — источник правды для индексации
    meta_path = UPLOAD_DIR / f"{external_id}.meta.json"
    meta = {
        "organization_id": int(organization_id),
        "title": title,
        "created_at": now.isoformat(),

        # 🔥 ТОЛЬКО ЭТО
        "index_normalize": bool(index_normalize),
    }

    try:
        meta_path.write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass

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
        index_normalize=index_normalize,
    )
