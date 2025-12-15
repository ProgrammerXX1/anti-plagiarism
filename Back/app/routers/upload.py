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
    return 0


class TextUploadResponse(BaseModel):
    doc_id: int
    status: str
    shard_id: int
    external_id: str
    organization_id: int
    title: str
    text_is_normalized: bool


@router.post("/upload-text", response_model=TextUploadResponse)
async def upload_text(
    text: str = Body(..., media_type="text/plain"),
    organization_id: int = Query(...),
    title: str = Query("text_upload"),
    for_level5: bool = Query(False),

    # NEW: если true — считаем что текст уже нормализован (не менять ни при индексации, ни при excerpt)
    text_is_normalized: bool = Query(False),

    db: AsyncSession = Depends(get_db),
):
    _validate_org_id(organization_id)

    if text is None or not text.strip():
        raise HTTPException(status_code=400, detail="Empty text")

    now = utcnow()
    status = "l5_uploaded" if for_level5 else "uploaded"
    shard_id = compute_shard_id_for_text(organization_id, N_SHARDS)

    ext = _safe_ext(title)
    norm_tag = "norm1" if text_is_normalized else "norm0"
    external_id = f"org_{organization_id}_text_{norm_tag}_{int(now.timestamp())}_{uuid.uuid4().hex}{ext}"
    upload_path = UPLOAD_DIR / external_id

    try:
        upload_path.write_text(text, encoding="utf-8")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cannot save text: {e}")

    # sidecar meta: воркер будет читать и решать нормализовать ли при индексации
    meta_path = UPLOAD_DIR / f"{external_id}.meta.json"
    try:
        meta_path.write_text(
            json.dumps(
                {
                    "organization_id": int(organization_id),
                    "text_is_normalized": bool(text_is_normalized),
                    "title": title,
                    "created_at": now.isoformat(),
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
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
        text_is_normalized=bool(text_is_normalized),
    )
