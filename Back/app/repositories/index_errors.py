# app/repositories/index_errors.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.index_error import IndexError


async def log_index_error(
    db: AsyncSession,
    *,
    stage: str,                # etl, build, compact, search
    message: str,
    doc_id: Optional[int] = None,
    segment_id: Optional[int] = None,
    error_code: Optional[str] = None,
    payload: Optional[dict[str, Any]] = None,
) -> IndexError:
    now = datetime.now(timezone.utc)
    err = IndexError(
        doc_id=doc_id,
        segment_id=segment_id,
        stage=stage,
        error_code=error_code,
        message=message,
        payload=payload,
        created_at=now,
        retry_count=0,
    )
    db.add(err)
    await db.flush()
    return err
