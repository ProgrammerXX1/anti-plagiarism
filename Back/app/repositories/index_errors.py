from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.index_error import IndexError


async def log_index_error(
    db: AsyncSession,
    *,
    stage: str,                # etl, build_l1, compact, search
    message: str,
    doc_id: Optional[int] = None,
    segment_id: Optional[int] = None,
    error_code: Optional[str] = None,
    payload: Optional[dict[str, Any]] = None,
) -> IndexError:
    """
    Safe logger:
    - If DB schema is behind (no doc_id column), we still try to write without doc_id.
    """
    now = datetime.now(timezone.utc)

    # Some deployments may still have old index_errors schema without doc_id.
    # IndexError model might include doc_id, but DB column might be missing.
    # We'll attempt insert with doc_id, and if flush fails, retry without it.
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
    try:
        await db.flush()
        return err
    except Exception:
        # rollback pending err insert and retry without doc_id
        await db.rollback()

        err2 = IndexError(
            segment_id=segment_id,
            stage=stage,
            error_code=error_code,
            message=message,
            payload=payload,
            created_at=now,
            retry_count=0,
        )
        db.add(err2)
        await db.flush()
        return err2
