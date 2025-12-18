# app/repositories/index_errors.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from app.core.logger import logger
from app.db.session import AsyncSessionLocal
from app.models.index_error import IndexError


async def log_index_error(
    db,  # kept for backward compatibility; NOT used for writes anymore
    *,
    stage: str,                # etl, build_l1, compact, search
    message: str,
    doc_id: Optional[int] = None,
    segment_id: Optional[int] = None,
    error_code: Optional[str] = None,
    payload: Optional[dict[str, Any]] = None,
) -> Optional[IndexError]:
    """
    BOOTSTRAP-SAFE + TX-SAFE error logger.

    Why:
      The old implementation used `await db.rollback()` on flush failure,
      which can rollback the caller's transaction (build/compact), causing data loss.

    New behavior:
      - Always writes in a separate session/transaction (AsyncSessionLocal)
      - Never touches caller's transaction
      - Best-effort insert: if schema is behind (e.g., missing doc_id column),
        retry without doc_id, without using rollback on caller.

    Returns:
      Created IndexError object (detached from caller), or None if logging failed.
    """
    now = datetime.now(timezone.utc)

    # keep small, avoid huge rows
    safe_payload = payload
    try:
        # prevent pathological payload size (optional; safe default)
        if safe_payload is not None and len(str(safe_payload)) > 200_000:
            safe_payload = {"_truncated": True}
    except Exception:
        safe_payload = {"_truncated": True}

    async with AsyncSessionLocal() as session:
        try:
            err = IndexError(
                doc_id=doc_id,
                segment_id=segment_id,
                stage=stage,
                error_code=error_code,
                message=message,
                payload=safe_payload,
                created_at=now,
                retry_count=0,
            )
            session.add(err)
            await session.flush()
            await session.commit()
            return err
        except Exception as e1:
            # If schema is behind (e.g., doc_id column missing) or any insert issue,
            # retry without doc_id. IMPORTANT: this rollback affects ONLY this local session.
            try:
                await session.rollback()
            except Exception:
                pass

            try:
                err2 = IndexError(
                    segment_id=segment_id,
                    stage=stage,
                    error_code=error_code,
                    message=message,
                    payload=safe_payload,
                    created_at=now,
                    retry_count=0,
                )
                session.add(err2)
                await session.flush()
                await session.commit()
                return err2
            except Exception as e2:
                try:
                    await session.rollback()
                except Exception:
                    pass

                logger.error(
                    "[log_index_error] failed: stage=%s doc_id=%s segment_id=%s e1=%s e2=%s",
                    stage,
                    doc_id,
                    segment_id,
                    str(e1),
                    str(e2),
                )
                return None
