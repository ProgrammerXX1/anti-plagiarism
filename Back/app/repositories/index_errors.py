# app/repositories/index_errors.py
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy.exc import ProgrammingError, OperationalError, DBAPIError

from app.core.logger import logger
from app.db.session import AsyncSessionLocal
from app.models.index_error import IndexError as IndexErrorRow  # чтобы не путать с built-in IndexError


def _truncate_payload(payload: Optional[dict[str, Any]], max_bytes: int = 200_000) -> Optional[dict[str, Any]]:
    if payload is None:
        return None
    try:
        raw = json.dumps(payload, ensure_ascii=False, default=str)
        if len(raw.encode("utf-8")) <= max_bytes:
            return payload
        return {"_truncated": True, "_max_bytes": max_bytes}
    except Exception:
        return {"_truncated": True, "_max_bytes": max_bytes}


def _looks_like_missing_column(e: Exception, column_name: str) -> bool:
    msg = str(e).lower()
    # покрывает “column ... does not exist”, “unknown column”, и т.п.
    return (column_name.lower() in msg) and ("column" in msg or "does not exist" in msg or "unknown" in msg)


async def log_index_error(
    _db_unused,  # оставлено для совместимости по сигнатуре; не используем
    *,
    stage: str,                # etl, build_l1, compact, search
    message: str,
    doc_id: Optional[int] = None,
    segment_id: Optional[int] = None,
    error_code: Optional[str] = None,
    payload: Optional[dict[str, Any]] = None,
) -> Optional[IndexErrorRow]:
    """
    TX-SAFE logger:
      - пишет всегда в отдельной сессии/транзакции
      - не трогает транзакцию вызывающего кода
      - fallback (без doc_id) только если реально похоже на проблему схемы
    """
    now = datetime.now(timezone.utc)
    safe_payload = _truncate_payload(payload)

    async with AsyncSessionLocal() as session:
        # попытка №1 — обычная
        try:
            row = IndexErrorRow(
                doc_id=doc_id,
                segment_id=segment_id,
                stage=stage,
                error_code=error_code,
                message=message,
                payload=safe_payload,
                created_at=now,
                retry_count=0,
            )
            session.add(row)
            await session.flush()
            await session.commit()
            return row
        except Exception as e1:
            try:
                await session.rollback()
            except Exception:
                pass

            # fallback — только если похоже на “колонки нет”
            should_fallback = isinstance(e1, (ProgrammingError, OperationalError, DBAPIError)) and _looks_like_missing_column(e1, "doc_id")
            if not should_fallback:
                logger.error(
                    "[log_index_error] failed (no fallback): stage=%s doc_id=%s segment_id=%s err=%s",
                    stage, doc_id, segment_id, str(e1),
                )
                return None

            try:
                row2 = IndexErrorRow(
                    segment_id=segment_id,
                    stage=stage,
                    error_code=error_code,
                    message=message,
                    payload=safe_payload,
                    created_at=now,
                    retry_count=0,
                )
                session.add(row2)
                await session.flush()
                await session.commit()
                return row2
            except Exception as e2:
                try:
                    await session.rollback()
                except Exception:
                    pass

                logger.error(
                    "[log_index_error] failed (fallback): stage=%s doc_id=%s segment_id=%s e1=%s e2=%s",
                    stage, doc_id, segment_id, str(e1), str(e2),
                )
                return None
