# app/repositories/index_tasks.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, func

from app.models.index_task import IndexTask


async def enqueue_task(
    db: AsyncSession,
    *,
    task_type: str,
    payload: Dict[str, Any],
) -> IndexTask:
    now = datetime.now(timezone.utc)
    task = IndexTask(
        task_type=task_type,
        status="pending",
        payload=payload,
        attempts=0,
        created_at=now,
    )
    db.add(task)
    await db.flush()
    return task


async def fetch_next_task_for_update(db: AsyncSession) -> Optional[IndexTask]:
    """
    Забираем одну pending-задачу с блокировкой.
    ВАЖНО: вызывать внутри ТРАНЗАКЦИИ.
    """
    stmt = (
        select(IndexTask)
        .where(IndexTask.status == "pending")
        .order_by(IndexTask.created_at)
        .with_for_update(skip_locked=True)
        .limit(1)
    )
    res = await db.execute(stmt)
    task = res.scalar_one_or_none()
    return task


async def mark_task_started(db: AsyncSession, task_id: int) -> None:
    now = datetime.now(timezone.utc)
    stmt = (
        update(IndexTask)
        .where(IndexTask.id == task_id)
        .values(
            status="running",
            started_at=now,
            attempts=IndexTask.attempts + 1,
            last_error=None,
        )
    )
    await db.execute(stmt)


async def mark_task_done(db: AsyncSession, task_id: int) -> None:
    now = datetime.now(timezone.utc)
    stmt = (
        update(IndexTask)
        .where(IndexTask.id == task_id)
        .values(
            status="done",
            finished_at=now,
        )
    )
    await db.execute(stmt)


async def mark_task_failed(
    db: AsyncSession,
    task_id: int,
    error_message: str,
) -> None:
    now = datetime.now(timezone.utc)
    stmt = (
        update(IndexTask)
        .where(IndexTask.id == task_id)
        .values(
            status="failed",
            finished_at=now,
            last_error=error_message[:2000],
        )
    )
    await db.execute(stmt)
