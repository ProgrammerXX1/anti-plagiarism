# app/scripts/enqueue_test_tasks.py
import asyncio
import os
from datetime import datetime, timezone
from typing import Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import AsyncSessionLocal
from app.models.document import Document
from app.models.index_task import IndexTask

# порог: сколько файлов превращаем в один L1-сегмент
DOCS_PER_L1_SEGMENT = int(os.getenv("PLAGIO_DOCS_PER_L1", "5"))
# порог: сколько сегментов уровня L превращаем в один сегмент уровня L+1
SEGMENTS_PER_COMPACT = int(os.getenv("PLAGIO_SEGMENTS_PER_COMPACT", "5"))
# максимальный уровень (0 — сырые файлы, 1..MAX_LEVEL — сегменты)
MAX_LEVEL = int(os.getenv("PLAGIO_MAX_LEVEL", "4"))


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ─────────────────────────────────────
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ─────────────────────────────────────

async def create_test_documents(session: AsyncSession) -> None:
    """
    Примитивный сидер: создаёт один тестовый документ, если в базе пусто.
    Можно не использовать в проде.
    """
    res = await session.execute(select(Document).limit(1))
    any_doc = res.scalar_one_or_none()
    if any_doc is not None:
        return

    now = utcnow()
    doc = Document(
        external_id="test-doc-1.pdf",  # ожидается файл UPLOAD_DIR / "test-doc-1.pdf"
        shard_id=0,
        status="uploaded",
        title="Test doc",
        created_at=now,
        updated_at=now,
    )
    session.add(doc)
    await session.commit()
    print("[enqueue] создан тестовый документ id=", doc.id)


async def list_unsegmented_docs_for_shard(
    session: AsyncSession,
    shard_id: int,
    limit: int,
) -> Sequence[Document]:
    """
    L0: документы, которые ещё не попали в сегменты.
    Берём только status='uploaded' (ETL/индексация ещё не ехали).
    """
    res = await session.execute(
        select(Document)
        .where(
            Document.shard_id == shard_id,
            Document.segment_id.is_(None),
            Document.status == "uploaded",
        )
        .order_by(Document.id)
        .limit(limit)
    )
    return list(res.scalars().all())


async def enqueue_build_l1_segment(
    session: AsyncSession,
    shard_id: int,
    doc_ids: list[int],
) -> None:
    """
    Кладёт задачу build_l1_segment (L0 → L1).
    """
    now = utcnow()
    task = IndexTask(
        task_type="build_l1_segment",
        status="pending",
        shard_id=shard_id,
        payload={
            "level": 1,
            "shard_id": shard_id,
            "doc_ids": doc_ids,
        },
        created_at=now,
        updated_at=now,
    )
    session.add(task)
    await session.commit()
    print(
        f"[enqueue] build_l1_segment: task_id={task.id}, "
        f"shard={shard_id}, docs={len(doc_ids)}"
    )


async def pick_ready_segments_for_level(
    session: AsyncSession,
    shard_id: int,
    level: int,
    limit: int,
):
    """
    Берём сегменты shard_id/level со статусом 'ready'.
    Используется для компактирования уровней.
    """
    from app.models.segment import Segment

    res = await session.execute(
        select(Segment)
        .where(
            Segment.shard_id == shard_id,
            Segment.level == level,
            Segment.status == "ready",
        )
        .order_by(Segment.id)
        .limit(limit)
    )
    return list(res.scalars().all())


async def enqueue_compact_level(
    session: AsyncSession,
    shard_id: int,
    from_level: int,
    to_level: int,
    segment_ids: list[int],
) -> None:
    """
    Кладёт задачу compact_level: L=from_level -> L=to_level.
    """
    now = utcnow()
    task = IndexTask(
        task_type="compact_level",
        status="pending",
        shard_id=shard_id,
        payload={
            "shard_id": shard_id,
            "from_level": from_level,
            "to_level": to_level,
            "segment_ids": segment_ids,
        },
        created_at=now,
        updated_at=now,
    )
    session.add(task)
    await session.commit()
    print(
        f"[enqueue] compact_level: task_id={task.id}, shard={shard_id}, "
        f"{from_level} -> {to_level}, segments={segment_ids}"
    )


# ─────────────────────────────────────
# РЕАЛТАЙМ-ПЛАНИРОВЩИК
# ─────────────────────────────────────

async def plan_l1_for_shard(session: AsyncSession, shard_id: int) -> None:
    """
    L0 → L1: если в шарде набралось хотя бы DOCS_PER_L1_SEGMENT незасегментированных
    документов — создаём одну задачу build_l1_segment.
    """
    docs = await list_unsegmented_docs_for_shard(
        session, shard_id=shard_id, limit=DOCS_PER_L1_SEGMENT
    )
    if len(docs) < DOCS_PER_L1_SEGMENT:
        # мало документов — подождём следующих загрузок
        print(
            f"[enqueue] shard={shard_id}: "
            f"документов {len(docs)}/{DOCS_PER_L1_SEGMENT}, пока рано для L1"
        )
        return

    doc_ids = [d.id for d in docs]
    await enqueue_build_l1_segment(session, shard_id=shard_id, doc_ids=doc_ids)


async def plan_compact_for_shard(session: AsyncSession, shard_id: int) -> None:
    """
    L1..L(N-1) -> L2..LN:
    для каждого уровня L берём до SEGMENTS_PER_COMPACT готовых сегментов
    и (если порог достигнут) создаём одну задачу compact_level(L -> L+1).
    """
    for from_level in range(1, MAX_LEVEL):
        to_level = from_level + 1
        segs = await pick_ready_segments_for_level(
            session,
            shard_id=shard_id,
            level=from_level,
            limit=SEGMENTS_PER_COMPACT,
        )
        if len(segs) < SEGMENTS_PER_COMPACT:
            continue

        segment_ids = [s.id for s in segs]
        await enqueue_compact_level(
            session=session,
            shard_id=shard_id,
            from_level=from_level,
            to_level=to_level,
            segment_ids=segment_ids,
        )


async def on_document_uploaded(
    session: AsyncSession,
    shard_id: int,
) -> None:
    """
    Главная точка входа для реального времени.

    Вызываешь её из эндпоинта/сервиса сразу после того,
    как создал Document(status='uploaded').

    Логика:
      1) попытаться собрать L1-сегмент (если ≥5 файлов);
      2) попытаться продвинуть готовые сегменты на более высокие уровни.
    """
    await plan_l1_for_shard(session, shard_id)
    await plan_compact_for_shard(session, shard_id)


# ─────────────────────────────────────
# CLI: однократный запуск для отладки
# ─────────────────────────────────────

async def main() -> None:
    async with AsyncSessionLocal() as session:
        await create_test_documents(session)
        await on_document_uploaded(session, shard_id=0)


if __name__ == "__main__":
    asyncio.run(main())
