# app/workers/index_worker.py
from __future__ import annotations

import asyncio
import json
import traceback
from typing import Any, Dict

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import AsyncSessionLocal as async_session

from app.repositories.index_tasks import (
    fetch_next_task_for_update,
    mark_task_started,
    mark_task_done,
    mark_task_failed,
)
from app.repositories.documents import get_document, set_document_status
from app.repositories.segments import list_ready_segments_for_shard
from app.core.logger import logger


# ───────────────────────────────────────────────────────────────
# Заглушки под реальные операции
# ───────────────────────────────────────────────────────────────

async def run_etl_for_doc(db: AsyncSession, doc_id: int) -> None:
    """
    Здесь делаешь:
    - достать документ
    - прочитать файл из FS
    - извлечь текст, нормализовать, шинглы
    - сохранить в corpus.jsonl / shard_dir
    - documents.status = 'etl_ok'
    """
    doc = await get_document(db, doc_id)
    if not doc:
        raise RuntimeError(f"doc {doc_id} not found")

    # TODO: твоя реальная логика ETL
    # ...

    await set_document_status(db, doc_id, status="etl_ok")


async def run_build_index_for_shard(db: AsyncSession, shard_id: int) -> None:
    """
    Тут вызываешь C++ index_builder для шарда:
    - взять новые doc'и в etl_ok для shard_id
    - построить/обновить L0/L1 сегмент
    - обновить segments + segment_docs
    """
    # TODO: твоя реальная логика
    logger.info(f"BUILD_INDEX for shard={shard_id}")
    # пример: просто заглушка
    # segments = await list_ready_segments_for_shard(db, shard_id)
    # ...


# ───────────────────────────────────────────────────────────────
# Основной цикл воркера
# ───────────────────────────────────────────────────────────────

async def handle_task(db: AsyncSession, task) -> None:
    await mark_task_started(db, task.id)

    ttype = task.task_type
    payload: Dict[str, Any] = task.payload or {}

    if ttype == "etl_doc":
        doc_id = int(payload["doc_id"])
        await run_etl_for_doc(db, doc_id)
    elif ttype == "build_index_shard":
        shard_id = int(payload["shard_id"])
        await run_build_index_for_shard(db, shard_id)
    else:
        raise RuntimeError(f"Unknown task_type={ttype!r}")

    await mark_task_done(db, task.id)


async def worker_loop() -> None:
    logger.info("index_worker started")

    while True:
        try:
            async with async_session() as db:
                async with db.begin():  # транзакция для выбора задачи
                    task = await fetch_next_task_for_update(db)
                    if not task:
                        # нет задач — просто выйдем из транзакции
                        # и подождём
                        pass
                    else:
                        try:
                            await handle_task(db, task)
                        except Exception as e:
                            err = "".join(
                                traceback.format_exception(type(e), e, e.__traceback__)
                            )
                            await mark_task_failed(db, task.id, err)
                # commit транзакции
                await db.commit()

        except Exception as e:
            logger.error(f"worker_loop error: {e}", exc_info=True)

        # маленький sleep, чтобы не крутиться на 100% CPU
        await asyncio.sleep(1.0)


def main() -> None:
    asyncio.run(worker_loop())


if __name__ == "__main__":
    main()
