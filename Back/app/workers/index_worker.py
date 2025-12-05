# app/workers/index_worker.py
import asyncio
from datetime import datetime, timezone
from typing import List, Sequence

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import AsyncSessionLocal
from app.models.document import Document
from app.models.segment import Segment
from app.models.segment_doc import SegmentDoc

# пороги и настройки
ETL_BATCH_SIZE = 16
SEGMENT_TARGET_DOCS = 20      # сколько доков в один сегмент (первый L1)
POLL_INTERVAL_SEC = 2.0       # задержка, если работы нет

# Пока без настоящего текста: заглушка для ETL
# Здесь позже подцепим extract_text_from_file_bytes / norm_for_local / mini_doc
async def run_etl_for_document(doc: Document) -> tuple[int, int]:
    """
    ВРЕМЕННО: фейковый ETL.
    Реальная версия должна:
      - прочитать файл по пути
      - извлечь текст
      - нормализовать текст
      - построить шинглы и simhash
      - положить данные либо в файловый корпус, либо в отдельную таблицу
    Возвращает (doc_length, shingle_count).
    """
    # просто чтобы что-то было, считаем длину псевдотекста
    fake_text = f"Dummy text for document {doc.id} external_id={doc.external_id or ''}"
    doc_length = len(fake_text.split())
    shingle_count = max(0, doc_length - 9 + 1)  # для k=9

    return doc_length, shingle_count


async def process_etl_batch(session: AsyncSession) -> int:
    """
    Берёт пачку документов в статусе 'uploaded',
    делает ETL и переводит в 'etl_ok' (или 'failed_etl').
    """
    now = datetime.now(timezone.utc)

    # SELECT ... FOR UPDATE SKIP LOCKED — чтобы несколько воркеров не дрались за одни и те же строки
    stmt = (
        select(Document)
        .where(Document.status == "uploaded")
        .order_by(Document.id)
        .limit(ETL_BATCH_SIZE)
        .with_for_update(skip_locked=True)
    )
    result = await session.scalars(stmt)
    docs: List[Document] = list(result)

    if not docs:
        return 0

    for doc in docs:
        try:
            doc_length, shingle_count = await run_etl_for_document(doc)

            # тут пока нигде не сохраняем длину/шинглы, только статус;
            # на следующем шаге можем добавить таблицу corpus_docs или JSONL-корпус.
            doc.status = "etl_ok"
            doc.updated_at = now

            # сюда можно будет положить simhash, когда его посчитаем:
            # doc.simhash_hi = ...
            # doc.simhash_lo = ...
        except Exception as e:
            # логирование можно добавить позже
            doc.status = "failed_etl"
            doc.updated_at = now

    await session.commit()
    return len(docs)


async def build_segment_for_shard(session: AsyncSession, shard_id: int) -> bool:
    """
    Собирает один L1-сегмент из документов shard_id,
    которые прошли ETL, но ещё не привязаны к сегменту.
    """
    now = datetime.now(timezone.utc)

    # Берём документы этого шарда в etl_ok без сегмента
    stmt_docs = (
        select(Document)
        .where(
            Document.shard_id == shard_id,
            Document.status == "etl_ok",
            Document.segment_id.is_(None),
        )
        .order_by(Document.id)
        .limit(SEGMENT_TARGET_DOCS)
        .with_for_update(skip_locked=True)
    )

    result = await session.scalars(stmt_docs)
    docs: List[Document] = list(result)

    if not docs:
        return False

    # Создаём Segment (L1)
    seg = Segment(
        shard_id=shard_id,
        level=1,
        status="ready",  # по факту уже собран
        path="",         # позже проставим, когда будет реальный путь к файлам
        doc_count=len(docs),
        shingle_count=0,  # пока не считаем, можно позже заполнить из ETL-данных
        size_bytes=0,     # позже: реальный размер файлов сегмента
        created_at=now,
        last_compacted_at=None,
        last_access_at=None,
    )
    session.add(seg)
    await session.flush()  # чтобы seg.id появился

    # Пример пути к сегменту — можно завязать на INDEX_DIR
    from app.core.config import INDEX_DIR

    seg_path = INDEX_DIR / f"shard_{shard_id}" / f"segment_{seg.id}"
    seg.path = str(seg_path)

    # Убедимся, что директория создана (экономичный вариант — mkdir по месту)
    seg_path.mkdir(parents=True, exist_ok=True)  # type: ignore[attr-defined]

    # Создаём связи segment_docs + обновляем документы
    for doc in docs:
        # пока ставим фиктивную длину/шинглы (0), позже сюда придут реальные данные из ETL
        seg_doc = SegmentDoc(
            shard_id=shard_id,
            segment_id=seg.id,
            document_id=doc.id,
            doc_length=0,
            shingle_count=0,
            created_at=now,
        )
        session.add(seg_doc)

        doc.segment_id = seg.id
        doc.status = "indexed"
        doc.updated_at = now

    await session.commit()
    return True


async def compact_segments(session: AsyncSession) -> None:
    """
    Заглушка под компакцию L1→L2 и выше.
    Сейчас просто оставляем, чтобы не мешать пайплайну.
    """
    return


async def main_loop() -> None:
    while True:
        async with AsyncSessionLocal() as session:
            # 1) ETL uploaded → etl_ok
            processed = await process_etl_batch(session)

            # 2) Попытка собрать сегмент для shard_id=0 (пока без настоящего шардинга)
            built = await build_segment_for_shard(session, shard_id=0)

            # 3) Компакция (пока пустая)
            if built:
                await compact_segments(session)

        if processed == 0 and not built:
            await asyncio.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    asyncio.run(main_loop())
