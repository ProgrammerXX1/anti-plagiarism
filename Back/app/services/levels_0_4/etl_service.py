from __future__ import annotations

from datetime import datetime, timezone
from typing import List

from sqlalchemy import select

from app.core.config import UPLOAD_DIR, ETL_BATCH_SIZE
from app.db.session import AsyncSessionLocal
from app.models.document import Document


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _upload_text_path_best_effort(doc: Document):
    # new layout: {doc.id}.txt
    p1 = UPLOAD_DIR / f"{int(doc.id)}.txt"
    if p1.exists():
        return p1
    # legacy: external_id as file-key
    if getattr(doc, "external_id", None):
        p2 = UPLOAD_DIR / str(doc.external_id)
        if p2.exists():
            return p2
    return p1  # default


async def process_uploaded_docs() -> int:
    """
    L0: uploaded -> etl_ok.

    Важно:
    - SELECT ... FOR UPDATE SKIP LOCKED должен выполняться внутри транзакции.
    - Здесь нет тяжелого ETL — только проверка наличия файла и перевод статуса.
    """
    async with AsyncSessionLocal() as session:
        async with session.begin():
            result = await session.execute(
                select(Document)
                .where(Document.status == "uploaded")
                .order_by(Document.id)
                .limit(ETL_BATCH_SIZE)
                .with_for_update(skip_locked=True)
            )
            docs: List[Document] = list(result.scalars())

            if not docs:
                return 0

            now = utcnow()
            processed = 0

            for doc in docs:
                # doc.id всегда есть, external_id может быть внешний document_id
                file_path = _upload_text_path_best_effort(doc)
                if not file_path.exists():
                    # файл не найден — оставляем uploaded (может приехать позже)
                    continue

                doc.status = "etl_ok"
                doc.updated_at = now
                processed += 1

            return processed
