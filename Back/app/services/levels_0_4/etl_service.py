# app/services/levels0_4/etl_service.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import List

from sqlalchemy import select

from app.core.config import UPLOAD_DIR, ETL_BATCH_SIZE
from app.db.session import AsyncSessionLocal
from app.models.document import Document


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


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
                # без лишнего спама
                return 0

            now = utcnow()
            processed = 0

            for doc in docs:
                if not doc.external_id:
                    # doc без файла — лучше оставлять uploaded и логировать отдельно
                    continue

                file_path = UPLOAD_DIR / doc.external_id
                if not file_path.exists():
                    # файл не найден — оставляем uploaded (может приехать позже)
                    continue

                doc.status = "etl_ok"
                doc.updated_at = now
                processed += 1

            return processed
