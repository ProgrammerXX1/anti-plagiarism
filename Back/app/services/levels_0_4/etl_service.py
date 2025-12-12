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
    В L0 лежат только неиндексированные документы.
    Тут нет "тяжелого ETL" — просто переводим статус, проверяя файл.
    """
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Document)
            .where(Document.status == "uploaded")
            .order_by(Document.id)
            .limit(ETL_BATCH_SIZE)
            .with_for_update(skip_locked=True)
        )
        docs: List[Document] = list(result.scalars())

        if not docs:
            print("[ETL] Нет документов со статусом 'uploaded'")
            return 0

        now = utcnow()
        processed = 0

        for doc in docs:
            if not doc.external_id:
                print(f"[ETL] WARNING: doc id={doc.id} без external_id, пропускаю")
                continue

            file_path = UPLOAD_DIR / doc.external_id
            if not file_path.exists():
                print(f"[ETL] WARNING: файл не найден: {file_path}, пропускаю doc id={doc.id}")
                continue

            doc.status = "etl_ok"
            doc.updated_at = now
            processed += 1

        await session.commit()
        print(f"[ETL] Переведено в etl_ok: {processed}")
        return processed
