# app/services/levels0_4/etl_service.py
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from sqlalchemy import select

from app.core.config import UPLOAD_DIR, ETL_BATCH_SIZE
from app.db.session import AsyncSessionLocal
from app.models.document import Document
from app.services.helpers.file_extract import extract_text_from_file_bytes


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def run_etl_index_builder(corpus: Path, out_dir: Path) -> bool:
    print(f"[worker] run_etl_index_builder: corpus={corpus}, out_dir={out_dir}")

    proc = await asyncio.create_subprocess_exec(
        "etl_index_builder",
        str(corpus),
        str(out_dir),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    stdout, stderr = await proc.communicate()

    if stdout:
        print("[etl_index_builder][stdout]")
        print(stdout.decode("utf-8", errors="ignore"))

    if stderr:
        print("[etl_index_builder][stderr]")
        print(stderr.decode("utf-8", errors="ignore"))

    if proc.returncode != 0:
        print(f"[worker] etl_index_builder FAILED, returncode={proc.returncode}")
        return False

    print("[worker] etl_index_builder OK")
    return True


async def process_uploaded_docs() -> int:
    """
    L0: uploaded -> etl_ok.
    В L0 лежат только неиндексированные документы.
    """
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Document)
            .where(Document.status == "uploaded")
            .limit(ETL_BATCH_SIZE)
        )
        docs: List[Document] = list(result.scalars())

        if not docs:
            print("[ETL] Нет документов со статусом 'uploaded'")
            return 0

        print(f"[ETL] Перевожу в etl_ok {len(docs)} документ(ов) (без тяжёлого ETL)")

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
