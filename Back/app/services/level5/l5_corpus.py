# app/services/level5/l5_corpus.py
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import UPLOAD_DIR, CORPUS_JSONL
from app.core.logger import logger
from app.models.document import Document
from app.services.helpers.file_extract import extract_text_from_file_bytes


def utcnow():
    return datetime.now(timezone.utc)


async def rebuild_l5_corpus(db: AsyncSession) -> Dict[str, Any]:
    """
    Собирает монолитный corpus.jsonl для L5.

    Сейчас берём ВСЕ документы из таблицы Document.
    При желании можешь сузить фильтр, например:
      - только определённый university/faculty;
      - только статус 'l5_uploaded' и т.п.
    """
    # TODO: при необходимости подправь фильтр под свои правила L5
    result = await db.execute(select(Document))
    docs: List[Document] = list(result.scalars())

    if not docs:
        logger.info("[L5] нет документов для корпуса L5")
        return {"docs_total": 0, "corpus_docs": 0}

    logger.info("[L5] документов для L5 корпуса: %d", len(docs))

    CORPUS_JSONL.parent.mkdir(parents=True, exist_ok=True)
    written = 0

    with CORPUS_JSONL.open("w", encoding="utf-8") as f:
        for doc in docs:
            if not doc.external_id:
                logger.warning("[L5] doc id=%s без external_id, пропускаю", doc.id)
                continue

            file_path: Path = UPLOAD_DIR / doc.external_id
            if not file_path.exists():
                logger.warning(
                    "[L5] файл не найден: %s, doc id=%s",
                    file_path,
                    doc.id,
                )
                continue

            try:
                raw_bytes = file_path.read_bytes()
            except Exception as e:
                logger.warning(
                    "[L5] не удалось прочитать %s: %s",
                    file_path,
                    e,
                )
                continue

            try:
                raw_text = extract_text_from_file_bytes(
                    raw_bytes,
                    filename=str(file_path),
                )
            except Exception as e:
                logger.warning(
                    "[L5] extract_text для %s: %s",
                    file_path,
                    e,
                )
                continue

            rec = {
                "doc_id": str(doc.id),
                "text": raw_text,
                "title": doc.title,
                "author": doc.student_name,
            }
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            written += 1

    logger.info("[L5] corpus.jsonl собран, записано документов: %d", written)

    return {
        "docs_total": len(docs),
        "corpus_docs": written,
    }
