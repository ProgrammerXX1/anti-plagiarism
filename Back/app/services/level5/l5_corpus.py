from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import UPLOAD_DIR, CORPUS_JSONL
from app.core.logger import logger
from app.models.document import Document
from app.services.helpers.file_extract import extract_text_from_file_bytes


def utcnow():
    return datetime.now(timezone.utc)


async def rebuild_l5_corpus(
    db: AsyncSession,
    only_doc_ids: Optional[List[int]] = None,
    corpus_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    Собирает corpus.jsonl для L5.

    Варианты:
      - only_doc_ids is None  → берём ВСЕ документы со статусом 'l5_uploaded'.
      - only_doc_ids задан    → берём только эти doc.id (фильтр по l5_uploaded тоже остаётся).

      - corpus_path is None   → пишем в глобальный CORPUS_JSONL.
      - corpus_path задан     → пишем в указанный путь.

    Формат строк:
      {"doc_id": "...", "text": "...", "title": "...", "author": "..."}
    """
    stmt = select(Document).where(Document.status == "l5_uploaded")
    if only_doc_ids:
        stmt = stmt.where(Document.id.in_(only_doc_ids))

    result = await db.execute(stmt)
    docs: List[Document] = list(result.scalars())

    if not docs:
        logger.info("[L5] нет документов для корпуса L5")
        return {"docs_total": 0, "corpus_docs": 0}

    logger.info(
        "[L5] документов для L5 корпуса (status='l5_uploaded'): %d (filtered=%s)",
        len(docs),
        bool(only_doc_ids),
    )

    out_path: Path = corpus_path if corpus_path is not None else CORPUS_JSONL
    out_path.parent.mkdir(parents=True, exist_ok=True)
    written = 0

    with out_path.open("w", encoding="utf-8") as f:
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

    logger.info(
        "[L5] corpus.jsonl собран, записано документов: %d в %s",
        written,
        out_path,
    )

    return {
        "docs_total": len(docs),
        "corpus_docs": written,
        "corpus_path": str(out_path),
    }
