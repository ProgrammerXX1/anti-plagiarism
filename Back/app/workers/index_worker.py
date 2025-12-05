# app/workers/index_worker.py
"""
Простой воркер индексации:

1) Берёт документы со статусом 'uploaded'
   → пишет заглушки в corpus.jsonl
   → статус 'etl_ok'.

2) Берёт документы со статусом 'etl_ok' и без segment_id
   → создаёт один тестовый сегмент (level=1, status='ready')
   → создаёт связи в segment_docs
   → статус документов 'indexed'.
"""
from app.core.config import INDEX_DIR, UPLOAD_DIR, CORPUS_JSONL
from app.services.helpers.file_extract import extract_text_from_file_bytes, norm_for_local

import asyncio
import json
from datetime import datetime, timezone
from typing import List

from sqlalchemy import select

from app.db.session import AsyncSessionLocal
from app.models.document import Document
from app.models.segment import Segment
from app.models.segment_doc import SegmentDoc
from app.core.config import CORPUS_JSONL


BATCH_SIZE = 100


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def process_uploaded_docs() -> int:
    """
    uploaded -> etl_ok + реальный ETL:
      - читаем файл из UPLOAD_DIR
      - вытаскиваем текст
      - нормализуем
      - пишем в corpus.jsonl
    """
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Document)
            .where(Document.status == "uploaded")
            .limit(BATCH_SIZE)
        )
        docs: List[Document] = list(result.scalars())

        if not docs:
            print("[ETL] Нет документов со статусом 'uploaded'")
            return 0

        print(f"[ETL] Обрабатываю {len(docs)} документ(ов)")

        with CORPUS_JSONL.open("a", encoding="utf-8") as f:
            for doc in docs:
                if not doc.external_id:
                    print(f"[ETL] WARNING: doc id={doc.id} без external_id, пропускаю")
                    continue

                file_path = UPLOAD_DIR / doc.external_id

                if not file_path.exists():
                    print(f"[ETL] WARNING: файл не найден: {file_path}, пропускаю doc id={doc.id}")
                    continue

                # читаем байты файла
                try:
                    raw_bytes = file_path.read_bytes()
                except Exception as e:
                    print(f"[ETL] ERROR: не удалось прочитать {file_path}: {e}")
                    continue

                # вытаскиваем текст (PDF/DOCX и т.п.)
                try:
                    raw_text = extract_text_from_file_bytes(raw_bytes, filename=str(file_path))
                except Exception as e:
                    print(f"[ETL] ERROR: extract_text для {file_path}: {e}")
                    continue

                # нормализация под шинглы (та же, что для онлайновой проверки)
                norm_text = norm_for_local(raw_text)

                # пишем в corpus.jsonl
                rec = {
                    "doc_id": doc.id,
                    "external_id": doc.external_id,
                    "shard_id": doc.shard_id,
                    "text": norm_text,
                }
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")

                doc.status = "etl_ok"
                doc.updated_at = utcnow()

        await session.commit()
        print(f"[ETL] Переведено в etl_ok: {len(docs)}")
        return len(docs)


async def build_first_segment() -> int:
    """
    etl_ok -> indexed + создание одного тестового сегмента (L1).
    """
    async with AsyncSessionLocal() as session:
        # берём все etl_ok без segment_id
        result = await session.execute(
            select(Document)
            .where(
                Document.status == "etl_ok",
                Document.segment_id.is_(None),
            )
            .limit(BATCH_SIZE)
        )
        docs: List[Document] = list(result.scalars())

        if not docs:
            print("[SEGMENT] Нет документов для сегмента (etl_ok без segment_id)")
            return 0

        # для простоты сейчас считаем, что все документы одного shard_id
        shard_id = docs[0].shard_id
        if any(d.shard_id != shard_id for d in docs):
            print("[SEGMENT] В выборке разные shard_id, пока не поддерживаем")
            return 0

        now = utcnow()

        # путь директории сегмента на диске
        segment_dir = INDEX_DIR / f"shard_{shard_id}" / f"segment_{shard_id}_{int(now.timestamp())}"
        segment_dir.mkdir(parents=True, exist_ok=True)

        # создаём сегмент (заглушка, path пока фиктивный)
        segment = Segment(
            shard_id=shard_id,
            level=1,
            status="ready",
            path=f"shard_{shard_id}/segment_1_dummy",
            doc_count=len(docs),
            shingle_count=0,
            size_bytes=0,
            created_at=now,
            last_compacted_at=None,
            last_access_at=None,
        )
        session.add(segment)
        await session.flush()  # чтобы получить segment.id

        print(f"[SEGMENT] Создан сегмент id={segment.id}, shard_id={shard_id}")

                # meta.json (пока минимальный)
        meta = {
            "segment_id": segment.id,
            "shard_id": shard_id,
            "level": 1,
            "doc_count": len(docs),
            "created_at": now.isoformat(),
            "documents": [d.id for d in docs],
        }
        meta_path = segment_dir / "meta.json"
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

        # заглушки под бинарники (пустые файлы)
        for fname in ("postings.bin", "lexicon.bin", "docs_meta.bin"):
            (segment_dir / fname).touch()

        # связи документ ↔ сегмент
        for doc in docs:
            doc.segment_id = segment.id
            doc.status = "indexed"
            doc.updated_at = now

            sd = SegmentDoc(
                segment_id=segment.id,
                document_id=doc.id,
                shard_id=doc.shard_id,
            )
            session.add(sd)

        # обновим size_bytes по факту
        size_bytes = sum(
            (segment_dir / fn).stat().st_size
            for fn in ("meta.json", "postings.bin", "lexicon.bin", "docs_meta.bin")
        )
        segment.size_bytes = size_bytes

        await session.commit()
        print(f"[SEGMENT] Документов привязано к сегменту: {len(docs)}")

        return len(docs)


async def main_once():
    print("=== index_worker: start single run ===")
    etl_cnt = await process_uploaded_docs()
    seg_cnt = await build_first_segment()
    print(f"=== index_worker: done (etl={etl_cnt}, segments_docs={seg_cnt}) ===")


if __name__ == "__main__":
    asyncio.run(main_once())
