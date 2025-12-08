from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from sqlalchemy import select

from app.core.config import INDEX_DIR, UPLOAD_DIR, DOCS_PER_L1_SEGMENT
from app.db.session import AsyncSessionLocal
from app.models.document import Document
from app.models.segment import Segment
from app.models.segment_doc import SegmentDoc
from app.services.helpers.file_extract import extract_text_from_file_bytes

# сколько документов за раз переводим uploaded -> etl_ok
BATCH_SIZE_ETL = 100


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ───────────────────────── etl_index_builder (C++) ───────────────────────── #

async def run_etl_index_builder(corpus: Path, out_dir: Path) -> bool:
    """
    Асинхронный запуск C++ бинарника etl_index_builder.

    Ожидания по формату corpus (JSONL):
      {"doc_id": "<строковый id>", "text": "<сырой текст документа>"}

    На выходе в out_dir должны появиться:
      - index_native.bin
      - index_native_docids.json
      - index_native_meta.json
    """
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


# ───────────────────────── Уровень 0: uploaded -> etl_ok ─────────────────── #

async def process_uploaded_docs() -> int:
    """
    Уровень 0: uploaded -> etl_ok.

    Здесь мы НЕ делаем тяжёлый индексный ETL — только проверяем,
    что исходный файл существует в UPLOAD_DIR, и помечаем документ
    как готовый к индексации (etl_ok).
    """
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Document)
            .where(Document.status == "uploaded")
            .limit(BATCH_SIZE_ETL)
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


# ────────────────────── Уровень 1: build L1 segments ─────────────────────── #

async def build_l1_segments() -> int:
    """
    Создание L1-сегментов (уровень 1) из документов уровня 0.

    Логика:
      - выбираем все shard_id, где есть etl_ok + segment_id IS NULL;
      - по каждому shard_id берём пачки по DOCS_PER_L1_SEGMENT документов;
      - для каждой пачки:
          * создаём Segment(level=1, status='building');
          * создаём директорию сегмента: INDEX_DIR / shard_0/segment_<id>;
          * собираем локальный segment_corpus.jsonl;
          * вызываем C++ etl_index_builder(corpus, segment_dir);
          * проверяем index_native.*;
          * помечаем Segment как ready;
          * у документов прописываем segment_id и status='indexed';
          * создаём записи SegmentDoc.

    Возвращает количество документов, попавших в L1-сегменты.
    """
    async with AsyncSessionLocal() as session:
        # Находим shard_id, где есть etl_ok без segment_id
        result = await session.execute(
            select(Document.shard_id)
            .where(
                Document.status == "etl_ok",
                Document.segment_id.is_(None),
            )
            .distinct()
        )
        shard_ids = [row[0] for row in result.fetchall()]

        if not shard_ids:
            print("[SEGMENT] Нет документов etl_ok без segment_id — L1-сегменты не нужны")
            return 0

        total_docs_processed = 0

        for shard_id in shard_ids:
            # Берём пачку документов по этому shard_id
            seg_result = await session.execute(
                select(Document)
                .where(
                    Document.status == "etl_ok",
                    Document.segment_id.is_(None),
                    Document.shard_id == shard_id,
                )
                .order_by(Document.id)
                .limit(DOCS_PER_L1_SEGMENT)
            )
            docs: List[Document] = list(seg_result.scalars())

            if len(docs) < DOCS_PER_L1_SEGMENT:
                print(
                    f"[SEGMENT] shard={shard_id}: найдено {len(docs)} etl_ok, "
                    f"меньше порога {DOCS_PER_L1_SEGMENT} — ждём"
                )
                continue

            now = utcnow()

            # Создаём запись Segment для уровня 1
            segment = Segment(
                shard_id=shard_id,
                level=1,
                status="building",
                path="",  # обновим после того как узнаем segment.id
                doc_count=len(docs),
                shingle_count=0,
                size_bytes=0,
                created_at=now,
                last_compacted_at=None,
                last_access_at=None,
            )
            session.add(segment)
            await session.flush()  # получаем segment.id

            segment_rel_path = f"shard_{shard_id}/segment_{segment.id}"
            segment_dir: Path = INDEX_DIR / segment_rel_path
            segment_dir.mkdir(parents=True, exist_ok=True)
            segment.path = segment_rel_path

            print(
                f"[SEGMENT] shard={shard_id}: строю L1-segment id={segment.id}, "
                f"docs={len(docs)}, dir={segment_dir}"
            )

            # Готовим локальный corpus JSONL для этого сегмента
            seg_corpus_path = segment_dir / "segment_corpus.jsonl"

            written_docs = 0
            with seg_corpus_path.open("w", encoding="utf-8") as f:
                for doc in docs:
                    if not doc.external_id:
                        print(f"[SEGMENT] WARNING: doc id={doc.id} без external_id, пропускаю")
                        continue

                    file_path = UPLOAD_DIR / doc.external_id
                    if not file_path.exists():
                        print(
                            f"[SEGMENT] WARNING: файл не найден: {file_path}, "
                            f"пропускаю doc id={doc.id}"
                        )
                        continue

                    try:
                        raw_bytes = file_path.read_bytes()
                    except Exception as e:
                        print(f"[SEGMENT] ERROR: не удалось прочитать {file_path}: {e}")
                        continue

                    try:
                        raw_text = extract_text_from_file_bytes(
                            raw_bytes,
                            filename=str(file_path),
                        )
                    except Exception as e:
                        print(f"[SEGMENT] ERROR: extract_text для {file_path}: {e}")
                        continue

                    # ВАЖНО: формат должен соответствовать ожиданиям etl_index_builder:
                    #   doc_id — строка
                    #   text   — сырой текст (нормализация/шинглы делаются в C++).
                    rec = {
                        "doc_id": str(doc.id),
                        "text": raw_text,
                    }
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    written_docs += 1

            if written_docs == 0:
                print(
                    f"[SEGMENT] shard={shard_id}, segment_id={segment.id}: "
                    "в segment_corpus.jsonl не попало ни одного документа, помечаем сегмент как error"
                )
                segment.status = "error"
                await session.commit()
                continue

            # Запускаем C++-индексатор для этого сегмента
            ok = await run_etl_index_builder(seg_corpus_path, segment_dir)
            if not ok:
                segment.status = "error"
                await session.commit()
                print(f"[SEGMENT] ERROR: etl_index_builder упал, segment_id={segment.id}")
                continue

            # Проверяем, что index_native.* создались
            bin_path = segment_dir / "index_native.bin"
            docids_path = segment_dir / "index_native_docids.json"
            meta_path = segment_dir / "index_native_meta.json"

            if not bin_path.exists() or not docids_path.exists():
                print(
                    f"[SEGMENT] ERROR: index_native.* не создался, "
                    f"segment_id={segment.id}"
                )
                segment.status = "error"
                await session.commit()
                continue

            size_bytes = 0
            for p in (bin_path, docids_path, meta_path):
                if p.exists():
                    size_bytes += p.stat().st_size

            segment.size_bytes = size_bytes
            segment.status = "ready"

            # Связи документ ↔ сегмент (SegmentDoc) и статусы документов
            updated_docs = 0
            for doc in docs:
                # если по какой-то причине doc уже куда-то привязан — не трогаем
                if doc.segment_id is not None:
                    continue

                doc.segment_id = segment.id
                doc.status = "indexed"
                doc.updated_at = now

                sd = SegmentDoc(
                    segment_id=segment.id,
                    document_id=doc.id,
                    shard_id=doc.shard_id,
                )
                session.add(sd)
                updated_docs += 1

            await session.commit()
            print(
                f"[SEGMENT] Готов L1-segment id={segment.id}, shard={shard_id}, "
                f"docs={updated_docs}, bytes={size_bytes}"
            )

            total_docs_processed += updated_docs

        return total_docs_processed


# ───────────────────────────── main loop ──────────────────────────────────── #

async def main_loop() -> None:
    print("[worker] index_worker стартовал")
    while True:
        try:
            etl_cnt = await process_uploaded_docs()
            seg_cnt = await build_l1_segments()
            print(f"[worker] tick: etl={etl_cnt}, seg_docs={seg_cnt}")
        except Exception as e:
            print(f"[worker] ERROR в main_loop: {e}")

        # если ничего не делали — отдыхаем
        if etl_cnt == 0 and seg_cnt == 0:
            await asyncio.sleep(5)
        else:
            # при активной работе пауза минимальная
            await asyncio.sleep(0.1)


if __name__ == "__main__":
    asyncio.run(main_loop())
