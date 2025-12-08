# app/workers/index_worker.py
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from sqlalchemy import select

from app.core.config import INDEX_DIR, UPLOAD_DIR, CORPUS_JSONL
from app.db.session import AsyncSessionLocal
from app.models.document import Document
from app.models.segment import Segment
from app.models.segment_doc import SegmentDoc
from app.services.helpers.file_extract import extract_text_from_file_bytes, norm_for_local
from app.core.config import DOCS_PER_L1_SEGMENT as DOCS_PER_L1_SEGMENT
# сколько документов в одном L1-сегменте
# DOCS_PER_L1_SEGMENT = 5
BATCH_SIZE_ETL = 100


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def run_index_builder(corpus: Path, out_dir: Path) -> bool:
    """
    Асинхронный запуск C++ бинарника index_builder.
    Возвращает True/False по коду выхода.
    """
    print(f"[worker] run_index_builder: corpus={corpus}, out_dir={out_dir}")

    proc = await asyncio.create_subprocess_exec(
        "index_builder",
        str(corpus),
        str(out_dir),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    stdout, stderr = await proc.communicate()

    if stdout:
        print("[index_builder][stdout]")
        print(stdout.decode("utf-8", errors="ignore"))

    if stderr:
        print("[index_builder][stderr]")
        print(stderr.decode("utf-8", errors="ignore"))

    if proc.returncode != 0:
        print(f"[worker] index_builder FAILED, returncode={proc.returncode}")
        return False

    print("[worker] index_builder OK")
    return True


async def process_uploaded_docs() -> int:
    """
    uploaded -> etl_ok:
      - читаем файл из UPLOAD_DIR
      - extract_text + norm_for_local
      - пишем в общий CORPUS_JSONL
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

        print(f"[ETL] Обрабатываю {len(docs)} документ(ов)")

        CORPUS_JSONL.parent.mkdir(parents=True, exist_ok=True)
        with CORPUS_JSONL.open("a", encoding="utf-8") as f:
            for doc in docs:
                if not doc.external_id:
                    print(f"[ETL] WARNING: doc id={doc.id} без external_id, пропускаю")
                    continue

                file_path = UPLOAD_DIR / doc.external_id
                if not file_path.exists():
                    print(f"[ETL] WARNING: файл не найден: {file_path}, пропускаю doc id={doc.id}")
                    continue

                try:
                    raw_bytes = file_path.read_bytes()
                except Exception as e:
                    print(f"[ETL] ERROR: не удалось прочитать {file_path}: {e}")
                    continue

                try:
                    raw_text = extract_text_from_file_bytes(raw_bytes, filename=str(file_path))
                except Exception as e:
                    print(f"[ETL] ERROR: extract_text для {file_path}: {e}")
                    continue

                norm_text = norm_for_local(raw_text)

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


async def build_l1_segments() -> int:
    """
    Создаём L1-сегменты по шард-группам:
      - для каждого shard_id берём etl_ok + segment_id is NULL
      - пачками по DOCS_PER_L1_SEGMENT строим L1-сегменты.
    Возвращает количество обработанных документов.
    """
    async with AsyncSessionLocal() as session:
        # сначала узнаём, по каким shard_id есть etl_ok без segment_id
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
            print("[SEGMENT] Нет документов etl_ok без segment_id — сегменты не нужны")
            return 0

        total_docs_processed = 0

        for shard_id in shard_ids:
            # берём пачку по этому шарду
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

            # создаём Segment-запись
            segment = Segment(
                shard_id=shard_id,
                level=1,
                status="building",
                path="",  # обновим позже
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
            print(f"[SEGMENT] corpus_jsonl={CORPUS_JSONL}")

            # вызываем C++ index_builder
            ok = await run_index_builder(CORPUS_JSONL, segment_dir)
            if not ok:
                segment.status = "error"
                await session.commit()
                print(f"[SEGMENT] ERROR: index_builder упал, segment_id={segment.id}")
                continue

            # проверяем файлы
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

            await session.commit()
            print(
                f"[SEGMENT] Готов L1-segment id={segment.id}, shard={shard_id}, "
                f"docs={len(docs)}, bytes={size_bytes}"
            )

            total_docs_processed += len(docs)

        return total_docs_processed


async def main_loop() -> None:
    print("[worker] index_worker стартовал")
    while True:
        try:
            etl_cnt = await process_uploaded_docs()
            seg_cnt = await build_l1_segments()
            print(f"[worker] tick: etl={etl_cnt}, seg_docs={seg_cnt}")
        except Exception as e:
            print(f"[worker] ERROR в main_loop: {e}")
        # чтобы не молотить по БД без паузы
        await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main_loop())
