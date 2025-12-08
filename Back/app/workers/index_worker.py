from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from sqlalchemy import select, delete, func

from app.core.config import (
    INDEX_DIR,
    UPLOAD_DIR,
    ETL_BATCH_SIZE,
    DOCS_PER_L1_SEGMENT,
    MAX_AUTO_LEVEL,
    segments_per_compact as cfg_segments_per_compact,
)
from app.db.session import AsyncSessionLocal
from app.models.document import Document
from app.models.segment import Segment
from app.models.segment_doc import SegmentDoc
from app.services.helpers.file_extract import extract_text_from_file_bytes


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ───────────────────────── Компакция уровней Lx -> Lx+1 ───────────────────── #

async def compact_segments_level(from_level: int) -> int:
    """
    Компакция сегментов: берём N сегментов уровня from_level
    и собираем их в один сегмент уровня from_level+1.

    Правила:
      - компактим по шардам отдельно;
      - всегда берём самые старые (по id) сегменты;
      - старые сегменты помечаем как status='merged';
      - документы перелинковываем на новый сегмент.
    """
    async with AsyncSessionLocal() as session:
        to_level = from_level + 1
        per_compact = cfg_segments_per_compact(from_level)

        # какие шарды имеют достаточно сегментов для компакции
        shard_rows = await session.execute(
            select(Segment.shard_id)
            .where(
                Segment.level == from_level,
                Segment.status == "ready",
            )
            .group_by(Segment.shard_id)
            .having(func.count(Segment.id) >= per_compact)
        )
        shard_ids = [row[0] for row in shard_rows.fetchall()]

        if not shard_ids:
            print(
                f"[COMPACT L{from_level}->L{to_level}] "
                f"нет шардов с достаточным числом сегментов (>= {per_compact})"
            )
            return 0

        total_docs_promoted = 0

        for shard_id in shard_ids:
            while True:
                # берём пачку из per_compact сегментов уровня from_level
                seg_rows = await session.execute(
                    select(Segment)
                    .where(
                        Segment.level == from_level,
                        Segment.status == "ready",
                        Segment.shard_id == shard_id,
                    )
                    .order_by(Segment.id)  # самые старые
                    .limit(per_compact)
                )
                batch_segments: List[Segment] = list(seg_rows.scalars())

                if len(batch_segments) < per_compact:
                    # меньше порога — для этого шарда хватит
                    break

                seg_ids = [s.id for s in batch_segments]

                # все документы, входящие в эти сегменты
                doc_rows = await session.execute(
                    select(Document)
                    .join(SegmentDoc, SegmentDoc.document_id == Document.id)
                    .where(SegmentDoc.segment_id.in_(seg_ids))
                    .order_by(Document.id)
                )
                docs: List[Document] = list(doc_rows.scalars())

                if not docs:
                    print(
                        f"[COMPACT L{from_level}->L{to_level}] shard={shard_id}: "
                        f"в сегментах {seg_ids} нет документов, помечаем их merged"
                    )
                    for s in batch_segments:
                        s.status = "merged"
                    await session.commit()
                    continue

                now = utcnow()

                # создаём новый сегмент уровня to_level
                new_segment = Segment(
                    shard_id=shard_id,
                    level=to_level,
                    status="building",
                    path="",
                    doc_count=len(docs),
                    shingle_count=0,
                    size_bytes=0,
                    created_at=now,
                    last_compacted_at=None,
                    last_access_at=None,
                )
                session.add(new_segment)
                await session.flush()  # new_segment.id

                segment_rel_path = f"shard_{shard_id}/segment_{new_segment.id}"
                segment_dir: Path = INDEX_DIR / segment_rel_path
                segment_dir.mkdir(parents=True, exist_ok=True)
                new_segment.path = segment_rel_path

                print(
                    f"[COMPACT L{from_level}->L{to_level}] shard={shard_id}: "
                    f"строю segment id={new_segment.id}, "
                    f"segments={seg_ids}, docs={len(docs)}, dir={segment_dir}"
                )

                seg_corpus_path = segment_dir / "segment_corpus.jsonl"
                written_docs = 0

                with seg_corpus_path.open("w", encoding="utf-8") as f:
                    for doc in docs:
                        if not doc.external_id:
                            print(
                                f"[COMPACT L{from_level}->L{to_level}] "
                                f"WARNING: doc id={doc.id} без external_id, пропускаю"
                            )
                            continue

                        file_path = UPLOAD_DIR / doc.external_id
                        if not file_path.exists():
                            print(
                                f"[COMPACT L{from_level}->L{to_level}] WARNING: "
                                f"файл не найден: {file_path}, пропускаю doc id={doc.id}"
                            )
                            continue

                        try:
                            raw_bytes = file_path.read_bytes()
                        except Exception as e:
                            print(
                                f"[COMPACT L{from_level}->L{to_level}] ERROR: "
                                f"не удалось прочитать {file_path}: {e}"
                            )
                            continue

                        try:
                            raw_text = extract_text_from_file_bytes(
                                raw_bytes,
                                filename=str(file_path),
                            )
                        except Exception as e:
                            print(
                                f"[COMPACT L{from_level}->L{to_level}] ERROR: "
                                f"extract_text для {file_path}: {e}"
                            )
                            continue

                        rec = {
                            "doc_id": str(doc.id),
                            "text": raw_text,
                        }
                        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                        written_docs += 1

                if written_docs == 0:
                    print(
                        f"[COMPACT L{from_level}->L{to_level}] shard={shard_id}, "
                        f"segment_id={new_segment.id}: corpus.jsonl пустой, "
                        f"помечаем сегмент как error"
                    )
                    new_segment.status = "error"
                    await session.commit()
                    # старые сегменты не трогаем, чтобы можно было повторить
                    continue

                ok = await run_etl_index_builder(seg_corpus_path, segment_dir)
                if not ok:
                    new_segment.status = "error"
                    await session.commit()
                    print(
                        f"[COMPACT L{from_level}->L{to_level}] ERROR: "
                        f"etl_index_builder упал, segment_id={new_segment.id}"
                    )
                    continue

                bin_path = segment_dir / "index_native.bin"
                docids_path = segment_dir / "index_native_docids.json"
                meta_path = segment_dir / "index_native_meta.json"

                if not bin_path.exists() or not docids_path.exists():
                    print(
                        f"[COMPACT L{from_level}->L{to_level}] ERROR: "
                        f"index_native.* не создался, segment_id={new_segment.id}"
                    )
                    new_segment.status = "error"
                    await session.commit()
                    continue

                size_bytes = 0
                for p in (bin_path, docids_path, meta_path):
                    if p.exists():
                        size_bytes += p.stat().st_size

                new_segment.size_bytes = size_bytes
                new_segment.status = "ready"

                # Перелинковываем документы на новый сегмент
                promoted = 0
                for doc in docs:
                    doc.segment_id = new_segment.id
                    doc.status = "indexed"
                    doc.updated_at = now
                    promoted += 1

                # Чистим старые связи segment_docs и создаём новые
                await session.execute(
                    delete(SegmentDoc).where(SegmentDoc.segment_id.in_(seg_ids))
                )
                for doc in docs:
                    sd = SegmentDoc(
                        segment_id=new_segment.id,
                        document_id=doc.id,
                        shard_id=doc.shard_id,
                    )
                    session.add(sd)

                # Старые сегменты помечаем как merged
                for s in batch_segments:
                    s.status = "merged"
                    s.last_compacted_at = now

                await session.commit()

                print(
                    f"[COMPACT L{from_level}->L{to_level}] "
                    f"готов segment id={new_segment.id}, shard={shard_id}, "
                    f"docs={promoted}, bytes={size_bytes}, merged_segments={seg_ids}"
                )

                total_docs_promoted += promoted

        return total_docs_promoted


# ───────────────────────── etl_index_builder (C++) ───────────────────────── #

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


# ───────────────────────── Уровень 0: uploaded -> etl_ok ─────────────────── #

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


# ────────────────────── Уровень 1: build L1 segments ─────────────────────── #

async def build_l1_segments() -> int:
    """
    L1: индексируем документы.

    Логика:
      - берём etl_ok + segment_id IS NULL;
      - НЕ ждём полного набора DOCS_PER_L1_SEGMENT — индексируем всё, что есть;
      - режем пачками (по DOCS_PER_L1_SEGMENT), но последний хвост тоже индексируем;
      - после индексации:
          * Segment.level = 1
          * Document.status = 'indexed'
          * Document.segment_id = новый L1-сегмент.
    """
    async with AsyncSessionLocal() as session:
        # шарды, где есть ещё etl_ok без segment_id
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
            print("[SEGMENT-L1] Нет документов etl_ok без segment_id — L1-сегменты не нужны")
            return 0

        total_docs_processed = 0

        for shard_id in shard_ids:
            # забираем сразу много etl_ok, потом режем на чанки в Python
            seg_result = await session.execute(
                select(Document)
                .where(
                    Document.status == "etl_ok",
                    Document.segment_id.is_(None),
                    Document.shard_id == shard_id,
                )
                .order_by(Document.id)
            )
            all_docs: List[Document] = list(seg_result.scalars())

            if not all_docs:
                continue

            print(
                f"[SEGMENT-L1] shard={shard_id}: найдено {len(all_docs)} etl_ok, "
                f"строим L1-сегменты батчами по {DOCS_PER_L1_SEGMENT}"
            )

            # режем на чанки по DOCS_PER_L1_SEGMENT, но хвост тоже берём
            for i in range(0, len(all_docs), DOCS_PER_L1_SEGMENT):
                docs = all_docs[i : i + DOCS_PER_L1_SEGMENT]
                if not docs:
                    continue

                now = utcnow()

                segment = Segment(
                    shard_id=shard_id,
                    level=1,
                    status="building",
                    path="",
                    doc_count=len(docs),
                    shingle_count=0,
                    size_bytes=0,
                    created_at=now,
                    last_compacted_at=None,
                    last_access_at=None,
                )
                session.add(segment)
                await session.flush()  # segment.id

                segment_rel_path = f"shard_{shard_id}/segment_{segment.id}"
                segment_dir: Path = INDEX_DIR / segment_rel_path
                segment_dir.mkdir(parents=True, exist_ok=True)
                segment.path = segment_rel_path

                print(
                    f"[SEGMENT-L1] shard={shard_id}: строю L1-segment id={segment.id}, "
                    f"docs={len(docs)}, dir={segment_dir}"
                )

                seg_corpus_path = segment_dir / "segment_corpus.jsonl"

                written_docs = 0
                with seg_corpus_path.open("w", encoding="utf-8") as f:
                    for doc in docs:
                        if not doc.external_id:
                            print(
                                f"[SEGMENT-L1] WARNING: doc id={doc.id} без external_id, пропускаю"
                            )
                            continue

                        file_path = UPLOAD_DIR / doc.external_id
                        if not file_path.exists():
                            print(
                                f"[SEGMENT-L1] WARNING: файл не найден: {file_path}, "
                                f"пропускаю doc id={doc.id}"
                            )
                            continue

                        try:
                            raw_bytes = file_path.read_bytes()
                        except Exception as e:
                            print(f"[SEGMENT-L1] ERROR: не удалось прочитать {file_path}: {e}")
                            continue

                        try:
                            raw_text = extract_text_from_file_bytes(
                                raw_bytes,
                                filename=str(file_path),
                            )
                        except Exception as e:
                            print(f"[SEGMENT-L1] ERROR: extract_text для {file_path}: {e}")
                            continue

                        rec = {
                            "doc_id": str(doc.id),
                            "text": raw_text,
                        }
                        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                        written_docs += 1

                if written_docs == 0:
                    print(
                        f"[SEGMENT-L1] shard={shard_id}, segment_id={segment.id}: "
                        "в segment_corpus.jsonl не попало ни одного документа, помечаем сегмент как error"
                    )
                    segment.status = "error"
                    await session.commit()
                    continue

                ok = await run_etl_index_builder(seg_corpus_path, segment_dir)
                if not ok:
                    segment.status = "error"
                    await session.commit()
                    print(f"[SEGMENT-L1] ERROR: etl_index_builder упал, segment_id={segment.id}")
                    continue

                bin_path = segment_dir / "index_native.bin"
                docids_path = segment_dir / "index_native_docids.json"
                meta_path = segment_dir / "index_native_meta.json"

                if not bin_path.exists() or not docids_path.exists():
                    print(
                        f"[SEGMENT-L1] ERROR: index_native.* не создался, "
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

                updated_docs = 0
                for doc in docs:
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
                    f"[SEGMENT-L1] Готов L1-segment id={segment.id}, shard={shard_id}, "
                    f"docs={updated_docs}, bytes={size_bytes}"
                )

                total_docs_processed += updated_docs

        return total_docs_processed


# ────────────────────── L1→L2, L2→L3, L3→L4 компакция ────────────────────── #

async def build_l2_segments() -> int:
    # L1 -> L2
    return await compact_segments_level(from_level=1)


async def build_l3_segments() -> int:
    # L2 -> L3
    return await compact_segments_level(from_level=2)


async def build_l4_segments() -> int:
    # L3 -> L4
    return await compact_segments_level(from_level=3)


# ───────────────────────────── main loop ──────────────────────────────────── #

async def main_loop() -> None:
    print("[worker] index_worker стартовал")
    while True:
        try:
            etl_cnt = await process_uploaded_docs()
            l1_cnt = await build_l1_segments()

            l2_cnt = l3_cnt = l4_cnt = 0

            if MAX_AUTO_LEVEL >= 2:
                l2_cnt = await build_l2_segments()   # L1 -> L2
            if MAX_AUTO_LEVEL >= 3:
                l3_cnt = await build_l3_segments()   # L2 -> L3
            if MAX_AUTO_LEVEL >= 4:
                l4_cnt = await build_l4_segments()   # L3 -> L4

            print(
                f"[worker] tick: etl={etl_cnt}, l1={l1_cnt}, "
                f"l2={l2_cnt}, l3={l3_cnt}, l4={l4_cnt}"
            )
        except Exception as e:
            print(f"[worker] ERROR в main_loop: {e}")

        if (
            etl_cnt == 0
            and l1_cnt == 0
            and l2_cnt == 0
            and l3_cnt == 0
            and l4_cnt == 0
        ):
            await asyncio.sleep(3)
        else:
            await asyncio.sleep(0.1)


if __name__ == "__main__":
    asyncio.run(main_loop())
