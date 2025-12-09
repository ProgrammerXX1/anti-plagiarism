# app/services/levels0_4/segments_service.py
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from sqlalchemy import select, delete, func

from app.core.config import (
    INDEX_DIR,
    UPLOAD_DIR,
    DOCS_PER_L1_SEGMENT,
    segments_per_compact as cfg_segments_per_compact,
)
from app.db.session import AsyncSessionLocal
from app.models.document import Document
from app.models.segment import Segment
from app.models.segment_doc import SegmentDoc
from app.services.helpers.file_extract import extract_text_from_file_bytes
from app.services.levels_0_4.etl_service import utcnow, run_etl_index_builder


async def build_l1_segments() -> int:
    """
    L1: индексируем документы (создаём L1-сегменты).
    """
    async with AsyncSessionLocal() as session:
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
                await session.flush()

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

                        rec = {"doc_id": str(doc.id), "text": raw_text}
                        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                        written_docs += 1

                if written_docs == 0:
                    print(
                        f"[SEGMENT-L1] shard={shard_id}, segment_id={segment.id}: "
                        "segment_corpus.jsonl пустой, помечаем сегмент как error"
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

async def compact_segments_level(from_level: int) -> int:
    async with AsyncSessionLocal() as session:
        to_level = from_level + 1
        per_compact = cfg_segments_per_compact(from_level)

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
                seg_rows = await session.execute(
                    select(Segment)
                    .where(
                        Segment.level == from_level,
                        Segment.status == "ready",
                        Segment.shard_id == shard_id,
                    )
                    .order_by(Segment.id)
                    .limit(per_compact)
                )
                batch_segments: List[Segment] = list(seg_rows.scalars())

                if len(batch_segments) < per_compact:
                    break

                seg_ids = [s.id for s in batch_segments]

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
                await session.flush()

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

                        rec = {"doc_id": str(doc.id), "text": raw_text}
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

                promoted = 0
                for doc in docs:
                    doc.segment_id = new_segment.id
                    doc.status = "indexed"
                    doc.updated_at = now
                    promoted += 1

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


async def build_l2_segments() -> int:
    return await compact_segments_level(from_level=1)


async def build_l3_segments() -> int:
    return await compact_segments_level(from_level=2)


async def build_l4_segments() -> int:
    return await compact_segments_level(from_level=3)
