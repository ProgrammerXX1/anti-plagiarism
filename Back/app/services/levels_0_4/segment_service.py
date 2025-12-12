# app/services/levels0_4/segments_service.py
from __future__ import annotations

import json
from pathlib import Path
from typing import List, Set

from sqlalchemy import select, delete, func
from sqlalchemy.ext.asyncio import AsyncSession

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
from app.repositories.index_errors import log_index_error
from app.services.helpers.file_extract import extract_text_from_file_bytes
from app.services.levels0_4.etl_service import utcnow


# -------------------------
# helpers
# -------------------------

async def _run_etl_index_builder(corpus: Path, out_dir: Path) -> bool:
    import asyncio

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


async def _select_docs_for_l1_locked(
    session: AsyncSession,
    shard_id: int,
    limit: int,
) -> List[Document]:
    """
    Берем пачку etl_ok документов для конкретного shard_id с блокировкой,
    чтобы несколько воркеров не строили один и тот же сегмент.
    """
    res = await session.execute(
        select(Document)
        .where(
            Document.shard_id == shard_id,
            Document.status == "etl_ok",
            Document.segment_id.is_(None),
        )
        .order_by(Document.id)
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    return list(res.scalars())


def _segment_dir(shard_id: int, segment_id: int) -> Path:
    rel = f"shard_{shard_id}/segment_{segment_id}"
    d = INDEX_DIR / rel
    d.mkdir(parents=True, exist_ok=True)
    return d


# -------------------------
# L1 build
# -------------------------

async def build_l1_segments() -> int:
    """
    L1: индексируем документы (создаём L1-сегменты).
    КЛЮЧЕВО:
      - берём документы пачками с FOR UPDATE SKIP LOCKED
      - помечаем indexed только те, кто реально попал в corpus.jsonl
    """
    async with AsyncSessionLocal() as session:
        shard_rows = await session.execute(
            select(Document.shard_id)
            .where(
                Document.status == "etl_ok",
                Document.segment_id.is_(None),
            )
            .distinct()
        )
        shard_ids = [row[0] for row in shard_rows.fetchall()]

        if not shard_ids:
            print("[SEGMENT-L1] Нет документов etl_ok без segment_id — L1-сегменты не нужны")
            return 0

    total_docs_processed = 0

    # shard loop — отдельными транзакциями, чтобы не держать long tx
    for shard_id in shard_ids:
        while True:
            async with AsyncSessionLocal() as session:
                docs = await _select_docs_for_l1_locked(session, shard_id, DOCS_PER_L1_SEGMENT)
                if not docs:
                    break

                now = utcnow()

                segment = Segment(
                    shard_id=shard_id,
                    level=1,
                    status="building",
                    path="",
                    doc_count=len(docs),      # позже обновим фактическим числом
                    shingle_count=0,
                    size_bytes=0,
                    created_at=now,
                    last_compacted_at=None,
                    last_access_at=None,
                )
                session.add(segment)
                await session.flush()

                seg_dir = _segment_dir(shard_id, segment.id)
                segment.path = f"shard_{shard_id}/segment_{segment.id}"

                print(
                    f"[SEGMENT-L1] shard={shard_id}: строю L1-segment id={segment.id}, "
                    f"docs(batch)={len(docs)}, dir={seg_dir}"
                )

                seg_corpus_path = seg_dir / "segment_corpus.jsonl"

                indexed_doc_ids: Set[int] = set()

                with seg_corpus_path.open("w", encoding="utf-8") as f:
                    for doc in docs:
                        if not doc.external_id:
                            continue
                        file_path = UPLOAD_DIR / doc.external_id
                        if not file_path.exists():
                            continue

                        try:
                            raw_bytes = file_path.read_bytes()
                            raw_text = extract_text_from_file_bytes(raw_bytes, filename=str(file_path))
                        except Exception as e:
                            # логируем и пропускаем
                            await log_index_error(
                                session,
                                stage="build_l1",
                                message=f"extract/read failed: {e}",
                                doc_id=doc.id,
                                payload={"file": str(file_path)},
                            )
                            continue

                        rec = {"doc_id": str(doc.id), "text": raw_text}
                        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                        indexed_doc_ids.add(doc.id)

                if not indexed_doc_ids:
                    segment.status = "error"
                    segment.doc_count = 0
                    await log_index_error(
                        session,
                        stage="build_l1",
                        message="segment_corpus.jsonl is empty (no indexable docs)",
                        segment_id=segment.id,
                        payload={"shard_id": shard_id},
                    )
                    await session.commit()
                    continue

                # ВАЖНО: выходим из транзакции, чтобы не держать lock пока C++ работает
                await session.commit()

            # запуск C++ — вне tx
            ok = await _run_etl_index_builder(seg_corpus_path, seg_dir)
            if not ok:
                async with AsyncSessionLocal() as session:
                    seg = await session.get(Segment, segment.id)
                    if seg:
                        seg.status = "error"
                        await log_index_error(
                            session,
                            stage="build_l1",
                            message="etl_index_builder failed",
                            segment_id=segment.id,
                            payload={"dir": str(seg_dir)},
                        )
                        await session.commit()
                continue

            # финализация — отдельная транзакция
            async with AsyncSessionLocal() as session:
                seg = await session.get(Segment, segment.id)
                if not seg:
                    continue

                bin_path = seg_dir / "index_native.bin"
                docids_path = seg_dir / "index_native_docids.json"
                meta_path = seg_dir / "index_native_meta.json"

                if (not bin_path.exists()) or (not docids_path.exists()):
                    seg.status = "error"
                    await log_index_error(
                        session,
                        stage="build_l1",
                        message="index_native.* was not created",
                        segment_id=segment.id,
                        payload={"dir": str(seg_dir)},
                    )
                    await session.commit()
                    continue

                size_bytes = 0
                for p in (bin_path, docids_path, meta_path):
                    if p.exists():
                        size_bytes += p.stat().st_size

                # обновляем только реально индексированные документы
                real_docs = await session.execute(
                    select(Document).where(Document.id.in_(list(indexed_doc_ids)))
                )
                real_docs_list: List[Document] = list(real_docs.scalars())

                for doc in real_docs_list:
                    doc.segment_id = seg.id
                    doc.status = "indexed"
                    doc.updated_at = utcnow()
                    session.add(
                        SegmentDoc(
                            segment_id=seg.id,
                            document_id=doc.id,
                            shard_id=doc.shard_id,
                        )
                    )

                seg.size_bytes = size_bytes
                seg.doc_count = len(real_docs_list)
                seg.status = "ready"

                await session.commit()

                print(
                    f"[SEGMENT-L1] Готов L1-segment id={seg.id}, shard={shard_id}, "
                    f"docs(indexed)={len(real_docs_list)}, bytes={size_bytes}"
                )

                total_docs_processed += len(real_docs_list)

    return total_docs_processed


# -------------------------
# Compaction Lx -> L(x+1)
# -------------------------

async def compact_segments_level(from_level: int) -> int:
    """
    Компакция сегментов: Lx -> L(x+1)
    Строгий режим: если не смогли извлечь текст хотя бы для одного документа —
    НЕ мерджим исходные сегменты (иначе тихая потеря данных).
    """
    to_level = from_level + 1
    per_compact = cfg_segments_per_compact(from_level)

    total_docs_promoted = 0

    async with AsyncSessionLocal() as session:
        shard_rows = await session.execute(
            select(Segment.shard_id)
            .where(Segment.level == from_level, Segment.status == "ready")
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

    for shard_id in shard_ids:
        while True:
            # 1) берём сегменты под lock
            async with AsyncSessionLocal() as session:
                seg_rows = await session.execute(
                    select(Segment)
                    .where(
                        Segment.level == from_level,
                        Segment.status == "ready",
                        Segment.shard_id == shard_id,
                    )
                    .order_by(Segment.id)
                    .limit(per_compact)
                    .with_for_update(skip_locked=True)
                )
                batch_segments: List[Segment] = list(seg_rows.scalars())
                if len(batch_segments) < per_compact:
                    break

                seg_ids = [s.id for s in batch_segments]

                # docs для этих сегментов
                doc_rows = await session.execute(
                    select(Document)
                    .join(SegmentDoc, SegmentDoc.document_id == Document.id)
                    .where(SegmentDoc.segment_id.in_(seg_ids))
                    .order_by(Document.id)
                    .with_for_update(skip_locked=True)
                )
                docs: List[Document] = list(doc_rows.scalars())

                if not docs:
                    for s in batch_segments:
                        s.status = "merged"
                        s.last_compacted_at = utcnow()
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

                seg_dir = _segment_dir(shard_id, new_segment.id)
                new_segment.path = f"shard_{shard_id}/segment_{new_segment.id}"

                seg_corpus_path = seg_dir / "segment_corpus.jsonl"

                indexed_doc_ids: List[int] = []
                failed = False

                with seg_corpus_path.open("w", encoding="utf-8") as f:
                    for doc in docs:
                        if not doc.external_id:
                            failed = True
                            await log_index_error(
                                session,
                                stage="compact",
                                message="doc has no external_id",
                                doc_id=doc.id,
                                segment_id=new_segment.id,
                            )
                            break

                        file_path = UPLOAD_DIR / doc.external_id
                        if not file_path.exists():
                            failed = True
                            await log_index_error(
                                session,
                                stage="compact",
                                message="file missing",
                                doc_id=doc.id,
                                segment_id=new_segment.id,
                                payload={"file": str(file_path)},
                            )
                            break

                        try:
                            raw_bytes = file_path.read_bytes()
                            raw_text = extract_text_from_file_bytes(raw_bytes, filename=str(file_path))
                        except Exception as e:
                            failed = True
                            await log_index_error(
                                session,
                                stage="compact",
                                message=f"extract/read failed: {e}",
                                doc_id=doc.id,
                                segment_id=new_segment.id,
                                payload={"file": str(file_path)},
                            )
                            break

                        f.write(json.dumps({"doc_id": str(doc.id), "text": raw_text}, ensure_ascii=False) + "\n")
                        indexed_doc_ids.append(doc.id)

                if failed or len(indexed_doc_ids) != len(docs):
                    # строгий режим: компакцию не делаем, исходники не трогаем
                    new_segment.status = "error"
                    await log_index_error(
                        session,
                        stage="compact",
                        message="strict mode: not all docs were indexable; compaction aborted",
                        segment_id=new_segment.id,
                        payload={"from_segments": seg_ids, "docs_total": len(docs), "docs_written": len(indexed_doc_ids)},
                    )
                    await session.commit()
                    continue

                await session.commit()

            # 2) запускаем C++ вне tx
            ok = await _run_etl_index_builder(seg_corpus_path, seg_dir)
            if not ok:
                async with AsyncSessionLocal() as session:
                    seg = await session.get(Segment, new_segment.id)
                    if seg:
                        seg.status = "error"
                        await log_index_error(
                            session,
                            stage="compact",
                            message="etl_index_builder failed",
                            segment_id=new_segment.id,
                            payload={"dir": str(seg_dir)},
                        )
                        await session.commit()
                continue

            # 3) финализация: обновить docs/segment_docs, пометить старые merged
            async with AsyncSessionLocal() as session:
                seg = await session.get(Segment, new_segment.id)
                if not seg:
                    continue

                bin_path = seg_dir / "index_native.bin"
                docids_path = seg_dir / "index_native_docids.json"
                meta_path = seg_dir / "index_native_meta.json"

                if (not bin_path.exists()) or (not docids_path.exists()):
                    seg.status = "error"
                    await log_index_error(
                        session,
                        stage="compact",
                        message="index_native.* was not created",
                        segment_id=new_segment.id,
                        payload={"dir": str(seg_dir)},
                    )
                    await session.commit()
                    continue

                size_bytes = 0
                for p in (bin_path, docids_path, meta_path):
                    if p.exists():
                        size_bytes += p.stat().st_size

                # переназначаем документы на новый сегмент
                res_docs = await session.execute(select(Document).where(Document.id.in_(indexed_doc_ids)))
                docs2: List[Document] = list(res_docs.scalars())

                for doc in docs2:
                    doc.segment_id = seg.id
                    doc.status = "indexed"
                    doc.updated_at = utcnow()

                # пересобираем SegmentDoc:
                await session.execute(delete(SegmentDoc).where(SegmentDoc.segment_id.in_(seg_ids)))

                for doc in docs2:
                    session.add(SegmentDoc(segment_id=seg.id, document_id=doc.id, shard_id=doc.shard_id))

                # помечаем старые сегменты merged
                res_old = await session.execute(select(Segment).where(Segment.id.in_(seg_ids)))
                old_segs: List[Segment] = list(res_old.scalars())
                for s in old_segs:
                    s.status = "merged"
                    s.last_compacted_at = utcnow()

                seg.size_bytes = size_bytes
                seg.doc_count = len(docs2)
                seg.status = "ready"

                await session.commit()

                print(
                    f"[COMPACT L{from_level}->L{to_level}] shard={shard_id}: "
                    f"готов segment id={seg.id}, docs={len(docs2)}, bytes={size_bytes}, merged_segments={seg_ids}"
                )
                total_docs_promoted += len(docs2)

    return total_docs_promoted


async def build_l2_segments() -> int:
    return await compact_segments_level(from_level=1)


async def build_l3_segments() -> int:
    return await compact_segments_level(from_level=2)


async def build_l4_segments() -> int:
    return await compact_segments_level(from_level=3)
