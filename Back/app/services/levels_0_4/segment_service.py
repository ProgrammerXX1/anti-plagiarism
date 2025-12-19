from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import (
    DOCS_PER_L1_SEGMENT,
    INDEX_DIR,
    UPLOAD_DIR,
    segments_per_compact as cfg_segments_per_compact,
)
from app.core.logger import logger
from app.db.session import AsyncSessionLocal
from app.models.document import Document
from app.models.segment import Segment
from app.models.segment_doc import SegmentDoc
from app.repositories.index_errors import log_index_error
from app.services.helpers.file_extract import extract_text_from_file_bytes
from app.services.levels_0_4.etl_service import utcnow


async def _run_etl_index_builder(corpus: Path, out_dir: Path) -> bool:
    proc = await asyncio.create_subprocess_exec(
        "etl_index_builder",
        str(corpus),
        str(out_dir),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()

    if stdout:
        logger.info("[etl_index_builder][stdout]\n%s", stdout.decode("utf-8", errors="ignore"))
    if stderr:
        logger.warning("[etl_index_builder][stderr]\n%s", stderr.decode("utf-8", errors="ignore"))

    if proc.returncode != 0:
        logger.error("[etl_index_builder] FAILED rc=%s corpus=%s out_dir=%s", proc.returncode, corpus, out_dir)
        return False

    logger.info("[etl_index_builder] OK corpus=%s out_dir=%s", corpus, out_dir)
    return True


async def _select_docs_for_l1_locked(
    session: AsyncSession,
    *,
    shard_id: int,
    organization_id: int,
    limit: int,
) -> List[Document]:
    res = await session.execute(
        select(Document)
        .where(
            Document.shard_id == shard_id,
            Document.organization_id == organization_id,
            Document.status == "etl_ok",
            Document.segment_id.is_(None),
        )
        .order_by(Document.id)
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    return list(res.scalars())


def _segment_dir(org_id: int, shard_id: int, segment_id: int) -> Path:
    rel = f"org_{org_id}/shard_{shard_id}/segment_{segment_id}"
    d = INDEX_DIR / rel
    d.mkdir(parents=True, exist_ok=True)
    return d


def _load_upload_meta(external_id: str) -> Dict[str, Any]:
    p = UPLOAD_DIR / f"{external_id}.meta.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _corpus_record(
    *,
    doc_id: int,
    text: str,
) -> Dict[str, Any]:
    """
    Invariant: ingest writes normalized texts always.
    Builder contract supports:
      - text_is_normalized (new)
      - normalized (legacy)
    Both must be True so builder does NOT normalize.
    """
    return {
        "doc_id": str(doc_id),
        "text": text,
        "text_is_normalized": True,
        "normalized": True,
    }


async def build_l1_segments() -> int:
    async with AsyncSessionLocal() as session:
        pair_rows = await session.execute(
            select(Document.shard_id, Document.organization_id)
            .where(
                Document.status == "etl_ok",
                Document.segment_id.is_(None),
            )
            .distinct()
        )
        pairs: List[Tuple[int, int]] = [
            (int(r[0]), int(r[1])) for r in pair_rows.fetchall() if r[1] is not None
        ]

    if not pairs:
        logger.info("[SEGMENT-L1] no docs etl_ok without segment_id")
        return 0

    total_docs_processed = 0

    for shard_id, org_id in pairs:
        while True:
            async with AsyncSessionLocal() as session:
                docs = await _select_docs_for_l1_locked(
                    session=session,
                    shard_id=shard_id,
                    organization_id=org_id,
                    limit=DOCS_PER_L1_SEGMENT,
                )
                if not docs:
                    break

                now = utcnow()
                segment = Segment(
                    organization_id=org_id,
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

                seg_dir = _segment_dir(org_id, shard_id, segment.id)
                segment.path = f"org_{org_id}/shard_{shard_id}/segment_{segment.id}"

                logger.info(
                    "[SEGMENT-L1] building segment_id=%s org=%s shard=%s docs=%s dir=%s",
                    segment.id, org_id, shard_id, len(docs), seg_dir
                )

                seg_corpus_path = seg_dir / "segment_corpus.jsonl"
                indexed_doc_ids: Set[int] = set()

                with seg_corpus_path.open("w", encoding="utf-8") as f:
                    for doc in docs:
                        if not doc.external_id:
                            await log_index_error(
                                session,
                                stage="build_l1",
                                message="doc has no external_id",
                                doc_id=doc.id,
                                segment_id=segment.id,
                            )
                            continue

                        file_path = UPLOAD_DIR / doc.external_id
                        if not file_path.exists():
                            await log_index_error(
                                session,
                                stage="build_l1",
                                message="file missing",
                                doc_id=doc.id,
                                segment_id=segment.id,
                                payload={"file": str(file_path)},
                            )
                            continue

                        try:
                            raw_bytes = file_path.read_bytes()
                            if file_path.suffix.lower() == ".txt":
                                raw_text = raw_bytes.decode("utf-8", errors="ignore")
                            else:
                                raw_text = extract_text_from_file_bytes(raw_bytes, filename=str(file_path))
                        except Exception as e:
                            await log_index_error(
                                session,
                                stage="build_l1",
                                message=f"extract/read failed: {e}",
                                doc_id=doc.id,
                                segment_id=segment.id,
                                payload={"file": str(file_path)},
                            )
                            continue

                        # sanity: meta must say normalized, but we don't rely on it here
                        _ = _load_upload_meta(doc.external_id)

                        rec = _corpus_record(doc_id=doc.id, text=raw_text)
                        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                        indexed_doc_ids.add(doc.id)

                if not indexed_doc_ids:
                    segment.status = "error"
                    segment.doc_count = 0
                    await log_index_error(
                        session,
                        stage="build_l1",
                        message="segment_corpus.jsonl empty (no indexable docs)",
                        segment_id=segment.id,
                        payload={"org_id": org_id, "shard_id": shard_id},
                    )
                    await session.commit()
                    continue

                await session.commit()

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

                res_docs = await session.execute(select(Document).where(Document.id.in_(list(indexed_doc_ids))))
                real_docs_list: List[Document] = list(res_docs.scalars())

                bad = [d.id for d in real_docs_list if d.organization_id != org_id]
                if bad:
                    seg.status = "error"
                    await log_index_error(
                        session,
                        stage="build_l1",
                        message="org invariant violated in finalization",
                        segment_id=seg.id,
                        payload={"expected_org_id": org_id, "bad_doc_ids": bad, "shard_id": shard_id},
                    )
                    await session.commit()
                    continue

                now2 = utcnow()
                for doc in real_docs_list:
                    doc.segment_id = seg.id
                    doc.status = "indexed"
                    doc.updated_at = now2
                    session.add(
                        SegmentDoc(
                            segment_id=seg.id,
                            document_id=doc.id,
                            shard_id=doc.shard_id,
                            organization_id=doc.organization_id,
                        )
                    )

                seg.size_bytes = int(size_bytes)
                seg.doc_count = int(len(real_docs_list))
                seg.status = "ready"
                await session.commit()

                logger.info(
                    "[SEGMENT-L1] ready segment_id=%s org=%s shard=%s docs=%s bytes=%s",
                    seg.id, org_id, shard_id, len(real_docs_list), size_bytes
                )
                total_docs_processed += len(real_docs_list)

    return total_docs_processed


async def compact_segments_level(from_level: int) -> int:
    to_level = from_level + 1
    per_compact = cfg_segments_per_compact(from_level)

    async with AsyncSessionLocal() as session:
        pair_rows = await session.execute(
            select(Segment.organization_id, Segment.shard_id)
            .where(Segment.level == from_level, Segment.status == "ready")
            .group_by(Segment.organization_id, Segment.shard_id)
            .having(func.count(Segment.id) >= per_compact)
        )
        pairs: List[Tuple[int, int]] = [
            (int(r[0]), int(r[1])) for r in pair_rows.fetchall() if r[0] is not None
        ]

    if not pairs:
        logger.info("[COMPACT L%s->L%s] no (org, shard) with >=%s segments", from_level, to_level, per_compact)
        return 0

    total_docs_promoted = 0

    for org_id, shard_id in pairs:
        while True:
            async with AsyncSessionLocal() as session:
                seg_rows = await session.execute(
                    select(Segment)
                    .where(
                        Segment.organization_id == org_id,
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

                seg_ids = [int(s.id) for s in batch_segments]

                doc_rows = await session.execute(
                    select(Document)
                    .join(SegmentDoc, SegmentDoc.document_id == Document.id)
                    .where(
                        SegmentDoc.segment_id.in_(seg_ids),
                        SegmentDoc.organization_id == org_id,
                    )
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
                    organization_id=org_id,
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

                seg_dir = _segment_dir(org_id, shard_id, new_segment.id)
                new_segment.path = f"org_{org_id}/shard_{shard_id}/segment_{new_segment.id}"

                logger.info(
                    "[COMPACT L%s->L%s] building segment_id=%s org=%s shard=%s from_segments=%s docs=%s",
                    from_level, to_level, new_segment.id, org_id, shard_id, seg_ids, len(docs)
                )

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
                            if file_path.suffix.lower() == ".txt":
                                raw_text = raw_bytes.decode("utf-8", errors="ignore")
                            else:
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

                        rec = _corpus_record(doc_id=doc.id, text=raw_text)
                        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                        indexed_doc_ids.append(int(doc.id))

                if failed or len(indexed_doc_ids) != len(docs):
                    new_segment.status = "error"
                    await log_index_error(
                        session,
                        stage="compact",
                        message="strict mode: not all docs were indexable; compaction aborted",
                        segment_id=new_segment.id,
                        payload={
                            "org_id": org_id,
                            "shard_id": shard_id,
                            "from_segments": seg_ids,
                            "docs_total": len(docs),
                            "docs_written": len(indexed_doc_ids),
                        },
                    )
                    await session.commit()
                    continue

                await session.commit()

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

                res_docs = await session.execute(select(Document).where(Document.id.in_(indexed_doc_ids)))
                docs2: List[Document] = list(res_docs.scalars())

                now2 = utcnow()
                for doc in docs2:
                    doc.segment_id = seg.id
                    doc.status = "indexed"
                    doc.updated_at = now2

                await session.execute(
                    delete(SegmentDoc).where(
                        SegmentDoc.segment_id.in_(seg_ids),
                        SegmentDoc.organization_id == org_id,
                    )
                )

                for doc in docs2:
                    session.add(
                        SegmentDoc(
                            segment_id=seg.id,
                            document_id=doc.id,
                            shard_id=doc.shard_id,
                            organization_id=doc.organization_id,
                        )
                    )

                res_old = await session.execute(select(Segment).where(Segment.id.in_(seg_ids)))
                old_segs: List[Segment] = list(res_old.scalars())
                for s in old_segs:
                    s.status = "merged"
                    s.last_compacted_at = now2

                seg.size_bytes = int(size_bytes)
                seg.doc_count = int(len(docs2))
                seg.status = "ready"

                await session.commit()

                logger.info(
                    "[COMPACT L%s->L%s] ready segment_id=%s org=%s shard=%s docs=%s bytes=%s merged=%s",
                    from_level, to_level, seg.id, org_id, shard_id, len(docs2), size_bytes, seg_ids
                )
                total_docs_promoted += len(docs2)

    return total_docs_promoted


async def build_l2_segments() -> int:
    return await compact_segments_level(from_level=1)


async def build_l3_segments() -> int:
    return await compact_segments_level(from_level=2)


async def build_l4_segments() -> int:
    return await compact_segments_level(from_level=3)
