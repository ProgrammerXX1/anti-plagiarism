# app/services/levels_0_4/segment_service.py
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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

SHARD_ID = 0

# marker to identify the single L1 buffer segment per org
L1_BUFFER_MARKER = ".l1_buffer"

# DB constraint-friendly stages (<=16 chars, and must be allowed by ck_index_errors_stage)
STAGE_BUILD_L1 = "build_l1"
STAGE_COMPACT = "compact"


# ───────────────────────────────────────────────
# Upload helpers
# ───────────────────────────────────────────────

def _upload_text_path_best_effort(doc: Document) -> Path:
    """
    Tries multiple common layouts:

    1) UPLOAD_DIR/{doc.id}.txt
    2) UPLOAD_DIR/{doc.id}
    3) UPLOAD_DIR/{external_id}.txt
    4) UPLOAD_DIR/{external_id}
    5) UPLOAD_DIR/{external_id}.* (pdf/docx/etc.)
    """
    # new layout: {doc.id}.txt
    p1 = UPLOAD_DIR / f"{int(doc.id)}.txt"
    if p1.exists():
        return p1

    # sometimes: {doc.id} without suffix
    p1b = UPLOAD_DIR / f"{int(doc.id)}"
    if p1b.exists():
        return p1b

    ext = getattr(doc, "external_id", None)
    if ext:
        ext_s = str(ext)

        # common: {external_id}.txt
        p2 = UPLOAD_DIR / f"{ext_s}.txt"
        if p2.exists():
            return p2

        # legacy: {external_id} without suffix
        p3 = UPLOAD_DIR / ext_s
        if p3.exists():
            return p3

        # best-effort: any file with that stem
        matches = sorted(UPLOAD_DIR.glob(f"{ext_s}.*"))
        if matches:
            return matches[0]

    # default
    return p1


def _load_upload_meta_best_effort(*, doc: Document) -> Dict[str, Any]:
    """
    Tries multiple meta layouts.

    1) UPLOAD_DIR/{doc.id}.meta.json
    2) UPLOAD_DIR/{external_id}.meta.json
    """
    p1 = UPLOAD_DIR / f"{int(doc.id)}.meta.json"
    if p1.exists():
        try:
            return json.loads(p1.read_text(encoding="utf-8"))
        except Exception:
            return {}

    ext = getattr(doc, "external_id", None)
    if ext:
        ext_s = str(ext)
        p2 = UPLOAD_DIR / f"{ext_s}.meta.json"
        if p2.exists():
            try:
                return json.loads(p2.read_text(encoding="utf-8"))
            except Exception:
                return {}

    return {}


def _read_doc_text_best_effort(*, doc: Document) -> Tuple[Optional[str], Path]:
    """
    Reads upload bytes and decodes/extracts text.
    Returns (text_or_None, path_used).
    """
    file_path = _upload_text_path_best_effort(doc)
    if not file_path.exists():
        return None, file_path

    try:
        raw_bytes = file_path.read_bytes()
        if file_path.suffix.lower() == ".txt":
            return raw_bytes.decode("utf-8", errors="ignore"), file_path
        return extract_text_from_file_bytes(raw_bytes, filename=str(file_path)), file_path
    except Exception:
        return None, file_path


# ───────────────────────────────────────────────
# External builder runner
# ───────────────────────────────────────────────

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


# ───────────────────────────────────────────────
# Paths
# ───────────────────────────────────────────────

def _segment_dir(org_id: int, shard_id: int, segment_id: int) -> Path:
    rel = f"org_{org_id}/shard_{shard_id}/segment_{segment_id}"
    d = INDEX_DIR / rel
    d.mkdir(parents=True, exist_ok=True)
    return d


def _buffer_marker_path(seg_dir: Path) -> Path:
    return seg_dir / L1_BUFFER_MARKER


def _write_json_atomic(path: Path, obj: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


# ───────────────────────────────────────────────
# IDs & corpus record (Variant A)
# ───────────────────────────────────────────────

def _external_document_id_best_effort(doc: Document) -> Optional[str]:
    """
    External document id used for provenance/UI.
    Current assumption: stored in Document.external_id.
    Change only here if you store it in another column.
    """
    v = getattr(doc, "external_id", None)
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _corpus_record(*, internal_doc_id: int, text: str) -> Dict[str, Any]:
    """
    Variant A:
      - doc_id must be numeric (builder expects it)
      - external id is stored separately in external_docid_map.json
    """
    return {
        "doc_id": str(int(internal_doc_id)),  # numeric string
        "text": text,
        "text_is_normalized": True,
        "normalized": True,
    }


# ───────────────────────────────────────────────
# L1 buffer segment discovery/creation
# ───────────────────────────────────────────────

async def _find_l1_buffer_segment(session: AsyncSession, *, org_id: int) -> Optional[Segment]:
    res = await session.execute(
        select(Segment)
        .where(
            Segment.organization_id == org_id,
            Segment.shard_id == SHARD_ID,
            Segment.level == 1,
            Segment.status.in_(("ready", "building", "error")),
        )
        .order_by(Segment.id.desc())
        .limit(50)
    )
    for seg in list(res.scalars()):
        seg_dir = _segment_dir(org_id, SHARD_ID, int(seg.id))
        if _buffer_marker_path(seg_dir).exists():
            return seg
    return None


async def _get_or_create_l1_buffer_segment(session: AsyncSession, *, org_id: int) -> Segment:
    seg = await _find_l1_buffer_segment(session, org_id=org_id)
    if seg:
        return seg

    now = utcnow()
    seg = Segment(
        organization_id=org_id,
        shard_id=SHARD_ID,
        level=1,
        status="building",
        path="",
        doc_count=0,
        shingle_count=0,
        size_bytes=0,
        created_at=now,
        last_compacted_at=None,
        last_access_at=None,
    )
    session.add(seg)
    await session.flush()

    seg_id = int(seg.id)
    seg_dir = _segment_dir(org_id, SHARD_ID, seg_id)
    seg.path = f"org_{org_id}/shard_{SHARD_ID}/segment_{seg_id}"

    try:
        _buffer_marker_path(seg_dir).write_text("buffer\n", encoding="utf-8")
    except Exception:
        logger.warning("[L1-BUFFER] failed to write marker for seg_id=%s org=%s", seg_id, org_id)

    return seg


# ───────────────────────────────────────────────
# Rebuild a segment index from its current membership (Document.segment_id)
# Also writes external_docid_map.json for provenance.
# ───────────────────────────────────────────────

async def _rebuild_segment_index(*, segment_id: int, stage: str) -> bool:
    async with AsyncSessionLocal() as session:
        seg = await session.get(Segment, segment_id)
        if not seg:
            return False
        org_id = int(seg.organization_id)
        level = int(seg.level)

    seg_dir = _segment_dir(org_id, SHARD_ID, segment_id)
    corpus_path = seg_dir / "segment_corpus.jsonl"
    map_path = seg_dir / "external_docid_map.json"

    async with AsyncSessionLocal() as session:
        res = await session.execute(
            select(Document)
            .where(
                Document.shard_id == SHARD_ID,
                Document.organization_id == org_id,
                Document.segment_id == segment_id,
            )
            .order_by(Document.id)
        )
        docs: List[Document] = list(res.scalars())

    if not docs:
        async with AsyncSessionLocal() as session:
            seg2 = await session.get(Segment, segment_id)
            if seg2:
                seg2.status = "ready"
                seg2.doc_count = 0
                seg2.size_bytes = 0
                # keep path as-is
                await session.execute(delete(SegmentDoc).where(SegmentDoc.segment_id == segment_id))
                await session.commit()
        try:
            _write_json_atomic(map_path, {})
        except Exception:
            pass
        return True

    good_db_ids: List[int] = []
    bad_db_ids: List[int] = []
    ext_map: Dict[str, str] = {}

    with corpus_path.open("w", encoding="utf-8") as f:
        for doc in docs:
            raw_text, used_path = _read_doc_text_best_effort(doc=doc)
            if raw_text is None or raw_text.strip() == "":
                bad_db_ids.append(int(doc.id))
                continue

            ext_id = _external_document_id_best_effort(doc)
            if ext_id is None:
                bad_db_ids.append(int(doc.id))
                continue

            _ = _load_upload_meta_best_effort(doc=doc)

            rec = _corpus_record(internal_doc_id=int(doc.id), text=raw_text)
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            good_db_ids.append(int(doc.id))
            ext_map[str(int(doc.id))] = ext_id

    # detach bad docs (terminal) + remove their SegmentDoc mapping to avoid "ghost ids"
    if bad_db_ids:
        async with AsyncSessionLocal() as session:
            now = utcnow()

            await session.execute(
                delete(SegmentDoc).where(
                    SegmentDoc.segment_id == segment_id,
                    SegmentDoc.document_id.in_(bad_db_ids),
                )
            )

            res_bad = await session.execute(select(Document).where(Document.id.in_(bad_db_ids)))
            for d in res_bad.scalars():
                raw_text, used_path = _read_doc_text_best_effort(doc=d)
                ext_id = _external_document_id_best_effort(d)

                if raw_text is None or (isinstance(raw_text, str) and raw_text.strip() == ""):
                    msg = "file missing/unreadable or empty text"
                else:
                    msg = "missing external document_id"

                await log_index_error(
                    session,
                    stage=stage,
                    message=msg,
                    doc_id=d.id,
                    segment_id=segment_id,
                    payload={"file": str(used_path), "external_id": ext_id},
                )
                d.status = "error"
                d.segment_id = None
                d.updated_at = now

            await session.commit()

    if not good_db_ids:
        async with AsyncSessionLocal() as session:
            seg2 = await session.get(Segment, segment_id)
            if seg2:
                seg2.status = "error"
                seg2.doc_count = 0
                seg2.size_bytes = 0
                # clear all mapping to avoid ghost document_ids in API
                await session.execute(delete(SegmentDoc).where(SegmentDoc.segment_id == segment_id))
                await log_index_error(
                    session,
                    stage=stage,
                    message="segment_corpus.jsonl empty (no indexable docs)",
                    doc_id=None,
                    segment_id=segment_id,
                    payload={"org_id": org_id, "level": level},
                )
                await session.commit()
        try:
            _write_json_atomic(map_path, {})
        except Exception:
            pass
        return False

    # write mapping (internal->external) atomically
    try:
        _write_json_atomic(map_path, ext_map)
    except Exception:
        logger.warning("[segment] failed to write external_docid_map.json seg_id=%s", segment_id)

    # mark building
    async with AsyncSessionLocal() as session:
        seg2 = await session.get(Segment, segment_id)
        if seg2:
            seg2.status = "building"
            await session.commit()

    ok = await _run_etl_index_builder(corpus_path, seg_dir)
    if not ok:
        async with AsyncSessionLocal() as session:
            seg2 = await session.get(Segment, segment_id)
            if seg2:
                seg2.status = "error"
                await log_index_error(
                    session,
                    stage=stage,
                    message="etl_index_builder failed",
                    doc_id=None,
                    segment_id=segment_id,
                    payload={"dir": str(seg_dir)},
                )
                await session.commit()
        return False

    bin_path = seg_dir / "index_native.bin"
    docids_path = seg_dir / "index_native_docids.json"
    meta_path = seg_dir / "index_native_meta.json"

    if (not bin_path.exists()) or (not docids_path.exists()):
        async with AsyncSessionLocal() as session:
            seg2 = await session.get(Segment, segment_id)
            if seg2:
                seg2.status = "error"
                await log_index_error(
                    session,
                    stage=stage,
                    message="index_native.* was not created",
                    doc_id=None,
                    segment_id=segment_id,
                    payload={"dir": str(seg_dir)},
                )
                await session.commit()
        return False

    size_bytes = 0
    for p in (bin_path, docids_path, meta_path):
        if p.exists():
            size_bytes += p.stat().st_size

    # finalize: replace SegmentDoc mapping, mark docs indexed
    async with AsyncSessionLocal() as session:
        seg2 = await session.get(Segment, segment_id)
        if not seg2:
            return False

        await session.execute(delete(SegmentDoc).where(SegmentDoc.segment_id == segment_id))

        now = utcnow()
        res_docs = await session.execute(select(Document).where(Document.id.in_(good_db_ids)))
        real_docs: List[Document] = list(res_docs.scalars())

        for d in real_docs:
            d.status = "indexed"
            d.updated_at = now
            session.add(
                SegmentDoc(
                    segment_id=segment_id,
                    document_id=d.id,
                    shard_id=SHARD_ID,
                    organization_id=d.organization_id,
                )
            )

        seg2.size_bytes = int(size_bytes)
        seg2.doc_count = int(len(real_docs))
        seg2.status = "ready"
        await session.commit()

    return True


# ───────────────────────────────────────────────
# L1: absorb new docs into org buffer (single L1 per org)
# ───────────────────────────────────────────────

async def build_l1_segments() -> int:
    async with AsyncSessionLocal() as session:
        org_rows = await session.execute(
            select(Document.organization_id)
            .where(
                Document.shard_id == SHARD_ID,
                Document.status == "etl_ok",
                Document.segment_id.is_(None),
                Document.organization_id.is_not(None),
            )
            .distinct()
        )
        org_ids: List[int] = [int(r[0]) for r in org_rows.fetchall() if r[0] is not None]

    if not org_ids:
        logger.info("[L1-BUFFER] no new docs shard=%s", SHARD_ID)
        return 0

    total_absorbed = 0
    add_limit = max(1, DOCS_PER_L1_SEGMENT * 5)

    for org_id in org_ids:
        buffer_segment_id: Optional[int] = None
        absorbed_ids: List[int] = []

        async with AsyncSessionLocal() as session:
            buf = await _get_or_create_l1_buffer_segment(session, org_id=org_id)
            buffer_segment_id = int(buf.id)

            res = await session.execute(
                select(Document)
                .where(
                    Document.shard_id == SHARD_ID,
                    Document.organization_id == org_id,
                    Document.status == "etl_ok",
                    Document.segment_id.is_(None),
                )
                .order_by(Document.id)
                .limit(add_limit)
                .with_for_update(skip_locked=True)
            )
            docs: List[Document] = list(res.scalars())

            if docs:
                now = utcnow()
                for d in docs:
                    # external id required for provenance/UI
                    if _external_document_id_best_effort(d) is None:
                        await log_index_error(
                            session,
                            stage=STAGE_BUILD_L1,
                            message="missing external document_id",
                            doc_id=d.id,
                            segment_id=buffer_segment_id,
                            payload={"org_id": org_id, "doc_db_id": int(d.id)},
                        )
                        d.status = "error"
                        d.updated_at = now
                        continue

                    d.segment_id = buffer_segment_id
                    d.updated_at = now
                    absorbed_ids.append(int(d.id))

                await session.commit()
            else:
                await session.commit()

        if not buffer_segment_id:
            continue

        if absorbed_ids:
            total_absorbed += len(absorbed_ids)

        ok = await _rebuild_segment_index(segment_id=buffer_segment_id, stage=STAGE_BUILD_L1)
        if not ok:
            logger.warning("[L1-BUFFER] rebuild failed org=%s seg_id=%s", org_id, buffer_segment_id)

    return total_absorbed


# ───────────────────────────────────────────────
# L2: promote full batches from L1 buffer to new L2 segments
# while buffer has >= DOCS_PER_L1_SEGMENT docs
# ───────────────────────────────────────────────

async def build_l2_segments() -> int:
    promoted_total = 0
    batch_n = max(1, DOCS_PER_L1_SEGMENT)

    async with AsyncSessionLocal() as session:
        org_rows = await session.execute(
            select(Document.organization_id)
            .where(
                Document.shard_id == SHARD_ID,
                Document.segment_id.is_not(None),
                Document.organization_id.is_not(None),
            )
            .distinct()
        )
        org_ids: List[int] = [int(r[0]) for r in org_rows.fetchall() if r[0] is not None]

    for org_id in org_ids:
        while True:
            async with AsyncSessionLocal() as session:
                buf = await _find_l1_buffer_segment(session, org_id=org_id)
                if not buf:
                    break

                # IMPORTANT: don't promote if buffer isn't in a healthy/ready state
                if buf.status != "ready":
                    break

                buffer_segment_id = int(buf.id)

                cnt = await session.execute(
                    select(func.count(Document.id))
                    .where(
                        Document.shard_id == SHARD_ID,
                        Document.organization_id == org_id,
                        Document.segment_id == buffer_segment_id,
                    )
                )
                count_in_buffer = int(cnt.scalar() or 0)
                if count_in_buffer < batch_n:
                    break

            moved_db_ids: List[int] = []
            new_l2_segment_id: Optional[int] = None

            # move one batch to a new L2 segment (DB)
            async with AsyncSessionLocal() as session:
                res_docs = await session.execute(
                    select(Document)
                    .where(
                        Document.shard_id == SHARD_ID,
                        Document.organization_id == org_id,
                        Document.segment_id == buffer_segment_id,
                    )
                    .order_by(Document.id)
                    .limit(batch_n)
                    .with_for_update(skip_locked=True)
                )
                docs_to_move: List[Document] = list(res_docs.scalars())
                if len(docs_to_move) < batch_n:
                    await session.commit()
                    break

                now = utcnow()
                new_seg = Segment(
                    organization_id=org_id,
                    shard_id=SHARD_ID,
                    level=2,
                    status="building",
                    path="",
                    doc_count=len(docs_to_move),
                    shingle_count=0,
                    size_bytes=0,
                    created_at=now,
                    last_compacted_at=None,
                    last_access_at=None,
                )
                session.add(new_seg)
                await session.flush()

                new_l2_segment_id = int(new_seg.id)
                _ = _segment_dir(org_id, SHARD_ID, new_l2_segment_id)
                new_seg.path = f"org_{org_id}/shard_{SHARD_ID}/segment_{new_l2_segment_id}"

                for d in docs_to_move:
                    d.segment_id = new_l2_segment_id
                    d.updated_at = now
                    moved_db_ids.append(int(d.id))

                await session.commit()

            assert new_l2_segment_id is not None

            ok_l2 = await _rebuild_segment_index(segment_id=new_l2_segment_id, stage=STAGE_COMPACT)
            if not ok_l2:
                # compensation: move docs back to buffer
                async with AsyncSessionLocal() as session:
                    now = utcnow()
                    seg2 = await session.get(Segment, new_l2_segment_id)
                    if seg2:
                        seg2.status = "error"
                    if moved_db_ids:
                        res_back = await session.execute(select(Document).where(Document.id.in_(moved_db_ids)))
                        for d in res_back.scalars():
                            d.segment_id = buffer_segment_id
                            d.updated_at = now
                    await log_index_error(
                        session,
                        stage=STAGE_COMPACT,
                        message="failed to build L2; moved docs back to L1 buffer",
                        doc_id=None,
                        segment_id=new_l2_segment_id,
                        payload={"org_id": org_id, "buffer_segment_id": buffer_segment_id, "docs": moved_db_ids},
                    )
                    await session.commit()

                # rebuild buffer to consistent state
                await _rebuild_segment_index(segment_id=buffer_segment_id, stage=STAGE_BUILD_L1)
                break

            # rebuild buffer after successful promotion
            await _rebuild_segment_index(segment_id=buffer_segment_id, stage=STAGE_BUILD_L1)
            promoted_total += batch_n

    return promoted_total


# ───────────────────────────────────────────────
# L3/L4 compaction (optional: merges N segments of same org)
# ───────────────────────────────────────────────

async def compact_segments_level(from_level: int) -> int:
    to_level = from_level + 1
    per_compact = cfg_segments_per_compact(from_level)

    async with AsyncSessionLocal() as session:
        org_rows = await session.execute(
            select(Segment.organization_id)
            .where(
                Segment.shard_id == SHARD_ID,
                Segment.level == from_level,
                Segment.status == "ready",
            )
            .group_by(Segment.organization_id)
            .having(func.count(Segment.id) >= per_compact)
        )
        org_ids: List[int] = [int(r[0]) for r in org_rows.fetchall() if r[0] is not None]

    if not org_ids:
        logger.info("[COMPACT L%s->L%s] no orgs with >=%s segments", from_level, to_level, per_compact)
        return 0

    total_docs_promoted = 0

    for org_id in org_ids:
        while True:
            new_segment_id: Optional[int] = None
            seg_ids: List[int] = []

            async with AsyncSessionLocal() as session:
                seg_rows = await session.execute(
                    select(Segment)
                    .where(
                        Segment.organization_id == org_id,
                        Segment.shard_id == SHARD_ID,
                        Segment.level == from_level,
                        Segment.status == "ready",
                    )
                    .order_by(Segment.id)
                    .limit(per_compact)
                    .with_for_update(skip_locked=True)
                )
                batch_segments: List[Segment] = list(seg_rows.scalars())
                if len(batch_segments) < per_compact:
                    break

                seg_ids = [int(s.id) for s in batch_segments]
                for s in batch_segments:
                    s.status = "building"
                await session.flush()

                doc_rows = await session.execute(
                    select(Document)
                    .join(SegmentDoc, SegmentDoc.document_id == Document.id)
                    .where(
                        SegmentDoc.segment_id.in_(seg_ids),
                        SegmentDoc.organization_id == org_id,
                        SegmentDoc.shard_id == SHARD_ID,
                        Document.shard_id == SHARD_ID,
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
                new_seg = Segment(
                    organization_id=org_id,
                    shard_id=SHARD_ID,
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
                session.add(new_seg)
                await session.flush()

                new_segment_id = int(new_seg.id)
                _ = _segment_dir(org_id, SHARD_ID, new_segment_id)
                new_seg.path = f"org_{org_id}/shard_{SHARD_ID}/segment_{new_segment_id}"

                # move docs to new segment
                now2 = utcnow()
                for d in docs:
                    d.segment_id = new_segment_id
                    d.updated_at = now2

                await session.commit()

            assert new_segment_id is not None

            ok = await _rebuild_segment_index(segment_id=new_segment_id, stage=STAGE_COMPACT)
            if not ok:
                async with AsyncSessionLocal() as session:
                    segx = await session.get(Segment, new_segment_id)
                    if segx:
                        segx.status = "error"
                    # release old segs
                    if seg_ids:
                        res_old = await session.execute(select(Segment).where(Segment.id.in_(seg_ids)))
                        for s in res_old.scalars():
                            s.status = "ready"
                    await log_index_error(
                        session,
                        stage=STAGE_COMPACT,
                        message="compaction build failed",
                        doc_id=None,
                        segment_id=new_segment_id,
                        payload={"org_id": org_id, "from_segments": seg_ids},
                    )
                    await session.commit()
                break

            # mark old segments merged
            async with AsyncSessionLocal() as session:
                now3 = utcnow()
                res_old = await session.execute(select(Segment).where(Segment.id.in_(seg_ids)))
                for s in res_old.scalars():
                    s.status = "merged"
                    s.last_compacted_at = now3
                await session.commit()

            total_docs_promoted += 1  # optional metric

    return total_docs_promoted


async def build_l3_segments() -> int:
    return await compact_segments_level(from_level=2)


async def build_l4_segments() -> int:
    return await compact_segments_level(from_level=3)
