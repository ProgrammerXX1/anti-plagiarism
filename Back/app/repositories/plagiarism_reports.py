from __future__ import annotations

from datetime import datetime
from typing import Iterable

from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.plagiarism_report import PlagiarismReport, PlagiarismReportSource, PlagiarismReportMatch

async def upsert_report(
    db: AsyncSession,
    *,
    organization_id: int,
    shard_id: int,
    document_id: str,
    status: str,
    processed_at: datetime,
    plagiarism_percentage: float,
    selfcite_percentage: float,
    legal_percentage: float,
    unknown_percentage: float,
    internal_doc_id: int | None = None,
) -> PlagiarismReport:
    res = await db.execute(
        select(PlagiarismReport).where(
            PlagiarismReport.organization_id == organization_id,
            PlagiarismReport.document_id == document_id,
        )
    )
    report = res.scalar_one_or_none()

    if report is None:
        report = PlagiarismReport(
            organization_id=organization_id,
            shard_id=shard_id,
            document_id=document_id,
            internal_doc_id=internal_doc_id,
            status=status,
            processed_at=processed_at,
            plagiarism_percentage=plagiarism_percentage,
            selfcite_percentage=selfcite_percentage,
            legal_percentage=legal_percentage,
            unknown_percentage=unknown_percentage,
        )
        db.add(report)
        await db.flush()
        return report

    report.shard_id = shard_id
    report.internal_doc_id = internal_doc_id
    report.status = status
    report.processed_at = processed_at
    report.plagiarism_percentage = plagiarism_percentage
    report.selfcite_percentage = selfcite_percentage
    report.legal_percentage = legal_percentage
    report.unknown_percentage = unknown_percentage
    await db.flush()
    return report


async def replace_report_children(
    db: AsyncSession,
    *,
    report_id: int,
    sources: Iterable[PlagiarismReportSource],
    matches: Iterable[PlagiarismReportMatch],
) -> None:
    # чистим старые
    await db.execute(delete(PlagiarismReportMatch).where(PlagiarismReportMatch.report_id == report_id))
    await db.execute(delete(PlagiarismReportSource).where(PlagiarismReportSource.report_id == report_id))

    # вставляем новые
    for s in sources:
        s.report_id = report_id
        db.add(s)

    for m in matches:
        m.report_id = report_id
        db.add(m)

    await db.flush()
