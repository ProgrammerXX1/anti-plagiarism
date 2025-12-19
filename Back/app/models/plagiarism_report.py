from __future__ import annotations

from datetime import datetime
from sqlalchemy import (
    BigInteger, Integer, Text, DateTime, Float, ForeignKey, UniqueConstraint, Index
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base  # где у тебя declarative base

class PlagiarismReport(Base):
    __tablename__ = "plagiarism_reports"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    shard_id: Mapped[int] = mapped_column(Integer, nullable=False)

    document_id: Mapped[str] = mapped_column(Text, nullable=False)  # внешний id запроса
    internal_doc_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    status: Mapped[str] = mapped_column(Text, nullable=False)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    plagiarism_percentage: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    selfcite_percentage: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    legal_percentage: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    unknown_percentage: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    sources = relationship("PlagiarismReportSource", back_populates="report", cascade="all, delete-orphan")
    matches = relationship("PlagiarismReportMatch", back_populates="report", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("organization_id", "document_id", name="uq_plagiarism_reports_org_doc"),
    )


class PlagiarismReportSource(Base):
    __tablename__ = "plagiarism_report_sources"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    report_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("plagiarism_reports.id", ondelete="CASCADE"), nullable=False)

    source_id: Mapped[str] = mapped_column(Text, nullable=False)
    internal_source_doc_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    module_id: Mapped[str] = mapped_column(Text, nullable=False, default="plagiarism")
    name: Mapped[str] = mapped_column(Text, nullable=False, default="")
    url: Mapped[str | None] = mapped_column(Text, nullable=True)
    author: Mapped[str | None] = mapped_column(Text, nullable=True)
    index_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    report = relationship("PlagiarismReport", back_populates="sources")

    __table_args__ = (
        UniqueConstraint("report_id", "source_id", name="uq_pr_sources_report_source"),
    )


class PlagiarismReportMatch(Base):
    __tablename__ = "plagiarism_report_matches"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    report_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("plagiarism_reports.id", ondelete="CASCADE"), nullable=False)

    source_id: Mapped[str] = mapped_column(Text, nullable=False)

    q_offset: Mapped[int] = mapped_column(Integer, nullable=False)
    q_limit: Mapped[int] = mapped_column(Integer, nullable=False)
    s_offset: Mapped[int] = mapped_column(Integer, nullable=False)
    s_limit: Mapped[int] = mapped_column(Integer, nullable=False)

    type: Mapped[str] = mapped_column(Text, nullable=False, default="1")

    q_from: Mapped[int | None] = mapped_column(Integer, nullable=True)
    q_to: Mapped[int | None] = mapped_column(Integer, nullable=True)
    d_from: Mapped[int | None] = mapped_column(Integer, nullable=True)
    d_to: Mapped[int | None] = mapped_column(Integer, nullable=True)

    report = relationship("PlagiarismReport", back_populates="matches")

    __table_args__ = (
        Index("idx_prm_report_id", "report_id"),
        Index("idx_prm_report_source", "report_id", "source_id"),
    )
