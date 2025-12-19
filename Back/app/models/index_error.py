# app/models/index_error.py
from __future__ import annotations

from datetime import datetime
from typing import Optional, Any, Dict

from sqlalchemy import BigInteger, Integer, String, Text, DateTime, ForeignKey, JSON, Index
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class IndexError(Base):
    __tablename__ = "index_errors"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    doc_id: Mapped[Optional[int]] = mapped_column(
        BigInteger,
        ForeignKey("documents.id", ondelete="SET NULL"),
        nullable=True,
    )
    segment_id: Mapped[Optional[int]] = mapped_column(
        BigInteger,
        ForeignKey("segments.id", ondelete="SET NULL"),
        nullable=True,
    )

    stage: Mapped[str] = mapped_column(String(16), nullable=False)
    error_code: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    payload: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    __table_args__ = (
        Index("idx_index_errors_stage_created", "stage", "created_at"),
        Index("idx_index_errors_doc_id", "doc_id"),
        Index("idx_index_errors_segment_id", "segment_id"),
        Index("idx_index_errors_created_at", "created_at"),
    )
