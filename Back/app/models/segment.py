# app/models/segment.py
from __future__ import annotations

from datetime import datetime
from typing import Optional, List

from sqlalchemy import BigInteger, Integer, SmallInteger, String, Text, DateTime, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class Segment(Base):
    __tablename__ = "segments"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    organization_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    shard_id: Mapped[int] = mapped_column(Integer, nullable=False)

    level: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)

    path: Mapped[str] = mapped_column(Text, nullable=False)

    doc_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    shingle_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_compacted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_access_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    segment_docs: Mapped[List["SegmentDoc"]] = relationship(
        "SegmentDoc",
        back_populates="segment",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    documents: Mapped[List["Document"]] = relationship(
        "Document",
        back_populates="segment",
    )

    __table_args__ = (
        # основной индекс: выбор ready сегментов по (org, shard), сортировка по level/id
        Index("idx_segments_org_shard_status_level", "organization_id", "shard_id", "status", "level"),
        # полезно при компактации: найти сегменты конкретного уровня (org, shard, level, status)
        Index("idx_segments_org_shard_level_status", "organization_id", "shard_id", "level", "status"),
    )
