from datetime import datetime
from typing import Optional, List

from sqlalchemy import (
    BigInteger,
    Integer,
    SmallInteger,
    String,
    Text,
    DateTime,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class Segment(Base):
    __tablename__ = "segments"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # NEW
    organization_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)

    shard_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    level: Mapped[int] = mapped_column(SmallInteger, nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, index=True)

    path: Mapped[str] = mapped_column(Text, nullable=False)
    doc_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    shingle_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_compacted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_access_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    # relations
    segment_docs: Mapped[List["SegmentDoc"]] = relationship(
        "SegmentDoc",
        back_populates="segment",
        cascade="all, delete-orphan",
    )

    documents: Mapped[List["Document"]] = relationship(
        "Document",
        back_populates="segment",
    )
