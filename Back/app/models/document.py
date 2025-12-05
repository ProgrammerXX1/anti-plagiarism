# app/models/document.py
from datetime import datetime
from typing import Optional, List

from sqlalchemy import (
    BigInteger,
    Integer,
    String,
    Text,
    DateTime,
    ForeignKey,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class Document(Base):
    __tablename__ = "documents"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    external_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    shard_id: Mapped[int] = mapped_column(Integer, nullable=False)

    segment_id: Mapped[Optional[int]] = mapped_column(
        BigInteger,
        ForeignKey("segments.id", ondelete="SET NULL"),
        nullable=True,
    )

    status: Mapped[str] = mapped_column(String(16), nullable=False)

    simhash_hi: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    simhash_lo: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    last_checked_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    title: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    student_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    university: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    faculty: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    group_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    segment: Mapped[Optional["Segment"]] = relationship("Segment", backref="documents")

    # связь с SegmentDoc
    segment_docs: Mapped[List["SegmentDoc"]] = relationship(
        "SegmentDoc",
        back_populates="document",
        cascade="all, delete-orphan",
    )
