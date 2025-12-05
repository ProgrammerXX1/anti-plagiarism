# app/models/segment_doc.py
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Integer,
    DateTime,
    ForeignKey,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class SegmentDoc(Base):
    __tablename__ = "segment_docs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    shard_id: Mapped[int] = mapped_column(Integer, nullable=False)

    segment_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("segments.id", ondelete="CASCADE"),
        nullable=False,
    )

    document_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("documents.id", ondelete="CASCADE"),
        nullable=False,
    )

    # на будущее: длина и кол-во шинглов — пригодится для статистики/поиска
    doc_length: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    shingle_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

    # связи
    segment: Mapped["Segment"] = relationship("Segment", back_populates="segment_docs")
    document: Mapped["Document"] = relationship("Document", back_populates="segment_docs")
