# app/models/segment_doc.py
from __future__ import annotations

from sqlalchemy import BigInteger, Integer, ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class SegmentDoc(Base):
    __tablename__ = "segment_docs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    organization_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
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

    segment: Mapped["Segment"] = relationship("Segment", back_populates="segment_docs")
    document: Mapped["Document"] = relationship("Document", back_populates="segment_docs")

    __table_args__ = (
        UniqueConstraint("segment_id", "document_id", name="uq_segment_docs_segment_document"),
        # компактация/overview: достать документы сегмента
        Index("idx_segment_docs_org_segment", "organization_id", "segment_id"),
        # иногда нужно понять, в каких сегментах участвует doc
        Index("idx_segment_docs_doc", "document_id"),
        # если используешь shard фильтры отдельно (можно убрать, если не надо)
        Index("idx_segment_docs_org_shard", "organization_id", "shard_id"),
    )
