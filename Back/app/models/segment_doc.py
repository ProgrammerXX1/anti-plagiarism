# app/models/segment_doc.py
from sqlalchemy import BigInteger, Integer, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class SegmentDoc(Base):
    __tablename__ = "segment_docs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

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

    shard_id: Mapped[int] = mapped_column(Integer, nullable=False)

    # связи (на будущее, можно не использовать прямо сейчас)
    segment: Mapped["Segment"] = relationship("Segment", back_populates="segment_docs")
    document: Mapped["Document"] = relationship("Document", back_populates="segment_docs")
