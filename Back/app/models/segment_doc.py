from sqlalchemy import BigInteger, Integer, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class SegmentDoc(Base):
    __tablename__ = "segment_docs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # NEW
    organization_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)

    segment_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("segments.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    document_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("documents.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    shard_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    segment: Mapped["Segment"] = relationship("Segment", back_populates="segment_docs")
    document: Mapped["Document"] = relationship("Document", back_populates="segment_docs")
