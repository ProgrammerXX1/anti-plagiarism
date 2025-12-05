# app/models/index_error.py
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    BigInteger,
    Integer,
    String,
    Text,
    DateTime,
    ForeignKey,
    JSON,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

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
    payload: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
