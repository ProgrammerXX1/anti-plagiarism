# app/models/index_task.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import Column, BigInteger, Text, Integer, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base

from app.db.base import Base  # если у тебя уже есть Base – используй его

class IndexTask(Base):
    __tablename__ = "index_tasks"

    id = Column(BigInteger, primary_key=True, index=True)
    task_type = Column(Text, nullable=False)
    status = Column(Text, nullable=False, default="pending")
    payload = Column(JSONB, nullable=False)
    attempts = Column(Integer, nullable=False, default=0)
    last_error = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False)
    started_at = Column(DateTime(timezone=True), nullable=True)
    finished_at = Column(DateTime(timezone=True), nullable=True)
