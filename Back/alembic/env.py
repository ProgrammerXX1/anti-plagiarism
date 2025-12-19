# alembic/env.py
from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool
from dotenv import load_dotenv
from app.db.base import Base

# ВАЖНО: импортируем модели (регистрация в metadata)
from app.models.document import Document  # noqa: F401
from app.models.segment import Segment  # noqa: F401
from app.models.segment_doc import SegmentDoc  # noqa: F401
from app.models.index_error import IndexError  # noqa: F401
from app.models.plagiarism_report import (  # noqa: F401
    PlagiarismReport,
    PlagiarismReportSource,
    PlagiarismReportMatch,
)
load_dotenv()
config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _build_url_from_pg_env() -> str:
    host = os.getenv("PG_HOST")
    port = os.getenv("PG_PORT", "5432")
    db = os.getenv("PG_DB")
    user = os.getenv("PG_USER")
    pw = os.getenv("PG_PASS")

    missing = [k for k, v in [("PG_HOST", host), ("PG_DB", db), ("PG_USER", user), ("PG_PASS", pw)] if not v]
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")

    return f"postgresql+psycopg://{user}:{pw}@{host}:{port}/{db}"


def get_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    return _build_url_from_pg_env()


def run_migrations_offline() -> None:
    context.configure(
        url=get_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    section = config.get_section(config.config_ini_section) or {}
    section["sqlalchemy.url"] = get_url()

    connectable = engine_from_config(
        section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        future=True,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
