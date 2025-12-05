# app/scripts/enqueue_test_tasks.py
import asyncio
import app.models
from app.db.session import AsyncSessionLocal
from app.models.document import Document
from datetime import datetime, timezone

now = datetime.now(timezone.utc)

async def main() -> None:
    async with AsyncSessionLocal() as session:
        # тут твоя логика — примеры:
        # 1. создать тестовый документ
        doc = Document(
    external_id="test-doc-1",
    shard_id=0,
    status="uploaded",
    title="Test doc",
    created_at=now,
    updated_at=now,
)
        session.add(doc)
        await session.commit()
        print("Test document enqueued")

if __name__ == "__main__":
    asyncio.run(main())
