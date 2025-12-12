# app/workers/index_worker.py
from __future__ import annotations

import asyncio

from app.core.config import MAX_AUTO_LEVEL
from app.services.levels0_4.etl_service import process_uploaded_docs
from app.services.levels0_4.segments_service import (
    build_l1_segments,
    build_l2_segments,
    build_l3_segments,
    build_l4_segments,
)


async def main_loop() -> None:
    print("[worker] index_worker стартовал")
    while True:
        etl_cnt = l1_cnt = l2_cnt = l3_cnt = l4_cnt = 0
        try:
            etl_cnt = await process_uploaded_docs()
            l1_cnt = await build_l1_segments()

            if MAX_AUTO_LEVEL >= 2:
                l2_cnt = await build_l2_segments()
            if MAX_AUTO_LEVEL >= 3:
                l3_cnt = await build_l3_segments()
            if MAX_AUTO_LEVEL >= 4:
                l4_cnt = await build_l4_segments()

            print(
                f"[worker] tick: etl={etl_cnt}, l1={l1_cnt}, "
                f"l2={l2_cnt}, l3={l3_cnt}, l4={l4_cnt}"
            )
        except Exception as e:
            print(f"[worker] ERROR в main_loop: {e}")

        # idle/backoff
        if etl_cnt == 0 and l1_cnt == 0 and l2_cnt == 0 and l3_cnt == 0 and l4_cnt == 0:
            await asyncio.sleep(3)
        else:
            await asyncio.sleep(0.1)


if __name__ == "__main__":
    asyncio.run(main_loop())
