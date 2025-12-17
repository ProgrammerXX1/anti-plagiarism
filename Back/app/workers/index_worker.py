# app/workers/index_worker.py
from __future__ import annotations

import asyncio
import traceback

from app.core.config import MAX_AUTO_LEVEL
from app.services.levels_0_4.etl_service import process_uploaded_docs
from app.services.levels_0_4.segment_service import (
    build_l1_segments,
    build_l2_segments,
    build_l3_segments,
    build_l4_segments,
)


async def main_loop() -> None:
    print("[worker] index_worker стартовал")
    idle_ticks = 0

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

            if etl_cnt or l1_cnt or l2_cnt or l3_cnt or l4_cnt:
                print(
                    f"[worker] tick: etl={etl_cnt}, l1={l1_cnt}, "
                    f"l2={l2_cnt}, l3={l3_cnt}, l4={l4_cnt}"
                )
        except Exception as e:
            print(f"[worker] ERROR в main_loop: {e}")
            traceback.print_exc()

        if etl_cnt == 0 and l1_cnt == 0 and l2_cnt == 0 and l3_cnt == 0 and l4_cnt == 0:
            idle_ticks += 1
            # плавный backoff, максимум 3 секунды
            await asyncio.sleep(min(3.0, 0.5 + idle_ticks * 0.1))
        else:
            idle_ticks = 0
            await asyncio.sleep(0.1)


if __name__ == "__main__":
    asyncio.run(main_loop())
