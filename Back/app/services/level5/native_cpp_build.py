from __future__ import annotations

import subprocess
import time
from pathlib import Path
from typing import Dict, Any

from app.core.logger import logger


def _find_index_builder() -> Path:
    """
    Ищем бинарь index_builder в нескольких типичных местах.
    Возвращаем первый существующий путь или кидаем RuntimeError.
    """
    here = Path(__file__).resolve()

    candidates = [
        Path("/usr/local/bin/index_builder"),
        here.parents[3] / "build" / "index_builder",
        here.parents[2] / "build" / "index_builder",
        here.parents[3] / "cpp" / "build" / "index_builder",
    ]

    existing = [p for p in candidates if p.exists()]
    if not existing:
        msg = "index_builder not found. Tried:\n" + "\n".join(str(p) for p in candidates)
        raise RuntimeError(msg)

    return existing[0]


def build_native_index_cpp(index_dir: Path, corpus_path: Path) -> Dict[str, Any]:
    """
    Запускает C++-бинарь index_builder, который:
      - читает corpus_path (corpus.jsonl)
      - пишет index_native.bin, index_native_docids.json, index_native_meta.json
        в директорию index_dir.
    """
    bin_path = _find_index_builder()

    corpus_path = Path(corpus_path)
    if not corpus_path.exists():
        raise RuntimeError(f"corpus.jsonl not found at {corpus_path}")

    index_dir = Path(index_dir)
    index_dir.mkdir(parents=True, exist_ok=True)

    cmd = [str(bin_path), str(corpus_path), str(index_dir)]
    logger.info("[L5/native_cpp] run: %s", " ".join(cmd))

    t0 = time.monotonic()
    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    t1 = time.monotonic()

    if proc.returncode != 0:
        logger.error(
            "[L5/native_cpp] index_builder failed rc=%s\nstdout:\n%s\nstderr:\n%s",
            proc.returncode,
            proc.stdout,
            proc.stderr,
        )
        raise RuntimeError(f"index_builder failed rc={proc.returncode}")

    duration = round(t1 - t0, 3)
    logger.info(
        "[L5/native_cpp] done rc=%s duration=%.3fs\nstdout:\n%s",
        proc.returncode,
        duration,
        proc.stdout.strip(),
    )

    return {
        "duration_sec": duration,
        "rc": proc.returncode,
        "corpus_path": str(corpus_path),
        "index_dir": str(index_dir),
    }
