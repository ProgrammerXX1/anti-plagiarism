# app/workers/queue.py
import json, os
from pathlib import Path
from typing import Optional, Dict, Any
from ..core.config import QUEUE_DIR
from ..core.io_utils import list_sorted

def enqueue(kind: str, payload: Dict[str, Any]) -> Path:
    qdir = QUEUE_DIR / kind
    qdir.mkdir(parents=True, exist_ok=True)
    name = f"job_{os.getpid()}_{abs(hash(str(payload))) & 0xffffffff:08x}.json"
    path = qdir / name
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    return path

def dequeue(kind: str) -> Optional[tuple[Path, Dict[str, Any]]]:
    qdir = QUEUE_DIR / kind
    jobs = list_sorted(qdir.glob("*.json"))
    if not jobs:
        return None
    path = jobs[0]
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        data = {}
    lock_path = path.with_suffix(".lock")
    path.rename(lock_path)
    return lock_path, data
