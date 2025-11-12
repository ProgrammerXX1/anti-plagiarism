import os, json, fcntl, time, hashlib, shutil
from pathlib import Path
from typing import Iterator
from contextlib import contextmanager

def sha256_bytes(b: bytes) -> str:
    import hashlib
    h = hashlib.sha256(); h.update(b); return h.hexdigest()

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1<<20), b""):
            h.update(chunk)
    return h.hexdigest()

@contextmanager
def file_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        fcntl.lockf(f, fcntl.LOCK_EX)
        try:
            yield
        finally:
            pass

def atomic_write_json(path: Path, obj: dict):
    tmp = path.with_suffix(path.suffix + f".{int(time.time()*1000)}.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)
    os.replace(tmp, path)

def atomic_append(path: Path, data: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "ab") as f:
        f.write(data)
    with open(path, "ab") as f:
        with open(tmp, "rb") as tf:
            shutil.copyfileobj(tf, f)
    tmp.unlink(missing_ok=True)

def list_sorted(glob_iter: Iterator[Path]) -> list[Path]:
    return sorted([p for p in glob_iter if p.is_file()])
