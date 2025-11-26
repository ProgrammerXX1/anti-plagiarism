# app/core/io_utils.py
import os, json, fcntl, time, hashlib, shutil
from pathlib import Path
from typing import Iterator
from contextlib import contextmanager


def sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


@contextmanager
def file_lock(path: Path):
    """
    Простой файловый лок (fcntl). Явно снимаем LOCK_UN в finally.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        fcntl.lockf(f, fcntl.LOCK_EX)
        try:
            yield
        finally:
            try:
                fcntl.lockf(f, fcntl.LOCK_UN)
            except OSError:
                # если файл уже закрыт/удалён — просто игнорируем
                pass


def atomic_write_json(path: Path, obj: dict):
    tmp = path.with_suffix(path.suffix + f".{int(time.time() * 1000)}.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)
    os.replace(tmp, path)


def atomic_append(path: Path, data: bytes):
    """
    Простейший «атомарный» append с файловым lock'ом на время записи.
    Для больших объёмов данных всё равно лучше сохранять снапшоты целиком.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(path.suffix + ".lock")
    with open(lock_path, "w") as lf:
        fcntl.lockf(lf, fcntl.LOCK_EX)
        try:
            with open(path, "ab") as f:
                f.write(data)
        finally:
            try:
                fcntl.lockf(lf, fcntl.LOCK_UN)
            except OSError:
                pass


def list_sorted(glob_iter: Iterator[Path]) -> list[Path]:
    return sorted([p for p in glob_iter if p.is_file()])
