# app/core/settings_index.py
import os
import hashlib

NUM_SHARDS = int(os.getenv("PLAGIO_NUM_SHARDS", "8"))  # регулируешь по серверу


def calc_shard_id_from_meta(
    *,
    university: str | None,
    faculty: str | None,
    group_name: str | None,
) -> int:
    """
    Простой стабильный shard_id по (университет, факультет, группа).
    Если мета пустая – сваливаемся в shard 0.
    """
    key = "|".join([
        university or "",
        faculty or "",
        group_name or "",
    ])
    if not key.strip():
        return 0

    h = hashlib.md5(key.encode("utf-8")).hexdigest()
    # берем нижние 8 байт как число
    v = int(h[:16], 16)
    return v % NUM_SHARDS
