# app/services/simhash.py
import hashlib
from typing import List, Tuple
from functools import lru_cache

@lru_cache(maxsize=1<<18)  # достаточно ~262k уникальных токенов
def _sha1_16(tok: str) -> int:
    return int.from_bytes(hashlib.sha1(tok.encode("utf-8")).digest()[:16], "big")

def simhash128(tokens: list[str]) -> str:
    v = [0]*128
    for tok in tokens:
        bits = _sha1_16(tok)
        # цикл по 128 бит — прост и быстрый в CPython
        for i in range(128):
            v[i] += 1 if ((bits >> i) & 1) else -1
    x = 0
    for i in range(128):
        if v[i] >= 0: x |= (1<<i)
    return format(x, "032x")


def hamming_hex128(a_hex: str, b_hex: str) -> int:
    return (int(a_hex, 16) ^ int(b_hex, 16)).bit_count()
