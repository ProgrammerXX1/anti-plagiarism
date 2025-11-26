# app/services/helpers/minhash.py
import hashlib, random
from typing import Tuple, List, Set, Dict, Any

try:
    import numpy as np  # используем если доступен
    _HAS_NUMPY = True
except Exception:  # noqa
    _HAS_NUMPY = False


def make_AB(K: int, seed: int = 1337) -> Tuple[List[int], List[int]]:
    rnd = random.Random(seed)
    # 32-битные коэффициенты достаточно и быстрее
    A = [(rnd.getrandbits(64) | 1) & 0xffffffff for _ in range(K)]
    B = [rnd.getrandbits(64) & 0xffffffff for _ in range(K)]
    return A, B


def _validate_lsh_shapes(sig: List[int], bands: List[Dict[str, Any]], rows: int) -> None:
    K = len(sig)
    if rows <= 0 or K % rows != 0:
        raise ValueError(f"K%rows!=0: K={K}, rows={rows}")
    need = K // rows
    if len(bands) != need:
        raise ValueError(f"bands mismatch: have {len(bands)}, need {need}")


def get_lsh_candidates(sig: List[int], bands: List[Dict[str, Any]], rows: int) -> set[str]:
    _validate_lsh_shapes(sig, bands, rows)
    out: set[str] = set()
    step = rows
    for b in range(len(bands)):
        st = b * step
        en = st + step
        chunk = sig[st:en]
        k = hashlib.sha1(b"".join(int(x).to_bytes(4, "big") for x in chunk)).digest()[:8].hex()
        lst = bands[b]["buckets"].get(k)
        if lst:
            out.update(lst)
    return out


def minhash_signature_from_set_fast(
    hset: Set[int],
    A: List[int],
    B: List[int],
    batch: int = 4096
) -> List[int]:
    """MinHash сигнатура множества 64-битных хэшей.
    Быстрый путь на NumPy, фолбэк — чистый Python. Возвращает список длины len(A).
    """
    K = len(A)

    # пустое множество
    if not hset:
        return [0xffffffff] * K

    if _HAS_NUMPY:
        # (K,1)
        A_arr = np.array(A, dtype=np.uint64)[:, None]
        B_arr = np.array(B, dtype=np.uint64)[:, None]

        # (|S|,)
        hs = np.fromiter(hset, dtype=np.uint64, count=-1)
        if hs.size == 0:
            return [0xffffffff] * K

        sig = np.full((K,), np.uint32(0xffffffff), dtype=np.uint32)

        SHIFT = np.uint64(32)
        MASK32 = np.uint64(0xffffffff)

        # батчами чтобы не дуть память
        for i in range(0, hs.size, batch):
            chunk = hs[i:i + batch][None, :]          # (1,b) -> (K,b) бродкастом
            vals = A_arr * chunk + B_arr              # uint64, overflow по 2**64
            vals32 = np.bitwise_and(vals ^ (vals >> SHIFT), MASK32).astype(np.uint32)
            sig = np.minimum(sig, vals32.min(axis=1))

        return sig.tolist()

    # --- фолбэк без NumPy ---
    sig = [0xffffffff] * K
    for i, (a, b) in enumerate(zip(A, B)):
        m = 0xffffffff
        for h in hset:
            val = ((a * (h & 0xffffffffffffffff)) + b) & 0xffffffffffffffff
            v32 = ((val ^ (val >> 32)) & 0xffffffff)
            if v32 < m:
                m = v32
        sig[i] = m
    return sig
