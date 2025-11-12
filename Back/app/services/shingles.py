import hashlib
from typing import List, Dict

def _sha1_first8_u64(s: str) -> int:
    h = hashlib.sha1(s.encode("utf-8")).digest()
    return int.from_bytes(h[:8], "big", signed=False)

def build_shingles(tokens: List[str], k: int) -> List[int]:
    n = len(tokens)
    if n < k: return []
    join = " ".join
    _sha = hashlib.sha1
    out = []
    for i in range(n - k + 1):
        h = _sha(join(tokens[i:i+k]).encode("utf-8")).digest()
        out.append(int.from_bytes(h[:8], "big"))
    return out

def build_shingles_multi(tokens: List[str], k_list: List[int]) -> Dict[int, List[int]]:
    """Вернёт {k: [hash,...]} за один линейный проход."""
    if not k_list: return {}
    n = len(tokens)
    res: Dict[int, List[int]] = {k: [] for k in k_list}
    if n < min(k_list): 
        return {k: [] for k in k_list}
    max_k = max(k_list)
    join = " ".join
    _sha = hashlib.sha1
    # основная полоса
    for i in range(0, n - max_k + 1):
        window = tokens[i:i+max_k]
        acc = {}
        for k in sorted(k_list):
            s = acc.get(k)
            if s is None:
                s = join(window[:k])
                acc[k] = s
            h = _sha(s.encode("utf-8")).digest()
            res[k].append(int.from_bytes(h[:8], "big"))
    # хвосты для k < max_k
    tail_start = max(0, n - max_k + 1)
    for k in k_list:
        for i in range(tail_start, n - k + 1):
            h = _sha(join(tokens[i:i+k]).encode("utf-8")).digest()
            res[k].append(int.from_bytes(h[:8], "big"))
    return res
