# services/normalizer.py

import unicodedata, re
from typing import List

_ws = re.compile(r"\s+")
_nonw = re.compile(r"[^\w\s]+", flags=re.UNICODE)

_ZW = re.compile(r"[\u200B-\u200F\u202A-\u202E\u2060\uFEFF]")
_SOFT_HYPHEN = "\u00AD"
_NBSP = "\u00A0"

def normalize_nfkc_lower(s: str) -> str:
    # убрать zero-width и soft-hyphen, NBSP → пробел, потом NFKC+lower
    s = _ZW.sub("", s).replace(_SOFT_HYPHEN, "").replace(_NBSP, " ")
    return unicodedata.normalize("NFKC", s).lower()

def clean_spaces_punct(s: str) -> str:
    t = _nonw.sub(" ", s)
    return _ws.sub(" ", t).strip()

def simple_tokens(text: str) -> List[str]:
    return [t for t in _ws.split(text) if t]
