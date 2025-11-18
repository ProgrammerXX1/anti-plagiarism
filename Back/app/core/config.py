# app/core/config.py
import os
from pathlib import Path
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from typing import List, Dict, Any

# env
load_dotenv()

ROOT = Path(
    os.environ.get(
        "PLAGIO_ROOT",
        Path(__file__).resolve().parents[2] / "runtime"
    )
)

UPLOAD_DIR = ROOT / "uploads"
CORPUS_DIR = ROOT / "corpus"
INDEX_DIR  = ROOT / "index" / "current"
SNAP_DIR   = ROOT / "index" / "snapshots"
QUEUE_DIR  = ROOT / "queue"
LOGS_DIR   = ROOT / "logs"
LOCKS_DIR  = ROOT / "locks"

# ensure dirs
for p in (
    UPLOAD_DIR,
    CORPUS_DIR,
    INDEX_DIR,
    SNAP_DIR,
    QUEUE_DIR / "normalize",
    QUEUE_DIR / "build_index",
    LOGS_DIR,
    LOCKS_DIR,
):
    p.mkdir(parents=True, exist_ok=True)

# files
CORPUS_JSONL  = CORPUS_DIR / "corpus.jsonl"
INDEX_JSON    = INDEX_DIR / "index.json"
MANIFEST_JSON = INDEX_DIR / "manifest.json"

# ── OCR дефолты ────────────────────────────────────────────────────────────────
OCR_LANG_DEFAULT = os.environ.get("PLAGIO_OCR_LANG", "kaz+rus+eng")
OCR_WORKERS_DEFAULT = int(os.environ.get("PLAGIO_OCR_WORKERS", "16"))

# ── Pydantic-конфиг индекса ───────────────────────────────────────────────────

class MinhashCfg(BaseModel):
    K: int = Field(gt=0, default=128)
    rows: int = Field(gt=0, default=4)
    seed: int = 1337
    hamming_bonus_bits: int = 6


class WeightsCfg(BaseModel):
    alpha: float = Field(ge=0.0, le=1.0, default=0.60)
    w13: float = 0.85
    w9: float = 0.90
    w5: float = 0.90


class ThresholdsCfg(BaseModel):
    plag_thr: float = 0.70
    partial_thr: float = 0.30


class IndexConfig(BaseModel):
    w_min_doc: int = Field(ge=1, default=8)
    w_min_query: int = Field(ge=1, default=5)
    k_list: List[int] = Field(default_factory=lambda: [5, 9, 13])

    weights: WeightsCfg = WeightsCfg()
    thresholds: ThresholdsCfg = ThresholdsCfg()
    minhash: MinhashCfg = MinhashCfg()

    simhash_bonus: float = 0.02

    fetch_per_k_doc: int = 1024
    fetch_per_k5_doc: int = 512
    max_cands_doc: int = 10_000
    fragments_for_top: int = 16


# дефолтный конфиг (dict, как и раньше)
DEFAULT_CFG_OBJ = IndexConfig()
DEFAULT_CFG: Dict[str, Any] = DEFAULT_CFG_OBJ.model_dump()


def ensure_index_cfg(cfg: Dict[str, Any] | None) -> Dict[str, Any]:
    """
    Нормализует и валидирует конфиг индекса.
    Если cfg=None — вернёт DEFAULT_CFG.
    Если передан словарь — провалидирует через IndexConfig и проверит K%rows==0.
    """
    if cfg is None:
        obj = DEFAULT_CFG_OBJ
    else:
        obj = IndexConfig(**cfg)

    m = obj.minhash
    if m.K % m.rows != 0:
        raise ValueError(f"Invalid MinHash/LSH config: K%rows!=0 (K={m.K}, rows={m.rows})")

    return obj.model_dump()
