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
INDEX_JSON = ROOT / "index" / "current" / "index.json"
MANIFEST_JSON = INDEX_DIR / "manifest.json"

# ограничитель индексирований по батчам
MAX_NEW_DOCS_PER_BUILD = int(os.environ.get("PLAGIO_MAX_NEW_DOCS_PER_BUILD", "100"))

# ── BM25 индекс ───────────────────────────────────────────────────────────────
BM25_INDEX = INDEX_DIR / "bm25_index.pkl.gz"  # бинарный, сжатый

# Можно сразу завести дефолтные параметры BM25
BM25_K1 = float(os.environ.get("PLAGIO_BM25_K1", "1.5"))
BM25_B  = float(os.environ.get("PLAGIO_BM25_B", "0.75"))
BM25_MAX_DOCS = int(os.environ.get("PLAGIO_BM25_MAX_DOCS", "200"))  # сколько доков отдавать кандидатом

# ── OCR дефолты ────────────────────────────────────────────────────────────────
OCR_LANG_DEFAULT = os.environ.get("PLAGIO_OCR_LANG", "kaz+rus+eng")
OCR_WORKERS_DEFAULT = int(os.environ.get("PLAGIO_OCR_WORKERS", "12"))

# ── Semantic / Reranker config ────────────────────────────────────────────────
SEMANTIC_BASE_URL = os.environ.get("PLAGIO_SEMANTIC_URL", "http://localhost:8003")
USE_SEMANTIC_RERANK = os.environ.get("PLAGIO_USE_SEMANTIC_RERANK", "false").lower() in {"1", "true", "yes", "y",}
USE_SEMANTIC_EMBED = os.environ.get("PLAGIO_USE_SEMANTIC_EMBED", "false").lower() in {"1", "true", "yes", "y",}
# коэффициент смешивания шинглового и семантического скорa
SEMANTIC_ALPHA = float(os.environ.get("PLAGIO_SEMANTIC_ALPHA", "0.7"))
# максимум документов, которые отправляем в реранкер поверх top
SEMANTIC_TOP_K = int(os.environ.get("PLAGIO_SEMANTIC_TOP_K", "20"))
# максимум фрагментов на документ, которые шлём в реранкер
SEMANTIC_FRAG_PER_DOC = int(os.environ.get("PLAGIO_SEMANTIC_FRAG_PER_DOC", "3"))

# ── Pydantic-конфиг индекса ───────────────────────────────────────────────────
class MinhashCfg(BaseModel):
    K: int = Field(gt=0, default=64)
    rows: int = Field(gt=0, default=4)
    seed: int = 1337
    hamming_bonus_bits: int = 6
    store_sig: bool = False
    use_lsh: bool = False          # ← новый флаг: использовать ли LSH вообще
    use_minhash_est: bool = False  # ← считать ли minhash_sim_est в деталях

class WeightsCfg(BaseModel):
    alpha: float = Field(ge=0.0, le=1.0, default=0.60)
    w13: float = 0.85
    w9: float = 0.90
    # w5: float = 0.90
class ThresholdsCfg(BaseModel):
    plag_thr: float = 0.70
    partial_thr: float = 0.30
class IndexConfig(BaseModel):
    w_min_doc: int = Field(ge=1, default=8)
    w_min_query: int = Field(ge=1, default=9)
    k_list: List[int] = Field(default_factory=lambda: [13])
    weights: WeightsCfg = WeightsCfg()
    thresholds: ThresholdsCfg = ThresholdsCfg()
    minhash: MinhashCfg = MinhashCfg()
    simhash_bonus: float = 0.0
    fetch_per_k_doc: int = 64
    fetch_per_k5_doc: int = 64
    max_cands_doc: int = 1000
    fragments_for_top: int = 1

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
