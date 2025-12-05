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
INDEX_JSON    = INDEX_DIR / "index.json"           # используется только как "маяк" директории
MANIFEST_JSON = INDEX_DIR / "manifest.json"

# ограничитель индексирований по батчам
MAX_NEW_DOCS_PER_BUILD = int(os.environ.get("PLAGIO_MAX_NEW_DOCS_PER_BUILD", "100"))

# ── BM25 индекс (если используешь BM25 как кандидат-генератор) ───────────────
BM25_INDEX = INDEX_DIR / "bm25_index.pkl.gz"  # бинарный, сжатый

BM25_K1 = float(os.environ.get("PLAGIO_BM25_K1", "1.5"))
BM25_B  = float(os.environ.get("PLAGIO_BM25_B", "0.75"))
BM25_MAX_DOCS = int(os.environ.get("PLAGIO_BM25_MAX_DOCS", "200"))  # сколько доков отдавать кандидатом

# ── OCR дефолты ───────────────────────────────────────────────────────────────
OCR_LANG_DEFAULT = os.environ.get("PLAGIO_OCR_LANG", "kaz+rus+eng")
OCR_WORKERS_DEFAULT = int(os.environ.get("PLAGIO_OCR_WORKERS", "12"))

# ── Semantic / Reranker config ────────────────────────────────────────────────
SEMANTIC_BASE_URL = os.environ.get("PLAGIO_SEMANTIC_URL", "http://localhost:8003")
USE_SEMANTIC_RERANK = os.environ.get("PLAGIO_USE_SEMANTIC_RERANK", "false").lower() in {
    "1", "true", "yes", "y",
}
USE_SEMANTIC_EMBED = os.environ.get("PLAGIO_USE_SEMANTIC_EMBED", "false").lower() in {
    "1", "true", "yes", "y",
}
# коэффициент смешивания шинглового и семантического скорa
SEMANTIC_ALPHA = float(os.environ.get("PLAGIO_SEMANTIC_ALPHA", "0.7"))
# максимум документов, которые отправляем в реранкер поверх top
SEMANTIC_TOP_K = int(os.environ.get("PLAGIO_SEMANTIC_TOP_K", "20"))
# максимум фрагментов на документ, которые шлём в реранкер
SEMANTIC_FRAG_PER_DOC = int(os.environ.get("PLAGIO_SEMANTIC_FRAG_PER_DOC", "3"))

PG_HOST = os.getenv("PG_HOST", "192.168.75.70")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "oysyn")
PG_USER = os.getenv("PG_USER", "oysyn")
PG_PASS = os.getenv("PG_PASS", "2123")
N_SHARDS = int(os.environ.get("PLAGIO_N_SHARDS", "4"))

# классический URL для SQLAlchemy
DATABASE_URL_SYNC  = os.getenv(
    "DATABASE_URL_SYNC",
    f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}",
)

DATABASE_URL_ASYNC = os.getenv(
    "DATABASE_URL_ASYNC",
    f"postgresql+asyncpg://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}",
)

# ── Pydantic-конфиг индекса (под чистый C++ k=9) ─────────────────────────────

class WeightsCfg(BaseModel):
    # общий коэффициент смешивания интерпретируем на стороне C++
    alpha: float = Field(ge=0.0, le=1.0, default=0.60)
    # вес k=9 (k=13/k=5 больше нет)
    w9: float = 0.90


class ThresholdsCfg(BaseModel):
    plag_thr: float = 0.70
    partial_thr: float = 0.30


class IndexConfig(BaseModel):
    # минимальная длина документа/запроса в токенах
    w_min_doc: int = Field(ge=1, default=8)
    w_min_query: int = Field(ge=1, default=9)

    # список k на будущее, но по факту используем только k=9
    k_list: List[int] = Field(default_factory=lambda: [9])

    weights: WeightsCfg = WeightsCfg()
    thresholds: ThresholdsCfg = ThresholdsCfg()

    # бонус за близкий simhash (используется в C++)
    simhash_bonus: float = 0.0

    # сколько кандидатов за "единицу длины" (имя синхронизировано с C++ Config.fetch_per_k)
    fetch_per_k: int = 64

    # максимум кандидатов на документ
    max_cands_doc: int = 1000

    # сколько фрагментов в итоге вытаскиваем на документ
    fragments_for_top: int = 1


# дефолтный конфиг (dict, как и раньше)
DEFAULT_CFG_OBJ = IndexConfig()
DEFAULT_CFG: Dict[str, Any] = DEFAULT_CFG_OBJ.model_dump()


def ensure_index_cfg(cfg: Dict[str, Any] | None) -> Dict[str, Any]:
    """
    Нормализует и валидирует конфиг индекса.
    Если cfg=None — вернёт DEFAULT_CFG (IndexConfig дефолт).
    Если передан словарь — провалидирует через IndexConfig.
    """
    if cfg is None:
        obj = DEFAULT_CFG_OBJ
    else:
        obj = IndexConfig(**cfg)

    return obj.model_dump()
