import os
from pathlib import Path
from dotenv import load_dotenv

# env
load_dotenv()
ROOT = Path(os.environ.get("PLAGIO_ROOT", Path(__file__).resolve().parents[2] / "runtime"))

UPLOAD_DIR = ROOT / "uploads"
CORPUS_DIR = ROOT / "corpus"
INDEX_DIR  = ROOT / "index" / "current"
SNAP_DIR   = ROOT / "index" / "snapshots"
QUEUE_DIR  = ROOT / "queue"
LOGS_DIR   = ROOT / "logs"
LOCKS_DIR  = ROOT / "locks"

# ensure dirs
for p in (UPLOAD_DIR, CORPUS_DIR, INDEX_DIR, SNAP_DIR, QUEUE_DIR / "normalize", QUEUE_DIR / "build_index", LOGS_DIR, LOCKS_DIR):
    p.mkdir(parents=True, exist_ok=True)

# files
CORPUS_JSONL = CORPUS_DIR / "corpus.jsonl"
INDEX_JSON   = INDEX_DIR / "index.json"
MANIFEST_JSON= INDEX_DIR / "manifest.json"

DEFAULT_CFG = {
    "w_min_doc": 8,
    "w_min_query": 5,
    "k_list": [5,9,13],
    "weights": {"alpha": 0.60, "w13": 0.85, "w9": 0.90, "w5": 0.90},
    "thresholds": {"plag_thr": 0.70, "partial_thr": 0.30},
    "minhash": {"K": 128, "rows": 4, "seed": 1337, "hamming_bonus_bits": 6},
    "simhash_bonus": 0.02,
    "fetch_per_k_doc": 1024,
    "fetch_per_k5_doc": 512,
    "max_cands_doc": 10000,
    "fragments_for_top": 16,
}

