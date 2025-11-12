from fastapi import APIRouter
from ..core.config import CORPUS_JSONL, INDEX_JSON, MANIFEST_JSON

router = APIRouter(prefix="/api", tags=["Health-Status"])

@router.get("/health")
def health():
    return {
        "corpus_exists": CORPUS_JSONL.exists(),
        "index_exists": INDEX_JSON.exists(),
        "manifest_exists": MANIFEST_JSON.exists(),
        "index_path": str(INDEX_JSON)
    }
