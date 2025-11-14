# app/models/schemas.py
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional

class UploadResp(BaseModel):
    doc_id: str
    bytes: int

class BuildReq(BaseModel):
    cfg: Optional[Dict[str, Any]] = None

class BuildResp(BaseModel):
    index_path: str
    docs: int
    k5: int
    k9: int
    k13: int

class SearchReq(BaseModel):
    query: str = Field(..., min_length=3)
    top: int = Field(5, ge=1, le=50)

class Frag(BaseModel):
    start: int
    end: int
    text: str

class HitDetails(BaseModel):
    J13: float; C13: float; J9: float; C9: float; J5: float; C5: float
    minhash_sim_est: float
    hamming_simhash: int
    cand_hits: int
    matching_fragments: List[Frag]

class Hit(BaseModel):
    doc_id: str
    max_score: float
    originality_pct: float
    decision: str
    details: HitDetails

class SearchResp(BaseModel):
    hits_total: int
    docs_found: int
    documents: List[Hit]
