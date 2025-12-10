# app/services/level5/l5_config.py
from pathlib import Path
from typing import Dict, Any

from app.core.config import DEFAULT_CFG_OBJ, ensure_index_cfg


def write_l5_index_config(index_dir: Path, cfg: Dict[str, Any] | None = None) -> None:
    """
    Пишет index_config.json в формате, который читает C++ (search_core.cpp).
    Если cfg=None — берём DEFAULT_CFG_OBJ.
    """
    import json

    cfg_norm = ensure_index_cfg(cfg)

    out = {
        "w_min_doc": cfg_norm["w_min_doc"],
        "w_min_query": cfg_norm["w_min_query"],
        "weights": {
            "alpha": cfg_norm["weights"]["alpha"],
            "w13": cfg_norm["weights"].get("w13", 0.85),
            "w9": cfg_norm["weights"]["w9"],
        },
        "thresholds": {
            "plag_thr": cfg_norm["thresholds"]["plag_thr"],
            "partial_thr": cfg_norm["thresholds"]["partial_thr"],
        },
        "fetch_per_k_doc": cfg_norm.get("fetch_per_k", 64),
        "max_cands_doc": cfg_norm.get("max_cands_doc", 1000),
    }

    index_dir.mkdir(parents=True, exist_ok=True)
    path = index_dir / "index_config.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
