# app/services/search/semantic_client.py

import os
from typing import List
import httpx

from ...core.config import SEMANTIC_BASE_URL
from ...core.logger import logger


class SemanticClient:
    def __init__(self, base_url: str = SEMANTIC_BASE_URL, timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def embed(self, texts: List[str]) -> List[List[float]]:
        if not texts:
            return []
        url = f"{self.base_url}/embed"
        try:
            r = httpx.post(url, json={"texts": texts}, timeout=self.timeout)
            r.raise_for_status()
            data = r.json()
            return data.get("vectors", [])
        except Exception as e:
            logger.error(f"[semantic] embed failed: {e}")
            return []

    def rerank(self, query: str, passages: List[str]) -> List[float]:
        if not passages:
            return []
        url = f"{self.base_url}/rerank"
        try:
            r = httpx.post(
                url,
                json={"query": query, "passages": passages},
                timeout=self.timeout,
            )
            r.raise_for_status()
            data = r.json()
            return [float(x) for x in data.get("scores", [])]
        except Exception as e:
            logger.error(f"[semantic] rerank failed: {e}")
            return []


semantic_client = SemanticClient()
