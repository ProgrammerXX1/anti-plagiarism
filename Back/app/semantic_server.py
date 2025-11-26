# # semantic_server.py
# from typing import List
# from fastapi import FastAPI
# from pydantic import BaseModel

# class EmbedReq(BaseModel):
#     texts: List[str]

# class EmbedResp(BaseModel):
#     vectors: List[List[float]]

# class RerankReq(BaseModel):
#     query: str
#     passages: List[str]

# class RerankResp(BaseModel):
#     scores: List[float]

# # import torch
# from sentence_transformers import SentenceTransformer
# from transformers import AutoTokenizer, AutoModelForSequenceClassification

# app = FastAPI(title="Plagio Semantic Service")

# DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
# DTYPE = torch.float16  # для 3090 ок

# # --- эмбеддер E5 ---
# EMBED_MODEL_NAME = "intfloat/multilingual-e5-large"
# embed_model = SentenceTransformer(
#     EMBED_MODEL_NAME,
#     device=DEVICE
# )
# # sentence-transformers сам утащит на CUDA, но dtype можно форснуть так:
# if hasattr(embed_model, "encode"):
#     # encode сам будет делать .to(DEVICE), но веса уже fp32.
#     # если захочешь, можно прогнать model.half() внутри:
#     try:
#         embed_model._first_module().auto_model = embed_model._first_module().auto_model.to(dtype=DTYPE)
#     except Exception:
#         pass

# # --- reranker BGE ---
# RERANK_MODEL_NAME = "BAAI/bge-reranker-v2-m3"
# rerank_tokenizer = AutoTokenizer.from_pretrained(RERANK_MODEL_NAME)
# rerank_model = AutoModelForSequenceClassification.from_pretrained(
#     RERANK_MODEL_NAME,
#     torch_dtype=DTYPE
# ).to(DEVICE)
# rerank_model.eval()

# import numpy as np

# @app.post("/embed", response_model=EmbedResp)
# def embed(req: EmbedReq):
#     if not req.texts:
#         return EmbedResp(vectors=[])

#     # batch encode
#     vecs = embed_model.encode(
#         req.texts,
#         batch_size=32,
#         normalize_embeddings=True,
#         convert_to_numpy=True,
#         show_progress_bar=False,
#     )
#     return EmbedResp(vectors=vecs.astype(float).tolist())

# @app.post("/rerank", response_model=RerankResp)
# def rerank(req: RerankReq):
#     if not req.passages:
#         return RerankResp(scores=[])

#     pairs = [(req.query, p) for p in req.passages]

#     with torch.no_grad():
#         encoded = rerank_tokenizer(
#             [p[0] for p in pairs],
#             [p[1] for p in pairs],
#             padding=True,
#             truncation=True,
#             max_length=512,
#             return_tensors="pt"
#         ).to(DEVICE)

#         logits = rerank_model(**encoded).logits  # shape [N, 1] или [N]
#         # BGE возвращает более высокий логит = выше релевантность
#         scores = logits.squeeze(-1).detach().cpu().tolist()

#     # можно сразу нормализовать в [0,1] через сигмоиду
#     if isinstance(scores, float):
#         scores = [scores]
#     scores = [float(torch.sigmoid(torch.tensor(x))) for x in scores]

#     return RerankResp(scores=scores)
