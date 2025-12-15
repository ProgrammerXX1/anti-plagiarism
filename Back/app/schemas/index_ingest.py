from __future__ import annotations

import base64
import binascii
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class IndexIngestRequest(BaseModel):
    document_id: int = Field(..., description="source_document_id (внешний id файла)")
    organization_id: Optional[int] = Field(None)
    title: Optional[str] = None
    author: Optional[str] = None
    document_type: Optional[str] = None
    created_at: Optional[str] = None

    text: Optional[str] = None            # пока не используем
    file_name: Optional[str] = None
    enable_ocr: bool = False
    index: bool = False     

class IndexIngestResponse(BaseModel):
    """
    ACK-ответ для POST /ingest
    (подтверждение приёма документа)
    """

    internal_doc_id: int = Field(..., description="Внутренний ID документа")
    status: str = Field(..., description="Текущий внутренний статус документа")
    shard_id: int = Field(..., description="Shard ID")
    external_id: str = Field(..., description="Имя сохранённого файла")
    indexed: bool = Field(..., description="Поставлен ли документ в индекс")
