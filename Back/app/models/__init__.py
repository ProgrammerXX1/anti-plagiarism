from app.db.base import Base

from .document import Document
from .segment import Segment
# from .segment_doc import SegmentDoc  # если есть

__all__ = ["Base", "Document", "Segment"]
