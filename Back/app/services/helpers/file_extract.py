# app/services/helpers/file_extract.py

from __future__ import annotations

import io
import re
from typing import Optional

import fitz  # PyMuPDF
import docx
from bs4 import BeautifulSoup


# ───────────────────────────────────────────────────────────────
# Универсальное извлечение текста из байтов
# ───────────────────────────────────────────────────────────────

def extract_text_from_file_bytes(
    data: bytes,
    filename: Optional[str] = None,
) -> str:
    """
    Универсальный extractor:
    - PDF
    - DOCX
    - TXT
    - HTML
    """

    name = (filename or "").lower()

    # ── PDF ──────────────────────────────
    if name.endswith(".pdf"):
        return _extract_pdf_text(data)

    # ── DOCX ─────────────────────────────
    if name.endswith(".docx"):
        return _extract_docx_text(data)

    # ── HTML ─────────────────────────────
    if name.endswith(".html") or name.endswith(".htm"):
        return _extract_html_text(data)

    # ── TXT / по умолчанию ───────────────
    try:
        return data.decode("utf-8", errors="ignore")
    except Exception:
        return data.decode("latin1", errors="ignore")


# ───────────────────────────────────────────────────────────────
# PDF
# ───────────────────────────────────────────────────────────────

def _extract_pdf_text(data: bytes) -> str:
    out = []
    with fitz.open(stream=data, filetype="pdf") as doc:
        for page in doc:
            out.append(page.get_text())
    return "\n".join(out)


# ───────────────────────────────────────────────────────────────
# DOCX
# ───────────────────────────────────────────────────────────────

def _extract_docx_text(data: bytes) -> str:
    file_like = io.BytesIO(data)
    document = docx.Document(file_like)
    return "\n".join(p.text for p in document.paragraphs)


# ───────────────────────────────────────────────────────────────
# HTML
# ───────────────────────────────────────────────────────────────

def _extract_html_text(data: bytes) -> str:
    try:
        text = data.decode("utf-8", errors="ignore")
    except Exception:
        text = data.decode("latin1", errors="ignore")

    soup = BeautifulSoup(text, "html.parser")
    return soup.get_text(separator=" ")


# ───────────────────────────────────────────────────────────────
# НОРМАЛИЗАЦИЯ ПОД ШИНГЛЫ (ru + kk + tr)
# ───────────────────────────────────────────────────────────────

_re_spaces = re.compile(r"\s+")
_re_bad = re.compile(r"[^a-zа-яёіқғңөұүһ0-9 ]", re.IGNORECASE)


def norm_for_local(text: str) -> str:
    """
    Финальная нормализация перед шинглами:
    - lowercase
    - удаление спецсимволов
    - схлопывание пробелов
    """

    text = text.lower()

    # убираем мусор
    text = _re_bad.sub(" ", text)

    # схлопываем пробелы
    text = _re_spaces.sub(" ", text)

    return text.strip()
