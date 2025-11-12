# app/services/pdf_heavy.py
import io, re
from pdfminer.high_level import extract_text
from pdfminer.layout import LAParams
from typing import Optional

# Регулярки для очистки текста
_ZW = r"[\u200B-\u200F\u202A-\u202E\u2060\uFEFF]"
_SOFT_HYPHEN = "\u00AD"
_NBSP = "\u00A0"
_LET = r"[^\W\d_]"

def extract_text_pdfminer(raw: bytes) -> str:
    laparams = LAParams(
        line_margin=0.12,
        word_margin=0.08,
        char_margin=2.0,
        detect_vertical=False,
        all_texts=False,
    )
    try:
        text = extract_text(io.BytesIO(raw), laparams=laparams) or ""
        return text
    except Exception as e:
        raise RuntimeError(f"pdfminer failed: {e}")

def repair_text(raw_txt: str) -> str:
    """Максимально чистим текст для надёжных шинглов."""
    t = raw_txt.replace("\r\n", "\n").replace("\r", "\n")
    t = re.sub(_ZW, "", t)
    t = t.replace(_SOFT_HYPHEN, "")
    t = t.replace(_NBSP, " ")

    # Склейка переносов со знаком
    t = re.sub(fr"({_LET}+)[\-–—]\s*\n\s*({_LET}+)", r"\1\2", t, flags=re.UNICODE)
    # Убираем переносы в середине абзаца
    t = re.sub(r"([^\n])\n(?!\n)", r"\1 ", t)
    # Склеиваем короткие разрывы
    for _ in range(3):
        t2 = re.sub(fr"\b({_LET}{{1,3}})\s({_LET}{{1,3}})\b", r"\1\2", t, flags=re.UNICODE)
        if t2 == t:
            break
        t = t2

    # Приводим множественные пробелы к одному
    t = re.sub(r"[ \t]{2,}", " ", t)
    return t.strip()

def extract_and_normalize_pdf(raw_pdf: bytes, fallback: Optional[bool] = True) -> str:
    """
    Извлекает текст из PDF (pdfminer) + heavy нормализация.
    Если не удаётся, пытается fallback (если указан).
    """
    try:
        text = extract_text_pdfminer(raw_pdf)
    except Exception as e:
        if not fallback:
            raise
        text = ""

    text = repair_text(text)
    return text
