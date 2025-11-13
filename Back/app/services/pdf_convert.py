# app/services/pdf_convert.py
import io, subprocess, tempfile, os
from pathlib import Path
from typing import Optional
from .pdf_heavy import pytess_ocr_pdf

def pdf_to_docx_bytes(raw_pdf: bytes, start: int = 0, end: Optional[int] = None) -> bytes:
    """Конвертирует 'текстовый' PDF в DOCX. Без OCR."""
    from pdf2docx import Converter  # требует pip install pdf2docx
    with tempfile.TemporaryDirectory() as td:
        pdf_path  = Path(td) / "in.pdf"
        docx_path = Path(td) / "out.docx"
        pdf_path.write_bytes(raw_pdf)
        cv = Converter(str(pdf_path))
        try:
            cv.convert(str(docx_path), start=start, end=end)
        finally:
            cv.close()
        return docx_path.read_bytes()

def ocr_pdf_bytes(raw_pdf: bytes, lang: str = "rus+eng") -> bytes:
    """OCR для сканов → searchable PDF. Нужен ocrmypdf + tesseract."""
    with tempfile.TemporaryDirectory() as td:
        inp = Path(td) / "in.pdf"
        out = Path(td) / "out.pdf"
        inp.write_bytes(raw_pdf)
        # --skip-text для гибридных pdf с текстом
        cmd = [
    "ocrmypdf",
    "--skip-text",      # или "--force-ocr", но не оба
    "-l", lang,
    str(inp),
    str(out),
]

        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if proc.returncode != 0 or not out.exists():
            raise RuntimeError(f"OCR failed: rc={proc.returncode}, err={proc.stderr.decode('utf-8', 'ignore')}")
        return out.read_bytes()

def smart_pdf_to_docx(
    raw_pdf: bytes,
    try_ocr: bool = True,
    lang: str = "kaz+rus+eng",
    ocr_workers: int = 16,       # ← явно задаём параллелизм
) -> bytes:
    """Сначала пробуем прямую конверсию, если пусто — OCR (через pytesseract, как в скрипте)."""
    docx = pdf_to_docx_bytes(raw_pdf)
    # если docx реально содержит текст, возвращаем
    if len(docx) >= 50_000 or not try_ocr:
        return docx

    # OCR путь (страничный, параллельный)
    txt, dbg = pytess_ocr_pdf(raw_pdf, lang=lang, workers=ocr_workers)
    # при желании можешь залогировать dbg["workers"], dbg["pages"]
    # logger.info(f"smart_pdf_to_docx: OCR dbg={dbg}")

    if not txt.strip():
        # fallback через ocrmypdf (он сам внутри уже юзает tesseract, но обычно однопоточно на уровне проц)
        searchable = ocr_pdf_bytes(raw_pdf, lang=lang)
        return pdf_to_docx_bytes(searchable)

    # создать DOCX из распознанного текста
    from docx import Document
    doc = Document()
    for para in txt.split("\n\n"):
        p = para.strip()
        if p:
            doc.add_paragraph(p)
    out = io.BytesIO()
    doc.save(out)
    return out.getvalue()
