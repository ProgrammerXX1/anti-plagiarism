# app/services/pdf_heavy.py
import io, re, subprocess, tempfile, os, shutil
from typing import List, Tuple, Dict, Any
from ..core.logger import logger
from PIL import ImageEnhance

# pdfminer (layout-aware)
try:
    from pdfminer.high_level import extract_pages
    from pdfminer.layout import LTTextContainer, LAParams
    _HAS_PDFMINER = True
except Exception:
    _HAS_PDFMINER = False

# fallbacks
try:
    from pdf2image import convert_from_bytes
    _HAS_PDF2IMAGE = True
except Exception:
    _HAS_PDF2IMAGE = False

try:
    import pytesseract
    _HAS_PYTESS = True
except Exception:
    _HAS_PYTESS = False

from PIL import Image, ImageOps, ImageFilter

_ZW = r"[\u200B-\u200F\u202A-\u202E\u2060\uFEFF]"
_SOFT = "\u00AD"
_NBSP = "\u00A0"
_LET = r"[^\W\d_]"

def _strip_invis(t: str) -> str:
    t = re.sub(_ZW, "", t)
    t = t.replace(_SOFT, "")
    t = t.replace(_NBSP, " ")
    return t

def _merge_hyphen_breaks(t: str) -> str:
    return re.sub(fr"({_LET}+)[\-–—]\s*\n\s*({_LET}+)", r"\1\2", t)

def _collapse_intraline_breaks(t: str) -> str:
    return re.sub(r"([^\n])\n(?!\n)", r"\1 ", t)

def _fix_small_splits(t: str) -> str:
    for _ in range(3):
        t2 = re.sub(fr"\b({_LET}{{1,3}})\s({_LET}{{1,3}})\b", r"\1\2", t)
        if t2 == t:
            break
        t = t2
    return t

def _norm_spaces(t: str) -> str:
    t = re.sub(r"[ \t]{2,}", " ", t)
    t = re.sub(r" *\n{2,} *", "\n\n", t)
    return t.strip()

def _extract_text_pdfminer(raw: bytes) -> str:
    if not _HAS_PDFMINER:
        return ""
    laparams = LAParams(
        line_margin=0.12,
        word_margin=0.08,
        char_margin=2.0,
        detect_vertical=False
    )
    parts: List[str] = []
    for page in extract_pages(io.BytesIO(raw), laparams=laparams):
        blocks = []
        for elt in page:
            if isinstance(elt, LTTextContainer):
                y1 = getattr(elt, "y1", 0.0)
                x0 = getattr(elt, "x0", 0.0)
                s = (elt.get_text() or "").rstrip()
                if s.strip():
                    blocks.append((y1, x0, s))
        blocks.sort(key=lambda t: (-t[0], t[1]))
        parts.append("\n".join(x[2] for x in blocks))
    return "\n\n".join(parts).strip()

def _alpha_density(s: str) -> float:
    if not s:
        return 0.0
    alnum = sum(1 for c in s if c.isalnum())
    return alnum / max(len(s), 1)

def _has_ocr_tools() -> bool:
    return shutil.which("ocrmypdf") is not None and shutil.which("tesseract") is not None

# ── exportable: OCR PDF→PDF ────────────────────────────────────────────────────
def ocrmypdf_bytes(raw: bytes, lang: str, oversample: int = 400) -> Tuple[bytes, Dict[str, Any]]:
    logger.info(f"ocrmypdf_bytes START: lang={lang}, oversample={oversample}, input_size={len(raw)}")
    dbg: Dict[str, Any] = {"ocrmypdf_rc": None, "ocrmypdf_err": "", "oversample": oversample}
    
    with tempfile.TemporaryDirectory() as td:
        inp, out = os.path.join(td, "in.pdf"), os.path.join(td, "out.pdf")
        with open(inp, "wb") as f:
            f.write(raw)
        
        cmd = [
            "ocrmypdf",
            "--skip-text", "--force-ocr",
            "-l", lang,
            "--deskew", "--rotate-pages", "--remove-background",
            f"--oversample={oversample}",
            "--optimize", "3",
            inp, out
        ]
        
        logger.info(f"Running: {' '.join(cmd)}")
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        dbg["ocrmypdf_rc"] = proc.returncode
        dbg["ocrmypdf_err"] = proc.stderr.decode("utf-8", "ignore")[-4000:]
        
        logger.info(f"ocrmypdf finished: rc={proc.returncode}, stderr_len={len(dbg['ocrmypdf_err'])}")
        
        if proc.returncode != 0 or not os.path.exists(out):
            logger.error(f"ocrmypdf FAILED: rc={proc.returncode}")
            logger.error(f"stderr: {dbg['ocrmypdf_err']}")
            raise RuntimeError("ocrmypdf failed")
        
        result = open(out, "rb").read()
        logger.info(f"ocrmypdf SUCCESS: output_size={len(result)}")
        return result, dbg
def _preprocess_for_ocr_strong(img: Image.Image) -> Image.Image:
    """Нежная, но эффективная предобработка как в рабочем скрипте."""
    g = ImageOps.grayscale(img)
    g = ImageOps.autocontrast(g)
    g = g.filter(ImageFilter.MedianFilter(3))
    g = ImageEnhance.Sharpness(g).enhance(1.3)
    g = ImageEnhance.Contrast(g).enhance(1.4)
    return g

def pytess_ocr_pdf(raw: bytes, lang: str, dpi: int = 500, psm: int = 6) -> Tuple[str, Dict[str, Any]]:
    # попытаемся учесть POPPLER_PATH (для pdf2image)
    import os
    poppler_path = os.environ.get("POPPLER_PATH") or os.environ.get("POPPLER_BIN")
    convert_kwargs = {"dpi": dpi}
    if poppler_path:
        convert_kwargs["poppler_path"] = poppler_path

    dbg: Dict[str, Any] = {"pytess_used": False, "pages": 0, "per_page": [], "dpi": dpi, "psm": psm, "poppler_path": poppler_path}
    if not (_HAS_PDF2IMAGE and _HAS_PYTESS):
        return "", dbg

    # конвертация PDF -> изображения
    try:
        images = convert_from_bytes(raw, **convert_kwargs)
    except Exception as e:
        dbg["convert_error"] = str(e)
        return "", dbg

    dbg["pytess_used"] = True
    dbg["pages"] = len(images)
    txt_parts: List[str] = []
    base_order = [psm] + [p for p in (6, 4, 7) if p != psm]  # как в скрипте

    for idx, img in enumerate(images, 1):
        best_txt, best_len, best_psm = "", 0, psm
        tried = []
        for p in base_order:
            cfg = f"--oem 1 --psm {p}"
            try:
                prep = _preprocess_for_ocr_strong(img)
                t = pytesseract.image_to_string(prep, lang=lang, config=cfg) or ""
            except Exception as e:
                tried.append((p, f"ERR:{e}"))
                continue
            L = len(t)
            tried.append((p, L))
            if L > best_len:
                best_txt, best_len, best_psm = t, L, p
            if L >= 1500:  # быстрый выход если текст уже приличный
                break
        txt_parts.append(best_txt)
        dbg["per_page"].append({"page": idx, "len": best_len, "psm": best_psm, "tries": tried})

    result = "\n\n".join(txt_parts).strip()
    return result, dbg

def _extract_text_pdftotext(raw: bytes) -> str:
    if shutil.which("pdftotext") is None:
        return ""
    with tempfile.TemporaryDirectory() as td:
        inp = os.path.join(td, "in.pdf")
        out = os.path.join(td, "out.txt")
        with open(inp, "wb") as f: f.write(raw)
        # -layout сохраняет колонки лучше для правого/двойного набора
        proc = subprocess.run(
            ["pdftotext", "-layout", "-enc", "UTF-8", inp, out],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        if proc.returncode != 0 or not os.path.exists(out):
            return ""
        return open(out, "r", encoding="utf-8", errors="ignore").read()
def extract_and_normalize_pdf(
    raw: bytes,
    try_ocr: bool = True,
    lang: str = "kaz+rus+eng",
    return_debug: bool = False,
    oversample: int = 400,
    dpi: int = 500,
    psm: int = 6,
):
    dbg: Dict[str, Any] = {
        "pdfminer_before_chars": 0,
        "alpha_before": 0.0,
        "ocr_tools": _has_ocr_tools(),
        "pdf2image": _HAS_PDF2IMAGE,
        "pytesseract": _HAS_PYTESS,
        "used": "pdfminer_or_pdftotext",
        "lang": lang,
        "oversample": oversample,
        "dpi": dpi,
        "psm": psm,
    }

    # 1) прямое извлечение
    txt0 = _extract_text_pdfminer(raw) or _extract_text_pdftotext(raw)
    dbg["pdfminer_before_chars"] = len(txt0)
    dbg["alpha_before"] = _alpha_density(txt0)
    txt = txt0

    # 2) OCR-путь
    if try_ocr and dbg["alpha_before"] < 0.02:
        # 2a) OCRmyPDF: делаем searchable PDF и снова вынимаем текст
        if dbg["ocr_tools"]:
            try:
                raw_ocr, d = ocrmypdf_bytes(raw, lang, oversample=oversample)
                dbg.update(d)
                txt = _extract_text_pdfminer(raw_ocr) or _extract_text_pdftotext(raw_ocr)
                dbg["used"] = "ocrmypdf" if txt else "ocrmypdf_no_text"
            except Exception as e:
                dbg["ocrmypdf_exc"] = str(e)

        # 2b) Если всё ещё пусто — жёсткий raster-OCR как в вашем скрипте
        if not (txt or "").strip():
            t, d = pytess_ocr_pdf(raw, lang, dpi=dpi, psm=psm)
            dbg.update(d)
            if t.strip():
                txt = t
                dbg["used"] = "pytesseract"
            else:
                # auto-retry по языкам
                for lg in [lang, "kaz+rus", "rus+eng", "kaz", "rus", "eng"]:
                    t2, d2 = pytess_ocr_pdf(raw, lg, dpi=dpi, psm=psm)
                    if len(t2.strip()) > len(txt.strip()):
                        txt = t2
                        dbg.update(d2)
                        dbg["used"] = f"pytesseract:{lg}"
                    if len(txt) >= 1500:
                        break

    # 3) нормализация
    t = (txt or "").replace("\r\n", "\n").replace("\r", "\n")
    t = _strip_invis(t)
    t = _merge_hyphen_breaks(t)
    t = _collapse_intraline_breaks(t)
    t = _fix_small_splits(t)
    t = _norm_spaces(t)
    dbg["final_chars"] = len(t)

    if return_debug:
        return t, dbg
    return t
