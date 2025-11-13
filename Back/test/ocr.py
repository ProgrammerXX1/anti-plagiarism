#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time, argparse, traceback
from pdf2image import convert_from_path
from PIL import Image, ImageOps, ImageFilter, ImageEnhance
import pytesseract

# ====== ПАРАМЕТРЫ ПО УМОЛЧАНИЮ ======
PDF_PATH = "test.pdf"
LANG = "kaz+rus+eng"
DPI = 500
PSM = 6
MAX_PAGES = 3
OUT_FILE = "ocr_result.txt"

# ====== ФУНКЦИИ ======
def preprocess(img):
    """Нежная предобработка под OCR."""
    g = ImageOps.grayscale(img)
    g = ImageOps.autocontrast(g)
    g = g.filter(ImageFilter.MedianFilter(3))
    # слегка повысим резкость/контраст — помогает на «сером» фоне
    g = ImageEnhance.Sharpness(g).enhance(1.3)
    g = ImageEnhance.Contrast(g).enhance(1.4)
    return g

def ocr_with_fallback(img, page_no, lang, psm):
    """
    OCR одной страницы.
    Если мало символов, автоматом пробуем альтернативные PSM.
    """
    tried = []
    order = [psm] + [p for p in (6, 4, 7) if p != psm]  # уникальный порядок
    best_txt, best_len, best_psm = "", 0, psm

    for p in order:
        start = time.time()
        cfg = f"--oem 1 --psm {p}"
        try:
            txt = pytesseract.image_to_string(img, lang=lang, config=cfg) or ""
            dur = time.time() - start
            L = len(txt)
            tried.append((p, L, round(dur, 2)))
            if L > best_len:
                best_txt, best_len, best_psm = txt, L, p
            # быстрый выход, если текст уже нормальный
            if L >= 1500:
                break
        except Exception as e:
            dur = time.time() - start
            tried.append((p, f"ERR:{e}", round(dur, 2)))
            traceback.print_exc()

    print(f"[OK] Стр. {page_no}: best_len={best_len}, best_psm={best_psm}, tries={tried}")
    return best_txt

# ====== MAIN ======
def main():
    ap = argparse.ArgumentParser(description="Простой OCR сканов PDF (Tesseract)")
    ap.add_argument("--pdf", default=PDF_PATH, help="Путь к PDF")
    ap.add_argument("--lang", default=LANG, help="Языки Tesseract, напр. kaz+rus или kaz+rus+eng")
    ap.add_argument("--dpi", type=int, default=DPI, help="DPI для рендеринга страниц")
    ap.add_argument("--psm", type=int, default=PSM, help="Базовый PSM (fallback 6/4/7 включен)")
    ap.add_argument("--max-pages", type=int, default=MAX_PAGES, help="Ограничить число страниц")
    ap.add_argument("--out", default=OUT_FILE, help="Файл для записи результата")
    args = ap.parse_args()

    print("=== OCR ТЕСТ НАЧАТ ===")
    print(f"Файл: {args.pdf}")
    print(f"Язык: {args.lang} | DPI: {args.dpi} | PSM: {args.psm}")

    try:
        pages = convert_from_path(args.pdf, dpi=args.dpi)
    except Exception as e:
        print(f"[FATAL] Ошибка при чтении PDF: {e}")
        traceback.print_exc()
        return

    print(f"Страниц получено: {len(pages)}")
    if args.max_pages:
        pages = pages[:args.max_pages]
        print(f"Обработка только первых {len(pages)} стр.")

    texts, total_chars = [], 0
    t0 = time.time()
    for i, img in enumerate(pages, 1):
        g = preprocess(img)
        txt = ocr_with_fallback(g, i, args.lang, args.psm)
        texts.append(txt)
        total_chars += len(txt)

    t_elapsed = time.time() - t0
    full_text = "\n\n".join(texts).strip()

    print("\n=== OCR ГОТОВ ===")
    print(f"Всего символов: {total_chars}")
    print(f"Время: {t_elapsed:.2f} сек")
    print(f"Сохраняем результат → {args.out}")

    with open(args.out, "w", encoding="utf-8") as f:
        f.write(full_text)

    if total_chars == 0:
        print("\n⚠️ OCR не распознал текст.")
        print("Проверьте языки: tesseract --list-langs (ищите kaz, rus, eng).")
        print("Попробуйте: --dpi 600, --psm 4 или --psm 7, --lang kaz+rus")
    else:
        print(f"\n✅ Распознано {total_chars} символов.")
        print("Превью:")
        print(full_text[:800])

if __name__ == "__main__":
    main()
