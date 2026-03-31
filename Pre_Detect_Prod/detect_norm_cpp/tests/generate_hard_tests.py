"""
Generate hard/edge-case test files for detect_norm_cpp.
Covers ALL detector edge cases:
  - symbol_substitution: all homoglyph pairs, mixed scripts, edge lengths
  - spacing: all 15 Unicode spaces + 8 zero-width chars + combos
  - hidden_text: RGB boundaries, color≈bg, micro-font boundaries
  - combined: all fraud types interleaved
"""
import os
from docx import Document
from docx.shared import Pt, RGBColor, Emu
from docx.oxml.ns import qn
from lxml import etree

OUT = os.path.dirname(os.path.abspath(__file__))


def _color(run, r, g, b):
    rPr = run._element.get_or_add_rPr()
    el = rPr.find(qn("w:color"))
    if el is None:
        el = etree.SubElement(rPr, qn("w:color"))
    el.set(qn("w:val"), f"{r:02X}{g:02X}{b:02X}")


def _shading(run, r, g, b):
    rPr = run._element.get_or_add_rPr()
    shd = rPr.find(qn("w:shd"))
    if shd is None:
        shd = etree.SubElement(rPr, qn("w:shd"))
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:fill"), f"{r:02X}{g:02X}{b:02X}")
    shd.set(qn("w:color"), "auto")


def _font_size(run, pt):
    """Set font size via XML (supports fractional points like 0.5)."""
    rPr = run._element.get_or_add_rPr()
    sz = rPr.find(qn("w:sz"))
    if sz is None:
        sz = etree.SubElement(rPr, qn("w:sz"))
    # w:sz val = half-points
    sz.set(qn("w:val"), str(int(pt * 2)))


# ═══════════════════════════════════════════════════
#  1. HARD SYMBOLS — test_hard_symbols.docx
# ═══════════════════════════════════════════════════

def make_hard_symbols_docx():
    doc = Document()
    doc.add_heading("HARD: Symbol Substitution (homoglyphs)", level=1)

    # --- All known Cyrillic→Latin homoglyph pairs (lowercase) ---
    doc.add_paragraph("=== Все пары гомоглифов (строчные) ===")

    # а→a: "катастрофа" с Latin 'a' вместо кириллической
    p = doc.add_paragraph()
    p.add_run("К")
    p.add_run("a")         # Latin a (U+0061) вместо кир а (U+0430)
    p.add_run("т")
    p.add_run("a")         # Latin a
    p.add_run("строф")
    p.add_run("a")         # Latin a — 3 подмены в одном слове

    # е→e: "перевести"
    p = doc.add_paragraph()
    p.add_run("П")
    p.add_run("e")         # Latin e
    p.add_run("р")
    p.add_run("e")         # Latin e
    p.add_run("в")
    p.add_run("e")         # Latin e
    p.add_run("сти")       # 3 подмены 'e'

    # о→o: "молоко"
    p = doc.add_paragraph()
    p.add_run("М")
    p.add_run("o")         # Latin o
    p.add_run("л")
    p.add_run("o")         # Latin o
    p.add_run("к")
    p.add_run("o")         # Latin o — 3 подмены

    # с→c: "сосиска"
    p = doc.add_paragraph()
    p.add_run("C")         # Latin C (uppercase)
    p.add_run("о")
    p.add_run("c")         # Latin c (lowercase)
    p.add_run("и")
    p.add_run("c")         # Latin c
    p.add_run("ка")        # 3 подмены 'c/C'

    # р→p: "проработка"
    p = doc.add_paragraph()
    p.add_run("П")
    p.add_run("p")         # Latin p
    p.add_run("о")
    p.add_run("p")         # Latin p
    p.add_run("аботка")    # 2 подмены 'p'

    # у→y: "улучшу"
    p = doc.add_paragraph()
    p.add_run("Ул")
    p.add_run("y")         # Latin y
    p.add_run("чш")
    p.add_run("y")         # Latin y — 2 подмены

    # х→x: "хохотал"
    p = doc.add_paragraph()
    p.add_run("X")         # Latin X
    p.add_run("о")
    p.add_run("x")         # Latin x
    p.add_run("отал")      # 2 подмены

    # і→i (Kazakh/Ukrainian): "існує" (Ukrainian)
    p = doc.add_paragraph()
    p.add_run("Пр")
    p.add_run("i")         # Latin i вместо кир і (U+0456)
    p.add_run("звище цього доробку")

    # --- UPPERCASE homoglyphs ---
    doc.add_paragraph("=== Uppercase гомоглифы ===")

    # А→A, Е→E, О→O, С→C, Р→P, У→Y, Х→X
    p = doc.add_paragraph()
    p.add_run("A")         # Latin A вместо кир А
    p.add_run("лгоритм ")
    p.add_run("O")         # Latin O
    p.add_run("бработки ")
    p.add_run("C")         # Latin C
    p.add_run("труктур")

    p = doc.add_paragraph()
    p.add_run("P")         # Latin P
    p.add_run("езультат ")
    p.add_run("E")         # Latin E
    p.add_run("стественный ")
    p.add_run("X")         # Latin X
    p.add_run("арактер")

    # --- Edge cases ---
    doc.add_paragraph("=== Граничные случаи ===")

    # Минимальное слово (2 символа, mixed)
    p = doc.add_paragraph()
    p.add_run("Тест: д")
    p.add_run("a")         # Latin a в слове "да" (2 символа)
    p.add_run(" — двухбуквенное слово")

    # Слово из 1 символа — НЕ должно быть flagged
    p = doc.add_paragraph()
    p.add_run("Союз a и o не должны быть ошибкой")  # single Latin chars

    # Слово-через-дефис
    p = doc.add_paragraph()
    p.add_run("Что-т")
    p.add_run("o")         # Latin o в "что-то"
    p.add_run(" пошло не т")
    p.add_run("a")         # Latin a в "так"
    p.add_run("к")

    # Много подмен в одном слове (5 foreign chars из 13)
    p = doc.add_paragraph()
    p.add_run("Х")
    p.add_run("a")         # 1 Latin
    p.add_run("р")
    p.add_run("a")         # 2
    p.add_run("кт")
    p.add_run("e")         # 3
    p.add_run("рист")
    p.add_run("o")         # 4 — "характеристока" с 4 Latin

    # Полностью латинское слово в кириллическом тексте — НЕ ошибка
    p = doc.add_paragraph()
    p.add_run("Использовали framework для разработки — это нормально")

    # Полностью кириллическое — НЕ ошибка
    doc.add_paragraph("Абсолютно чистый текст без единой подмены символов.")

    # Казахский текст с Latin i вместо Kazakh і
    p = doc.add_paragraph()
    p.add_run("Қазақстанның б")
    p.add_run("i")         # Latin i вместо кир і
    p.add_run("л")
    p.add_run("i")         # Latin i
    p.add_run("м беру жүйесі")

    path = os.path.join(OUT, "test_hard_symbols.docx")
    doc.save(path)
    print(f"  OK {path}")


# ═══════════════════════════════════════════════════
#  2. HARD SPACES — test_hard_spaces.docx
# ═══════════════════════════════════════════════════

def make_hard_spaces_docx():
    doc = Document()
    doc.add_heading("HARD: Spacing anomalies", level=1)

    doc.add_paragraph("=== Все 15 типов подозрительных Unicode пробелов ===")

    spaces = [
        (0x00A0, "NBSP"),
        (0x2000, "EN QUAD"),
        (0x2001, "EM QUAD"),
        (0x2002, "EN SPACE"),
        (0x2003, "EM SPACE"),
        (0x2004, "THREE-PER-EM"),
        (0x2005, "FOUR-PER-EM"),
        (0x2006, "SIX-PER-EM"),
        (0x2007, "FIGURE SPACE"),
        (0x2008, "PUNCTUATION SPACE"),
        (0x2009, "THIN SPACE"),
        (0x200A, "HAIR SPACE"),
        (0x202F, "NARROW NBSP"),
        (0x205F, "MEDIUM MATH SPACE"),
        (0x3000, "IDEOGRAPHIC SPACE"),
    ]

    for cp, name in spaces:
        doc.add_paragraph(f"Между{chr(cp)}словами{chr(cp)}стоит{chr(cp)}{name}")

    doc.add_paragraph("=== Все 8 zero-width символов ===")

    zw = [
        (0x200B, "ZWSP"),
        (0x200C, "ZWNJ"),
        (0x200D, "ZWJ"),
        (0x200E, "LRM"),
        (0x200F, "RLM"),
        (0x2060, "WORD JOINER"),
        (0xFEFF, "BOM/ZWNBSP"),
        (0x034F, "COMBINING GRAPHEME JOINER"),
    ]

    for cp, name in zw:
        doc.add_paragraph(f"Внут{chr(cp)}ри{chr(cp)}сло{chr(cp)}ва{chr(cp)}{name}")

    doc.add_paragraph("=== Множественные ASCII пробелы ===")

    doc.add_paragraph("Два  пробела")
    doc.add_paragraph("Три   пробела")
    doc.add_paragraph("Пять     пробелов")
    doc.add_paragraph("Десять          пробелов подряд")
    doc.add_paragraph("Пятьдесят" + " " * 50 + "пробелов")

    doc.add_paragraph("=== Комбинации ===")

    # NBSP + ZWSP + multi-space
    doc.add_paragraph(
        f"Ком\u200Bби\u200Bна\u200Bция: "
        f"NBSP\u00A0здесь,   три\u00A0пробела\u00A0и\u200Bзеро-ширина"
    )

    # EM SPACE + THIN SPACE + HAIR SPACE в одной строке
    doc.add_paragraph(
        f"EM\u2003thin\u2009hair\u200Aвсё\u2003в\u2009одной\u200Aстроке"
    )

    # Множественные разные zero-width подряд
    doc.add_paragraph(
        f"Слово\u200B\u200C\u200D\u200Eскрытая\u200B\u200Bдвойная\uFEFFбом"
    )

    # Чистая строка — 0 находок
    doc.add_paragraph("Обычный текст без пробельных аномалий.")

    path = os.path.join(OUT, "test_hard_spaces.docx")
    doc.save(path)
    print(f"  OK {path}")


# ═══════════════════════════════════════════════════
#  3. HARD HIDDEN — test_hard_hidden.docx
# ═══════════════════════════════════════════════════

def make_hard_hidden_docx():
    doc = Document()
    doc.add_heading("HARD: Hidden text detection", level=1)

    doc.add_paragraph("=== Белый / почти-белый текст (RGB > 240) ===")

    # Чисто белый (255,255,255) — ДОЛЖЕН сработать
    p = doc.add_paragraph()
    r = p.add_run("Белый255: ")
    _color(r, 0, 0, 0)
    r2 = p.add_run("Этот текст полностью белый и невидим на белом фоне документа")
    _color(r2, 255, 255, 255)

    # (250,250,250) — ДОЛЖЕН
    p = doc.add_paragraph()
    r = p.add_run("250: ")
    _color(r, 0, 0, 0)
    r2 = p.add_run("Текст цвета 250 250 250 почти белый")
    _color(r2, 250, 250, 250)

    # (241,241,241) — ДОЛЖЕН (>240)
    p = doc.add_paragraph()
    r = p.add_run("241: ")
    _color(r, 0, 0, 0)
    r2 = p.add_run("Граничный случай 241 — должен сработать")
    _color(r2, 241, 241, 241)

    # (240,240,240) — НЕ должен (порог > 240, а не >= 240)
    p = doc.add_paragraph()
    r = p.add_run("240: ")
    _color(r, 0, 0, 0)
    r2 = p.add_run("Граничный 240 — НЕ должен сработать")
    _color(r2, 240, 240, 240)

    # (255,255,200) — НЕ должен (B=200 < 240)
    p = doc.add_paragraph()
    r = p.add_run("255-255-200: ")
    _color(r, 0, 0, 0)
    r2 = p.add_run("Один компонент ниже порога — не near-white")
    _color(r2, 255, 255, 200)

    # (241,255,241) — ДОЛЖЕН (все > 240)
    p = doc.add_paragraph()
    r = p.add_run("241-255-241: ")
    _color(r, 0, 0, 0)
    r2 = p.add_run("Все три выше 240 — near-white")
    _color(r2, 241, 255, 241)

    doc.add_paragraph("=== Цвет текста ≈ цвет фона (distance < 30) ===")

    # Exact match: красный на красном (dist=0)
    p = doc.add_paragraph()
    r = p.add_run("Красный на красном невидим")
    _color(r, 200, 0, 0)
    _shading(r, 200, 0, 0)

    # Синий на синем (dist=0)
    p = doc.add_paragraph()
    r = p.add_run("Синий на синем фоне скрыт")
    _color(r, 0, 0, 180)
    _shading(r, 0, 0, 180)

    # Чёрный на чёрном (dist=0)
    p = doc.add_paragraph()
    r = p.add_run("Чёрный текст на чёрном фоне")
    _color(r, 0, 0, 0)
    _shading(r, 0, 0, 0)

    # Близкий: dist = sqrt(10^2+10^2+10^2) = 17.3 < 30 — ДОЛЖЕН
    p = doc.add_paragraph()
    r = p.add_run("Почти совпадение: разница 10 по каждому каналу")
    _color(r, 100, 100, 100)
    _shading(r, 110, 110, 110)

    # Граница: dist = sqrt(17^2+17^2+17^2) ≈ 29.4 < 30 — ДОЛЖЕН
    p = doc.add_paragraph()
    r = p.add_run("Граница distance=29.4 — должен сработать")
    _color(r, 100, 100, 100)
    _shading(r, 117, 117, 117)

    # Чуть дальше: dist = sqrt(18^2+18^2+18^2) ≈ 31.2 > 30 — НЕ должен
    p = doc.add_paragraph()
    r = p.add_run("За границей distance=31.2 — НЕ должен")
    _color(r, 100, 100, 100)
    _shading(r, 118, 118, 118)

    doc.add_paragraph("=== Микро-шрифт (< 2pt) ===")

    # 0.5pt — ДОЛЖЕН
    p = doc.add_paragraph()
    r = p.add_run("Шрифт 0.5pt невозможно прочитать без лупы")
    _font_size(r, 0.5)

    # 1pt — ДОЛЖЕН
    p = doc.add_paragraph()
    r = p.add_run("Шрифт 1pt ещё не читается нормально")
    _font_size(r, 1)

    # 1.5pt — ДОЛЖЕН (< 2.0)
    p = doc.add_paragraph()
    r = p.add_run("Шрифт 1.5pt — граничный, но < 2")
    _font_size(r, 1.5)

    # 2pt — НЕ должен (порог < 2.0, а 2.0 == 2.0, not < 2.0)
    # Но! _font_size пишет half-points как int: 2.0 * 2 = 4 → 4/2=2.0 → not < 2.0
    p = doc.add_paragraph()
    r = p.add_run("Шрифт 2pt — НЕ должен сработать (ровно на границе)")
    _font_size(r, 2)

    # 3pt — точно НЕ должен
    p = doc.add_paragraph()
    r = p.add_run("Шрифт 3pt — нормальный маленький")
    _font_size(r, 3)

    doc.add_paragraph("=== Комбинация: near-white + micro-font (НЕ двойной флаг) ===")

    p = doc.add_paragraph()
    r = p.add_run("Белый 1pt — сработает ОДИН раз (near-white имеет приоритет)")
    _color(r, 255, 255, 255)
    _font_size(r, 1)

    doc.add_paragraph("=== Нормальный текст (0 находок) ===")

    p = doc.add_paragraph()
    r = p.add_run("Чёрный текст нормального размера — чисто.")
    _color(r, 0, 0, 0)

    path = os.path.join(OUT, "test_hard_hidden.docx")
    doc.save(path)
    print(f"  OK {path}")


# ═══════════════════════════════════════════════════
#  4. HARD COMBINED — test_hard_combined.docx
# ═══════════════════════════════════════════════════

def make_hard_combined_docx():
    doc = Document()
    doc.add_heading("HARD: Combined — all fraud types", level=1)

    # 1) Symbol substitution: 4 homoglyphs
    p = doc.add_paragraph()
    p.add_run("К")
    p.add_run("a")         # Latin
    p.add_run("т")
    p.add_run("a")         # Latin
    p.add_run("строф")
    p.add_run("a")         # Latin
    p.add_run(" произошла в р")
    p.add_run("e")         # Latin e
    p.add_run("зультате")

    # 2) Exotic spaces: NBSP + ZWSP + multi-space + EM SPACE
    doc.add_paragraph(
        f"Между\u00A0словами\u00A0неразрывные "
        f"и\u200Bзеро\u200Bвидс "
        f"и     пять     пробелов "
        f"и\u2003EM\u2003space"
    )

    # 3) Hidden: white text
    p = doc.add_paragraph()
    r = p.add_run("Видимая часть абзаца. ")
    _color(r, 0, 0, 0)
    r2 = p.add_run("Скрытый белый текст для увеличения объёма курсовой работы студента.")
    _color(r2, 255, 255, 255)

    # 4) Hidden: color ≈ background
    p = doc.add_paragraph()
    r = p.add_run("Зелёный текст на зелёном фоне полностью невидим для проверяющего.")
    _color(r, 50, 150, 50)
    _shading(r, 50, 150, 50)

    # 5) Hidden: micro-font
    p = doc.add_paragraph()
    r = p.add_run("Этот параграф написан шрифтом 0.5pt и содержит важный текст")
    _font_size(r, 0.5)

    # 6) Symbols + spaces в одном абзаце
    p = doc.add_paragraph()
    p.add_run("Студ")
    p.add_run("e")         # Latin e
    p.add_run("нт\u00A0н")
    p.add_run("a")         # Latin a
    p.add_run("пис")
    p.add_run("a")         # Latin a
    p.add_run("л\u200B\u200Bработу   с\u00A0ошибками")

    # 7) Чистый параграф
    doc.add_paragraph("Этот абзац абсолютно чистый.")

    path = os.path.join(OUT, "test_hard_combined.docx")
    doc.save(path)
    print(f"  OK {path}")


# ═══════════════════════════════════════════════════
#  5. HARD TXT — test_hard.txt
# ═══════════════════════════════════════════════════

def make_hard_txt():
    lines = [
        "=== Symbol substitution ===",
        # а→a, е→e, о→o в разных словах
        "К\u0061т\u0061строф\u0061 в р\u0065зульт\u0061те",
        # с→c, р→p
        "\u0063облюдение \u0070равил \u0070роверки",
        # у→y, х→x
        "Ул\u0079чшить \u0078\u006Fд исследования",
        # uppercase: А→A, О→O, С→C, Е→E
        "\u0041лгоритм \u004Fбработки \u0043труктур \u0045стественный",
        # Kazakh і→i
        "Қазақстанның б\u0069л\u0069м беру жүйесі",
        # минимальное 2-символьное: "да" с Latin a
        "д\u0061 нет",
        # чисто латинское — OK
        "English framework is fine",
        # чисто кириллическое — OK
        "Чистый русский текст без подмен",
        "",
        "=== Spacing anomalies ===",
        # все exotic spaces
        f"NBSP:\u00A0между\u00A0слов",
        f"EN_QUAD:\u2000между\u2000слов",
        f"EM_QUAD:\u2001между\u2001слов",
        f"EN_SPACE:\u2002между\u2002слов",
        f"EM_SPACE:\u2003между\u2003слов",
        f"THREE_PER_EM:\u2004между\u2004слов",
        f"FOUR_PER_EM:\u2005между\u2005слов",
        f"SIX_PER_EM:\u2006между\u2006слов",
        f"FIGURE_SPACE:\u2007между\u2007слов",
        f"PUNCTUATION:\u2008между\u2008слов",
        f"THIN_SPACE:\u2009между\u2009слов",
        f"HAIR_SPACE:\u200Aмежду\u200Aслов",
        f"NARROW_NBSP:\u202Fмежду\u202Fслов",
        f"MEDIUM_MATH:\u205Fмежду\u205Fслов",
        f"IDEOGRAPHIC:\u3000между\u3000слов",
        # zero-width chars
        f"ZWSP:вну\u200Bтри\u200Bслова",
        f"ZWNJ:вну\u200Cтри\u200Cслова",
        f"ZWJ:вну\u200Dтри\u200Dслова",
        f"LRM:вну\u200Eтри\u200Eслова",
        f"RLM:вну\u200Fтри\u200Fслова",
        f"WORD_JOINER:вну\u2060три\u2060слова",
        f"BOM:вну\uFEFFтри\uFEFFслова",
        f"CGJ:вну\u034Fтри\u034Fслова",
        # multi-space
        "Два  пробела",
        "Пять     пробелов",
        "Двадцать" + " " * 20 + "пробелов",
        # combo
        f"Комбо:\u00A0NBSP\u200Bи   три\u2003EM\u200B\u200Bдвойной-zwsp",
        # чистая
        "Обычный текст без аномалий.",
        "",
        "=== Symbols + Spaces together ===",
        f"Студ\u0065нт\u00A0н\u0061пис\u0061л\u200B\u200Bработу   с\u00A0ошибками",
    ]

    path = os.path.join(OUT, "test_hard.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"  OK {path}")


# ═══════════════════════════════════════════════════
#  6. HARD DOC — test_hard.doc (plain text fallback)
# ═══════════════════════════════════════════════════

def make_hard_doc():
    """
    detect_norm_cpp treats .doc as plain text fallback (no color/font info).
    So only symbol_substitution and spacing detectors apply.
    """
    lines = [
        "=== .doc test (parsed as plain text) ===",
        "",
        "--- Symbol substitution ---",
        # все гомоглифы в одной строке
        "К\u0061т\u0061строф\u0061 — 3x Latin a",
        "П\u0065р\u0065в\u0065сти — 3x Latin e",
        "М\u006Fл\u006Fк\u006F — 3x Latin o",
        "\u0063облюдение пр\u0061вил — Latin c + a",
        "П\u0070ораб\u006Fтка — Latin p + o",
        "Ул\u0079чш\u0079 — 2x Latin y",
        "\u0078\u006Fд — Latin x + o в 'ход'",
        "\u0041лгоритм \u004Fбработки — uppercase A + O",
        "Б\u0069л\u0069м (Kazakh і→i)",
        "",
        "--- Spacing ---",
        f"NBSP:\u00A0\u00A0\u00A0три подряд",
        f"ZWSP:\u200B\u200B\u200Bтри подряд",
        f"EM:\u2003\u2003два подряд",
        "Обычные     пять     пробелов     подряд",
        f"Микс:\u00A0+\u200B+   +\u2003",
        "",
        "--- Clean ---",
        "Чистая строка без аномалий.",
        "English text without any issues.",
        "Қазақ мәтіні дұрыс жазылған.",
    ]

    path = os.path.join(OUT, "test_hard.doc")
    with open(path, "wb") as f:
        f.write("\n".join(lines).encode("utf-8"))
    print(f"  OK {path}")


# ═══════════════════════════════════════════════════

if __name__ == "__main__":
    print("Generating hard test files...")
    make_hard_symbols_docx()
    make_hard_spaces_docx()
    make_hard_hidden_docx()
    make_hard_combined_docx()
    make_hard_txt()
    make_hard_doc()
    print(f"\nDone! Files in: {OUT}")
