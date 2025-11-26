# app/services/helpers/normalizer.py
import unicodedata
import re
from typing import List

_ws = re.compile(r"\s+")
_nonw = re.compile(r"[^\w\s]+", flags=re.UNICODE)

_ZW = re.compile(r"[\u200B-\u200F\u202A-\u202E\u2060\uFEFF]")
_SOFT_HYPHEN = "\u00AD"
_NBSP = "\u00A0"


def normalize_nfkc_lower(s: str) -> str:
    """
    Базовая нормализация: NFKC + lower, чистим zero-width/soft hyphen/NBSP.
    Используется в других местах, оставляем поведение прежним.
    """
    s = _ZW.sub("", s).replace(_SOFT_HYPHEN, "").replace(_NBSP, " ")
    return unicodedata.normalize("NFKC", s).lower()


def clean_spaces_punct(s: str) -> str:
    """
    Сжатие пробелов и выкидывание пунктуации через \w.
    Старое поведение сохраняем.
    """
    t = _nonw.sub(" ", s)
    return _ws.sub(" ", t).strip()


def simple_tokens(text: str) -> List[str]:
    """
    Токенизация по пробелам, как и было.
    """
    return [t for t in _ws.split(text) if t]


# ───────────────────────────────────────────────────────────────
# Вспомогательные функции для продвинутой нормализации шинглов
# ───────────────────────────────────────────────────────────────

def _fold_equiv_char(ch: str) -> str:
    """
    Сведение «эквивалентных» букв.
    Сейчас только:
      - ё -> е
    При желании можно расширить (ә->а и т.д., но это уже спорно для точности).
    """
    cp = ord(ch)
    if cp == 0x0451:  # 'ё'
        return "\u0435"  # 'е'
    return ch


def _is_word_cp(cp: int) -> bool:
    """
    Должно быть максимально похоже на C++ is_word_cp:

      - '_' (подчёркивание)
      - ASCII цифры
      - ASCII латиница
      - Latin-1 Supplement + Latin Extended-A/B + часть IPA: 0x00C0..0x02AF
      - Вся кириллица: 0x0400..0x04FF

    Combining accents (0x0300..0x036F) считаем НЕ-словом.
    """
    # combining accents — не считаем словом
    if 0x0300 <= cp <= 0x036F:
        return False

    # underscore
    if cp == 0x5F:  # '_'
        return True

    # ASCII digits
    if 0x30 <= cp <= 0x39:  # '0'..'9'
        return True

    # ASCII latin
    if 0x41 <= cp <= 0x5A or 0x61 <= cp <= 0x7A:  # 'A'..'Z' 'a'..'z'
        return True

    # Latin extended (включая турецкие, европейские буквы с диакритикой)
    if 0x00C0 <= cp <= 0x02AF:
        return True

    # Кириллица (включая казахские буквы)
    if 0x0400 <= cp <= 0x04FF:
        return True

    return False


def normalize_for_shingles(text: str) -> str:
    """
    Убирает мусор, нормализует Unicode, пробелы, дефисы, невидимые символы.
    Готовит текст для шинглов.

    Новое поведение:
      - чистим zero-width, soft hyphen, NBSP
      - склеиваем перенесённые по дефису слова: "сло-\nво" -> "слово"
      - NFKC
      - Unicode casefold (lower для латиницы/кириллицы/турецких)
      - выкидываем combining marks (Mn, в т.ч. акценты)
      - fold_equiv (ё->е)
      - всё, что не "слово" (см. _is_word_cp), превращаем в пробел
      - последовательные пробелы схлопываются
    """
    if not text:
        return ""

    # убрать невидимые символы и zero-width / soft hyphen / NBSP
    text = _ZW.sub("", text).replace(_SOFT_HYPHEN, "").replace(_NBSP, " ")

    # дефисы и переносы строк: "сло-\nво" -> "слово"
    text = re.sub(r"([^\W\d_])-\s+([^\W\d_])", r"\1\2", text, flags=re.UNICODE)
    # переносы строк -> пробел
    text = re.sub(r"[\r\n]+", " ", text)

    # Unicode нормализация
    text = unicodedata.normalize("NFKC", text)

    # casefold лучше обычного lower для турецких/кириллицы
    text = text.casefold()

    out_chars: List[str] = []
    prev_space = False

    for ch in text:
        cp = ord(ch)

        # выброс combining marks (категория Mn или диапазон 0300..036F)
        if unicodedata.category(ch) == "Mn" or (0x0300 <= cp <= 0x036F):
            continue

        # сводим эквиваленты (ё -> е)
        ch = _fold_equiv_char(ch)
        cp = ord(ch)

        # проверяем, является ли символ "словом"
        if _is_word_cp(cp):
            out_chars.append(ch)
            prev_space = False
        else:
            if not prev_space:
                out_chars.append(" ")
                prev_space = True

    # финальная строка + trim пробелов
    res = "".join(out_chars).strip()
    return res
