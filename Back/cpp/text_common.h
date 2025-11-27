#pragma once

#include <string>
#include <vector>
#include <cstdint>

// ───────────────────────────────────────────────────────────────
// UTF-8 decode / encode
// ───────────────────────────────────────────────────────────────

inline bool decode_utf8_cp(
    const unsigned char* data,
    std::size_t n,
    std::size_t& i,
    std::uint32_t& cp
) {
    if (i >= n) {
        return false;
    }

    unsigned char c = data[i];

    // 1-byte (ASCII)
    if (c < 0x80) {
        cp = c;
        ++i;
        return true;
    }

    // 2-byte
    if ((c & 0xE0) == 0xC0 && i + 1 < n) {
        unsigned char c1 = data[i + 1];
        if ((c1 & 0xC0) != 0x80) {
            cp = 0x20; // space
            ++i;
            return false;
        }
        cp = ((std::uint32_t)(c & 0x1F) << 6) |
             (std::uint32_t)(c1 & 0x3F);
        i += 2;
        return true;
    }

    // 3-byte
    if ((c & 0xF0) == 0xE0 && i + 2 < n) {
        unsigned char c1 = data[i + 1];
        unsigned char c2 = data[i + 2];
        if (((c1 & 0xC0) != 0x80) || ((c2 & 0xC0) != 0x80)) {
            cp = 0x20;
            ++i;
            return false;
        }
        cp = ((std::uint32_t)(c  & 0x0F) << 12) |
             ((std::uint32_t)(c1 & 0x3F) << 6)  |
             (std::uint32_t)(c2 & 0x3F);
        i += 3;
        return true;
    }

    // 4-byte
    if ((c & 0xF8) == 0xF0 && i + 3 < n) {
        unsigned char c1 = data[i + 1];
        unsigned char c2 = data[i + 2];
        unsigned char c3 = data[i + 3];
        if (((c1 & 0xC0) != 0x80) ||
            ((c2 & 0xC0) != 0x80) ||
            ((c3 & 0xC0) != 0x80)) {
            cp = 0x20;
            ++i;
            return false;
        }
        cp = ((std::uint32_t)(c  & 0x07) << 18) |
             ((std::uint32_t)(c1 & 0x3F) << 12) |
             ((std::uint32_t)(c2 & 0x3F) << 6)  |
             (std::uint32_t)(c3 & 0x3F);
        i += 4;
        return true;
    }

    // invalid leading byte — считаем пробелом
    cp = 0x20;
    ++i;
    return false;
}

inline void append_utf8_cp(std::string& out, std::uint32_t cp) {
    if (cp <= 0x7F) {
        out.push_back(static_cast<char>(cp));
    } else if (cp <= 0x7FF) {
        out.push_back(static_cast<char>(0xC0 | ((cp >> 6) & 0x1F)));
        out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    } else if (cp <= 0xFFFF) {
        out.push_back(static_cast<char>(0xE0 | ((cp >> 12) & 0x0F)));
        out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    } else {
        out.push_back(static_cast<char>(0xF0 | ((cp >> 18) & 0x07)));
        out.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    }
}

// ───────────────────────────────────────────────────────────────
// Case-fold: латиница + рус + каз + турецкий
// ───────────────────────────────────────────────────────────────

inline std::uint32_t to_lower_ru_kk_tr(std::uint32_t cp) {
    // ASCII латиница
    if (cp >= 'A' && cp <= 'Z') {
        return cp + 32; // 'A'->'a'
    }

    // Базовая кириллица: А..Я -> а..я
    if (cp >= 0x0410 && cp <= 0x042F) {
        return cp + 0x20;
    }

    // Ё / ё
    if (cp == 0x0401) return 0x0451; // Ё -> ё

    // І / і (kaz)
    if (cp == 0x0406) return 0x0456; // І -> і

    // Казахские специфические заглавные:
    if (cp == 0x04D8) return 0x04D9; // Ә / ә
    if (cp == 0x0492) return 0x0493; // Ғ / ғ
    if (cp == 0x049A) return 0x049B; // Қ / қ
    if (cp == 0x04A2) return 0x04A3; // Ң / ң
    if (cp == 0x04E8) return 0x04E9; // Ө / ө
    if (cp == 0x04B0) return 0x04B1; // Ұ / ұ
    if (cp == 0x04AE) return 0x04AF; // Ү / ү
    if (cp == 0x04BA) return 0x04BB; // Һ / һ

    // Турецкие буквы (латиница с диакритикой):
    if (cp == 0x00C7) return 0x00E7; // Ç / ç
    if (cp == 0x00D6) return 0x00F6; // Ö / ö
    if (cp == 0x00DC) return 0x00FC; // Ü / ü
    if (cp == 0x011E) return 0x011F; // Ğ / ğ
    if (cp == 0x015E) return 0x015F; // Ş / ş
    if (cp == 0x0130) return 0x0069; // İ -> 'i'

    return cp;
}

// Дополнительное сведение «эквивалентных» букв (опционально).
inline std::uint32_t fold_equiv(std::uint32_t cp) {
    switch (cp) {
        case 0x0451: // ё -> е
            return 0x0435;
        default:
            return cp;
    }
}

// «Символ слова» для шинглов
inline bool is_word_cp(std::uint32_t cp) {
    // Игнорируем combining accents: U+0300..U+036F
    if (cp >= 0x0300 && cp <= 0x036F) {
        return false;
    }

    if (cp == '_') return true;

    // ASCII цифры
    if (cp >= '0' && cp <= '9') return true;

    // ASCII латиница
    if ((cp >= 'a' && cp <= 'z') || (cp >= 'A' && cp <= 'Z')) {
        return true;
    }

    // Расширенная латиница
    if (cp >= 0x00C0 && cp <= 0x02AF) {
        return true;
    }

    // Вся кириллица: U+0400..U+04FF
    if (cp >= 0x0400 && cp <= 0x04FF) {
        return true;
    }

    return false;
}

// ───────────────────────────────────────────────────────────────
// trim helper
// ───────────────────────────────────────────────────────────────

inline void trim_spaces(std::string& s) {
    std::size_t start = 0;
    std::size_t end   = s.size();

    while (start < end && s[start] == ' ') ++start;
    while (end > start && s[end - 1] == ' ') --end;

    if (start == 0 && end == s.size()) return;
    if (start >= end) {
        s.clear();
        return;
    }
    s = s.substr(start, end - start); // один memmove
}

// ───────────────────────────────────────────────────────────────
// Нормализация под шинглы (UTF-8, ru+kk+tr friendly)
// ───────────────────────────────────────────────────────────────

inline std::string normalize_for_shingles_simple(const std::string& in) {
    std::string out;
    out.reserve(in.size());

    const unsigned char* data = reinterpret_cast<const unsigned char*>(in.data());
    const std::size_t n = in.size();

    bool prev_space = false;
    std::size_t i = 0;

    while (i < n) {
        std::uint32_t cp = 0;
        bool ok = false;

        unsigned char c = data[i];
        if (c < 0x80) {
            // ASCII fast-path
            cp = c;
            ++i;
            ok = true;
        } else {
            ok = decode_utf8_cp(data, n, i, cp);
        }

        if (!ok) {
            if (!prev_space) {
                out.push_back(' ');
                prev_space = true;
            }
            continue;
        }

        // ========= FIX 1: normalize special Unicode spaces =========
        if (cp == 0x00A0 || cp == 0x2009 || cp == 0x200A ||
            cp == 0x202F || cp == 0x2007 || cp == 0x2002 ||
            cp == 0x2003 || cp == 0x2001 || cp == 0x2004 ||
            cp == 0x2005 || cp == 0x2006) {
            if (!prev_space) {
                out.push_back(' ');
                prev_space = true;
            }
            continue;
        }

        cp = to_lower_ru_kk_tr(cp);
        cp = fold_equiv(cp);

        // ========= FIX 2: fold Turkish/Kazakh dotless i =========
        if (cp == 0x0131) cp = 0x0069;

        // combining accents: выкидываем
        if (cp >= 0x0300 && cp <= 0x036F) {
            continue;
        }

        // ========= FIX 3: remove Extended Latin =========
        if (cp >= 0x00C0 && cp <= 0x02AF) {
            if (!prev_space) {
                out.push_back(' ');
                prev_space = true;
            }
            continue;
        }

        if (is_word_cp(cp)) {
            append_utf8_cp(out, cp);
            prev_space = false;
        } else {
            if (!prev_space) {
                out.push_back(' ');
                prev_space = true;
            }
        }
    }

    trim_spaces(out);
    return out;
}

// ───────────────────────────────────────────────────────────────
// СТАРАЯ токенизация (оставлена для совместимости)
// ───────────────────────────────────────────────────────────────

inline std::vector<std::string> simple_tokens(const std::string& text) {
    std::vector<std::string> toks;
    std::string cur;
    toks.reserve(128);

    for (unsigned char c : text) {
        if (c == ' ') {
            if (!cur.empty()) {
                toks.push_back(cur);
                cur.clear();
            }
        } else {
            cur.push_back(static_cast<char>(c));
        }
    }
    if (!cur.empty()) toks.push_back(cur);
    return toks;
}

// ───────────────────────────────────────────────────────────────
// НОВЫЙ формат токенов: spans (offset,len) по одному norm-буферу
// ───────────────────────────────────────────────────────────────

struct TokenSpan {
    std::uint32_t off;
    std::uint32_t len;
};

inline void tokenize_spans(
    const std::string& text,
    std::vector<TokenSpan>& toks
) {
    toks.clear();
    toks.reserve(128);

    const std::size_t n = text.size();
    std::size_t i = 0;

    while (i < n) {
        while (i < n && text[i] == ' ') {
            ++i;
        }
        if (i >= n) break;

        std::size_t start = i;
        while (i < n && text[i] != ' ') {
            ++i;
        }
        std::size_t len = i - start;
        if (len > 0) {
            TokenSpan ts;
            ts.off = static_cast<std::uint32_t>(start);
            ts.len = static_cast<std::uint32_t>(len);
            toks.push_back(ts);
        }
    }
}

// ───────────────────────────────────────────────────────────────
// FNV-1a 64 и шинглы
// ───────────────────────────────────────────────────────────────

// базовый FNV без seed (старый интерфейс)
inline std::uint64_t fnv1a64_bytes(
    const unsigned char* data,
    std::size_t len
) {
    const std::uint64_t FNV_OFFSET = 1469598103934665603ULL;
    const std::uint64_t FNV_PRIME  = 1099511628211ULL;

    std::uint64_t h = FNV_OFFSET;
    for (std::size_t i = 0; i < len; ++i) {
        h ^= data[i];
        h *= FNV_PRIME;
    }
    return h;
}

// вариант с seed — для simhash
inline std::uint64_t fnv1a64_bytes_seed(
    const unsigned char* data,
    std::size_t len,
    std::uint64_t seed
) {
    const std::uint64_t FNV_PRIME  = 1099511628211ULL;
    std::uint64_t h = seed;
    for (std::size_t i = 0; i < len; ++i) {
        h ^= data[i];
        h *= FNV_PRIME;
    }
    return h;
}

// старый интерфейс для совместимости
inline std::uint64_t fnv1a64(const std::string& s) {
    return fnv1a64_bytes(
        reinterpret_cast<const unsigned char*>(s.data()),
        s.size()
    );
}

inline std::uint64_t hash_shingle(const std::string& s) {
    return fnv1a64(s);
}

// старый вариант: шинглы по vector<string>
inline std::uint64_t hash_shingle_tokens(
    const std::vector<std::string>& toks,
    int start,
    int k
) {
    const std::uint64_t FNV_OFFSET = 1469598103934665603ULL;
    const std::uint64_t FNV_PRIME  = 1099511628211ULL;

    std::uint64_t h = FNV_OFFSET;
    bool first = true;

    for (int j = 0; j < k; ++j) {
        const std::string& token = toks[start + j];

        if (!first) {
            unsigned char sp = static_cast<unsigned char>(' ');
            h ^= sp;
            h *= FNV_PRIME;
        } else {
            first = false;
        }

        const unsigned char* data =
            reinterpret_cast<const unsigned char*>(token.data());
        const std::size_t len = token.size();
        for (std::size_t i = 0; i < len; ++i) {
            h ^= data[i];
            h *= FNV_PRIME;
        }
    }

    return h;
}

inline std::vector<std::uint64_t> build_shingles(
    const std::vector<std::string>& toks,
    int k
) {
    std::vector<std::uint64_t> out;
    const int n = static_cast<int>(toks.size());
    if (n < k) return out;

    const int cnt = n - k + 1;
    out.reserve(cnt);

    for (int i = 0; i < cnt; ++i) {
        out.push_back(hash_shingle_tokens(toks, i, k));
    }

    return out;
}

// ───────────────────────────────────────────────────────────────
// НОВЫЕ шинглы по TokenSpan
// ───────────────────────────────────────────────────────────────

inline std::uint64_t fnv1a64_span(
    const std::string& norm,
    const TokenSpan& span,
    std::uint64_t seed
) {
    const auto* data = reinterpret_cast<const unsigned char*>(
        norm.data() + span.off
    );
    return fnv1a64_bytes_seed(data, span.len, seed);
}

// "toks[i] + ' ' + ... + toks[i+k-1]" без промежуточной строки
inline std::uint64_t hash_shingle_tokens_spans(
    const std::string& norm,
    const std::vector<TokenSpan>& toks,
    int start,
    int k
) {
    const std::uint64_t FNV_OFFSET = 1469598103934665603ULL;
    const std::uint64_t FNV_PRIME  = 1099511628211ULL;

    std::uint64_t h = FNV_OFFSET;
    bool first = true;

    for (int j = 0; j < k; ++j) {
        const TokenSpan& ts = toks[start + j];

        if (!first) {
            unsigned char sp = static_cast<unsigned char>(' ');
            h ^= sp;
            h *= FNV_PRIME;
        } else {
            first = false;
        }

        const auto* data = reinterpret_cast<const unsigned char*>(
            norm.data() + ts.off
        );
        for (std::size_t i = 0; i < ts.len; ++i) {
            h ^= data[i];
            h *= FNV_PRIME;
        }
    }

    return h;
}

inline std::vector<std::uint64_t> build_shingles_spans(
    const std::string& norm,
    const std::vector<TokenSpan>& toks,
    int k
) {
    std::vector<std::uint64_t> out;
    const int n = static_cast<int>(toks.size());
    if (n < k) return out;

    const int cnt = n - k + 1;
    out.reserve(cnt);

    for (int i = 0; i < cnt; ++i) {
        out.push_back(hash_shingle_tokens_spans(norm, toks, i, k));
    }

    return out;
}

// ───────────────────────────────────────────────────────────────
// simhash128 по TokenSpan (для index_builder / поиска)
// ───────────────────────────────────────────────────────────────

inline std::pair<std::uint64_t, std::uint64_t> simhash128_spans(
    const std::string& norm,
    const std::vector<TokenSpan>& toks
) {
    long long v[128] = {0};

    constexpr std::uint64_t SEED1 = 1469598103934665603ULL; // offset
    constexpr std::uint64_t SEED2 = 1099511628211ULL;       // prime

    for (const auto& ts : toks) {
        std::uint64_t lo = fnv1a64_span(norm, ts, SEED1);
        std::uint64_t hi = fnv1a64_span(norm, ts, SEED2);

        for (int i = 0; i < 64; ++i) {
            v[i]      += ((lo >> i) & 1ull) ? 1 : -1;
            v[64 + i] += ((hi >> i) & 1ull) ? 1 : -1;
        }
    }

    std::uint64_t hi = 0, lo = 0;
    for (int i = 0; i < 64; ++i) {
        if (v[i]      >= 0) lo |= (1ull << i);
        if (v[64 + i] >= 0) hi |= (1ull << i);
    }
    return {hi, lo};
}
