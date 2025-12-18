// cpp/common/text_common.h  (ENG+RU+KZ, robust raw-mode + FULL legacy shingles API)
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

// ───────────────────────────────────────────────────────────────
// UTF-8 decode / encode (robust + scalar validation)
// ───────────────────────────────────────────────────────────────

inline bool _is_valid_scalar(std::uint32_t cp) {
    if (cp > 0x10FFFF) return false;
    if (cp >= 0xD800 && cp <= 0xDFFF) return false; // surrogates
    return true;
}

inline bool decode_utf8_cp(
    const unsigned char* data,
    std::size_t n,
    std::size_t& i,
    std::uint32_t& cp
) {
    if (i >= n) return false;

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
        if ((c1 & 0xC0) != 0x80) { cp = 0x20; ++i; return false; }

        std::uint32_t v =
            ((std::uint32_t)(c & 0x1F) << 6) |
            (std::uint32_t)(c1 & 0x3F);

        // anti-overlong: 2-byte must be >= 0x80
        if (v < 0x80) { cp = 0x20; ++i; return false; }
        if (!_is_valid_scalar(v)) { cp = 0x20; ++i; return false; }

        cp = v;
        i += 2;
        return true;
    }

    // 3-byte
    if ((c & 0xF0) == 0xE0 && i + 2 < n) {
        unsigned char c1 = data[i + 1];
        unsigned char c2 = data[i + 2];
        if (((c1 & 0xC0) != 0x80) || ((c2 & 0xC0) != 0x80)) { cp = 0x20; ++i; return false; }

        std::uint32_t v =
            ((std::uint32_t)(c  & 0x0F) << 12) |
            ((std::uint32_t)(c1 & 0x3F) << 6)  |
            (std::uint32_t)(c2 & 0x3F);

        // anti-overlong: 3-byte must be >= 0x800
        if (v < 0x800) { cp = 0x20; ++i; return false; }
        if (!_is_valid_scalar(v)) { cp = 0x20; ++i; return false; }

        cp = v;
        i += 3;
        return true;
    }

    // 4-byte
    if ((c & 0xF8) == 0xF0 && i + 3 < n) {
        unsigned char c1 = data[i + 1];
        unsigned char c2 = data[i + 2];
        unsigned char c3 = data[i + 3];
        if (((c1 & 0xC0) != 0x80) || ((c2 & 0xC0) != 0x80) || ((c3 & 0xC0) != 0x80)) {
            cp = 0x20; ++i; return false;
        }

        std::uint32_t v =
            ((std::uint32_t)(c  & 0x07) << 18) |
            ((std::uint32_t)(c1 & 0x3F) << 12) |
            ((std::uint32_t)(c2 & 0x3F) << 6)  |
            (std::uint32_t)(c3 & 0x3F);

        // anti-overlong: 4-byte must be >= 0x10000
        if (v < 0x10000) { cp = 0x20; ++i; return false; }
        if (!_is_valid_scalar(v)) { cp = 0x20; ++i; return false; }

        cp = v;
        i += 4;
        return true;
    }

    // invalid leading byte
    cp = 0x20;
    ++i;
    return false;
}

inline void append_utf8_cp(std::string& out, std::uint32_t cp) {
    if (!_is_valid_scalar(cp)) {
        out.push_back(' ');
        return;
    }

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
// Case-fold: ENG + RU + KZ (no Turkish)
// ───────────────────────────────────────────────────────────────

inline std::uint32_t to_lower_ru_kk_en(std::uint32_t cp) {
    // ASCII latin
    if (cp >= 'A' && cp <= 'Z') return cp + 32;

    // Cyrillic А..Я
    if (cp >= 0x0410 && cp <= 0x042F) return cp + 0x20;

    // Ё
    if (cp == 0x0401) return 0x0451;

    // І (kaz)
    if (cp == 0x0406) return 0x0456;

    // KZ specific capitals
    if (cp == 0x04D8) return 0x04D9; // Ә
    if (cp == 0x0492) return 0x0493; // Ғ
    if (cp == 0x049A) return 0x049B; // Қ
    if (cp == 0x04A2) return 0x04A3; // Ң
    if (cp == 0x04E8) return 0x04E9; // Ө
    if (cp == 0x04B0) return 0x04B1; // Ұ
    if (cp == 0x04AE) return 0x04AF; // Ү
    if (cp == 0x04BA) return 0x04BB; // Һ

    return cp;
}

inline std::uint32_t fold_equiv(std::uint32_t cp) {
    switch (cp) {
        case 0x0451: // ё -> е
            return 0x0435;
        default:
            return cp;
    }
}

inline bool is_word_cp(std::uint32_t cp) {
    // ignore combining accents
    if (cp >= 0x0300 && cp <= 0x036F) return false;

    if (cp == '_') return true;

    // digits
    if (cp >= '0' && cp <= '9') return true;

    // ASCII latin
    if ((cp >= 'a' && cp <= 'z') || (cp >= 'A' && cp <= 'Z')) return true;

    // Latin-1 Supplement + Latin Extended-A/B
    if (cp >= 0x00C0 && cp <= 0x02AF) return true;

    // Cyrillic block (RU+KZ)
    if (cp >= 0x0400 && cp <= 0x04FF) return true;

    return false;
}

// ───────────────────────────────────────────────────────────────
// trim helper (ASCII spaces only, because normalize emits ' ' only)
// ───────────────────────────────────────────────────────────────

inline void trim_spaces(std::string& s) {
    std::size_t start = 0;
    std::size_t end   = s.size();

    while (start < end && s[start] == ' ') ++start;
    while (end > start && s[end - 1] == ' ') --end;

    if (start == 0 && end == s.size()) return;
    if (start >= end) { s.clear(); return; }
    s = s.substr(start, end - start);
}

// ───────────────────────────────────────────────────────────────
// Normalization for shingles (ENG+RU+KZ friendly)
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
        if (c < 0x80) { cp = c; ++i; ok = true; }
        else          { ok = decode_utf8_cp(data, n, i, cp); }

        if (!ok) {
            if (!prev_space) { out.push_back(' '); prev_space = true; }
            continue;
        }

        // normalize special Unicode spaces to delimiter
        if (cp == 0x00A0 || cp == 0x2009 || cp == 0x200A ||
            cp == 0x202F || cp == 0x2007 || cp == 0x2002 ||
            cp == 0x2003 || cp == 0x2001 || cp == 0x2004 ||
            cp == 0x2005 || cp == 0x2006) {
            if (!prev_space) { out.push_back(' '); prev_space = true; }
            continue;
        }

        cp = to_lower_ru_kk_en(cp);
        cp = fold_equiv(cp);

        // drop combining accents
        if (cp >= 0x0300 && cp <= 0x036F) continue;

        if (is_word_cp(cp)) {
            append_utf8_cp(out, cp);
            prev_space = false;
        } else {
            if (!prev_space) { out.push_back(' '); prev_space = true; }
        }
    }

    trim_spaces(out);
    return out;
}

// ───────────────────────────────────────────────────────────────
// Legacy tokenization (space-only) kept for compatibility
// ───────────────────────────────────────────────────────────────

inline std::vector<std::string> simple_tokens(const std::string& text) {
    std::vector<std::string> toks;
    std::string cur;
    toks.reserve(128);

    for (unsigned char c : text) {
        if (c == ' ') {
            if (!cur.empty()) { toks.push_back(cur); cur.clear(); }
        } else {
            cur.push_back(static_cast<char>(c));
        }
    }
    if (!cur.empty()) toks.push_back(cur);
    return toks;
}

// ───────────────────────────────────────────────────────────────
// Token spans (raw-safe, UTF-8 aware, no casefold, no byte changes)
// ───────────────────────────────────────────────────────────────

struct TokenSpan {
    std::uint32_t off;
    std::uint32_t len;
};

inline void tokenize_spans(const std::string& text, std::vector<TokenSpan>& toks) {
    toks.clear();
    toks.reserve(128);

    const unsigned char* data = reinterpret_cast<const unsigned char*>(text.data());
    const std::size_t n = text.size();

    std::size_t i = 0;
    bool in_tok = false;
    std::size_t tok_start = 0;

    while (i < n) {
        const std::size_t byte_pos = i;
        std::uint32_t cp = 0;
        bool ok = false;

        unsigned char c = data[i];
        if (c < 0x80) { cp = c; ++i; ok = true; }
        else          { ok = decode_utf8_cp(data, n, i, cp); }

        if (!ok) {
            if (in_tok) {
                const std::size_t len = byte_pos - tok_start;
                if (len) toks.push_back(TokenSpan{(std::uint32_t)tok_start, (std::uint32_t)len});
                in_tok = false;
            }
            continue;
        }

        if (!is_word_cp(cp)) {
            if (in_tok) {
                const std::size_t len = byte_pos - tok_start;
                if (len) toks.push_back(TokenSpan{(std::uint32_t)tok_start, (std::uint32_t)len});
                in_tok = false;
            }
            continue;
        }

        if (!in_tok) {
            tok_start = byte_pos;
            in_tok = true;
        }
    }

    if (in_tok) {
        const std::size_t len = n - tok_start;
        if (len) toks.push_back(TokenSpan{(std::uint32_t)tok_start, (std::uint32_t)len});
    }
}

// ───────────────────────────────────────────────────────────────
// FNV-1a 64 and shingles (FULL legacy API)
// ───────────────────────────────────────────────────────────────

inline std::uint64_t fnv1a64_bytes(const unsigned char* data, std::size_t len) {
    const std::uint64_t FNV_OFFSET = 1469598103934665603ULL;
    const std::uint64_t FNV_PRIME  = 1099511628211ULL;

    std::uint64_t h = FNV_OFFSET;
    for (std::size_t i = 0; i < len; ++i) {
        h ^= data[i];
        h *= FNV_PRIME;
    }
    return h;
}

inline std::uint64_t fnv1a64_bytes_seed(const unsigned char* data, std::size_t len, std::uint64_t seed) {
    const std::uint64_t FNV_PRIME  = 1099511628211ULL;
    std::uint64_t h = seed;
    for (std::size_t i = 0; i < len; ++i) {
        h ^= data[i];
        h *= FNV_PRIME;
    }
    return h;
}

inline std::uint64_t fnv1a64(const std::string& s) {
    return fnv1a64_bytes(reinterpret_cast<const unsigned char*>(s.data()), s.size());
}

inline std::uint64_t hash_shingle(const std::string& s) {
    return fnv1a64(s);
}

// legacy: shingles by vector<string>
inline std::uint64_t hash_shingle_tokens(const std::vector<std::string>& toks, int start, int k) {
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

        const unsigned char* data = reinterpret_cast<const unsigned char*>(token.data());
        const std::size_t len = token.size();
        for (std::size_t i = 0; i < len; ++i) { h ^= data[i]; h *= FNV_PRIME; }
    }
    return h;
}

inline std::vector<std::uint64_t> build_shingles(const std::vector<std::string>& toks, int k) {
    std::vector<std::uint64_t> out;
    const int n = (int)toks.size();
    if (n < k) return out;

    const int cnt = n - k + 1;
    out.reserve(cnt);
    for (int i = 0; i < cnt; ++i) out.push_back(hash_shingle_tokens(toks, i, k));
    return out;
}

// spans: toks[i] + ' ' + ... + toks[i+k-1] without temp string
inline std::uint64_t hash_shingle_tokens_spans(
    const std::string& buf,
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

        const auto* data = reinterpret_cast<const unsigned char*>(buf.data() + ts.off);
        for (std::size_t i = 0; i < ts.len; ++i) { h ^= data[i]; h *= FNV_PRIME; }
    }
    return h;
}

inline std::vector<std::uint64_t> build_shingles_spans(
    const std::string& buf,
    const std::vector<TokenSpan>& toks,
    int k
) {
    std::vector<std::uint64_t> out;
    const int n = (int)toks.size();
    if (n < k) return out;

    const int cnt = n - k + 1;
    out.reserve(cnt);
    for (int i = 0; i < cnt; ++i) out.push_back(hash_shingle_tokens_spans(buf, toks, i, k));
    return out;
}

// simhash128 by TokenSpan
inline std::pair<std::uint64_t, std::uint64_t> simhash128_spans(
    const std::string& buf,
    const std::vector<TokenSpan>& toks
) {
    long long v[128] = {0};

    constexpr std::uint64_t SEED1 = 1469598103934665603ULL;
    constexpr std::uint64_t SEED2 = 1099511628211ULL;

    for (const auto& ts : toks) {
        const auto* data = reinterpret_cast<const unsigned char*>(buf.data() + ts.off);

        std::uint64_t lo = fnv1a64_bytes_seed(data, ts.len, SEED1);
        std::uint64_t hi = fnv1a64_bytes_seed(data, ts.len, SEED2);

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
