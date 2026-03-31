/*
 * Symbol-substitution (homoglyph) detector.
 *
 * For each word, determine the dominant Unicode script (CYRILLIC or LATIN).
 * If a word contains characters from both scripts, flag each foreign character.
 *
 * Handles Russian, English, Kazakh, Turkish.
 * We use ICU for robust Unicode script detection (UScriptCode).
 */
#include "symbol_substitution.h"

#include <unicode/uchar.h>
#include <unicode/uscript.h>
#include <unicode/utf8.h>

#include <algorithm>
#include <cstring>

namespace dn {

// ── Script classification ──
enum class ScriptClass { CYRILLIC, LATIN, OTHER };

static ScriptClass classify_cp(UChar32 cp) {
    UErrorCode err = U_ZERO_ERROR;
    UScriptCode sc = uscript_getScript(cp, &err);
    if (U_FAILURE(err)) return ScriptClass::OTHER;
    switch (sc) {
        case USCRIPT_CYRILLIC: return ScriptClass::CYRILLIC;
        case USCRIPT_LATIN:    return ScriptClass::LATIN;
        default:               return ScriptClass::OTHER;
    }
}

// Known Cyrillic→Latin homoglyphs (by Unicode codepoint)
// а→a, е→e, о→o, с→c, р→p, у→y, х→x, ...
static bool is_known_homoglyph(UChar32 cp, ScriptClass foreign) {
    // Cyrillic chars that look like Latin:
    // а(0x430)→a, е(0x435)→e, о(0x43E)→o, с(0x441)→c,
    // р(0x440)→p, у(0x443)→y, х(0x445)→x,
    // А(0x410)→A, Е(0x415)→E, О(0x41E)→O, С(0x421)→C,
    // Р(0x420)→P, У(0x423)→Y, Х(0x425)→X
    // Latin chars that look like Cyrillic:
    // a,e,o,c,p,y,x,A,E,O,C,P,Y,X
    static const UChar32 cyr_homos[] = {
        0x430, 0x435, 0x43E, 0x441, 0x440, 0x443, 0x445,
        0x410, 0x415, 0x41E, 0x421, 0x420, 0x423, 0x425,
        0x456, // і (Ukrainian/Kazakh i)
        0x406, // І
    };
    static const UChar32 lat_homos[] = {
        'a','e','o','c','p','y','x',
        'A','E','O','C','P','Y','X',
        'i','I',
    };
    if (foreign == ScriptClass::LATIN) {
        for (auto h : lat_homos) if (cp == h) return true;
    } else if (foreign == ScriptClass::CYRILLIC) {
        for (auto h : cyr_homos) if (cp == h) return true;
    }
    return false;
}

// ── UTF-8 helpers ──
struct CodepointInfo {
    UChar32 cp;
    int byte_offset;
    int byte_len;
};

static std::vector<CodepointInfo> decode_utf8(const std::string& s) {
    std::vector<CodepointInfo> cps;
    cps.reserve(s.size());  // upper bound
    int32_t i = 0;
    int32_t len = static_cast<int32_t>(s.size());
    while (i < len) {
        UChar32 cp;
        int32_t prev = i;
        U8_NEXT(s.data(), i, len, cp);
        if (cp < 0) cp = 0xFFFD;  // replacement
        cps.push_back({cp, static_cast<int>(prev), static_cast<int>(i - prev)});
    }
    return cps;
}

static bool is_word_char(UChar32 cp) {
    return u_isalpha(cp) || cp == '-' || cp == 0x2019 /*'*/;
}

std::vector<Finding> detect_symbols(const std::vector<TextRun>& runs) {
    std::vector<Finding> findings;

    for (auto& run : runs) {
        if (run.text.empty()) continue;

        auto cps = decode_utf8(run.text);
        size_t n = cps.size();
        size_t i = 0;

        while (i < n) {
            // Skip non-word chars
            if (!is_word_char(cps[i].cp)) { ++i; continue; }

            // Collect word
            size_t word_start = i;
            while (i < n && is_word_char(cps[i].cp)) ++i;
            size_t word_end = i;

            if (word_end - word_start < 2) continue; // single char words can't be mixed

            // Count scripts
            int cyr_count = 0, lat_count = 0;
            for (size_t k = word_start; k < word_end; ++k) {
                auto sc = classify_cp(cps[k].cp);
                if (sc == ScriptClass::CYRILLIC) ++cyr_count;
                else if (sc == ScriptClass::LATIN) ++lat_count;
            }

            if (cyr_count == 0 || lat_count == 0) continue; // pure script — OK

            // Mixed! Dominant script is the one with more chars
            ScriptClass dominant = (cyr_count >= lat_count) ? ScriptClass::CYRILLIC : ScriptClass::LATIN;
            ScriptClass foreign_sc = (dominant == ScriptClass::CYRILLIC) ? ScriptClass::LATIN : ScriptClass::CYRILLIC;

            // Find foreign characters
            for (size_t k = word_start; k < word_end; ++k) {
                auto sc = classify_cp(cps[k].cp);
                if (sc == foreign_sc) {
                    int byte_off = cps[k].byte_offset;
                    int byte_len = cps[k].byte_len;

                    // Build word for snippet
                    std::string word_str;
                    for (size_t w = word_start; w < word_end; ++w)
                        word_str += run.text.substr(cps[w].byte_offset, cps[w].byte_len);

                    // Char being flagged
                    std::string char_str = run.text.substr(byte_off, byte_len);

                    std::string dom_name = (dominant == ScriptClass::CYRILLIC) ? "CYRILLIC" : "LATIN";
                    std::string for_name = (foreign_sc == ScriptClass::CYRILLIC) ? "CYRILLIC" : "LATIN";

                    Finding f;
                    f.fraud_type = FraudType::change_word;
                    f.offset = run.offset + byte_off;
                    f.word = word_str;
                    f.limit = 1;  // always 1 foreign char
                    findings.push_back(std::move(f));
                }
            }
        }
    }

    return findings;
}

} // namespace dn
