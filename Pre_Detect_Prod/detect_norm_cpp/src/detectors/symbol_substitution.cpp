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
enum class ScriptClass { CYRILLIC, LATIN, GREEK, OTHER };

static ScriptClass classify_cp(UChar32 cp) {
    UErrorCode err = U_ZERO_ERROR;
    UScriptCode sc = uscript_getScript(cp, &err);
    if (U_FAILURE(err)) return ScriptClass::OTHER;
    switch (sc) {
        case USCRIPT_CYRILLIC: return ScriptClass::CYRILLIC;
        case USCRIPT_LATIN:    return ScriptClass::LATIN;
        case USCRIPT_GREEK:    return ScriptClass::GREEK;
        default:               return ScriptClass::OTHER;
    }
}

// ── Homoglyph tables (by Unicode codepoint) ──
// Cyrillic chars that look like Latin:
//   а(0x430)→a, е(0x435)→e, о(0x43E)→o, с(0x441)→c,
//   р(0x440)→p, у(0x443)→y, х(0x445)→x (+ capitals, і/І)
static const UChar32 cyr_homos[] = {
    0x430, 0x435, 0x43E, 0x441, 0x440, 0x443, 0x445,
    0x410, 0x415, 0x41E, 0x421, 0x420, 0x423, 0x425,
    0x456, // і (Ukrainian/Kazakh i)
    0x406, // І
};
// Latin chars that look like Cyrillic: a,e,o,c,p,y,x,i (+ capitals)
static const UChar32 lat_homos[] = {
    'a','e','o','c','p','y','x',
    'A','E','O','C','P','Y','X',
    'i','I',
};
// Greek chars that visually imitate Cyrillic OR Latin letters. Real-world fraud
// replaces e.g. Cyrillic к/о with Greek κ(0x3BA)/ο(0x3BF) — visually identical,
// but breaks dictionary lookups and text matching.
static const UChar32 grk_homos[] = {
    // lowercase: α ε ι κ μ ν ο ρ τ υ χ γ
    0x3B1, 0x3B5, 0x3B9, 0x3BA, 0x3BC, 0x3BD, 0x3BF, 0x3C1, 0x3C4, 0x3C5, 0x3C7, 0x3B3,
    // uppercase: Α Β Ε Ζ Η Ι Κ Μ Ν Ο Ρ Τ Υ Χ
    0x391, 0x392, 0x395, 0x396, 0x397, 0x399, 0x39A, 0x39C, 0x39D, 0x39F, 0x3A1, 0x3A4, 0x3A5, 0x3A7,
};

// Is `cp` (belonging to script `foreign`) a visual homoglyph of a letter in the
// word's `dominant` script? Only Cyrillic/Latin dominant words are checked, so
// genuinely Greek-dominant words (real formulas/terms) are never flagged.
static bool is_known_homoglyph(UChar32 cp, ScriptClass foreign, ScriptClass dominant) {
    if (dominant == ScriptClass::CYRILLIC) {
        if (foreign == ScriptClass::LATIN) {
            for (auto h : lat_homos) if (cp == h) return true;
        } else if (foreign == ScriptClass::GREEK) {
            for (auto h : grk_homos) if (cp == h) return true;
        }
    } else if (dominant == ScriptClass::LATIN) {
        if (foreign == ScriptClass::CYRILLIC) {
            for (auto h : cyr_homos) if (cp == h) return true;
        } else if (foreign == ScriptClass::GREEK) {
            for (auto h : grk_homos) if (cp == h) return true;
        }
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
    // Hyphen excluded: splits formula-terms like "I-категория", "pH-метр",
    // "x-координата" into separate tokens, preventing false positives.
    // Apostrophe kept for Ukrainian/Kazakh contractions (e.g. об'єкт).
    return u_isalpha(cp) || cp == 0x2019 /*'*/ || cp == '\'';
}

// Per-codepoint info inside a merged paragraph segment.
struct Glyph {
    UChar32 cp;
    int seg_off;   // byte offset within the segment string (for snippet extraction)
    int byte_len;
    int abs_off;   // byte offset within the paragraph (run.offset + local offset)
};

std::vector<Finding> detect_symbols(const std::vector<TextRun>& runs) {
    std::vector<Finding> findings;

    // A single word is frequently split across several <w:r> runs by Word
    // (proofing state, rsid marks, or — a common homoglyph-fraud artifact — a
    // single substituted character placed in its own run). We therefore merge
    // consecutive non-formula runs of the same paragraph into one segment and
    // tokenize ACROSS run boundaries. Offsets stay valid because the parser
    // assigns contiguous per-paragraph byte offsets to runs. Formula runs
    // (is_formula) act as hard boundaries — Latin variable names next to
    // Cyrillic subscripts (Iдоп, ΔРпост) are normal in OMML math.
    size_t ri = 0;
    while (ri < runs.size()) {
        if (runs[ri].text.empty() || runs[ri].is_formula) { ++ri; continue; }

        int para = runs[ri].paragraph;
        std::string seg_text;
        std::vector<Glyph> seg;

        while (ri < runs.size() && runs[ri].paragraph == para
               && !runs[ri].is_formula && !runs[ri].text.empty()) {
            const auto& run = runs[ri];
            auto cps = decode_utf8(run.text);
            for (auto& ci : cps) {
                Glyph g;
                g.cp = ci.cp;
                g.seg_off = static_cast<int>(seg_text.size());
                g.byte_len = ci.byte_len;
                g.abs_off = run.offset + ci.byte_offset;
                seg_text.append(run.text, ci.byte_offset, ci.byte_len);
                seg.push_back(g);
            }
            ++ri;
        }

        size_t n = seg.size();
        size_t i = 0;
        while (i < n) {
            // Skip non-word chars
            if (!is_word_char(seg[i].cp)) { ++i; continue; }

            // Collect word
            size_t word_start = i;
            while (i < n && is_word_char(seg[i].cp)) ++i;
            size_t word_end = i;

            if (word_end - word_start < 2) continue; // single char words can't be mixed

            // Count scripts
            int cyr = 0, lat = 0, grk = 0;
            for (size_t k = word_start; k < word_end; ++k) {
                switch (classify_cp(seg[k].cp)) {
                    case ScriptClass::CYRILLIC: ++cyr; break;
                    case ScriptClass::LATIN:    ++lat; break;
                    case ScriptClass::GREEK:    ++grk; break;
                    default: break;
                }
            }

            // Need at least two distinct scripts among Cyrillic/Latin/Greek.
            int scripts_present = (cyr > 0) + (lat > 0) + (grk > 0);
            if (scripts_present < 2) continue; // pure script — OK

            // NOTE: mixed-script words in regular text (Iдоп, κафедра) are reported
            // as findings — the reviewer decides if they are formula variables or fraud.

            // Dominant = most frequent script; the rest are "foreign".
            ScriptClass dominant = ScriptClass::CYRILLIC;
            int best = cyr;
            if (lat > best) { best = lat; dominant = ScriptClass::LATIN; }
            if (grk > best) { best = grk; dominant = ScriptClass::GREEK; }

            // Build word snippet from the merged segment.
            int w_start = seg[word_start].seg_off;
            int w_end = seg[word_end - 1].seg_off + seg[word_end - 1].byte_len;
            std::string word_str = seg_text.substr(w_start, w_end - w_start);

            // Flag each foreign character that is a known homoglyph.
            for (size_t k = word_start; k < word_end; ++k) {
                ScriptClass sc = classify_cp(seg[k].cp);
                if (sc != dominant && is_known_homoglyph(seg[k].cp, sc, dominant)) {
                    Finding f;
                    f.fraud_type = FraudType::change_word;
                    f.paragraph = para;
                    f.offset = seg[k].abs_off;
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
