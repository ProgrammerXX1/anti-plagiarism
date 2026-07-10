/*
 * Spacing detector:
 *   1) Multiple consecutive ASCII spaces (2+)
 *   2) Non-breaking spaces (U+00A0, etc.)
 *   3) Zero-width characters (ZWSP, ZWJ, ZWNJ, etc.)
 *   4) Exotic Unicode spaces (em-space, en-space, thin-space, etc.)
 */
#include "spacing_detector.h"

#include <unicode/utf8.h>
#include <cstring>

namespace dn {

// NBSP-like: common in Word, only suspicious when 2+ consecutive
static bool is_nbsp_like(UChar32 cp) {
    return cp == 0x00A0 || cp == 0x202F;  // NBSP, NARROW NBSP
}

// Exotic Unicode spaces: always suspicious (single occurrence = fraud)
static bool is_suspicious_space(UChar32 cp) {
    switch (cp) {
        case 0x2000: // EN QUAD
        case 0x2001: // EM QUAD
        case 0x2002: // EN SPACE
        case 0x2003: // EM SPACE
        case 0x2004: // THREE-PER-EM
        case 0x2005: // FOUR-PER-EM
        case 0x2006: // SIX-PER-EM
        case 0x2007: // FIGURE SPACE
        case 0x2008: // PUNCTUATION SPACE
        case 0x2009: // THIN SPACE
        case 0x200A: // HAIR SPACE
        case 0x205F: // MEDIUM MATHEMATICAL SPACE
        case 0x3000: // IDEOGRAPHIC SPACE
            return true;
        default:
            return false;
    }
}

static bool is_zero_width(UChar32 cp) {
    switch (cp) {
        case 0x200B: // ZWSP
        case 0x200C: // ZWNJ
        case 0x200D: // ZWJ
        case 0x200E: // LRM
        case 0x200F: // RLM
        case 0x2060: // WORD JOINER
        case 0xFEFF: // BOM / ZWNBSP
        case 0x034F: // COMBINING GRAPHEME JOINER
            return true;
        default:
            return false;
    }
}

static const char* space_name(UChar32 cp) {
    switch (cp) {
        case 0x00A0: return "NBSP (U+00A0)";
        case 0x2000: return "EN QUAD (U+2000)";
        case 0x2001: return "EM QUAD (U+2001)";
        case 0x2002: return "EN SPACE (U+2002)";
        case 0x2003: return "EM SPACE (U+2003)";
        case 0x2004: return "THREE-PER-EM SPACE";
        case 0x2005: return "FOUR-PER-EM SPACE";
        case 0x2006: return "SIX-PER-EM SPACE";
        case 0x2007: return "FIGURE SPACE";
        case 0x2008: return "PUNCTUATION SPACE";
        case 0x2009: return "THIN SPACE";
        case 0x200A: return "HAIR SPACE";
        case 0x202F: return "NARROW NBSP";
        case 0x205F: return "MEDIUM MATH SPACE";
        case 0x3000: return "IDEOGRAPHIC SPACE";
        case 0x200B: return "ZERO-WIDTH SPACE";
        case 0x200C: return "ZWNJ";
        case 0x200D: return "ZWJ";
        case 0x200E: return "LRM";
        case 0x200F: return "RLM";
        case 0x2060: return "WORD JOINER";
        case 0xFEFF: return "BOM/ZWNBSP";
        case 0x034F: return "COMBINING GRAPHEME JOINER";
        default: return "SPECIAL SPACE";
    }
}

std::vector<Finding> detect_spaces(const std::vector<TextRun>& runs) {
    std::vector<Finding> findings;

    for (auto& run : runs) {
        if (run.text.empty()) continue;

        const char* s = run.text.data();
        int32_t len = static_cast<int32_t>(run.text.size());
        int32_t i = 0;

        while (i < len) {
            UChar32 cp;
            int32_t prev = i;
            U8_NEXT(s, i, len, cp);
            if (cp < 0) continue;

            int byte_off = static_cast<int>(prev);
            int byte_len = static_cast<int>(i - prev);

            // 1) Exotic Unicode spaces (always suspicious, even single)
            if (is_suspicious_space(cp)) {
                Finding f;
                f.fraud_type = FraudType::spaces;
                f.paragraph = run.paragraph;
                f.offset = run.offset + byte_off;
                f.word = run.text.substr(byte_off, byte_len);
                f.limit = 1;
                findings.push_back(std::move(f));
                continue;
            }

            // 2) Zero-width characters
            if (is_zero_width(cp)) {
                Finding f;
                f.fraud_type = FraudType::spaces;
                f.paragraph = run.paragraph;
                f.offset = run.offset + byte_off;
                f.word = "[invisible]";
                f.limit = 1;
                findings.push_back(std::move(f));
                continue;
            }

            // 3) NBSP-like: only flag 2+ consecutive (single NBSP is normal in Word)
            if (is_nbsp_like(cp)) {
                int nbsp_start = byte_off;
                int count = 1;
                // Count consecutive NBSP-like codepoints
                while (i < len) {
                    int32_t save_i = i;
                    UChar32 next_cp;
                    U8_NEXT(s, i, len, next_cp);
                    if (next_cp >= 0 && is_nbsp_like(next_cp)) {
                        ++count;
                    } else {
                        i = save_i;  // put back
                        break;
                    }
                }
                if (count >= 2) {
                    Finding f;
                    f.fraud_type = FraudType::spaces;
                    f.paragraph = run.paragraph;
                    f.offset = run.offset + nbsp_start;
                    f.word = run.text.substr(nbsp_start, i - nbsp_start);
                    f.limit = count;
                    findings.push_back(std::move(f));
                }
                continue;
            }

            // 4) Multiple consecutive ASCII spaces (2+)
            if (cp == ' ') {
                int space_start = byte_off;
                int count = 1;
                while (i < len && s[i] == ' ') { ++i; ++count; }
                if (count >= 2) {
                    Finding f;
                    f.fraud_type = FraudType::spaces;
                    f.paragraph = run.paragraph;
                    f.offset = run.offset + space_start;
                    f.word = run.text.substr(space_start, count);
                    f.limit = count;
                    findings.push_back(std::move(f));
                }
            }
        }
    }

    return findings;
}

} // namespace dn
