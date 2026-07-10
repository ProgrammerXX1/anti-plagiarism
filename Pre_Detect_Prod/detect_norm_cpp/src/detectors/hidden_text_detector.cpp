/*
 * Hidden-text detector:
 *   1) Near-white text (all RGB components > 240)
 *   2) Text color ≈ background color (Euclidean distance < 30)
 *   3) Micro-font (size < 2pt)
 */
#include "hidden_text_detector.h"
#include <cmath>
#include <algorithm>
#include <unicode/utf8.h>

namespace dn {

// Safe UTF-8 truncation — never cut in the middle of a multi-byte sequence
static std::string safe_substr_utf8(const std::string& s, size_t max_bytes) {
    if (s.size() <= max_bytes) return s;
    size_t pos = max_bytes;
    // Walk back until we're at the start of a UTF-8 character
    while (pos > 0 && (static_cast<unsigned char>(s[pos]) & 0xC0) == 0x80) --pos;
    return s.substr(0, pos);
}

static constexpr int    NEAR_WHITE_THRESHOLD   = 240;
static constexpr double MIN_FONT_SIZE_PT       = 2.0;
static constexpr double COLOR_DISTANCE_THRESH  = 30.0;

static bool is_near_white(const std::vector<int>& rgb) {
    if (rgb.size() < 3) return false;
    return rgb[0] > NEAR_WHITE_THRESHOLD &&
           rgb[1] > NEAR_WHITE_THRESHOLD &&
           rgb[2] > NEAR_WHITE_THRESHOLD;
}

static double color_distance(const std::vector<int>& a, const std::vector<int>& b) {
    if (a.size() < 3 || b.size() < 3) return 999.0;
    double dr = a[0] - b[0], dg = a[1] - b[1], db = a[2] - b[2];
    return std::sqrt(dr*dr + dg*dg + db*db);
}

static bool is_only_spaces(const std::string& s) {
    for (unsigned char c : s) {
        if (c != ' ' && c != '\t' && c != '\n' && c != '\r') return false;
    }
    return true;
}

std::vector<Finding> detect_hidden(const std::vector<TextRun>& runs) {
    std::vector<Finding> findings;

    for (auto& run : runs) {
        if (run.text.empty() || is_only_spaces(run.text)) continue;

        // Compute text length for limit (UTF-8 codepoint count)
        int text_len = 0;
        {
            int32_t ci = 0;
            int32_t cl = static_cast<int32_t>(run.text.size());
            const char* cs = run.text.data();
            while (ci < cl) {
                UChar32 ccp;
                U8_NEXT(cs, ci, cl, ccp);
                ++text_len;
            }
        }

        // 1) Near-white text
        if (!run.color_rgb.empty() && is_near_white(run.color_rgb)) {
            Finding f;
            f.fraud_type = FraudType::hidden_symbols;
            f.paragraph = run.paragraph;
            f.offset = run.offset;
            f.word = safe_substr_utf8(run.text, 80);
            f.limit = text_len;
            findings.push_back(std::move(f));
            continue; // don't double-flag
        }

        // 2) Text color ≈ background
        if (!run.color_rgb.empty() && !run.background_rgb.empty()) {
            double dist = color_distance(run.color_rgb, run.background_rgb);
            if (dist < COLOR_DISTANCE_THRESH) {
                Finding f;
                f.fraud_type = FraudType::hidden_symbols;
                f.paragraph = run.paragraph;
                f.offset = run.offset;
                f.word = safe_substr_utf8(run.text, 80);
                f.limit = text_len;
                findings.push_back(std::move(f));
                continue;
            }
        }

        // 3) Micro-font
        if (run.font_size > 0 && run.font_size < MIN_FONT_SIZE_PT) {
            Finding f;
            f.fraud_type = FraudType::hidden_symbols;
            f.paragraph = run.paragraph;
            f.offset = run.offset;
            f.word = safe_substr_utf8(run.text, 80);
            f.limit = text_len;
            findings.push_back(std::move(f));
        }
    }

    return findings;
}

} // namespace dn
