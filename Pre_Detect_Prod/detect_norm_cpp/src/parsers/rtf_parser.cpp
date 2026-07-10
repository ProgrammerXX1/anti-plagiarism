/*
 * RTF parser: strip RTF markup → plain text → parse as TXT.
 *
 * Handles:
 * - Control words (\par, \tab, \line, etc.)
 * - Unicode escapes (\uN?)
 * - Codepage text (ansicpg1251 = Windows-1251 for Cyrillic)
 * - Nested groups { }
 * - Skips binary data (\bin, \pict, \stylesheet, \fonttbl, \colortbl, etc.)
 *
 * Port of Python rtf_strip.py to C++.
 */
#include "rtf_parser.h"
#include "txt_parser.h"

#include <algorithm>
#include <cstring>
#include <string>
#include <vector>
#include <cstdlib>

namespace dn {

// ── Groups whose content should be skipped entirely ──
static bool is_skip_group(const std::string& word) {
    static const char* groups[] = {
        "fonttbl", "colortbl", "stylesheet", "info", "pict",
        "header", "footer", "headerl", "headerr", "headerf",
        "footerl", "footerr", "footerf", "field", "fldinst",
        "xe", "tc", "rxe", nullptr
    };
    for (const char** g = groups; *g; ++g) {
        if (word == *g) return true;
    }
    return false;
}

// ── Decode a single byte via codepage (cp1251 only for now) ──
// Full Windows-1251 table for 0x80..0xFF
static char32_t cp1251_to_unicode(unsigned char b) {
    if (b < 0x80) return b;
    // Windows-1251 upper half (0x80-0xFF) → Unicode
    static const char32_t table[128] = {
        0x0402, 0x0403, 0x201A, 0x0453, 0x201E, 0x2026, 0x2020, 0x2021, // 80-87
        0x20AC, 0x2030, 0x0409, 0x2039, 0x040A, 0x040C, 0x040B, 0x040F, // 88-8F
        0x0452, 0x2018, 0x2019, 0x201C, 0x201D, 0x2022, 0x2013, 0x2014, // 90-97
        0x0000, 0x2122, 0x0459, 0x203A, 0x045A, 0x045C, 0x045B, 0x045F, // 98-9F
        0x00A0, 0x040E, 0x045E, 0x0408, 0x00A4, 0x0490, 0x00A6, 0x00A7, // A0-A7
        0x0401, 0x00A9, 0x0404, 0x00AB, 0x00AC, 0x00AD, 0x00AE, 0x0407, // A8-AF
        0x00B0, 0x00B1, 0x0406, 0x0456, 0x0491, 0x00B5, 0x00B6, 0x00B7, // B0-B7
        0x0451, 0x2116, 0x0454, 0x00BB, 0x0458, 0x0405, 0x0455, 0x0457, // B8-BF
        0x0410, 0x0411, 0x0412, 0x0413, 0x0414, 0x0415, 0x0416, 0x0417, // C0-C7
        0x0418, 0x0419, 0x041A, 0x041B, 0x041C, 0x041D, 0x041E, 0x041F, // C8-CF
        0x0420, 0x0421, 0x0422, 0x0423, 0x0424, 0x0425, 0x0426, 0x0427, // D0-D7
        0x0428, 0x0429, 0x042A, 0x042B, 0x042C, 0x042D, 0x042E, 0x042F, // D8-DF
        0x0430, 0x0431, 0x0432, 0x0433, 0x0434, 0x0435, 0x0436, 0x0437, // E0-E7
        0x0438, 0x0439, 0x043A, 0x043B, 0x043C, 0x043D, 0x043E, 0x043F, // E8-EF
        0x0440, 0x0441, 0x0442, 0x0443, 0x0444, 0x0445, 0x0446, 0x0447, // F0-F7
        0x0448, 0x0449, 0x044A, 0x044B, 0x044C, 0x044D, 0x044E, 0x044F, // F8-FF
    };
    return table[b - 0x80];
}

// ── Encode a Unicode codepoint to UTF-8 ──
static void append_utf8(std::string& out, char32_t cp) {
    if (cp < 0x80) {
        out += static_cast<char>(cp);
    } else if (cp < 0x800) {
        out += static_cast<char>(0xC0 | (cp >> 6));
        out += static_cast<char>(0x80 | (cp & 0x3F));
    } else if (cp < 0x10000) {
        out += static_cast<char>(0xE0 | (cp >> 12));
        out += static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
        out += static_cast<char>(0x80 | (cp & 0x3F));
    } else if (cp < 0x110000) {
        out += static_cast<char>(0xF0 | (cp >> 18));
        out += static_cast<char>(0x80 | ((cp >> 12) & 0x3F));
        out += static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
        out += static_cast<char>(0x80 | (cp & 0x3F));
    }
}

std::string strip_rtf(const std::string& rtf) {
    if (rtf.size() < 5) return rtf;

    // Check if it starts with {\rtf
    size_t start = 0;
    while (start < rtf.size() && (rtf[start] == ' ' || rtf[start] == '\t' ||
           rtf[start] == '\r' || rtf[start] == '\n')) ++start;
    if (start + 4 >= rtf.size() || rtf.compare(start, 5, "{\\rtf") != 0)
        return rtf;  // Not RTF — return as-is

    std::string out;
    out.reserve(rtf.size() / 2);

    size_t i = 0;
    size_t n = rtf.size();
    int skip_depth = 0;
    int uc_skip = 1;  // chars to skip after \uN

    while (i < n) {
        char ch = rtf[i];

        if (ch == '{') {
            ++i;
            // Check if group starts with a skip-worthy destination
            if (i < n && rtf[i] == '\\') {
                // Peek control word
                size_t j = i + 1;
                bool star = false;
                if (j < n && rtf[j] == '*') {
                    // \* destination
                    star = true;
                    ++j;
                    if (j < n && rtf[j] == '\\') ++j;
                }
                // Read alpha word
                size_t wstart = j;
                while (j < n && rtf[j] >= 'a' && rtf[j] <= 'z') ++j;
                if (j > wstart) {
                    std::string word(rtf, wstart, j - wstart);
                    if (star || is_skip_group(word)) {
                        ++skip_depth;
                        continue;
                    }
                }
            }
            if (skip_depth > 0) {
                ++skip_depth;
            }
            continue;
        }

        if (ch == '}') {
            ++i;
            if (skip_depth > 0) --skip_depth;
            continue;
        }

        if (skip_depth > 0) {
            ++i;
            continue;
        }

        if (ch == '\\') {
            ++i;
            if (i >= n) break;

            // Escaped literal: \\ \{ \}
            if (rtf[i] == '\\' || rtf[i] == '{' || rtf[i] == '}') {
                out += rtf[i];
                ++i;
                continue;
            }

            // Control word: \word[-]NNN
            if (rtf[i] >= 'a' && rtf[i] <= 'z') {
                size_t j = i;
                while (j < n && rtf[j] >= 'a' && rtf[j] <= 'z') ++j;
                std::string word(rtf, i, j - i);

                // Optional numeric parameter
                long param = 0;
                bool has_param = false;
                size_t k = j;
                if (k < n && (rtf[k] == '-' || (rtf[k] >= '0' && rtf[k] <= '9'))) {
                    size_t pstart = k;
                    if (rtf[k] == '-') ++k;
                    while (k < n && rtf[k] >= '0' && rtf[k] <= '9') ++k;
                    param = std::atol(rtf.c_str() + pstart);
                    has_param = true;
                }
                // Space delimiter consumed
                if (k < n && rtf[k] == ' ') ++k;
                i = k;

                // Handle control words
                if (word == "par" || word == "line") {
                    out += '\n';
                } else if (word == "tab") {
                    out += '\t';
                } else if (word == "emspace" || word == "enspace") {
                    out += ' ';
                } else if (word == "lquote" || word == "rquote") {
                    out += '\'';
                } else if (word == "ldblquote" || word == "rdblquote") {
                    out += '"';
                } else if (word == "bullet") {
                    append_utf8(out, 0x2022);
                } else if (word == "endash") {
                    append_utf8(out, 0x2013);
                } else if (word == "emdash") {
                    append_utf8(out, 0x2014);
                } else if (word == "u" && has_param) {
                    // Unicode: \uN?
                    char32_t cp = (param >= 0) ? static_cast<char32_t>(param)
                                               : static_cast<char32_t>(param + 65536);
                    if (cp > 0 && cp < 0x110000) {
                        append_utf8(out, cp);
                    }
                    // Skip uc_skip replacement chars
                    int skipped = 0;
                    while (skipped < uc_skip && i < n) {
                        if (rtf[i] == '\\' && i + 1 < n && rtf[i + 1] == '\'') {
                            i += 4;  // skip \'XX
                        } else if (rtf[i] == '{' || rtf[i] == '}') {
                            break;
                        } else {
                            ++i;
                        }
                        ++skipped;
                    }
                } else if (word == "uc" && has_param) {
                    uc_skip = static_cast<int>(param);
                } else if (word == "ansicpg") {
                    // We only support cp1251 for now (Cyrillic documents)
                    // param gives the codepage number
                } else if (is_skip_group(word)) {
                    ++skip_depth;
                } else if (word == "bin" && has_param && param > 0) {
                    i += static_cast<size_t>(param);
                }
                continue;
            }

            // Hex escape: \'XX
            if (rtf[i] == '\'') {
                ++i;
                if (i + 1 < n) {
                    char hex[3] = {rtf[i], rtf[i + 1], 0};
                    unsigned long val = std::strtoul(hex, nullptr, 16);
                    i += 2;
                    if (val > 0) {
                        char32_t cp = cp1251_to_unicode(static_cast<unsigned char>(val));
                        if (cp > 0) append_utf8(out, cp);
                    }
                }
                continue;
            }

            // Other: skip
            ++i;
            continue;
        }

        // Newlines in RTF source are ignored
        if (ch == '\r' || ch == '\n') {
            ++i;
            continue;
        }

        // Regular character
        out += ch;
        ++i;
    }

    // Clean up excessive blank lines (3+ → 2)
    std::string cleaned;
    cleaned.reserve(out.size());
    int consecutive_nl = 0;
    for (char c : out) {
        if (c == '\n') {
            ++consecutive_nl;
            if (consecutive_nl <= 2) cleaned += c;
        } else {
            consecutive_nl = 0;
            cleaned += c;
        }
    }

    // Trim leading/trailing whitespace
    size_t left = 0;
    while (left < cleaned.size() && (cleaned[left] == ' ' || cleaned[left] == '\n' ||
           cleaned[left] == '\t' || cleaned[left] == '\r')) ++left;
    size_t right = cleaned.size();
    while (right > left && (cleaned[right-1] == ' ' || cleaned[right-1] == '\n' ||
           cleaned[right-1] == '\t' || cleaned[right-1] == '\r')) --right;

    return cleaned.substr(left, right - left);
}

ParsedDocument parse_rtf(const std::string& filename, const std::string& content) {
    std::string plain = strip_rtf(content);
    return parse_txt(filename, plain);
}

} // namespace dn
