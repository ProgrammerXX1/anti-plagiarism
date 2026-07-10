/*
 * DOCX parser: extract text runs with color/font info from word/document.xml
 *
 * DOCX is a ZIP archive. The main text body is in word/document.xml.
 * Structure: w:body > w:p (paragraphs) > w:r (runs) > w:t (text)
 * Run properties: w:r > w:rPr > w:color (val attr), w:sz (val = half-points),
 *                 w:shd (fill attr for background), w:rFonts (ascii attr)
 *
 * OMML formula support: paragraphs may contain m:oMath / m:oMathPara elements
 * with math runs (m:r > m:t).  Formula text is extracted as a single TextRun
 * with is_formula=true so downstream detectors can skip homoglyph checks
 * (Latin variable names next to Cyrillic subscripts are normal in formulas).
 */
#include "docx_parser.h"
#include "zip_xml_util.h"
#include <pugixml.hpp>
#include <sstream>
#include <cstdlib>

namespace dn {

// Parse hex color string "RRGGBB" to [r,g,b]
static std::vector<int> parse_hex_color(const char* hex) {
    if (!hex || strlen(hex) < 6) return {};
    // skip leading '#' if present
    if (hex[0] == '#') ++hex;
    if (strlen(hex) < 6) return {};

    char buf[3] = {};
    std::vector<int> rgb(3);
    for (int i = 0; i < 3; ++i) {
        buf[0] = hex[i*2]; buf[1] = hex[i*2+1];
        rgb[i] = static_cast<int>(strtol(buf, nullptr, 16));
    }
    return rgb;
}

// Recursively collect all m:t text from an OMML node (m:oMath, m:oMathPara, etc.)
// Strips leading/trailing whitespace-only padding that OMML uses for alignment.
static void collect_math_text(const pugi::xml_node& node, std::string& out) {
    for (auto child = node.first_child(); child; child = child.next_sibling()) {
        if (strcmp(child.name(), "m:t") == 0) {
            const char* raw = child.text().get();
            if (raw) out += raw;
        } else {
            collect_math_text(child, out);
        }
    }
}

// Normalize OMML text: trim edges, collapse internal whitespace runs to single space.
// OMML uses long space padding for alignment of equation numbers like "(1)".
static std::string normalize_math_text(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    bool in_space = true;  // trim leading
    for (char c : s) {
        if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
            if (!in_space) {
                out += ' ';
                in_space = true;
            }
        } else {
            out += c;
            in_space = false;
        }
    }
    // trim trailing space
    if (!out.empty() && out.back() == ' ') out.pop_back();
    return out;
}

// Extract a w:r text run with style properties
static void extract_wr_run(const pugi::xml_node& r_node,
                           int para_idx, int& offset_in_para,
                           bool& para_has_content, ParsedDocument& doc) {
    std::string text;
    for (auto t_node = r_node.child("w:t"); t_node; t_node = t_node.next_sibling("w:t")) {
        text += t_node.text().get();
    }
    if (text.empty()) return;

    para_has_content = true;
    TextRun run;
    run.text = text;
    run.page = 1;
    run.paragraph = para_idx;
    run.offset = offset_in_para;

    // Run properties
    auto rpr = r_node.child("w:rPr");
    if (rpr) {
        // Hidden text: w:vanish (Word's "hidden" formatting)
        if (rpr.child("w:vanish")) {
            run.is_vanish = true;
        }

        // Font color: w:color val="RRGGBB"
        auto color_node = rpr.child("w:color");
        if (color_node) {
            const char* val = color_node.attribute("w:val").as_string(nullptr);
            if (val && strcmp(val, "auto") != 0) {
                run.color_rgb = parse_hex_color(val);
            }
        }

        // Font size: w:sz val="24" (half-points, so 24 = 12pt)
        auto sz_node = rpr.child("w:sz");
        if (sz_node) {
            int half_pt = sz_node.attribute("w:val").as_int(0);
            if (half_pt > 0) run.font_size = half_pt / 2.0;
        }

        // Background/highlight: w:shd fill="RRGGBB"
        auto shd_node = rpr.child("w:shd");
        if (shd_node) {
            const char* fill = shd_node.attribute("w:fill").as_string(nullptr);
            if (fill && strcmp(fill, "auto") != 0) {
                run.background_rgb = parse_hex_color(fill);
            }
        }

        // Font name
        auto fonts_node = rpr.child("w:rFonts");
        if (fonts_node) {
            const char* ascii = fonts_node.attribute("w:ascii").as_string(nullptr);
            if (ascii) run.font_name = ascii;
        }
    }

    offset_in_para += static_cast<int>(text.size());
    doc.runs.push_back(std::move(run));
}

// Extract text from an OMML formula node (m:oMath or m:oMathPara)
static void extract_math_run(const pugi::xml_node& math_node,
                             int para_idx, int& offset_in_para,
                             bool& para_has_content, ParsedDocument& doc) {
    std::string raw;
    collect_math_text(math_node, raw);
    std::string text = normalize_math_text(raw);
    if (text.empty()) return;

    para_has_content = true;
    TextRun run;
    run.text = text;
    run.page = 1;
    run.paragraph = para_idx;
    run.offset = offset_in_para;
    run.is_formula = true;

    offset_in_para += static_cast<int>(text.size());
    doc.runs.push_back(std::move(run));
}

ParsedDocument parse_docx(const std::string& filename, const std::string& data) {
    ParsedDocument doc;
    doc.filename = filename;

    // Extract word/document.xml from ZIP
    std::string xml_data = zip_extract_file(data, "word/document.xml");
    if (xml_data.empty()) return doc;

    pugi::xml_document xdoc;
    // parse_ws_pcdata: preserve whitespace-only text nodes (spaces between runs)
    unsigned int flags = pugi::parse_default | pugi::parse_ws_pcdata;
    if (!xdoc.load_buffer(xml_data.data(), xml_data.size(), flags)) return doc;

    // Navigate: w:document > w:body > w:p*
    auto body = xdoc.child("w:document").child("w:body");
    if (!body) return doc;

    int para_idx = 0;
    for (auto p_node = body.child("w:p"); p_node; p_node = p_node.next_sibling("w:p")) {
        int offset_in_para = 0;
        bool para_has_content = false;

        // Iterate ALL children of w:p in document order:
        // w:r (text runs), m:oMath (inline formulas), m:oMathPara (display formulas)
        for (auto child = p_node.first_child(); child; child = child.next_sibling()) {
            const char* name = child.name();
            if (strcmp(name, "w:r") == 0) {
                extract_wr_run(child, para_idx, offset_in_para, para_has_content, doc);
            } else if (strcmp(name, "m:oMath") == 0) {
                extract_math_run(child, para_idx, offset_in_para, para_has_content, doc);
            } else if (strcmp(name, "m:oMathPara") == 0) {
                // m:oMathPara wraps one or more m:oMath elements
                for (auto om = child.child("m:oMath"); om; om = om.next_sibling("m:oMath")) {
                    extract_math_run(om, para_idx, offset_in_para, para_has_content, doc);
                }
            }
        }

        // Only increment paragraph index for non-empty paragraphs
        // This ensures paragraph numbers match line indices in report text
        if (para_has_content) ++para_idx;
    }

    return doc;
}

} // namespace dn
