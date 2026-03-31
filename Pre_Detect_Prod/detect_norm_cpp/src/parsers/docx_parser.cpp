/*
 * DOCX parser: extract text runs with color/font info from word/document.xml
 *
 * DOCX is a ZIP archive. The main text body is in word/document.xml.
 * Structure: w:body > w:p (paragraphs) > w:r (runs) > w:t (text)
 * Run properties: w:r > w:rPr > w:color (val attr), w:sz (val = half-points),
 *                 w:shd (fill attr for background), w:rFonts (ascii attr)
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

ParsedDocument parse_docx(const std::string& filename, const std::string& data) {
    ParsedDocument doc;
    doc.filename = filename;

    // Extract word/document.xml from ZIP
    std::string xml_data = zip_extract_file(data, "word/document.xml");
    if (xml_data.empty()) return doc;

    pugi::xml_document xdoc;
    if (!xdoc.load_buffer(xml_data.data(), xml_data.size())) return doc;

    // Navigate: w:document > w:body > w:p*
    auto body = xdoc.child("w:document").child("w:body");
    if (!body) return doc;

    int para_idx = 0;
    for (auto p_node = body.child("w:p"); p_node; p_node = p_node.next_sibling("w:p")) {
        int offset_in_para = 0;

        for (auto r_node = p_node.child("w:r"); r_node; r_node = r_node.next_sibling("w:r")) {
            // Collect text (may have multiple w:t nodes)
            std::string text;
            for (auto t_node = r_node.child("w:t"); t_node; t_node = t_node.next_sibling("w:t")) {
                text += t_node.text().get();
            }
            if (text.empty()) continue;

            TextRun run;
            run.text = text;
            run.page = 1;
            run.paragraph = para_idx;
            run.offset = offset_in_para;

            // Run properties
            auto rpr = r_node.child("w:rPr");
            if (rpr) {
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

        ++para_idx;
    }

    return doc;
}

} // namespace dn
