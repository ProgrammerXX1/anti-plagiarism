/*
 * PPTX parser: extract text runs with color/font info from slides.
 *
 * PPTX is a ZIP archive. Slides are in ppt/slides/slide1.xml, slide2.xml, ...
 * Structure: p:sld > p:cSld > p:spTree > p:sp > p:txBody > a:p > a:r > a:t
 * Run properties: a:rPr (sz = hundredths of a point, e.g. 2400 = 24pt)
 * Color: a:rPr > a:solidFill > a:srgbClr val="RRGGBB"
 */
#include "pptx_parser.h"
#include "zip_xml_util.h"
#include <pugixml.hpp>
#include <algorithm>
#include <cstring>
#include <cstdlib>

namespace dn {

static std::vector<int> parse_hex_color(const char* hex) {
    if (!hex || strlen(hex) < 6) return {};
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

// Recursively find all <p:txBody> / <a:txBody> nodes and extract text runs
static void extract_text_bodies(pugi::xml_node node, int page, int& para_idx,
                                std::vector<TextRun>& runs)
{
    const char* name = node.name();
    if (strcmp(name, "p:txBody") == 0 || strcmp(name, "a:txBody") == 0) {
        // Extract paragraphs from this text body
        for (auto p = node.child("a:p"); p; p = p.next_sibling("a:p")) {
            int offset_in_para = 0;
            bool para_has_content = false;
            for (auto r = p.child("a:r"); r; r = r.next_sibling("a:r")) {
                std::string text;
                for (auto t = r.child("a:t"); t; t = t.next_sibling("a:t"))
                    text += t.text().get();
                if (text.empty()) continue;

                para_has_content = true;
                TextRun run;
                run.text = text;
                run.page = page;
                run.paragraph = para_idx;
                run.offset = offset_in_para;

                auto rpr = r.child("a:rPr");
                if (rpr) {
                    // Font size: sz in hundredths of a point (2400 = 24pt)
                    int sz = rpr.attribute("sz").as_int(0);
                    if (sz > 0) run.font_size = sz / 100.0;

                    // Text color: <a:solidFill><a:srgbClr val="RRGGBB"/>
                    auto fill = rpr.child("a:solidFill");
                    if (fill) {
                        auto clr = fill.child("a:srgbClr");
                        if (clr) {
                            const char* val = clr.attribute("val").as_string(nullptr);
                            if (val) run.color_rgb = parse_hex_color(val);
                        }
                    }
                }

                offset_in_para += static_cast<int>(text.size());
                runs.push_back(std::move(run));
            }
            if (para_has_content) ++para_idx;
        }
        return; // don't recurse into children of txBody
    }

    for (auto child : node.children())
        extract_text_bodies(child, page, para_idx, runs);
}

ParsedDocument parse_pptx(const std::string& filename, const std::string& data) {
    ParsedDocument doc;
    doc.filename = filename;

    auto entries = zip_list_files(data);

    // Collect slide entries: ppt/slides/slideN.xml (not _rels, not layout, etc.)
    std::vector<std::string> slides;
    for (auto& e : entries) {
        if (e.size() > 16 &&
            e.compare(0, 11, "ppt/slides/") == 0 &&
            e.compare(11, 5, "slide") == 0 &&
            e.size() > 20 &&
            e.compare(e.size() - 4, 4, ".xml") == 0 &&
            e.find('/', 11) == std::string::npos) // no subdirectories
        {
            slides.push_back(e);
        }
    }
    std::sort(slides.begin(), slides.end());

    int para_idx = 0;
    for (int si = 0; si < static_cast<int>(slides.size()); ++si) {
        std::string xml_data = zip_extract_file(data, slides[si]);
        if (xml_data.empty()) continue;

        pugi::xml_document xdoc;
        unsigned int flags = pugi::parse_default | pugi::parse_ws_pcdata;
        if (!xdoc.load_buffer(xml_data.data(), xml_data.size(), flags)) continue;

        extract_text_bodies(xdoc.document_element(), si + 1, para_idx, doc.runs);
    }

    return doc;
}

} // namespace dn
