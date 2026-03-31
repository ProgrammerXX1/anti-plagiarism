/*
 * ODT parser: extract text runs with style info from content.xml
 *
 * ODT is a ZIP archive (OpenDocument Format).
 * Main text in content.xml:
 *   office:document-content > office:body > office:text > text:p / text:h
 * Styles in office:automatic-styles > style:style > style:text-properties
 * Inline: fo:color="#RRGGBB", fo:font-size="12pt", fo:background-color
 */
#include "odt_parser.h"
#include "zip_xml_util.h"
#include <pugixml.hpp>
#include <cstring>
#include <cstdlib>
#include <map>

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

struct TextStyle {
    std::vector<int> color_rgb;
    std::vector<int> background_rgb;
    double font_size = 0;
    std::string font_name;
};

static std::map<std::string, TextStyle> parse_styles(pugi::xml_node root) {
    std::map<std::string, TextStyle> styles;

    auto auto_styles = root.child("office:automatic-styles");
    if (!auto_styles) return styles;

    for (auto st = auto_styles.child("style:style"); st; st = st.next_sibling("style:style")) {
        const char* name = st.attribute("style:name").as_string(nullptr);
        if (!name) continue;
        const char* family = st.attribute("style:family").as_string("");
        if (strcmp(family, "text") != 0 && strcmp(family, "paragraph") != 0) continue;

        TextStyle ts;
        auto tp = st.child("style:text-properties");
        if (tp) {
            const char* color = tp.attribute("fo:color").as_string(nullptr);
            if (color) ts.color_rgb = parse_hex_color(color);

            const char* bg = tp.attribute("fo:background-color").as_string(nullptr);
            if (bg && strcmp(bg, "transparent") != 0)
                ts.background_rgb = parse_hex_color(bg);

            const char* sz = tp.attribute("fo:font-size").as_string(nullptr);
            if (sz) ts.font_size = std::atof(sz); // "12pt" -> 12.0

            const char* fn = tp.attribute("style:font-name").as_string(nullptr);
            if (fn) ts.font_name = fn;
        }
        styles[name] = ts;
    }
    return styles;
}

static void apply_style(TextRun& run, const std::string& style_name,
                         const std::map<std::string, TextStyle>& styles)
{
    auto it = styles.find(style_name);
    if (it == styles.end()) return;
    auto& ts = it->second;
    if (!ts.color_rgb.empty())      run.color_rgb = ts.color_rgb;
    if (!ts.background_rgb.empty()) run.background_rgb = ts.background_rgb;
    if (ts.font_size > 0)           run.font_size = ts.font_size;
    if (!ts.font_name.empty())      run.font_name = ts.font_name;
}

// Extract text from one paragraph (text:p or text:h)
static void extract_paragraph(pugi::xml_node p_node, int para_idx,
                               const std::map<std::string, TextStyle>& styles,
                               std::vector<TextRun>& runs)
{
    int offset = 0;
    const char* pstyle = p_node.attribute("text:style-name").as_string(nullptr);

    for (auto child = p_node.first_child(); child; child = child.next_sibling()) {

        if (child.type() == pugi::node_pcdata) {
            std::string text = child.value();
            if (text.empty()) continue;
            TextRun run;
            run.text = text;
            run.page = 1;
            run.paragraph = para_idx;
            run.offset = offset;
            if (pstyle) apply_style(run, pstyle, styles);
            offset += static_cast<int>(text.size());
            runs.push_back(std::move(run));
        }
        else if (strcmp(child.name(), "text:span") == 0) {
            // text:span may contain direct text and nested elements (text:s, text:tab)
            for (auto sc = child.first_child(); sc; sc = sc.next_sibling()) {
                if (sc.type() == pugi::node_pcdata) {
                    std::string text = sc.value();
                    if (text.empty()) continue;
                    TextRun run;
                    run.text = text;
                    run.page = 1;
                    run.paragraph = para_idx;
                    run.offset = offset;
                    const char* sn = child.attribute("text:style-name").as_string(nullptr);
                    if (sn) apply_style(run, sn, styles);
                    else if (pstyle) apply_style(run, pstyle, styles);
                    offset += static_cast<int>(text.size());
                    runs.push_back(std::move(run));
                }
                else if (strcmp(sc.name(), "text:s") == 0) {
                    int count = sc.attribute("text:c").as_int(1);
                    TextRun run;
                    run.text = std::string(count, ' ');
                    run.page = 1;
                    run.paragraph = para_idx;
                    run.offset = offset;
                    offset += count;
                    runs.push_back(std::move(run));
                }
                else if (strcmp(sc.name(), "text:tab") == 0) {
                    TextRun run;
                    run.text = "\t";
                    run.page = 1;
                    run.paragraph = para_idx;
                    run.offset = offset;
                    offset += 1;
                    runs.push_back(std::move(run));
                }
            }
        }
        else if (strcmp(child.name(), "text:s") == 0) {
            int count = child.attribute("text:c").as_int(1);
            TextRun run;
            run.text = std::string(count, ' ');
            run.page = 1;
            run.paragraph = para_idx;
            run.offset = offset;
            offset += count;
            runs.push_back(std::move(run));
        }
        else if (strcmp(child.name(), "text:tab") == 0) {
            TextRun run;
            run.text = "\t";
            run.page = 1;
            run.paragraph = para_idx;
            run.offset = offset;
            offset += 1;
            runs.push_back(std::move(run));
        }
    }
}

ParsedDocument parse_odt(const std::string& filename, const std::string& data) {
    ParsedDocument doc;
    doc.filename = filename;

    std::string xml_data = zip_extract_file(data, "content.xml");
    if (xml_data.empty()) return doc;

    pugi::xml_document xdoc;
    if (!xdoc.load_buffer(xml_data.data(), xml_data.size())) return doc;

    auto root = xdoc.document_element();
    auto styles = parse_styles(root);

    auto body = root.child("office:body");
    if (!body) return doc;

    auto text_body = body.child("office:text");
    if (!text_body) return doc;

    int para_idx = 0;
    // Iterate all children: text:p (paragraphs), text:h (headings), text:list, etc.
    for (auto node : text_body.children()) {
        const char* name = node.name();
        if (strcmp(name, "text:p") == 0 || strcmp(name, "text:h") == 0) {
            extract_paragraph(node, para_idx, styles, doc.runs);
            ++para_idx;
        }
        else if (strcmp(name, "text:list") == 0) {
            // Flatten list items: text:list > text:list-item > text:p
            for (auto li : node.children("text:list-item")) {
                for (auto p : li.children("text:p")) {
                    extract_paragraph(p, para_idx, styles, doc.runs);
                    ++para_idx;
                }
            }
        }
    }

    return doc;
}

} // namespace dn
