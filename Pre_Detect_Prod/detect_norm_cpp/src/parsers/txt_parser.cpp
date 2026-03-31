#include "txt_parser.h"
#include <sstream>

namespace dn {

ParsedDocument parse_txt(const std::string& filename, const std::string& content) {
    ParsedDocument doc;
    doc.filename = filename;

    std::istringstream iss(content);
    std::string line;
    int para = 0;
    int offset = 0;

    while (std::getline(iss, line)) {
        TextRun run;
        run.text = line;
        run.page = 1;
        run.paragraph = para;
        run.offset = 0;  // offset within paragraph
        run.font_size = 12.0;
        doc.runs.push_back(std::move(run));
        ++para;
    }

    return doc;
}

} // namespace dn
