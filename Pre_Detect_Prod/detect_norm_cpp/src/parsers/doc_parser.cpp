/*
 * Legacy .doc (OLE2) parser via antiword.
 *
 * Writes data to a temp file, runs `antiword -m UTF-8.txt <file>`,
 * captures stdout as UTF-8 text, then parses line-by-line into TextRuns.
 *
 * Style information (color, font size) is NOT available from antiword,
 * so hidden_text detection will not work for .doc files.
 */
#include "doc_parser.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <sstream>
#include <array>
#include <filesystem>

namespace fs = std::filesystem;

namespace dn {

ParsedDocument parse_doc(const std::string& filename, const std::string& data,
                         std::string& error_out) {
    ParsedDocument doc;
    doc.filename = filename;
    error_out.clear();

    // Write data to a temp file
    auto tmp_dir = fs::temp_directory_path();
    auto tmp_path = tmp_dir / ("detect_doc_" + std::to_string(std::hash<std::string>{}(filename)) + ".doc");
    {
        std::ofstream ofs(tmp_path, std::ios::binary);
        if (!ofs) {
            error_out = "Не удалось создать временный файл для .doc парсинга.";
            return doc;
        }
        ofs.write(data.data(), data.size());
    }

    // Run antiword
    // -m UTF-8.txt: output UTF-8
    // -w 0: no line wrapping (avoids artificial spaces from text formatting)
    std::string cmd = "antiword -m UTF-8.txt -w 0 '" + tmp_path.string() + "' 2>&1";
    std::array<char, 8192> buf;
    std::string output;

    FILE* pipe = popen(cmd.c_str(), "r");
    if (!pipe) {
        fs::remove(tmp_path);
        error_out = "Не удалось запустить antiword для парсинга .doc.";
        return doc;
    }

    while (fgets(buf.data(), buf.size(), pipe) != nullptr) {
        output += buf.data();
    }

    int status = pclose(pipe);
    fs::remove(tmp_path);

    if (status != 0 || output.empty()) {
        error_out = "antiword не смог извлечь текст из '" + filename + "'. "
                    "Файл повреждён или защищён паролем.";
        return doc;
    }

    // Strip trailing whitespace
    while (!output.empty() && (output.back() == '\n' || output.back() == '\r' || output.back() == ' ')) {
        output.pop_back();
    }

    // Parse output line-by-line into TextRuns (same as txt_parser)
    std::istringstream iss(output);
    std::string line;
    int para = 0;

    while (std::getline(iss, line)) {
        // Remove trailing \r
        if (!line.empty() && line.back() == '\r') line.pop_back();

        TextRun run;
        run.text = line;
        run.page = 1;
        run.paragraph = para;
        run.offset = 0;
        run.font_size = 12.0;
        doc.runs.push_back(std::move(run));
        ++para;
    }

    return doc;
}

} // namespace dn
