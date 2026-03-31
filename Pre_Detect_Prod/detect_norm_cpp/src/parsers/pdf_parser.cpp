/*
 * PDF parser: extract text layer from PDF using poppler-cpp.
 *
 * - Extracts text from each page via poppler::page::text()
 * - Scanned pages (images without text layer) are automatically skipped
 * - No OCR — only the embedded text layer is read
 * - Encrypted/password-protected PDFs return empty document
 * - No font/color info available (PDF text extraction is plain text)
 */
#include "pdf_parser.h"

#include <poppler/cpp/poppler-document.h>
#include <poppler/cpp/poppler-page.h>

#include <limits>
#include <sstream>

namespace dn {

ParsedDocument parse_pdf(const std::string& filename, const std::string& data) {
    ParsedDocument doc;
    doc.filename = filename;

    if (data.empty() || data.size() > static_cast<size_t>(std::numeric_limits<int>::max()))
        return doc;  // 0-byte or > 2GB — can't pass to poppler safely

    auto* pdf_doc = poppler::document::load_from_raw_data(
        data.data(), static_cast<int>(data.size()));
    if (!pdf_doc) return doc;  // corrupt, encrypted, or not a PDF

    int num_pages = pdf_doc->pages();
    int para_idx = 0;

    for (int pi = 0; pi < num_pages; ++pi) {
        poppler::page* pg = pdf_doc->create_page(pi);
        if (!pg) continue;

        poppler::ustring utext = pg->text();
        poppler::byte_array bytes = utext.to_utf8();
        std::string page_text(bytes.begin(), bytes.end());
        delete pg;

        if (page_text.empty()) continue;  // scanned page — no text layer

        // Split page text into lines → paragraphs
        std::istringstream iss(page_text);
        std::string line;
        while (std::getline(iss, line)) {
            if (line.empty()) continue;
            TextRun run;
            run.text = line;
            run.page = pi + 1;
            run.paragraph = para_idx;
            run.offset = 0;
            run.font_size = 12.0;  // unknown — default
            doc.runs.push_back(std::move(run));
            ++para_idx;
        }
    }

    delete pdf_doc;
    return doc;
}

} // namespace dn
