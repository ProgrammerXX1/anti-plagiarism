#pragma once
#include "../models.h"
#include <string>

namespace dn {
// Parse legacy OLE2 .doc files via antiword.
// Returns extracted text as TextRuns (no style info — hidden text detection unavailable).
// On failure, returns empty document; error message is set in error_out.
ParsedDocument parse_doc(const std::string& filename, const std::string& data,
                         std::string& error_out);
}
