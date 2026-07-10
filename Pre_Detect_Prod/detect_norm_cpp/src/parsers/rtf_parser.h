#pragma once
#include "../models.h"
#include <string>

namespace dn {
/// Strip RTF markup → plain text, then parse as TXT
ParsedDocument parse_rtf(const std::string& filename, const std::string& content);

/// Low-level RTF → plain text extraction (no dependencies)
std::string strip_rtf(const std::string& rtf);
}
