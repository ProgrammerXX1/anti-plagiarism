#pragma once
#include "../models.h"
#include <string>

namespace dn {
ParsedDocument parse_pdf(const std::string& filename, const std::string& data);
}
