#pragma once
#include "../models.h"
#include <string>

namespace dn {
ParsedDocument parse_pptx(const std::string& filename, const std::string& data);
}
