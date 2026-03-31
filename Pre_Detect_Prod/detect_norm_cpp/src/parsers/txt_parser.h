#pragma once
#include "../models.h"
#include <string>

namespace dn {
ParsedDocument parse_txt(const std::string& filename, const std::string& content);
}
