#pragma once
#include "../models.h"
#include <string>

namespace dn {
ParsedDocument parse_odt(const std::string& filename, const std::string& data);
}
