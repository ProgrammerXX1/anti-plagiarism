#pragma once
#include "../models.h"
#include <vector>

namespace dn {
std::vector<Finding> detect_spaces(const std::vector<TextRun>& runs);
}
