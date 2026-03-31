#pragma once
#include "../models.h"
#include <vector>

namespace dn {
std::vector<Finding> detect_hidden(const std::vector<TextRun>& runs);
}
