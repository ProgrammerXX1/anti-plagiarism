#pragma once
#include "../models.h"
#include <string>
#include <vector>

namespace dn {

// Extract a file from a ZIP archive in memory
// Returns the file content or empty on failure
std::string zip_extract_file(const std::string& archive_data, const std::string& entry_name);

// List file names in a ZIP archive
std::vector<std::string> zip_list_files(const std::string& archive_data);

} // namespace dn
