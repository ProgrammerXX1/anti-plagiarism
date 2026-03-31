#include "zip_xml_util.h"
#include <zip.h>
#include <cstring>

namespace dn {

std::string zip_extract_file(const std::string& archive_data, const std::string& entry_name) {
    zip_error_t ze;
    zip_error_init(&ze);

    zip_source_t* src = zip_source_buffer_create(
        archive_data.data(), archive_data.size(), 0, &ze);
    if (!src) { zip_error_fini(&ze); return {}; }

    zip_t* za = zip_open_from_source(src, ZIP_RDONLY, &ze);
    if (!za) { zip_source_free(src); zip_error_fini(&ze); return {}; }

    zip_int64_t idx = zip_name_locate(za, entry_name.c_str(), 0);
    if (idx < 0) { zip_close(za); return {}; }

    zip_stat_t st;
    zip_stat_init(&st);
    if (zip_stat_index(za, idx, 0, &st) < 0) { zip_close(za); return {}; }

    // Bounds check: reject entries claiming > 500 MB (zip bomb protection)
    if (!(st.valid & ZIP_STAT_SIZE) || st.size > 500ULL * 1024 * 1024) {
        zip_close(za); return {};
    }

    zip_file_t* zf = zip_fopen_index(za, idx, 0);
    if (!zf) { zip_close(za); return {}; }

    std::string result;
    result.resize(st.size);
    zip_int64_t nread = zip_fread(zf, result.data(), st.size);
    zip_fclose(zf);
    zip_close(za);
    zip_error_fini(&ze);

    if (nread < 0 || static_cast<zip_uint64_t>(nread) != st.size) return {};
    return result;
}

std::vector<std::string> zip_list_files(const std::string& archive_data) {
    zip_error_t ze;
    zip_error_init(&ze);

    zip_source_t* src = zip_source_buffer_create(
        archive_data.data(), archive_data.size(), 0, &ze);
    if (!src) { zip_error_fini(&ze); return {}; }

    zip_t* za = zip_open_from_source(src, ZIP_RDONLY, &ze);
    if (!za) { zip_source_free(src); zip_error_fini(&ze); return {}; }

    std::vector<std::string> files;
    zip_int64_t n = zip_get_num_entries(za, 0);
    for (zip_int64_t i = 0; i < n; ++i) {
        const char* name = zip_get_name(za, i, 0);
        if (name) files.emplace_back(name);
    }
    zip_close(za);
    zip_error_fini(&ze);
    return files;
}

} // namespace dn
