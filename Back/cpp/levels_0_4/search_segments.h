#pragma once
#include <cstdint>

#ifdef __cplusplus
extern "C" {
#endif

// old API (kept)
char* seg_search_many_json(
    const char* query_utf8,
    int top_k,
    const char** index_dirs_utf8,
    int n_dirs
);

// new API: normalize_query (1/0)
char* seg_search_many_json_v2(
    const char* query_utf8,
    int top_k,
    const char** index_dirs_utf8,
    int n_dirs,
    int normalize_query
);

// old API (kept)
char* seg_excerpt_for_span_json(
    const char* text_utf8,
    int d_from,
    int d_to,
    int k_shingle,
    int max_chars
);

// new API: normalize_text (1/0)
char* seg_excerpt_for_span_json_v2(
    const char* text_utf8,
    int d_from,
    int d_to,
    int k_shingle,
    int max_chars,
    int normalize_text
);

void seg_free(void* p);

#ifdef __cplusplus
}
#endif
