#pragma once
#include <cstdint>

#ifdef __cplusplus
extern "C" {
#endif

char* seg_search_many_json(
    const char* query_utf8,
    int top_k,
    const char** index_dirs_utf8,
    int n_dirs
);

char* seg_search_many_json_v2(
    const char* query_utf8,
    int top_k,
    const char** index_dirs_utf8,
    int n_dirs,
    int normalize_query
);

char* seg_search_many_json_v3(
    const char* query_utf8,
    int top_k,
    const char** index_dirs_utf8,
    int n_dirs,
    int normalize_query,
    int include_matches
);

// Windowed search that returns READY segments (sources) for UI.
// Output JSON:
// {
//   "segments": [
//     { "source_doc_id": "...", "q_tok_from": int, "q_tok_to": int },
//     ...
//   ]
// }
char* seg_search_windowed_json_v1(
    const char* query_utf8,
    int top_k,
    const char** index_dirs_utf8,
    int n_dirs,
    int normalize_query,
    int include_matches,
    int win_tokens,
    int stride_tokens
);

char* seg_excerpt_for_span_json(
    const char* text_utf8,
    int d_from,
    int d_to,
    int k_shingle,
    int max_chars
);

char* seg_excerpt_for_span_json_v2(
    const char* text_utf8,
    int d_from,
    int d_to,
    int k_shingle,
    int max_chars,
    int normalize_text
);

char* seg_normalize_json_v1(const char* text_utf8);

void seg_free(void* p);

#ifdef __cplusplus
}
#endif
