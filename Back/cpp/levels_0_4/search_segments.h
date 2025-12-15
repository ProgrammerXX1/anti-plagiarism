// cpp/common/search_segments.h
#pragma once
#include <cstdint>

#ifdef __cplusplus
extern "C" {
#endif

// returns malloc-string JSON; free with seg_free()
char* seg_search_many_json(
    const char* query_utf8,
    int top_k,
    const char** index_dirs_utf8,
    int n_dirs
);

void seg_free(void* p);

#ifdef __cplusplus
}
#endif


#ifdef __cplusplus
extern "C" {
#endif

char* seg_search_many_json(
    const char* query_utf8,
    int top_k,
    const char** index_dirs_utf8,
    int n_dirs
);

// NEW: excerpt for a shingle-span using C++ normalization/tokenization
// d_from/d_to are shingle offsets in document (as in match_spans.d_from/d_to)
char* seg_excerpt_for_span_json(
    const char* text_utf8,
    int d_from,
    int d_to,
    int k_shingle,          // pass 9
    int max_chars           // safety cap for excerpt length, e.g. 800
);

void seg_free(void* p);

#ifdef __cplusplus
}
#endif