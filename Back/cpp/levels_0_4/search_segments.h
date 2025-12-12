#pragma once
#include <cstdint>

#ifdef __cplusplus
extern "C" {
#endif

// возвращает malloc-строку JSON; освобождать seg_free()
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
