// cpp/common/search_core.h
#pragma once
#include <cstdint>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    int    doc_id_int;
    double score;
    double j9;
    double c9;
    double j13;
    double c13;
    int    cand_hits; // NOTE: this is "seed_hits" (how many times doc appeared in seed postings)
} SeHit;

typedef struct {
    int count;
} SeSearchResult;

int se_load_index(const char* index_dir_utf8);

SeSearchResult se_search_text(
    const char* text_utf8,
    int top_k,
    SeHit* out_hits,
    int max_hits
);

#ifdef __cplusplus
}
#endif
