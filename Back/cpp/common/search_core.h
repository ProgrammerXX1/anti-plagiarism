// cpp/search_core.h
#pragma once
#include <cstdint>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    int    doc_id_int;   // индекс в массиве doc_ids
    double score;
    double j9;           // метрики для k=9
    double c9;
    double j13;          // сейчас всегда 0
    double c13;          // сейчас всегда 0
    int    cand_hits;
} SeHit;

typedef struct {
    int count;           // сколько элементов валидно в out_hits
} SeSearchResult;

/**
 * Загрузить бинарный индекс из каталога (ожидает index_native.bin + index_native_docids.json).
 * index_dir_utf8 — путь к каталогу (например, "/runtime/index/current").
 * 0 = OK, !=0 — ошибка.
 */
int se_load_index(const char* index_dir_utf8);

/**
 * Поиск по тексту (UTF-8).
 * top_k     — сколько документов хотим вернуть (≤ max_hits).
 * out_hits  — заранее выделенный массив длиной max_hits.
 * max_hits  — максимум элементов в out_hits.
 */
SeSearchResult se_search_text(
    const char* text_utf8,
    int top_k,
    SeHit* out_hits,
    int max_hits
);

#ifdef __cplusplus
}
#endif
