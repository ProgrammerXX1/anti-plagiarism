// cpp/index_builder.cpp
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <utility>
#include <cstdint>
#include <algorithm>

#include <nlohmann/json.hpp>
#include "text_common.h"

using json = nlohmann::json;

namespace {

constexpr int K = 9;  // длина шингла k=9, k=13 больше не используем

struct DocMeta {
    std::uint32_t tok_len;
    std::uint64_t simhash_hi;
    std::uint64_t simhash_lo;
};

// Простой 128-битный simhash по токенам
std::pair<std::uint64_t, std::uint64_t> simhash128_tokens(
    const std::vector<std::string>& toks
) {
    long long v[128] = {0};

    for (const auto& t : toks) {
        std::size_t h1 = std::hash<std::string>{}(t + std::string("#1"));
        std::size_t h2 = std::hash<std::string>{}(t + std::string("#2"));

        std::uint64_t lo = static_cast<std::uint64_t>(h1);
        std::uint64_t hi = static_cast<std::uint64_t>(h2);

        for (int i = 0; i < 64; ++i) {
            v[i]      += ((lo >> i) & 1ull) ? 1 : -1;
            v[64 + i] += ((hi >> i) & 1ull) ? 1 : -1;
        }
    }

    std::uint64_t hi = 0, lo = 0;
    for (int i = 0; i < 64; ++i) {
        if (v[i]      >= 0) lo |= (1ull << i);
        if (v[64 + i] >= 0) hi |= (1ull << i);
    }
    return {hi, lo};
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: index_builder <corpus_jsonl> <out_dir>\n";
        return 1;
    }

    const std::string corpus_path = argv[1];
    const std::string out_dir     = argv[2];

    std::ifstream in(corpus_path);
    if (!in) {
        std::cerr << "cannot open " << corpus_path << "\n";
        return 1;
    }

    // Чтобы не тормозить на std::endl/синке
    std::ios::sync_with_stdio(false);
    std::cin.tie(nullptr);

    std::vector<DocMeta> docs;
    std::vector<std::string> doc_ids;

    // Плоский список постингов k=9: (hash9, doc_id_int)
    std::vector<std::pair<std::uint64_t, std::uint32_t>> postings9;
    postings9.reserve(1024 * 1024); // стартовая оценка, потом сам вырастет

    std::string line;
    std::uint32_t doc_id_int = 0;

    while (std::getline(in, line)) {
        if (line.empty()) {
            continue;
        }

        json j;
        try {
            j = json::parse(line);
        } catch (...) {
            // битая строка — пропускаем
            continue;
        }

        std::string did  = j.value("doc_id", "");
        std::string text = j.value("text", "");
        if (did.empty() || text.empty()) {
            continue;
        }

        // Нормализация и токены — тот же пайплайн, что в C++ поиске
        std::string norm = normalize_for_shingles_simple(text);
        auto toks = simple_tokens(norm);
        if (toks.size() < static_cast<std::size_t>(K)) {
            // слишком короткий документ — пропускаем
            continue;
        }

        // Только k=9
        auto sh9 = build_shingles(toks, K);
        if (sh9.empty()) {
            continue;
        }

        auto [hi, lo] = simhash128_tokens(toks);

        DocMeta dm{};
        dm.tok_len    = static_cast<std::uint32_t>(toks.size());
        dm.simhash_hi = hi;
        dm.simhash_lo = lo;
        docs.push_back(dm);

        // Записываем постинги в плоский вектор
        for (auto h : sh9) {
            postings9.emplace_back(h, doc_id_int);
        }

        doc_ids.push_back(std::move(did));
        ++doc_id_int;
    }

    const std::uint32_t N_docs   = static_cast<std::uint32_t>(docs.size());
    const std::uint64_t N_post9  = static_cast<std::uint64_t>(postings9.size());
    const std::uint64_t N_post13 = 0;  // k=13 не используем

    if (N_docs == 0) {
        std::cerr << "no valid docs in corpus (N_docs=0)\n";
        return 1;
    }

    // Для лучшей локальности — отсортировать по hash9, затем по doc_id_int
    std::sort(
        postings9.begin(),
        postings9.end(),
        [](const auto& a, const auto& b) {
            if (a.first < b.first) return true;
            if (a.first > b.first) return false;
            return a.second < b.second;
        }
    );

    // ── пишем бинарный индекс ─────────────────────────────────────

    const std::string bin_path = out_dir + "/index_native.bin";
    std::ofstream bout(bin_path, std::ios::binary);
    if (!bout) {
        std::cerr << "cannot open " << bin_path << " for write\n";
        return 1;
    }

    // Заголовок совместим с se_load_index:
    // magic[4] = "PLAG"
    // u32 version
    // u32 N_docs
    // u64 N_post9
    // u64 N_post13
    const char magic[4] = { 'P', 'L', 'A', 'G' };
    bout.write(magic, 4);
    std::uint32_t version = 1;
    bout.write(reinterpret_cast<const char*>(&version), sizeof(version));
    bout.write(reinterpret_cast<const char*>(&N_docs),  sizeof(N_docs));
    bout.write(reinterpret_cast<const char*>(&N_post9), sizeof(N_post9));
    bout.write(reinterpret_cast<const char*>(&N_post13),sizeof(N_post13));

    // docs_meta (в том же порядке, что doc_ids / doc_id_int)
    for (const auto& dm : docs) {
        bout.write(reinterpret_cast<const char*>(&dm.tok_len),    sizeof(dm.tok_len));
        bout.write(reinterpret_cast<const char*>(&dm.simhash_hi), sizeof(dm.simhash_hi));
        bout.write(reinterpret_cast<const char*>(&dm.simhash_lo), sizeof(dm.simhash_lo));
    }

    // postings k9: (h9, doc_id_int) подряд
    for (const auto& p : postings9) {
        const std::uint64_t h   = p.first;
        const std::uint32_t did = p.second;
        bout.write(reinterpret_cast<const char*>(&h),   sizeof(h));
        bout.write(reinterpret_cast<const char*>(&did), sizeof(did));
    }

    // postings k13 отсутствуют, т.к. N_post13 = 0
    bout.close();

    // ── index_native_docids.json ──────────────────────────────────

    const std::string docids_path = out_dir + "/index_native_docids.json";
    {
        std::ofstream dout(docids_path);
        if (!dout) {
            std::cerr << "cannot open " << docids_path << " for write\n";
            return 1;
        }
        json docids_json(doc_ids);
        dout << docids_json.dump(2);
    }

    // ── index_native_meta.json ────────────────────────────────────
    //
    // Формат под твой Python:
    // {
    //   "docs_meta": {
    //       "<doc_id>": {
    //           "tok_len": 123,
    //           "simhash_hi": "<uint64>",
    //           "simhash_lo": "<uint64>"
    //       },
    //       ...
    //   },
    //   "config": {
    //       "thresholds": {
    //           "plag_thr": 0.7,
    //           "partial_thr": 0.3
    //       }
    //   },
    //   "stats": {
    //       "docs": N_docs,
    //       "k9": N_post9,
    //       "k13": 0
    //   }
    // }

    json j_docs_meta = json::object();
    for (std::size_t i = 0; i < doc_ids.size(); ++i) {
        const auto& did = doc_ids[i];
        const auto& dm  = docs[i];

        json mobj;
        mobj["tok_len"]    = dm.tok_len;
        // Сохраняем simhash как uint64 (можно и строкой, но Pythonу всё равно — сейчас он их не использует)
        mobj["simhash_hi"] = dm.simhash_hi;
        mobj["simhash_lo"] = dm.simhash_lo;

        j_docs_meta[did] = std::move(mobj);
    }

    json j_meta;
    j_meta["docs_meta"] = std::move(j_docs_meta);

    // базовые пороги на всякий случай (Python всё равно имеет дефолты)
    json j_cfg;
    json j_thr;
    j_thr["plag_thr"]    = 0.7;
    j_thr["partial_thr"] = 0.3;
    j_cfg["thresholds"]  = std::move(j_thr);
    j_meta["config"]     = std::move(j_cfg);

    json j_stats;
    j_stats["docs"] = N_docs;
    j_stats["k9"]   = N_post9;
    j_stats["k13"]  = 0;
    j_meta["stats"] = std::move(j_stats);

    const std::string meta_path = out_dir + "/index_native_meta.json";
    {
        std::ofstream mout(meta_path);
        if (!mout) {
            std::cerr << "cannot open " << meta_path << " for write\n";
            return 1;
        }
        mout << j_meta.dump(2);
    }

    std::cout << "[index_builder] built index_native.bin docs=" << N_docs
              << " post9=" << N_post9
              << " (k13=0, k9-only)\n";
    return 0;
}
