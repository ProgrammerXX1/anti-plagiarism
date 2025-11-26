// cpp/index_builder.cpp
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <cstdint>

#include <nlohmann/json.hpp>
#include "text_common.h"

using json = nlohmann::json;

namespace {

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

    std::string corpus_path = argv[1];
    std::string out_dir     = argv[2];

    std::ifstream in(corpus_path);
    if (!in) {
        std::cerr << "cannot open " << corpus_path << "\n";
        return 1;
    }

    std::vector<DocMeta> docs;
    // Только k=13
    std::unordered_map<std::uint64_t, std::vector<std::uint32_t>> inv13;
    std::vector<std::string> doc_ids;

    std::string line;
    std::uint32_t doc_id_int = 0;

    while (std::getline(in, line)) {
        if (line.empty()) continue;

        json j;
        try {
            j = json::parse(line);
        } catch (...) {
            continue;
        }

        std::string did  = j.value("doc_id", "");
        std::string text = j.value("text", "");
        if (did.empty() || text.empty()) continue;

        // Нормализация и токены — тот же пайплайн, что в C++ поиске
        std::string norm = normalize_for_shingles_simple(text);
        auto toks = simple_tokens(norm);
        if (toks.size() < 8) continue;

        // Только k=13
        auto sh13 = build_shingles(toks, 13);
        if (sh13.empty()) continue;

        auto [hi, lo] = simhash128_tokens(toks);

        DocMeta dm{};
        dm.tok_len    = static_cast<std::uint32_t>(toks.size());
        dm.simhash_hi = hi;
        dm.simhash_lo = lo;
        docs.push_back(dm);

        for (auto h : sh13) {
            inv13[h].push_back(doc_id_int);
        }

        doc_ids.push_back(did);
        ++doc_id_int;
    }

    const std::uint32_t N_docs = static_cast<std::uint32_t>(docs.size());

    // k9 полностью отключён, но оставляем N_post9 в заголовке = 0
    std::uint64_t N_post9  = 0;
    std::uint64_t N_post13 = 0;
    for (auto& kv : inv13) {
        N_post13 += kv.second.size();
    }

    // ── пишем бинарный индекс ─────────────────────────────────────

    std::string bin_path = out_dir + "/index_native.bin";
    std::ofstream bout(bin_path, std::ios::binary);
    if (!bout) {
        std::cerr << "cannot open " << bin_path << " for write\n";
        return 1;
    }

    // Заголовок совместим с se_load_index:
    // magic[4] = "PLAG"
    // u32 version
    // u32 N_docs
    // u64 N_post9   (здесь всегда 0)
    // u64 N_post13
    bout.write("PLAG", 4);
    std::uint32_t version = 1;
    bout.write(reinterpret_cast<const char*>(&version), sizeof(version));
    bout.write(reinterpret_cast<const char*>(&N_docs),  sizeof(N_docs));
    bout.write(reinterpret_cast<const char*>(&N_post9), sizeof(N_post9));
    bout.write(reinterpret_cast<const char*>(&N_post13),sizeof(N_post13));

    // docs_meta
    for (const auto& dm : docs) {
        bout.write(reinterpret_cast<const char*>(&dm.tok_len),    sizeof(dm.tok_len));
        bout.write(reinterpret_cast<const char*>(&dm.simhash_hi), sizeof(dm.simhash_hi));
        bout.write(reinterpret_cast<const char*>(&dm.simhash_lo), sizeof(dm.simhash_lo));
    }

    // postings k9 отсутствуют, т.к. N_post9 = 0

    // postings k13
    for (const auto& kv : inv13) {
        std::uint64_t h = kv.first;
        for (auto did : kv.second) {
            bout.write(reinterpret_cast<const char*>(&h),   sizeof(h));
            bout.write(reinterpret_cast<const char*>(&did), sizeof(did));
        }
    }

    bout.close();

    // ── doc_ids JSON ──────────────────────────────────────────────

    std::string docids_path = out_dir + "/index_native_docids.json";
    std::ofstream dout(docids_path);
    if (!dout) {
        std::cerr << "cannot open " << docids_path << " for write\n";
        return 1;
    }
    json docids_json(doc_ids);
    dout << docids_json.dump();
    dout.close();

    std::cout << "built index_native.bin docs=" << N_docs
              << " post13=" << N_post13 << "\n";
    return 0;
}
