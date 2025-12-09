// cpp/etl_index_builder.cpp
#include <fstream>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>
#include <utility>
#include <cstdint>
#include <algorithm>
#include <thread>

#include <simdjson.h>
#include <nlohmann/json.hpp>

#include "text_common.h"

using json = nlohmann::json;

namespace {

constexpr int K = 9;  // длина шингла k=9

// лимиты для контроля монстров-документов
constexpr std::uint32_t MAX_TOKENS_PER_DOC   = 100000;   // 0 = без лимита
constexpr std::uint32_t MAX_SHINGLES_PER_DOC = 50000;    // 0 = без лимита
constexpr int           SHINGLE_STRIDE       = 1;        // 1 = каждый шингл

struct DocMeta {
    std::uint32_t tok_len;
    std::uint64_t simhash_hi;
    std::uint64_t simhash_lo;
};

struct ThreadResult {
    std::vector<DocMeta> docs;
    std::vector<std::string> doc_ids;
    // postings9: (hash, local_doc_idx)
    std::vector<std::pair<std::uint64_t, std::uint32_t>> postings9;
};

void process_range(
    const std::vector<std::string>& lines,
    std::size_t start,
    std::size_t end,
    ThreadResult& out
) {
    out.docs.clear();
    out.doc_ids.clear();
    out.postings9.clear();

    out.docs.reserve(end - start);
    out.doc_ids.reserve(end - start);
    out.postings9.reserve((end - start) * 64);

    simdjson::dom::parser parser;
    std::vector<TokenSpan> spans;
    spans.reserve(128);

    for (std::size_t i = start; i < end; ++i) {
        const std::string& line = lines[i];
        if (line.empty()) {
            continue;
        }

        simdjson::dom::element doc;
        auto err = parser.parse(line).get(doc);
        if (err) {
            // можно залогировать при желании
            continue;
        }

        // doc_id как строка (совпадает с Python: str(doc.id))
        std::string_view did_sv;
        err = doc["doc_id"].get(did_sv);
        if (err || did_sv.empty()) {
            continue;
        }

        std::string_view text_sv;
        err = doc["text"].get(text_sv);
        if (err || text_sv.empty()) {
            continue;
        }

        std::string did{did_sv};
        std::string text{text_sv};

        // нормализация под шинглы
        std::string norm = normalize_for_shingles_simple(text);

        spans.clear();
        tokenize_spans(norm, spans);
        if (spans.empty()) {
            continue;
        }

        // лимитируем длину документа по токенам
        if (MAX_TOKENS_PER_DOC > 0 &&
            spans.size() > static_cast<std::size_t>(MAX_TOKENS_PER_DOC)) {
            spans.resize(MAX_TOKENS_PER_DOC);
        }

        if (spans.size() < static_cast<std::size_t>(K)) {
            continue;
        }

        const int n   = static_cast<int>(spans.size());
        const int cnt = n - K + 1;
        if (cnt <= 0) {
            continue;
        }

        // simhash по укороченному списку токенов
        auto [hi, lo] = simhash128_spans(norm, spans);

        DocMeta dm{};
        dm.tok_len    = static_cast<std::uint32_t>(spans.size());
        dm.simhash_hi = hi;
        dm.simhash_lo = lo;

        std::uint32_t local_doc_id =
            static_cast<std::uint32_t>(out.docs.size());

        out.docs.push_back(dm);
        out.doc_ids.push_back(std::move(did));

        // шинглы прямо в postings9, без промежуточного вектора
        const int step = (SHINGLE_STRIDE > 0 ? SHINGLE_STRIDE : 1);
        std::uint32_t produced = 0;
        const std::uint32_t max_sh =
            (MAX_SHINGLES_PER_DOC > 0)
                ? MAX_SHINGLES_PER_DOC
                : static_cast<std::uint32_t>(cnt);

        for (int pos = 0; pos < cnt && produced < max_sh; pos += step) {
            std::uint64_t h = hash_shingle_tokens_spans(norm, spans, pos, K);
            out.postings9.emplace_back(h, local_doc_id);
            ++produced;
        }
    }
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: etl_index_builder <corpus_jsonl> <out_dir>\n";
        return 1;
    }

    const std::string corpus_path = argv[1];
    const std::string out_dir     = argv[2];

    std::ifstream in(corpus_path);
    if (!in) {
        std::cerr << "[etl_index_builder] cannot open " << corpus_path << "\n";
        return 1;
    }

    std::ios::sync_with_stdio(false);
    std::cin.tie(nullptr);

    // 1) читаем segment_corpus.jsonl в память
    std::vector<std::string> lines;
    {
        std::string line;
        while (std::getline(in, line)) {
            if (!line.empty()) {
                lines.push_back(line);
            }
        }
    }

    if (lines.empty()) {
        std::cerr << "[etl_index_builder] corpus is empty: no lines\n";
        return 1;
    }

    const std::size_t total_lines = lines.size();

    // 2) выбираем количество потоков (до 16)
    unsigned hw = std::thread::hardware_concurrency();
    if (hw == 0) hw = 4;
    unsigned num_threads = std::min<unsigned>(hw, 16u);
    if (num_threads > total_lines) {
        num_threads = static_cast<unsigned>(total_lines);
    }
    if (num_threads == 0) num_threads = 1;

    // 3) делим по чанкам
    std::vector<ThreadResult> results(num_threads);
    std::vector<std::thread>  workers;
    workers.reserve(num_threads);

    std::size_t chunk_size = (total_lines + num_threads - 1) / num_threads;
    std::size_t cur_start = 0;

    for (unsigned t = 0; t < num_threads; ++t) {
        std::size_t start = cur_start;
        std::size_t end   = std::min<std::size_t>(start + chunk_size, total_lines);
        cur_start = end;

        if (start >= end) break;

        workers.emplace_back(
            [&, start, end, t]() {
                process_range(lines, start, end, results[t]);
            }
        );
    }

    const unsigned used_threads = static_cast<unsigned>(workers.size());
    for (auto& th : workers) {
        if (th.joinable()) th.join();
    }

    // 4) собираем результаты
    std::uint64_t total_docs   = 0;
    std::uint64_t total_posts9 = 0;
    for (unsigned t = 0; t < used_threads; ++t) {
        total_docs   += results[t].docs.size();
        total_posts9 += results[t].postings9.size();
    }

    if (total_docs == 0) {
        std::cerr << "[etl_index_builder] no valid docs in corpus (N_docs=0)\n";
        return 1;
    }

    std::vector<std::uint32_t> doc_id_offsets(used_threads, 0);
    {
        std::uint32_t acc = 0;
        for (unsigned t = 0; t < used_threads; ++t) {
            doc_id_offsets[t] = acc;
            acc += static_cast<std::uint32_t>(results[t].docs.size());
        }
    }

    std::vector<DocMeta> docs;
    std::vector<std::string> doc_ids;
    std::vector<std::pair<std::uint64_t, std::uint32_t>> postings9;

    docs.reserve(static_cast<std::size_t>(total_docs));
    doc_ids.reserve(static_cast<std::size_t>(total_docs));
    postings9.reserve(static_cast<std::size_t>(total_posts9));

    for (unsigned t = 0; t < used_threads; ++t) {
        auto& r = results[t];
        for (std::size_t i = 0; i < r.docs.size(); ++i) {
            docs.push_back(r.docs[i]);
            doc_ids.push_back(std::move(r.doc_ids[i]));
        }
    }

    for (unsigned t = 0; t < used_threads; ++t) {
        const std::uint32_t base = doc_id_offsets[t];
        auto& r = results[t];

        for (const auto& p : r.postings9) {
            std::uint64_t h      = p.first;
            std::uint32_t local  = p.second;
            std::uint32_t global = base + local;
            postings9.emplace_back(h, global);
        }
    }

    const std::uint32_t N_docs   = static_cast<std::uint32_t>(docs.size());
    const std::uint64_t N_post9  = static_cast<std::uint64_t>(postings9.size());
    const std::uint64_t N_post13 = 0;

    // 5) бинарный индекс (тот же формат, что у старого index_builder)
    const std::string bin_path = out_dir + "/index_native.bin";
    std::ofstream bout(bin_path, std::ios::binary);
    if (!bout) {
        std::cerr << "[etl_index_builder] cannot open " << bin_path << " for write\n";
        return 1;
    }

    const char magic[4] = { 'P', 'L', 'A', 'G' };
    bout.write(magic, 4);
    std::uint32_t version = 1;
    bout.write(reinterpret_cast<const char*>(&version), sizeof(version));
    bout.write(reinterpret_cast<const char*>(&N_docs),  sizeof(N_docs));
    bout.write(reinterpret_cast<const char*>(&N_post9), sizeof(N_post9));
    bout.write(reinterpret_cast<const char*>(&N_post13),sizeof(N_post13));

    for (const auto& dm : docs) {
        bout.write(reinterpret_cast<const char*>(&dm.tok_len),    sizeof(dm.tok_len));
        bout.write(reinterpret_cast<const char*>(&dm.simhash_hi), sizeof(dm.simhash_hi));
        bout.write(reinterpret_cast<const char*>(&dm.simhash_lo), sizeof(dm.simhash_lo));
    }

    for (const auto& p : postings9) {
        const std::uint64_t h   = p.first;
        const std::uint32_t did = p.second;
        bout.write(reinterpret_cast<const char*>(&h),   sizeof(h));
        bout.write(reinterpret_cast<const char*>(&did), sizeof(did));
    }

    bout.close();

    // 6) docids
    const std::string docids_path = out_dir + "/index_native_docids.json";
    {
        std::ofstream dout(docids_path);
        if (!dout) {
            std::cerr << "[etl_index_builder] cannot open " << docids_path << " for write\n";
            return 1;
        }
        json docids_json(doc_ids);
        // без отступов для компактности
        dout << docids_json.dump();
    }

    // 7) meta (совместимо со старым индексом)
    json j_docs_meta = json::object();
    for (std::size_t i = 0; i < doc_ids.size(); ++i) {
        const auto& did = doc_ids[i];
        const auto& dm  = docs[i];

        json mobj;
        mobj["tok_len"]    = dm.tok_len;
        mobj["simhash_hi"] = dm.simhash_hi;
        mobj["simhash_lo"] = dm.simhash_lo;

        j_docs_meta[did] = std::move(mobj);
    }

    json j_meta;
    j_meta["docs_meta"] = std::move(j_docs_meta);

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
            std::cerr << "[etl_index_builder] cannot open " << meta_path << " for write\n";
            return 1;
        }
        // без отступов
        mout << j_meta.dump();
    }

    std::cout << "[etl_index_builder] built index_native.bin docs=" << N_docs
              << " post9=" << N_post9
              << " (k9-only, spans, parallel=" << used_threads
              << ", max_tokens=" << MAX_TOKENS_PER_DOC
              << ", max_shingles=" << MAX_SHINGLES_PER_DOC
              << ")\n";

    return 0;
}
