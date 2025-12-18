// cpp/levels_0_4/etl_index_builder.cpp
// VERSION 2: postings with positions + per-doc normalization flag (text_is_normalized)
// + ATOMIC OUTPUT (write *.tmp then rename/replace)

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>
#include <filesystem>

#include <nlohmann/json.hpp>
#include <simdjson.h>

#include "text_common.h"

using json = nlohmann::json;
namespace fs = std::filesystem;

namespace {

constexpr int K = 9;

constexpr std::uint32_t MAX_TOKENS_PER_DOC   = 100000;
constexpr std::uint32_t MAX_SHINGLES_PER_DOC = 50000;
constexpr int           SHINGLE_STRIDE       = 1;

struct DocMeta {
    std::uint32_t tok_len;
    std::uint64_t simhash_hi;
    std::uint64_t simhash_lo;
};

struct Posting9 {
    std::uint64_t h;
    std::uint32_t did;
    std::uint32_t pos;
};

struct ThreadResult {
    std::vector<DocMeta> docs;
    std::vector<std::string> doc_ids;
    std::vector<Posting9> postings9;
};

static bool get_bool_safe(const simdjson::dom::element& e, const char* key, bool defv) {
    simdjson::dom::element v;
    if (e.at_key(key).get(v)) return defv;

    bool b = defv;
    if (!v.get(b)) return b;
    return defv;
}

static bool get_text_is_normalized(const simdjson::dom::element& doc) {
    // Prefer new key
    bool v = get_bool_safe(doc, "text_is_normalized", true);

    simdjson::dom::element tmp;
    if (!doc.at_key("text_is_normalized").get(tmp)) {
        return v; // new key exists
    }
    // fallback legacy
    return get_bool_safe(doc, "normalized", true);
}

static bool atomic_replace_file(const fs::path& tmp, const fs::path& fin) {
    try {
        std::error_code ec;
        fs::remove(fin, ec); // best-effort
        fs::rename(tmp, fin);
        return true;
    } catch (const std::exception& e) {
        std::cerr << "[etl_index_builder] atomic_replace failed: " << e.what()
                  << " tmp=" << tmp << " fin=" << fin << "\n";
        return false;
    }
}

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
    spans.reserve(256);

    for (std::size_t i = start; i < end; ++i) {
        const std::string& line = lines[i];
        if (line.empty()) continue;

        simdjson::dom::element doc;
        if (parser.parse(line).get(doc)) continue;

        std::string_view did_sv;
        if (doc["doc_id"].get(did_sv) || did_sv.empty()) continue;

        std::string_view text_sv;
        if (doc["text"].get(text_sv) || text_sv.empty()) continue;

        const bool text_is_norm = get_text_is_normalized(doc);

        std::string did{did_sv};
        std::string text{text_sv};

        std::string norm;
        if (text_is_norm) {
            norm = std::move(text);
        } else {
            norm = normalize_for_shingles_simple(text);
        }

        spans.clear();
        tokenize_spans(norm, spans);
        if (spans.empty()) continue;

        if (MAX_TOKENS_PER_DOC > 0 && spans.size() > (std::size_t)MAX_TOKENS_PER_DOC) {
            spans.resize(MAX_TOKENS_PER_DOC);
        }
        if (spans.size() < (std::size_t)K) continue;

        const int n   = (int)spans.size();
        const int cnt = n - K + 1;
        if (cnt <= 0) continue;

        auto [hi, lo] = simhash128_spans(norm, spans);

        DocMeta dm{};
        dm.tok_len    = (std::uint32_t)spans.size();
        dm.simhash_hi = hi;
        dm.simhash_lo = lo;

        std::uint32_t local_doc_id = (std::uint32_t)out.docs.size();
        out.docs.push_back(dm);
        out.doc_ids.push_back(std::move(did));

        const int step = (SHINGLE_STRIDE > 0 ? SHINGLE_STRIDE : 1);
        std::uint32_t produced = 0;
        const std::uint32_t max_sh =
            (MAX_SHINGLES_PER_DOC > 0) ? MAX_SHINGLES_PER_DOC : (std::uint32_t)cnt;

        for (int pos = 0; pos < cnt && produced < max_sh; pos += step) {
            std::uint64_t h = hash_shingle_tokens_spans(norm, spans, pos, K);
            out.postings9.push_back(Posting9{h, local_doc_id, (std::uint32_t)pos});
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
    const fs::path out_dir = fs::path(argv[2]);

    std::ifstream in(corpus_path);
    if (!in) {
        std::cerr << "[etl_index_builder] cannot open " << corpus_path << "\n";
        return 1;
    }

    std::vector<std::string> lines;
    lines.reserve(4096);

    {
        std::string line;
        while (std::getline(in, line)) {
            if (!line.empty()) lines.push_back(line);
        }
    }
    if (lines.empty()) {
        std::cerr << "[etl_index_builder] corpus is empty\n";
        return 1;
    }

    const std::size_t total_lines = lines.size();

    unsigned hw = std::thread::hardware_concurrency();
    if (hw == 0) hw = 4;
    unsigned num_threads = std::min<unsigned>(hw, 16u);
    if (num_threads > total_lines) num_threads = (unsigned)total_lines;
    if (num_threads == 0) num_threads = 1;

    std::vector<ThreadResult> results(num_threads);
    std::vector<std::thread> workers;
    workers.reserve(num_threads);

    std::size_t chunk_size = (total_lines + num_threads - 1) / num_threads;
    std::size_t cur_start = 0;

    for (unsigned t = 0; t < num_threads; ++t) {
        std::size_t start = cur_start;
        std::size_t end   = std::min<std::size_t>(start + chunk_size, total_lines);
        cur_start = end;
        if (start >= end) break;

        workers.emplace_back([&, start, end, t]() {
            process_range(lines, start, end, results[t]);
        });
    }

    const unsigned used_threads = (unsigned)workers.size();
    for (auto& th : workers) th.join();

    std::uint64_t total_docs = 0, total_posts9 = 0;
    for (unsigned t = 0; t < used_threads; ++t) {
        total_docs += results[t].docs.size();
        total_posts9 += results[t].postings9.size();
    }
    if (total_docs == 0) {
        std::cerr << "[etl_index_builder] no valid docs\n";
        return 1;
    }

    std::vector<std::uint32_t> doc_id_offsets(used_threads, 0);
    {
        std::uint32_t acc = 0;
        for (unsigned t = 0; t < used_threads; ++t) {
            doc_id_offsets[t] = acc;
            acc += (std::uint32_t)results[t].docs.size();
        }
    }

    std::vector<DocMeta> docs;
    std::vector<std::string> doc_ids;
    std::vector<Posting9> postings9;

    docs.reserve((std::size_t)total_docs);
    doc_ids.reserve((std::size_t)total_docs);
    postings9.reserve((std::size_t)total_posts9);

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
            postings9.push_back(Posting9{p.h, base + p.did, p.pos});
        }
    }

    std::sort(postings9.begin(), postings9.end(), [](const Posting9& a, const Posting9& b) {
        if (a.h != b.h) return a.h < b.h;
        if (a.did != b.did) return a.did < b.did;
        return a.pos < b.pos;
    });

    const std::uint32_t N_docs   = (std::uint32_t)docs.size();
    const std::uint64_t N_post9  = (std::uint64_t)postings9.size();
    const std::uint64_t N_post13 = 0;

    const fs::path bin_fin  = out_dir / "index_native.bin";
    const fs::path doc_fin  = out_dir / "index_native_docids.json";
    const fs::path meta_fin = out_dir / "index_native_meta.json";

    const fs::path bin_tmp  = out_dir / "index_native.bin.tmp";
    const fs::path doc_tmp  = out_dir / "index_native_docids.json.tmp";
    const fs::path meta_tmp = out_dir / "index_native_meta.json.tmp";

    // binary tmp
    {
        std::ofstream bout(bin_tmp, std::ios::binary);
        if (!bout) {
            std::cerr << "[etl_index_builder] cannot open " << bin_tmp << "\n";
            return 1;
        }

        const char magic[4] = {'P','L','A','G'};
        bout.write(magic, 4);

        std::uint32_t version = 2;
        bout.write(reinterpret_cast<const char*>(&version), sizeof(version));
        bout.write(reinterpret_cast<const char*>(&N_docs), sizeof(N_docs));
        bout.write(reinterpret_cast<const char*>(&N_post9), sizeof(N_post9));
        bout.write(reinterpret_cast<const char*>(&N_post13), sizeof(N_post13));

        for (const auto& dm : docs) {
            bout.write(reinterpret_cast<const char*>(&dm.tok_len), sizeof(dm.tok_len));
            bout.write(reinterpret_cast<const char*>(&dm.simhash_hi), sizeof(dm.simhash_hi));
            bout.write(reinterpret_cast<const char*>(&dm.simhash_lo), sizeof(dm.simhash_lo));
        }

        for (const auto& p : postings9) {
            bout.write(reinterpret_cast<const char*>(&p.h), sizeof(p.h));
            bout.write(reinterpret_cast<const char*>(&p.did), sizeof(p.did));
            bout.write(reinterpret_cast<const char*>(&p.pos), sizeof(p.pos));
        }

        bout.flush();
        if (!bout) {
            std::cerr << "[etl_index_builder] write failed " << bin_tmp << "\n";
            return 1;
        }
    }

    // docids tmp
    {
        std::ofstream dout(doc_tmp, std::ios::binary);
        if (!dout) {
            std::cerr << "[etl_index_builder] cannot open " << doc_tmp << "\n";
            return 1;
        }
        dout << json(doc_ids).dump();
        dout.flush();
        if (!dout) {
            std::cerr << "[etl_index_builder] write failed " << doc_tmp << "\n";
            return 1;
        }
    }

    // meta tmp
    {
        json j_meta;
        j_meta["stats"] = {{"docs", N_docs}, {"k9", N_post9}, {"k13", 0}};
        j_meta["config"] = {
            {"max_matches_per_doc", 256},
            {"span_min_len", 6},
            {"span_gap", 0},
            {"max_spans_per_doc", 10},
            {"w_min_doc", 8},
            {"w_min_query", 9},
            {"alpha", 0.60}
        };

        std::ofstream mout(meta_tmp, std::ios::binary);
        if (!mout) {
            std::cerr << "[etl_index_builder] cannot open " << meta_tmp << "\n";
            return 1;
        }
        mout << j_meta.dump();
        mout.flush();
        if (!mout) {
            std::cerr << "[etl_index_builder] write failed " << meta_tmp << "\n";
            return 1;
        }
    }

    if (!atomic_replace_file(bin_tmp, bin_fin)) return 1;
    if (!atomic_replace_file(doc_tmp, doc_fin)) return 1;
    if (!atomic_replace_file(meta_tmp, meta_fin)) return 1;

    std::cout << "[etl_index_builder] built v2 index docs=" << N_docs
              << " post9=" << N_post9
              << " threads=" << used_threads << "\n";
    return 0;
}
