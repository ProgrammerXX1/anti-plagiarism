// cpp/levels_0_4/search_engine.cpp
#include "search_engine.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include "text_common.h"  // normalize_for_shingles_simple, tokenize_spans, hash_shingle_tokens_spans

using json = nlohmann::json;

namespace {

constexpr int K = 9;

static bool read_all_text(const std::string& path, std::string& out) {
    std::ifstream in(path);
    if (!in) return false;
    in.seekg(0, std::ios::end);
    std::streamoff n = in.tellg();
    in.seekg(0, std::ios::beg);
    if (n < 0) n = 0;
    out.resize((std::size_t)n);
    if (!out.empty()) in.read(out.data(), (std::streamsize)out.size());
    return true;
}

static bool read_u32(std::ifstream& in, std::uint32_t& v) {
    in.read(reinterpret_cast<char*>(&v), sizeof(v));
    return (bool)in;
}
static bool read_u64(std::ifstream& in, std::uint64_t& v) {
    in.read(reinterpret_cast<char*>(&v), sizeof(v));
    return (bool)in;
}

} // namespace

// ----------------- helpers -----------------

inline double SearchEngine::clamp01(double x) {
    if (x < 0.0) return 0.0;
    if (x > 1.0) return 1.0;
    return x;
}

inline int SearchEngine::clamp_int(int x, int lo, int hi) {
    if (x < lo) return lo;
    if (x > hi) return hi;
    return x;
}

inline void SearchEngine::jc_compute(int inter, int q_size, int t_size, double& J, double& C) {
    if (inter <= 0 || q_size <= 0 || t_size <= 0) {
        J = 0.0;
        C = 0.0;
        return;
    }
    const int uni = q_size + t_size - inter;
    J = (uni > 0) ? (double)inter / (double)uni : 0.0;
    C = (q_size > 0) ? (double)inter / (double)q_size : 0.0;
}

// postings9 должны быть отсортированы по h
std::pair<std::size_t, std::size_t> SearchEngine::find_postings9_range(std::uint64_t h) const {
    auto lb = std::lower_bound(
        post9_.begin(), post9_.end(), h,
        [](const Posting9& p, std::uint64_t key) { return p.h < key; }
    );
    auto ub = std::upper_bound(
        post9_.begin(), post9_.end(), h,
        [](std::uint64_t key, const Posting9& p) { return key < p.h; }
    );
    return { (std::size_t)std::distance(post9_.begin(), lb),
             (std::size_t)std::distance(post9_.begin(), ub) };
}

IndexConfig SearchEngine::load_config_from_json(const std::string& index_dir) {
    IndexConfig cfg; // дефолты из .h
    std::string meta_txt;
    const std::string meta_path = index_dir + "/index_native_meta.json";
    if (!read_all_text(meta_path, meta_txt)) {
        return cfg;
    }

    try {
        auto j = json::parse(meta_txt);

        // если хочешь — можешь позже расширить формат config под свои веса
        // сейчас просто читаем thresholds как факт наличия meta
        (void)j;

        return cfg;
    } catch (...) {
        return cfg;
    }
}

// ----------------- load -----------------

bool SearchEngine::load(const std::string& index_dir) {
    loaded_ = false;
    docs_.clear();
    post9_.clear();
    doc_ids_.clear();
    cfg_ = load_config_from_json(index_dir);

    // 1) docids
    {
        std::string txt;
        const std::string p = index_dir + "/index_native_docids.json";
        if (!read_all_text(p, txt)) {
            std::cerr << "[SearchEngine] cannot read docids: " << p << "\n";
            return false;
        }
        try {
            auto j = json::parse(txt);
            if (!j.is_array()) {
                std::cerr << "[SearchEngine] docids json is not array\n";
                return false;
            }
            doc_ids_.reserve(j.size());
            for (auto& x : j) {
                if (x.is_string()) doc_ids_.push_back(x.get<std::string>());
                else doc_ids_.push_back(x.dump());
            }
        } catch (const std::exception& e) {
            std::cerr << "[SearchEngine] docids json parse error: " << e.what() << "\n";
            return false;
        }
    }

    // 2) bin
    const std::string bin_path = index_dir + "/index_native.bin";
    std::ifstream in(bin_path, std::ios::binary);
    if (!in) {
        std::cerr << "[SearchEngine] cannot open bin: " << bin_path << "\n";
        return false;
    }

    char magic[4] = {0,0,0,0};
    in.read(magic, 4);
    if (!in || magic[0] != 'P' || magic[1] != 'L' || magic[2] != 'A' || magic[3] != 'G') {
        std::cerr << "[SearchEngine] bad magic in " << bin_path << "\n";
        return false;
    }

    std::uint32_t version = 0;
    if (!read_u32(in, version) || version != 1) {
        std::cerr << "[SearchEngine] bad version in " << bin_path << ": " << version << "\n";
        return false;
    }

    std::uint32_t N_docs = 0;
    std::uint64_t N_post9 = 0, N_post13 = 0;
    if (!read_u32(in, N_docs) || !read_u64(in, N_post9) || !read_u64(in, N_post13)) {
        std::cerr << "[SearchEngine] header read failed\n";
        return false;
    }

    if (N_docs == 0) {
        std::cerr << "[SearchEngine] N_docs=0\n";
        return false;
    }

    if (doc_ids_.size() != (std::size_t)N_docs) {
        // не фатально, но лучше держать консистентность
        std::cerr << "[SearchEngine] WARNING: docids.size != N_docs ("
                  << doc_ids_.size() << " vs " << N_docs << ")\n";
        // если больше — обрежем, если меньше — тоже работать будет, но часть doc_id не будет резолвиться
        if (doc_ids_.size() > (std::size_t)N_docs) doc_ids_.resize((std::size_t)N_docs);
    }

    docs_.resize((std::size_t)N_docs);

    // ВАЖНО: твой etl_index_builder пишет для DocMeta 3 поля:
    // tok_len (u32), simhash_hi (u64), simhash_lo (u64)
    // А наш DocMeta в search_engine.h содержит ещё bm25_len (u32) — заполним = tok_len.
    for (std::size_t i = 0; i < (std::size_t)N_docs; ++i) {
        std::uint32_t tok_len = 0;
        std::uint64_t hi = 0, lo = 0;
        if (!read_u32(in, tok_len) || !read_u64(in, hi) || !read_u64(in, lo)) {
            std::cerr << "[SearchEngine] doc meta read failed at i=" << i << "\n";
            return false;
        }
        DocMeta dm{};
        dm.tok_len = tok_len;
        dm.bm25_len = tok_len;
        dm.simhash_hi = hi;
        dm.simhash_lo = lo;
        docs_[i] = dm;
    }

    post9_.resize((std::size_t)N_post9);
    for (std::size_t i = 0; i < (std::size_t)N_post9; ++i) {
        std::uint64_t h = 0;
        std::uint32_t did = 0;
        if (!read_u64(in, h) || !read_u32(in, did)) {
            std::cerr << "[SearchEngine] postings9 read failed at i=" << i << "\n";
            return false;
        }
        post9_[i] = Posting9{h, did};
    }

    // 3) сортируем postings по hash, иначе find_postings9_range бессмысленен
    std::sort(post9_.begin(), post9_.end(), [](const Posting9& a, const Posting9& b) {
        if (a.h != b.h) return a.h < b.h;
        return a.did < b.did;
    });

    loaded_ = true;
    return true;
}

// ----------------- search -----------------

int SearchEngine::search_text(
    const std::string& text_utf8,
    int top_k,
    std::vector<SeHitLite>& out
) const {
    out.clear();
    if (!loaded_ || top_k <= 0) return 0;

    // нормализуем и токенизируем как в билдере
    std::string norm = normalize_for_shingles_simple(text_utf8);

    std::vector<TokenSpan> spans;
    spans.reserve(256);
    tokenize_spans(norm, spans);

    if ((int)spans.size() < K) return 0;

    const int q_tok = (int)spans.size();
    const int q_sh  = q_tok - K + 1;
    if (q_sh <= 0) return 0;

    // шинглы запроса (hashes)
    std::vector<std::uint64_t> q_hashes;
    q_hashes.reserve((std::size_t)q_sh);
    for (int pos = 0; pos < q_sh; ++pos) {
        std::uint64_t h = hash_shingle_tokens_spans(norm, spans, pos, K);
        q_hashes.push_back(h);
    }
    std::sort(q_hashes.begin(), q_hashes.end());
    q_hashes.erase(std::unique(q_hashes.begin(), q_hashes.end()), q_hashes.end());

    const int q_size = (int)q_hashes.size();
    if (q_size < cfg_.w_min_query) {
        // слишком короткий запрос для устойчивой метрики
        // но можно не резать, если хочешь:
        // return 0;
    }

    // кандидаты: doc_id -> hits
    std::unordered_map<std::uint32_t, int> hits;
    hits.reserve((std::size_t)q_size * 8);

    for (std::uint64_t h : q_hashes) {
        auto [L, R] = find_postings9_range(h);
        if (L == R) continue;

        // добавляем все did из диапазона
        for (std::size_t i = L; i < R; ++i) {
            const std::uint32_t did = post9_[i].did;
            auto it = hits.find(did);
            if (it == hits.end()) hits.emplace(did, 1);
            else it->second += 1;
        }
    }

    if (hits.empty()) return 0;

    // соберём вектор кандидатов и посчитаем score
    struct Cand {
        std::uint32_t did;
        int inter;
        double score;
        double J;
        double C;
    };

    std::vector<Cand> cands;
    cands.reserve(hits.size());

    for (const auto& kv : hits) {
        const std::uint32_t did = kv.first;
        const int inter = kv.second;

        if (did >= docs_.size()) continue;

        const auto& dm = docs_[did];
        if ((int)dm.tok_len < cfg_.w_min_doc) continue;

        // оценка размера множества шинглов документа
        int t_size = 0;
        if ((int)dm.tok_len >= K) t_size = (int)dm.tok_len - K + 1;
        if (t_size <= 0) continue;

        double J = 0.0, C = 0.0;
        jc_compute(inter, q_size, t_size, J, C);

        // простой скоринг: смесь Jaccard + containment
        double s = cfg_.alpha * J + (1.0 - cfg_.alpha) * C;
        s = clamp01(s);

        cands.push_back(Cand{did, inter, s, J, C});
    }

    if (cands.empty()) return 0;

    std::partial_sort(
        cands.begin(),
        cands.begin() + std::min<int>((int)cands.size(), top_k),
        cands.end(),
        [](const Cand& a, const Cand& b) { return a.score > b.score; }
    );

    const int take = std::min<int>((int)cands.size(), top_k);
    out.reserve((std::size_t)take);

    for (int i = 0; i < take; ++i) {
        const auto& c = cands[i];
        SeHitLite h{};
        h.doc_id_int = c.did;
        h.score = c.score;
        h.j9 = c.J;
        h.c9 = c.C;
        h.cand_hits = c.inter;
        out.push_back(h);
    }

    return take;
}
