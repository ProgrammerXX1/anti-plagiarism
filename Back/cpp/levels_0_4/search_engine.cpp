// cpp/levels_0_4/search_engine.cpp
#include "search_engine.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
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

struct QSh {
    std::uint64_t h;
    std::uint32_t qpos; // shingle index in query
};

static bool build_query_shingles(
    const std::string& text_utf8,
    std::string& norm_out,
    std::vector<TokenSpan>& spans_out,
    std::vector<QSh>& qsh_out
) {
    norm_out = normalize_for_shingles_simple(text_utf8);

    spans_out.clear();
    tokenize_spans(norm_out, spans_out);
    if ((int)spans_out.size() < K) return false;

    const int q_tok = (int)spans_out.size();
    const int q_sh  = q_tok - K + 1;
    if (q_sh <= 0) return false;

    qsh_out.clear();
    qsh_out.reserve((std::size_t)q_sh);
    for (int pos = 0; pos < q_sh; ++pos) {
        std::uint64_t h = hash_shingle_tokens_spans(norm_out, spans_out, pos, K);
        qsh_out.push_back(QSh{h, (std::uint32_t)pos});
    }

    // sort by hash for cache locality (keep duplicates -> offsets are important)
    std::sort(qsh_out.begin(), qsh_out.end(), [](const QSh& a, const QSh& b) {
        if (a.h != b.h) return a.h < b.h;
        return a.qpos < b.qpos;
    });

    return true;
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

// postings9 must be sorted by h
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
    IndexConfig cfg;
    std::string meta_txt;
    const std::string meta_path = index_dir + "/index_native_meta.json";
    if (!read_all_text(meta_path, meta_txt)) {
        return cfg;
    }

    try {
        auto j = json::parse(meta_txt);
        // optional: read config.max_matches_per_doc etc
        if (j.contains("config") && j["config"].is_object()) {
            auto& c = j["config"];
            if (c.contains("max_matches_per_doc")) {
                cfg.max_matches_per_doc = c["max_matches_per_doc"].get<int>();
            }
        }
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
    if (!read_u32(in, version) || (version != 1 && version != 2)) {
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
        std::cerr << "[SearchEngine] WARNING: docids.size != N_docs ("
                  << doc_ids_.size() << " vs " << N_docs << ")\n";
        if (doc_ids_.size() > (std::size_t)N_docs) doc_ids_.resize((std::size_t)N_docs);
    }

    docs_.resize((std::size_t)N_docs);

    // Builder writes: tok_len(u32), simhash_hi(u64), simhash_lo(u64)
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
    if (version == 1) {
        // legacy: (h,u64) (did,u32) without dpos -> set pos=0
        for (std::size_t i = 0; i < (std::size_t)N_post9; ++i) {
            std::uint64_t h = 0;
            std::uint32_t did = 0;
            if (!read_u64(in, h) || !read_u32(in, did)) {
                std::cerr << "[SearchEngine] postings9(v1) read failed at i=" << i << "\n";
                return false;
            }
            post9_[i] = Posting9{h, did, 0};
        }
    } else {
        // v2: (h,u64) (did,u32) (pos,u32)
        for (std::size_t i = 0; i < (std::size_t)N_post9; ++i) {
            std::uint64_t h = 0;
            std::uint32_t did = 0;
            std::uint32_t pos = 0;
            if (!read_u64(in, h) || !read_u32(in, did) || !read_u32(in, pos)) {
                std::cerr << "[SearchEngine] postings9(v2) read failed at i=" << i << "\n";
                return false;
            }
            post9_[i] = Posting9{h, did, pos};
        }
    }

    // postings must be sorted for range queries
    std::sort(post9_.begin(), post9_.end(), [](const Posting9& a, const Posting9& b) {
        if (a.h != b.h) return a.h < b.h;
        if (a.did != b.did) return a.did < b.did;
        return a.pos < b.pos;
    });

    loaded_ = true;
    return true;
}

// ----------------- search (phase 1: top-k) -----------------

int SearchEngine::search_text(
    const std::string& text_utf8,
    int top_k,
    std::vector<SeHitLite>& out
) const {
    out.clear();
    if (!loaded_ || top_k <= 0) return 0;

    std::string norm;
    std::vector<TokenSpan> spans;
    std::vector<QSh> qsh;
    spans.reserve(256);

    if (!build_query_shingles(text_utf8, norm, spans, qsh)) return 0;

    const int q_tok = (int)spans.size();
    const int q_sh_total = q_tok - K + 1;
    if (q_sh_total <= 0) return 0;

    // IMPORTANT: for scoring use unique hashes (set size), but offsets are collected later
    std::vector<std::uint64_t> q_uniq;
    q_uniq.reserve((std::size_t)q_sh_total);
    for (auto& it : qsh) q_uniq.push_back(it.h);
    std::sort(q_uniq.begin(), q_uniq.end());
    q_uniq.erase(std::unique(q_uniq.begin(), q_uniq.end()), q_uniq.end());

    const int q_size = (int)q_uniq.size();
    if (q_size <= 0) return 0;

    // Dense hits + touched list (fast)
    const std::size_t N = docs_.size();
    std::vector<int> hits(N, 0);
    std::vector<std::uint32_t> touched;
    touched.reserve((std::size_t)q_size * 64);

    for (std::uint64_t h : q_uniq) {
        auto [L, R] = find_postings9_range(h);
        for (std::size_t i = L; i < R; ++i) {
            const std::uint32_t did = post9_[i].did;
            if (did >= N) continue;
            if (hits[did] == 0) touched.push_back(did);
            hits[did] += 1;
        }
    }

    if (touched.empty()) return 0;

    struct Cand {
        std::uint32_t did;
        int inter;
        double score;
        double J;
        double C;
    };

    std::vector<Cand> cands;
    cands.reserve(touched.size());

    for (std::uint32_t did : touched) {
        const int inter = hits[did];
        hits[did] = 0; // reset

        const auto& dm = docs_[did];
        if ((int)dm.tok_len < cfg_.w_min_doc) continue;

        int t_size = 0;
        if ((int)dm.tok_len >= K) t_size = (int)dm.tok_len - K + 1;
        if (t_size <= 0) continue;

        double J = 0.0, C = 0.0;
        jc_compute(inter, q_size, t_size, J, C);

        double s = cfg_.alpha * J + (1.0 - cfg_.alpha) * C;
        s = clamp01(s);

        cands.push_back(Cand{did, inter, s, J, C});
    }

    if (cands.empty()) return 0;

    const int keep = std::min<int>((int)cands.size(), top_k);
    std::partial_sort(
        cands.begin(),
        cands.begin() + keep,
        cands.end(),
        [](const Cand& a, const Cand& b) { return a.score > b.score; }
    );

    out.reserve((std::size_t)keep);
    for (int i = 0; i < keep; ++i) {
        SeHitLite h{};
        h.doc_id_int = cands[i].did;
        h.score = cands[i].score;
        h.j9 = cands[i].J;
        h.c9 = cands[i].C;
        h.cand_hits = cands[i].inter;
        out.push_back(h);
    }

    return keep;
}

// ----------------- offsets (phase 2: collect matches for top hits) -----------------

void SearchEngine::collect_matches_for_hits(
    const std::string& text_utf8,
    const std::vector<SeHitLite>& hits,
    std::vector<std::vector<MatchPair>>& out_matches
) const {
    out_matches.clear();
    out_matches.resize(hits.size());
    if (!loaded_ || hits.empty()) return;

    std::string norm;
    std::vector<TokenSpan> spans;
    std::vector<QSh> qsh;
    spans.reserve(256);

    if (!build_query_shingles(text_utf8, norm, spans, qsh)) return;

    // Build a set of target doc ids (top docs)
    std::unordered_set<std::uint32_t> target;
    target.reserve(hits.size() * 2);
    for (auto& h : hits) target.insert(h.doc_id_int);

    // For speed: per-doc counter for match limit
    std::unordered_map<std::uint32_t, int> used;
    used.reserve(hits.size() * 2);

    // We will collect by scanning postings range for each query shingle.
    for (const auto& qs : qsh) {
        auto [L, R] = find_postings9_range(qs.h);
        if (L == R) continue;

        for (std::size_t i = L; i < R; ++i) {
            const auto& p = post9_[i];
            if (target.find(p.did) == target.end()) continue;

            int& cnt = used[p.did];
            if (cnt >= cfg_.max_matches_per_doc) continue;
            cnt += 1;

            // push to corresponding hit bucket (keep stable ordering by hits vector)
            // small hits.size(), linear scan is OK; if you want faster, build did->idx map once.
            for (std::size_t k = 0; k < hits.size(); ++k) {
                if (hits[k].doc_id_int == p.did) {
                    out_matches[k].push_back(MatchPair{qs.qpos, p.pos, qs.h});
                    break;
                }
            }
        }
    }

    // Optional: sort per-doc matches by qpos then dpos
    for (auto& v : out_matches) {
        std::sort(v.begin(), v.end(), [](const MatchPair& a, const MatchPair& b) {
            if (a.qpos != b.qpos) return a.qpos < b.qpos;
            if (a.dpos != b.dpos) return a.dpos < b.dpos;
            return a.h < b.h;
        });
    }
}
