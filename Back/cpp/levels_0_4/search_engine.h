// cpp/levels_0_4/search_engine.h
#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <utility>

struct SeHitLite {
    std::uint32_t doc_id_int;
    double score;
    double j9;
    double c9;
    int    cand_hits;
};

struct IndexConfig {
    int    w_min_doc       = 8;
    int    w_min_query     = 9;
    double alpha           = 0.60;
    double w9              = 0.90;

    int    fetch_per_k     = 64;
    int    max_cands_doc   = 1000;

    // how many match pairs to return per doc in JSON
    int    max_matches_per_doc = 256;
};

struct MatchPair {
    std::uint32_t qpos;   // shingle index in query
    std::uint32_t dpos;   // shingle index in doc
    std::uint64_t h;      // shingle hash (optional but useful)
};

class SearchEngine {
public:
    SearchEngine() = default;

    bool load(const std::string& index_dir); // reads bin + docids + meta/config (optional)
    int  docs_count() const { return (int)doc_ids_.size(); }
    const std::vector<std::string>& doc_ids() const { return doc_ids_; }

    // Fast top-k scoring (no offsets returned)
    int search_text(const std::string& text_utf8, int top_k, std::vector<SeHitLite>& out) const;

    // Collect offsets for a given set of hits (top docs), returns per-doc matches aligned with hits order.
    // This is phase-2, called only for top results.
    void collect_matches_for_hits(
        const std::string& text_utf8,
        const std::vector<SeHitLite>& hits,
        std::vector<std::vector<MatchPair>>& out_matches
    ) const;

private:
    struct DocMeta {
        std::uint32_t tok_len;
        std::uint32_t bm25_len;
        std::uint64_t simhash_hi;
        std::uint64_t simhash_lo;
    };

    // postings: (hash, doc_id_int, dpos)
    struct Posting9 {
        std::uint64_t h;
        std::uint32_t did;
        std::uint32_t pos;
    };

    static inline double clamp01(double x);
    static inline int clamp_int(int x, int lo, int hi);
    static inline void jc_compute(int inter, int q_size, int t_size, double& J, double& C);

    // postings9 must be sorted by (h, did, pos)
    std::pair<std::size_t, std::size_t> find_postings9_range(std::uint64_t h) const;

    IndexConfig load_config_from_json(const std::string& index_dir);

private:
    bool loaded_ = false;
    IndexConfig cfg_{};

    std::vector<DocMeta> docs_;
    std::vector<Posting9> post9_;
    std::vector<std::string> doc_ids_;
};
