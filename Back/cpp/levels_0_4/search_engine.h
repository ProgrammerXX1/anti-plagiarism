#pragma once
#include <string>
#include <vector>
#include <cstdint>
#include <utility>

struct SeHitLite {
    std::uint32_t doc_id_int;
    double score;
    double j9;
    double c9;
    int    cand_hits;
};

struct IndexConfig {
    int  w_min_doc   = 8;
    int  w_min_query = 9;
    double alpha = 0.60;
    double w9    = 0.90;
    int    fetch_per_k   = 64;
    int    max_cands_doc = 1000;
};

class SearchEngine {
public:
    SearchEngine() = default;

    bool load(const std::string& index_dir);                 // читает bin + docids + config
    int  docs_count() const { return (int)doc_ids_.size(); }
    const std::vector<std::string>& doc_ids() const { return doc_ids_; }

    int search_text(const std::string& text_utf8, int top_k, std::vector<SeHitLite>& out) const;

private:
    struct DocMeta {
        std::uint32_t tok_len;
        std::uint32_t bm25_len;
        std::uint64_t simhash_hi;
        std::uint64_t simhash_lo;
    };
    struct Posting9 {
        std::uint64_t h;
        std::uint32_t did;
    };

    static inline double clamp01(double x);
    static inline int clamp_int(int x, int lo, int hi);
    static inline void jc_compute(int inter, int q_size, int t_size, double& J, double& C);
    std::pair<std::size_t, std::size_t> find_postings9_range(std::uint64_t h) const;
    IndexConfig load_config_from_json(const std::string& index_dir);

private:
    bool loaded_ = false;
    IndexConfig cfg_{};

    std::vector<DocMeta> docs_;
    std::vector<Posting9> post9_;
    std::vector<std::string> doc_ids_;
};
