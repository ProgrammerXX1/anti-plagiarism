// ==============================
// search_engine.h  (V5.1 full, 10/10)
// ==============================
#pragma once
#include <cstdint>
#include <string>
#include <vector>

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
    int    max_df_for_seed = 200000;

    int    max_q_uniq9     = 4096;

    // soft budget (0 = use hard budget)
    std::uint64_t max_sum_df_seeds = 2'000'000;

    // hard safety even if max_sum_df_seeds==0 (prevents memory bombs)
    std::uint64_t hard_max_sum_df_seeds = 20'000'000;

    // validation knobs
    int    validate_postings_samples = 64;
    int    validate_postings_maxlen  = 4096;
    int    validate_did_samples      = 200000;
    int    validate_uniq_samples     = 50000; // moderate default for startup cost

    // perf stats knobs
    int    perf_stats = 0; // 0/1 (phase timings in microseconds)
};

struct SearchStats {
    int q_uniq_shingles = 0;

    int seeds_total = 0;
    int seeds_used  = 0;

    int cand_total_before_cap = 0;
    int cand_after_cap        = 0;

    int inter_scanned_shingles = 0;
    int scored = 0;

    int index_version = 0; // 1 or 2
    int mmap_on = 0;       // 0/1

    // optional perf timings (us)
    std::uint64_t t_norm_us   = 0;
    std::uint64_t t_token_us  = 0;
    std::uint64_t t_hash_us   = 0;
    std::uint64_t t_qterms_us = 0;
    std::uint64_t t_seeds_us  = 0;
    std::uint64_t t_raw_us    = 0;
    std::uint64_t t_inter_us  = 0;
    std::uint64_t t_score_us  = 0;
    std::uint64_t t_topk_us   = 0;
};

class SearchEngine {
public:
    SearchEngine() = default;
    ~SearchEngine();

    SearchEngine(const SearchEngine&) = delete;
    SearchEngine& operator=(const SearchEngine&) = delete;

    bool load(const std::string& index_dir);

    int  docs_count() const { return (int)N_docs_; }
    const std::vector<std::string>& doc_ids() const { return doc_ids_; }

    int search_text(
        const std::string& text_utf8,
        int top_k,
        std::vector<SeHitLite>& out,
        SearchStats* stats = nullptr
    ) const;

    std::uint64_t approx_bytes() const;

private:
    struct DocMetaMem {
        std::uint32_t tok_len;
        std::uint32_t bm25_len;
        std::uint64_t simhash_hi;
        std::uint64_t simhash_lo;
    };

#pragma pack(push,1)
    struct DocMetaDisk {
        std::uint32_t tok_len;
        std::uint64_t simhash_hi;
        std::uint64_t simhash_lo;
    };
    struct HeaderV2 {
        char     magic[4];      // "PLAG"
        std::uint32_t version;  // 2
        std::uint32_t N_docs;
        std::uint64_t uniq9_cnt;
        std::uint64_t did9_cnt;
        std::uint64_t reserved0;
        std::uint64_t reserved1;
    };
#pragma pack(pop)

    static inline bool is_little_endian();
    static inline double clamp01(double x);
    static inline void jc_compute(int inter, int q_size, int t_size, double& J, double& C);

    IndexConfig load_config_from_json(const std::string& index_dir);
    bool load_docids_json(const std::string& index_dir);

    bool load_v2_mmap(const std::string& bin_path);
    bool load_v1_build_csr(const std::string& bin_path);

    bool validate_csr_basic(std::uint32_t N, std::uint64_t U, std::uint64_t D) const;
    bool validate_postings_sorted_sample() const;
    bool validate_uniq_sorted_sample() const;

    inline bool find_postings_hint(std::uint64_t h, std::uint64_t& L, std::uint64_t& R, std::uint64_t& hint_pos) const;

    inline std::uint32_t did_at(std::uint64_t pos) const;
    inline std::uint32_t tok_len_at(std::uint32_t did) const;

    void reset_all();

private:
    bool loaded_ = false;
    IndexConfig cfg_{};

    std::vector<std::string> doc_ids_;

    bool mmap_on_ = false;
    int  index_version_ = 0;

    const std::uint64_t* uniq9_ = nullptr;
    const std::uint64_t* off9_  = nullptr;
    const std::uint32_t* did9_  = nullptr;
    std::uint64_t uniqN_ = 0;
    std::uint64_t didN_  = 0;

    std::vector<std::uint64_t> uniq9_mem_;
    std::vector<std::uint64_t> off9_mem_;
    std::vector<std::uint32_t> did9_mem_;

    const DocMetaDisk* docs_disk_ = nullptr;
    std::uint32_t N_docs_ = 0;

    std::vector<DocMetaMem> docs_mem_;

#if defined(__linux__)
    int    fd_ = -1;
    void*  map_ = nullptr;
    std::size_t map_size_ = 0;
#endif
};


