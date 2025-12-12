// ==============================
// search_engine.cpp  (V5.1 full, 10/10)
// ==============================
#include "search_engine.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <random>
#include <string>
#include <utility>
#include <vector>
#include <chrono>

#include <nlohmann/json.hpp>
#include "text_common.h"

#if defined(__linux__)
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#endif

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

static inline std::uint64_t now_us() {
    using namespace std::chrono;
    return (std::uint64_t)duration_cast<microseconds>(steady_clock::now().time_since_epoch()).count();
}

// ---- TLS memory caps (P0) ----
constexpr std::size_t TLS_MAX_RAW_CAP   = 4'000'000; // ~16MB for uint32_t
constexpr std::size_t TLS_MAX_QHASH_CAP = 8192;
constexpr std::size_t TLS_MAX_CAND_CAP  = 4096;

// Query term with cached postings range
struct QTerm {
    std::uint64_t h;
    std::uint64_t df;
    std::uint64_t L;
    std::uint64_t R;
};

struct CandScore {
    std::uint32_t did;
    double score;
    double J;
    double C;
    int hits;
};

struct TLSBufs {
    std::vector<TokenSpan> spans;
    std::vector<std::uint64_t> q_hashes;

    std::vector<std::uint32_t> raw;
    std::vector<std::pair<std::uint32_t,int>> cand;

    std::vector<std::uint16_t> inter_cnt;
    std::vector<int> idx_all;

    // NEW (V5.1): no-alloc hot path
    std::vector<QTerm> qterms;
    std::vector<CandScore> scored;

    void clear_soft() {
        spans.clear();
        q_hashes.clear();
        raw.clear();
        cand.clear();
        inter_cnt.clear();
        idx_all.clear();
        qterms.clear();
        scored.clear();

        // soft shrink only if blown up
        if (raw.capacity() > TLS_MAX_RAW_CAP) std::vector<std::uint32_t>().swap(raw);
        if (q_hashes.capacity() > TLS_MAX_QHASH_CAP) std::vector<std::uint64_t>().swap(q_hashes);
        if (cand.capacity() > TLS_MAX_CAND_CAP) std::vector<std::pair<std::uint32_t,int>>().swap(cand);
        if (inter_cnt.capacity() > TLS_MAX_CAND_CAP) std::vector<std::uint16_t>().swap(inter_cnt);
        if (idx_all.capacity() > TLS_MAX_QHASH_CAP) std::vector<int>().swap(idx_all);
        if (qterms.capacity() > TLS_MAX_QHASH_CAP) std::vector<QTerm>().swap(qterms);
        if (scored.capacity() > TLS_MAX_CAND_CAP) std::vector<CandScore>().swap(scored);
    }
};

static thread_local TLSBufs g_tls;
} // namespace

SearchEngine::~SearchEngine() { reset_all(); }

inline bool SearchEngine::is_little_endian() {
    const std::uint32_t x = 1;
    return *(const std::uint8_t*)&x == 1;
}

inline double SearchEngine::clamp01(double x) {
    if (x < 0.0) return 0.0;
    if (x > 1.0) return 1.0;
    return x;
}

inline void SearchEngine::jc_compute(int inter, int q, int t, double& J, double& C) {
    if (inter <= 0 || q <= 0 || t <= 0) { J = 0.0; C = 0.0; return; }
    const int uni = q + t - inter;
    J = (uni > 0) ? (double)inter / (double)uni : 0.0;
    C = (double)inter / (double)q;
}

void SearchEngine::reset_all() {
    loaded_ = false;

    doc_ids_.clear();
    cfg_ = {};

    mmap_on_ = false;
    index_version_ = 0;

    uniq9_ = off9_ = nullptr;
    did9_ = nullptr;
    uniqN_ = didN_ = 0;

    uniq9_mem_.clear();
    off9_mem_.clear();
    did9_mem_.clear();

    docs_disk_ = nullptr;
    N_docs_ = 0;
    docs_mem_.clear();

#if defined(__linux__)
    if (map_) {
        munmap(map_, map_size_);
        map_ = nullptr;
        map_size_ = 0;
    }
    if (fd_ != -1) {
        close(fd_);
        fd_ = -1;
    }
#endif
}

IndexConfig SearchEngine::load_config_from_json(const std::string& index_dir) {
    IndexConfig cfg;
    std::ifstream in(index_dir + "/index_config.json");
    if (!in) return cfg;

    json j;
    try { in >> j; } catch (...) { return cfg; }

    if (j.contains("w_min_doc")) cfg.w_min_doc = j["w_min_doc"].get<int>();
    if (j.contains("w_min_query")) cfg.w_min_query = j["w_min_query"].get<int>();
    if (j.contains("fetch_per_k_doc")) cfg.fetch_per_k = j["fetch_per_k_doc"].get<int>();
    if (j.contains("max_cands_doc")) cfg.max_cands_doc = j["max_cands_doc"].get<int>();
    if (j.contains("max_df_for_seed")) cfg.max_df_for_seed = j["max_df_for_seed"].get<int>();
    if (j.contains("max_q_uniq9")) cfg.max_q_uniq9 = j["max_q_uniq9"].get<int>();
    if (j.contains("max_sum_df_seeds")) cfg.max_sum_df_seeds = j["max_sum_df_seeds"].get<std::uint64_t>();
    if (j.contains("hard_max_sum_df_seeds")) cfg.hard_max_sum_df_seeds = j["hard_max_sum_df_seeds"].get<std::uint64_t>();

    if (j.contains("validate_postings_samples")) cfg.validate_postings_samples = j["validate_postings_samples"].get<int>();
    if (j.contains("validate_postings_maxlen")) cfg.validate_postings_maxlen = j["validate_postings_maxlen"].get<int>();
    if (j.contains("validate_did_samples")) cfg.validate_did_samples = j["validate_did_samples"].get<int>();
    if (j.contains("validate_uniq_samples")) cfg.validate_uniq_samples = j["validate_uniq_samples"].get<int>();

    if (j.contains("perf_stats")) cfg.perf_stats = j["perf_stats"].get<int>();

    if (j.contains("weights")) {
        auto w = j["weights"];
        if (w.contains("alpha")) cfg.alpha = w["alpha"].get<double>();
        if (w.contains("w9")) cfg.w9 = w["w9"].get<double>();
    }

    if (cfg.w_min_doc < 1) cfg.w_min_doc = 1;
    if (cfg.w_min_query < 1) cfg.w_min_query = 1;
    if (cfg.fetch_per_k < 1) cfg.fetch_per_k = 1;
    if (cfg.max_cands_doc < 1) cfg.max_cands_doc = 1;
    if (cfg.max_df_for_seed < 1) cfg.max_df_for_seed = 1;

    if (cfg.max_q_uniq9 < 128) cfg.max_q_uniq9 = 128;

    if (cfg.hard_max_sum_df_seeds < 1'000'000) cfg.hard_max_sum_df_seeds = 1'000'000;

    if (cfg.validate_postings_samples < 0) cfg.validate_postings_samples = 0;
    if (cfg.validate_postings_maxlen < 16) cfg.validate_postings_maxlen = 16;
    if (cfg.validate_did_samples < 0) cfg.validate_did_samples = 0;
    if (cfg.validate_uniq_samples < 0) cfg.validate_uniq_samples = 0;

    cfg.alpha = clamp01(cfg.alpha);
    cfg.w9    = clamp01(cfg.w9);
    return cfg;
}

bool SearchEngine::load_docids_json(const std::string& index_dir) {
    std::string txt;
    const std::string p = index_dir + "/index_native_docids.json";
    if (!read_all_text(p, txt)) return false;

    try {
        auto j = json::parse(txt);
        if (!j.is_array()) return false;

        doc_ids_.clear();
        doc_ids_.reserve(j.size());
        for (auto& x : j) {
            if (!x.is_string()) return false;
            doc_ids_.push_back(x.get<std::string>());
        }
    } catch (...) {
        return false;
    }
    return true;
}

bool SearchEngine::validate_csr_basic(std::uint32_t N, std::uint64_t U, std::uint64_t D) const {
    if (!uniq9_ || !off9_ || !did9_) return false;
    if (N == 0 || U == 0) return false;

    if (off9_[0] != 0) return false;
    if (off9_[U] != D) return false;

    for (std::uint64_t i = 0; i < U; ++i) {
        const auto a = off9_[i];
        const auto b = off9_[i + 1];
        if (a > b) return false;
        if (b > D) return false;
    }

    if (D > 0) {
        auto check_window = [&](std::uint64_t start, std::uint64_t len) -> bool {
            const std::uint64_t end = std::min<std::uint64_t>(D, start + len);
            for (std::uint64_t i = start; i < end; ++i) {
                if (did9_[i] >= N) return false;
            }
            return true;
        };

        const std::uint64_t win = 65536;
        if (!check_window(0, win)) return false;

        if (D > win) {
            const std::uint64_t mid = (D / 2);
            const std::uint64_t s1 = (mid > win/2) ? (mid - win/2) : 0;
            if (!check_window(s1, win)) return false;

            const std::uint64_t s2 = (D > win) ? (D - win) : 0;
            if (!check_window(s2, win)) return false;
        }

        const int samples = cfg_.validate_did_samples;
        if (samples > 0 && D > 1) {
            std::mt19937_64 rng(0xC0FFEEULL ^ (std::uint64_t)D ^ ((std::uint64_t)N << 1));
            std::uniform_int_distribution<std::uint64_t> dist(0, D - 1);
            for (int i = 0; i < samples; ++i) {
                const std::uint64_t pos = dist(rng);
                if (did9_[pos] >= N) return false;
            }
        }
    }

    return true;
}

bool SearchEngine::validate_postings_sorted_sample() const {
    if (N_docs_ == 0) return false;
    if (!uniq9_ || !off9_ || !did9_ || uniqN_ == 0) return false;

    const int S = cfg_.validate_postings_samples;
    if (S <= 0) return true;

    std::mt19937_64 rng(0xBADC0DEULL ^ uniqN_ ^ (didN_ << 1) ^ (std::uint64_t)N_docs_);
    std::uniform_int_distribution<std::uint64_t> dist(0, uniqN_ - 1);

    for (int s = 0; s < S; ++s) {
        const std::uint64_t i = dist(rng);
        const std::uint64_t L = off9_[i];
        const std::uint64_t R = off9_[i + 1];
        if (L > R || R > didN_) return false;

        const std::uint64_t len = R - L;
        if (len <= 1) continue;

        const std::uint64_t check_len = std::min<std::uint64_t>(len, (std::uint64_t)cfg_.validate_postings_maxlen);
        const std::uint32_t* p = did9_ + L;

        std::uint32_t prev = p[0];
        if (prev >= N_docs_) return false;

        for (std::uint64_t k = 1; k < check_len; ++k) {
            const std::uint32_t cur = p[k];
            if (cur >= N_docs_) return false;
            if (cur <= prev) return false; // strictly increasing => sorted+unique
            prev = cur;
        }
    }
    return true;
}

bool SearchEngine::validate_uniq_sorted_sample() const {
    if (!uniq9_ || uniqN_ == 0) return false;
    const int samples = cfg_.validate_uniq_samples;
    if (samples <= 0) return true;

    const std::uint64_t win = 65536;

    auto check_win = [&](std::uint64_t start) -> bool {
        if (start >= uniqN_) return true;
        const std::uint64_t end = std::min<std::uint64_t>(uniqN_, start + win);
        if (end <= start + 1) return true;
        std::uint64_t prev = uniq9_[start];
        for (std::uint64_t i = start + 1; i < end; ++i) {
            const std::uint64_t cur = uniq9_[i];
            if (cur <= prev) return false;
            prev = cur;
        }
        return true;
    };

    if (!check_win(0)) return false;
    if (uniqN_ > win) {
        if (!check_win(uniqN_ / 2)) return false;
        if (!check_win(uniqN_ > win ? (uniqN_ - win) : 0)) return false;
    }

    const int rcount = std::min<int>(samples, 200000);
    if (uniqN_ <= 1) return true;

    std::mt19937_64 rng(0x12345678ULL ^ uniqN_);
    std::uniform_int_distribution<std::uint64_t> dist(1, uniqN_ - 1);

    for (int i = 0; i < rcount; ++i) {
        const std::uint64_t k = dist(rng);
        if (uniq9_[k] <= uniq9_[k - 1]) return false;
    }
    return true;
}

#if defined(__linux__)
struct MMapGuard {
    int fd = -1;
    void* map = nullptr;
    std::size_t size = 0;

    ~MMapGuard() {
        if (map) { munmap(map, size); map = nullptr; size = 0; }
        if (fd != -1) { close(fd); fd = -1; }
    }

    void release_to(SearchEngine& se) {
        se.fd_ = fd;
        se.map_ = map;
        se.map_size_ = size;
        fd = -1; map = nullptr; size = 0;
    }
};
#endif

bool SearchEngine::load_v2_mmap(const std::string& bin_path) {
#if !defined(__linux__)
    (void)bin_path;
    return false;
#else
    if (!is_little_endian()) return false;

    if (map_) { munmap(map_, map_size_); map_ = nullptr; map_size_ = 0; }
    if (fd_ != -1) { close(fd_); fd_ = -1; }
    mmap_on_ = false;

    MMapGuard g;
    g.fd = open(bin_path.c_str(), O_RDONLY);
    if (g.fd < 0) return false;

    struct stat st{};
    if (fstat(g.fd, &st) != 0) return false;
    if (st.st_size < (off_t)sizeof(HeaderV2)) return false;

    g.size = (std::size_t)st.st_size;
    g.map = mmap(nullptr, g.size, PROT_READ, MAP_PRIVATE, g.fd, 0);
    if (g.map == MAP_FAILED) { g.map = nullptr; return false; }

    const std::uint8_t* base = (const std::uint8_t*)g.map;
    const std::uint8_t* end  = base + g.size;
    auto p = base;

    auto need = [&](std::size_t bytes) -> bool {
        return (std::size_t)(end - p) >= bytes;
    };

    if (!need(sizeof(HeaderV2))) return false;

    const auto* hdr = (const HeaderV2*)p;
    if (std::memcmp(hdr->magic, "PLAG", 4) != 0) return false;
    if (hdr->version != 2) return false;

    const std::uint32_t N = hdr->N_docs;
    const std::uint64_t U = hdr->uniq9_cnt;
    const std::uint64_t D = hdr->did9_cnt;

    if (N == 0 || U == 0) return false;

    p += sizeof(HeaderV2);

    if (!need((std::size_t)N * sizeof(DocMetaDisk))) return false;
    docs_disk_ = (const DocMetaDisk*)p;
    N_docs_ = N;
    p += (std::size_t)N * sizeof(DocMetaDisk);

    if (!need((std::size_t)U * sizeof(std::uint64_t))) return false;
    uniq9_ = (const std::uint64_t*)p;
    p += (std::size_t)U * sizeof(std::uint64_t);

    if (!need((std::size_t)(U + 1) * sizeof(std::uint64_t))) return false;
    off9_ = (const std::uint64_t*)p;
    p += (std::size_t)(U + 1) * sizeof(std::uint64_t);

    if (!need((std::size_t)D * sizeof(std::uint32_t))) return false;
    did9_ = (const std::uint32_t*)p;
    p += (std::size_t)D * sizeof(std::uint32_t);

    uniqN_ = U;
    didN_  = D;

    if (!validate_csr_basic(N, U, D)) return false;
    if (!validate_uniq_sorted_sample()) return false;
    if (!validate_postings_sorted_sample()) return false;

    g.release_to(*this);

    mmap_on_ = true;
    index_version_ = 2;
    return true;
#endif
}

bool SearchEngine::load_v1_build_csr(const std::string& bin_path) {
    std::ifstream in(bin_path, std::ios::binary);
    if (!in) return false;

    char magic[4]{0,0,0,0};
    in.read(magic, 4);
    if (!in || std::memcmp(magic, "PLAG", 4) != 0) return false;

    std::uint32_t version = 0;
    std::uint32_t N_docs = 0;
    std::uint64_t N_post9 = 0, N_post13 = 0;

    in.read((char*)&version, sizeof(version));
    in.read((char*)&N_docs,  sizeof(N_docs));
    in.read((char*)&N_post9, sizeof(N_post9));
    in.read((char*)&N_post13,sizeof(N_post13));
    if (!in || version != 1 || N_docs == 0) return false;

    docs_mem_.clear();
    docs_mem_.resize(N_docs);
    for (std::uint32_t i = 0; i < N_docs; ++i) {
        std::uint32_t tok=0; std::uint64_t hi=0, lo=0;
        in.read((char*)&tok, sizeof(tok));
        in.read((char*)&hi,  sizeof(hi));
        in.read((char*)&lo,  sizeof(lo));
        if (!in) return false;

        DocMetaMem dm{};
        dm.tok_len = tok;
        dm.bm25_len = tok;
        dm.simhash_hi = hi;
        dm.simhash_lo = lo;
        docs_mem_[i] = dm;
    }

    std::vector<std::pair<std::uint64_t, std::uint32_t>> postings;
    postings.reserve((std::size_t)N_post9);

    for (std::uint64_t i = 0; i < N_post9; ++i) {
        std::uint64_t h=0; std::uint32_t did=0;
        in.read((char*)&h, sizeof(h));
        in.read((char*)&did,sizeof(did));
        if (!in) return false;
        postings.emplace_back(h, did);
    }

    for (std::uint64_t i = 0; i < N_post13; ++i) {
        std::uint64_t h=0; std::uint32_t did=0;
        in.read((char*)&h, sizeof(h));
        in.read((char*)&did,sizeof(did));
        if (!in) return false;
    }

    std::sort(postings.begin(), postings.end(),
              [](auto& a, auto& b){ return (a.first != b.first) ? (a.first < b.first) : (a.second < b.second); });

    uniq9_mem_.clear(); off9_mem_.clear(); did9_mem_.clear();
    uniq9_mem_.reserve(postings.size()/4 + 1);
    off9_mem_.reserve(postings.size()/4 + 2);
    did9_mem_.reserve(postings.size());

    for (std::size_t i = 0; i < postings.size();) {
        std::uint64_t h = postings[i].first;
        uniq9_mem_.push_back(h);
        off9_mem_.push_back((std::uint64_t)did9_mem_.size());
        while (i < postings.size() && postings[i].first == h) {
            did9_mem_.push_back(postings[i].second);
            ++i;
        }
    }
    off9_mem_.push_back((std::uint64_t)did9_mem_.size());

    uniq9_ = uniq9_mem_.data();
    off9_  = off9_mem_.data();
    did9_  = did9_mem_.data();
    uniqN_ = (std::uint64_t)uniq9_mem_.size();
    didN_  = (std::uint64_t)did9_mem_.size();

    docs_disk_ = nullptr;
    N_docs_ = N_docs;

    mmap_on_ = false;
    index_version_ = 1;

    if (!validate_csr_basic(N_docs_, uniqN_, didN_)) return false;
    if (!validate_uniq_sorted_sample()) return false;

    return true;
}

bool SearchEngine::load(const std::string& index_dir) {
    reset_all();
    cfg_ = load_config_from_json(index_dir);
    if (!load_docids_json(index_dir)) return false;

    const std::string bin_path = index_dir + "/index_native.bin";

    if (load_v2_mmap(bin_path)) {
        if (doc_ids_.size() > (std::size_t)N_docs_) doc_ids_.resize((std::size_t)N_docs_);
        loaded_ = true;
        return true;
    }

    if (!load_v1_build_csr(bin_path)) return false;

    if (doc_ids_.size() > (std::size_t)N_docs_) doc_ids_.resize((std::size_t)N_docs_);
    loaded_ = true;
    return true;
}

inline bool SearchEngine::find_postings_hint(std::uint64_t h, std::uint64_t& L, std::uint64_t& R, std::uint64_t& hint_pos) const {
    if (!uniq9_ || !off9_ || uniqN_ == 0) return false;
    if (hint_pos > uniqN_) hint_pos = 0;

    auto it = std::lower_bound(uniq9_ + hint_pos, uniq9_ + uniqN_, h);
    hint_pos = (std::uint64_t)(it - uniq9_);
    if (it == uniq9_ + uniqN_ || *it != h) return false;

    const std::uint64_t idx = hint_pos;
    L = off9_[idx];
    R = off9_[idx + 1];
    return (L < R);
}

inline std::uint32_t SearchEngine::did_at(std::uint64_t pos) const {
    return did9_ ? did9_[pos] : 0;
}

inline std::uint32_t SearchEngine::tok_len_at(std::uint32_t did) const {
    if (did >= N_docs_) return 0;
    if (mmap_on_) return docs_disk_[did].tok_len;
    return docs_mem_[did].tok_len;
}

int SearchEngine::search_text(
    const std::string& text_utf8,
    int top_k,
    std::vector<SeHitLite>& out,
    SearchStats* stats
) const {
    out.clear();
    if (!loaded_ || top_k <= 0) return 0;

    SearchStats st{};
    st.index_version = index_version_;
    st.mmap_on = mmap_on_ ? 1 : 0;

    const bool perf = (cfg_.perf_stats != 0) && (stats != nullptr);

    auto mark = [&](std::uint64_t& dst, std::uint64_t t0) {
        if (perf) dst += (now_us() - t0);
    };

    g_tls.clear_soft();

    std::uint64_t t0 = 0;

    if (perf) t0 = now_us();
    std::string norm = normalize_for_shingles_simple(text_utf8);
    if (perf) mark(st.t_norm_us, t0);

    if (perf) t0 = now_us();
    g_tls.spans.reserve(256);
    tokenize_spans(norm, g_tls.spans);
    if (perf) mark(st.t_token_us, t0);

    if ((int)g_tls.spans.size() < K) { if (stats) *stats = st; return 0; }

    const int q_tok = (int)g_tls.spans.size();
    const int q_sh  = q_tok - K + 1;
    if (q_sh <= 0) { if (stats) *stats = st; return 0; }

    if (perf) t0 = now_us();
    g_tls.q_hashes.reserve((std::size_t)q_sh);
    for (int pos = 0; pos < q_sh; ++pos) {
        g_tls.q_hashes.push_back(hash_shingle_tokens_spans(norm, g_tls.spans, pos, K));
    }
    std::sort(g_tls.q_hashes.begin(), g_tls.q_hashes.end());
    g_tls.q_hashes.erase(std::unique(g_tls.q_hashes.begin(), g_tls.q_hashes.end()), g_tls.q_hashes.end());
    if (perf) mark(st.t_hash_us, t0);

    if (g_tls.q_hashes.empty()) { if (stats) *stats = st; return 0; }

    // TLS qterms (no per-request alloc)
    auto& qterms = g_tls.qterms;
    qterms.clear();
    qterms.reserve(g_tls.q_hashes.size());

    if (perf) t0 = now_us();

    std::uint64_t hint = 0;
    for (auto h : g_tls.q_hashes) {
        std::uint64_t L=0, R=0;
        if (!find_postings_hint(h, L, R, hint)) continue;
        const std::uint64_t df = R - L;
        if (df == 0) continue;
        if (df > (std::uint64_t)cfg_.max_df_for_seed) continue;
        qterms.push_back(QTerm{h, df, L, R});
    }

    if (qterms.empty()) { if (stats) *stats = st; return 0; }

    if ((int)qterms.size() > cfg_.max_q_uniq9) {
        std::nth_element(
            qterms.begin(),
            qterms.begin() + cfg_.max_q_uniq9,
            qterms.end(),
            [](const QTerm& a, const QTerm& b){ return a.df < b.df; }
        );
        qterms.resize(cfg_.max_q_uniq9);
    }

    std::sort(qterms.begin(), qterms.end(), [](const QTerm& a, const QTerm& b){ return a.h < b.h; });

    st.q_uniq_shingles = (int)qterms.size();
    if (st.q_uniq_shingles <= 0) { if (stats) *stats = st; return 0; }

    if (perf) mark(st.t_qterms_us, t0);

    // Seeds selection (rare-first) using TLS idx_all
    if (perf) t0 = now_us();

    g_tls.idx_all.resize(qterms.size());
    for (int i = 0; i < (int)qterms.size(); ++i) g_tls.idx_all[i] = i;

    const int max_seeds = std::min(cfg_.fetch_per_k, (int)qterms.size());
    if ((int)g_tls.idx_all.size() > max_seeds) {
        std::nth_element(
            g_tls.idx_all.begin(),
            g_tls.idx_all.begin() + max_seeds,
            g_tls.idx_all.end(),
            [&](int a, int b){ return qterms[a].df < qterms[b].df; }
        );
        g_tls.idx_all.resize(max_seeds);
    }
    std::sort(g_tls.idx_all.begin(), g_tls.idx_all.end(), [&](int a, int b){ return qterms[a].df < qterms[b].df; });

    st.seeds_total = (int)g_tls.idx_all.size();

    // V5.1 hard safety budget even if max_sum_df_seeds==0
    const std::uint64_t budget =
        (cfg_.max_sum_df_seeds > 0) ? cfg_.max_sum_df_seeds : cfg_.hard_max_sum_df_seeds;

    int seeds_used = 0;
    std::uint64_t sum_df = 0;
    for (int i = 0; i < (int)g_tls.idx_all.size(); ++i) {
        const std::uint64_t df = qterms[g_tls.idx_all[i]].df;
        if (seeds_used > 0 && sum_df + df > budget) break;
        sum_df += df;
        seeds_used++;
    }
    if (seeds_used <= 0) { if (stats) *stats = st; return 0; }

    st.seeds_used = seeds_used;

    if (perf) mark(st.t_seeds_us, t0);

    // Candidates: raw -> sort -> run-length
    if (perf) t0 = now_us();

    g_tls.raw.reserve((std::size_t)sum_df + 16);
    for (int si = 0; si < seeds_used; ++si) {
        const QTerm& qt = qterms[g_tls.idx_all[si]];
        for (std::uint64_t p = qt.L; p < qt.R; ++p) g_tls.raw.push_back(did_at(p));
    }
    if (g_tls.raw.empty()) { if (stats) *stats = st; return 0; }

    std::sort(g_tls.raw.begin(), g_tls.raw.end());

    g_tls.cand.reserve(g_tls.raw.size() / 4 + 16);
    for (std::size_t i = 0; i < g_tls.raw.size();) {
        const std::uint32_t did = g_tls.raw[i];
        int cnt = 1;
        ++i;
        while (i < g_tls.raw.size() && g_tls.raw[i] == did) { ++cnt; ++i; }
        g_tls.cand.push_back({did, cnt});
    }

    st.cand_total_before_cap = (int)g_tls.cand.size();
    if (g_tls.cand.empty()) { if (stats) *stats = st; return 0; }

    if ((int)g_tls.cand.size() > cfg_.max_cands_doc) {
        std::nth_element(
            g_tls.cand.begin(),
            g_tls.cand.begin() + cfg_.max_cands_doc,
            g_tls.cand.end(),
            [](auto& a, auto& b){ return a.second > b.second; }
        );
        g_tls.cand.resize(cfg_.max_cands_doc);
    }
    std::sort(g_tls.cand.begin(), g_tls.cand.end(), [](auto& a, auto& b){ return a.first < b.first; });

    st.cand_after_cap = (int)g_tls.cand.size();

    if (perf) mark(st.t_raw_us, t0);

    // Intersection via merge postings × candidates (TLS inter_cnt)
    if (perf) t0 = now_us();

    g_tls.inter_cnt.assign(g_tls.cand.size(), 0);
    auto& inter_cnt = g_tls.inter_cnt;

    auto merge_intersect = [&](std::uint64_t L, std::uint64_t R) {
        const std::uint32_t* post = did9_ + L;
        const std::size_t npost = (std::size_t)(R - L);

        std::size_t i = 0, j = 0;
        while (i < npost && j < g_tls.cand.size()) {
            const std::uint32_t didp = post[i];
            const std::uint32_t didc = g_tls.cand[j].first;
            if (didp < didc) { ++i; }
            else if (didp > didc) { ++j; }
            else {
                if (inter_cnt[j] != 0xFFFF) inter_cnt[j] += 1;
                ++i;
                while (i < npost && post[i] == didp) ++i; // skip dup docids if any
            }
        }
    };

    for (const auto& qt : qterms) {
        st.inter_scanned_shingles += 1;
        merge_intersect(qt.L, qt.R);
    }

    if (perf) mark(st.t_inter_us, t0);

    // Scoring (TLS scored)
    if (perf) t0 = now_us();

    auto& scored = g_tls.scored;
    scored.clear();
    scored.reserve(g_tls.cand.size());

    const double alpha = clamp01(cfg_.alpha);
    const double w9    = clamp01(cfg_.w9);
    const int q_size = (int)qterms.size();

    for (std::size_t i = 0; i < g_tls.cand.size(); ++i) {
        const std::uint32_t did = g_tls.cand[i].first;
        const int hits = g_tls.cand[i].second;
        const int inter = (int)inter_cnt[i];
        if (inter <= 0) continue;

        const std::uint32_t tok_len = tok_len_at(did);
        if ((int)tok_len < cfg_.w_min_doc) continue;

        const int t_size = ((int)tok_len >= K) ? ((int)tok_len - K + 1) : 0;
        if (t_size <= 0) continue;

        double J=0.0, C=0.0;
        jc_compute(inter, q_size, t_size, J, C);

        const double s = w9 * (alpha * J + (1.0 - alpha) * C);
        scored.push_back(CandScore{did, s, J, C, hits});
    }

    st.scored = (int)scored.size();
    if (scored.empty()) { if (stats) *stats = st; return 0; }

    if (perf) mark(st.t_score_us, t0);

    // TopK
    if (perf) t0 = now_us();

    const int take = std::min((int)scored.size(), top_k);

    std::nth_element(
        scored.begin(),
        scored.begin() + take,
        scored.end(),
        [](const CandScore& a, const CandScore& b){ return a.score > b.score; }
    );
    std::sort(
        scored.begin(),
        scored.begin() + take,
        [](const CandScore& a, const CandScore& b){ return a.score > b.score; }
    );

    out.reserve((std::size_t)take);
    for (int i = 0; i < take; ++i) {
        SeHitLite h{};
        h.doc_id_int = scored[i].did;
        h.score = scored[i].score;
        h.j9 = scored[i].J;
        h.c9 = scored[i].C;
        h.cand_hits = scored[i].hits;
        out.push_back(h);
    }

    if (perf) mark(st.t_topk_us, t0);

    if (stats) *stats = st;
    return take;
}

std::uint64_t SearchEngine::approx_bytes() const {
    std::uint64_t b = 0;

    b += (std::uint64_t)doc_ids_.capacity() * sizeof(std::string);
    for (const auto& s : doc_ids_) b += (std::uint64_t)s.capacity();

    if (mmap_on_) {
#if defined(__linux__)
        b += (std::uint64_t)map_size_;
#endif
        return b;
    }

    b += (std::uint64_t)uniq9_mem_.capacity() * sizeof(std::uint64_t);
    b += (std::uint64_t)off9_mem_.capacity()  * sizeof(std::uint64_t);
    b += (std::uint64_t)did9_mem_.capacity()  * sizeof(std::uint32_t);
    b += (std::uint64_t)docs_mem_.capacity()  * sizeof(DocMetaMem);
    return b;
}
