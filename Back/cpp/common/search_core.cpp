// cpp/common/search_core.cpp
#include "search_core.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>
#include "text_common.h"

#if defined(__linux__)
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

#if defined(__BYTE_ORDER__) && (__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__)
#error "search_core: index format is little-endian only"
#endif

using json = nlohmann::json;

namespace {

constexpr int K = 9;

// hard safety limits (against memory bombs / crazy configs)
constexpr int TOPK_HARD_MAX          = 2000;
constexpr int FETCH_PER_K_HARD_MAX   = 8192;
constexpr int MAX_CANDS_DOC_HARD_MAX = 2'000'000;
constexpr int MAX_Q_UNIQ9_HARD_MAX   = 200'000;
constexpr std::uint64_t MAX_SUM_DF_HARD_MAX = 500'000'000ULL;

// hard cap on raw candidate list (OOM-safe even if budgets disabled)
constexpr std::uint64_t RAW_HARD_MAX = 50'000'000ULL; // ~200MB uint32

// intersect only rare shingles (from seeds)
constexpr int INTERSECT_SHINGLES_MAX = 256;

static inline double clamp01(double x) { return x < 0 ? 0 : (x > 1 ? 1 : x); }

static inline void jc_compute(int inter, int q, int t, double& J, double& C) {
    if (inter <= 0 || q <= 0 || t <= 0) { J = C = 0; return; }
    int uni = q + t - inter;
    J = (uni > 0) ? (double)inter / (double)uni : 0.0;
    C = (double)inter / (double)q;
}

struct DocMeta {
    std::uint32_t tok_len;
    std::uint32_t bm25_len;
    std::uint64_t simhash_hi;
    std::uint64_t simhash_lo;
};

struct Config {
    int    w_min_doc       = 8;
    int    w_min_query     = 9;
    double alpha           = 0.60;
    double w9              = 0.90;
    int    fetch_per_k     = 64;
    int    max_cands_doc   = 1000;
    int    max_df_for_seed = 200000;

    // budgets
    int    max_q_uniq9     = 4096;
    std::uint64_t max_sum_df_seeds = 2'000'000; // 0 = no limit (still clamped by RAW_HARD_MAX)
};

static Config load_config_from_json(const std::string& dir) {
    Config cfg;
    std::ifstream in(dir + "/index_config.json");
    if (!in) return cfg;

    json j;
    try { in >> j; } catch (...) { return cfg; }

    if (j.contains("w_min_doc"))        cfg.w_min_doc       = j["w_min_doc"].get<int>();
    if (j.contains("w_min_query"))      cfg.w_min_query     = j["w_min_query"].get<int>();
    if (j.contains("fetch_per_k_doc"))  cfg.fetch_per_k     = j["fetch_per_k_doc"].get<int>();
    if (j.contains("max_cands_doc"))    cfg.max_cands_doc   = j["max_cands_doc"].get<int>();
    if (j.contains("max_df_for_seed"))  cfg.max_df_for_seed = j["max_df_for_seed"].get<int>();

    // budgets (optional)
    if (j.contains("max_q_uniq9"))      cfg.max_q_uniq9 = j["max_q_uniq9"].get<int>();
    if (j.contains("max_sum_df_seeds")) cfg.max_sum_df_seeds = j["max_sum_df_seeds"].get<std::uint64_t>();

    if (j.contains("weights")) {
        auto w = j["weights"];
        if (w.contains("alpha")) cfg.alpha = w["alpha"].get<double>();
        if (w.contains("w9"))    cfg.w9    = w["w9"].get<double>();
    }

    cfg.alpha = clamp01(cfg.alpha);
    cfg.w9    = clamp01(cfg.w9);

    if (cfg.fetch_per_k < 1) cfg.fetch_per_k = 1;
    if (cfg.fetch_per_k > FETCH_PER_K_HARD_MAX) cfg.fetch_per_k = FETCH_PER_K_HARD_MAX;

    if (cfg.max_cands_doc < 1) cfg.max_cands_doc = 1;
    if (cfg.max_cands_doc > MAX_CANDS_DOC_HARD_MAX) cfg.max_cands_doc = MAX_CANDS_DOC_HARD_MAX;

    if (cfg.max_df_for_seed < 1) cfg.max_df_for_seed = 1;

    if (cfg.max_q_uniq9 < 1) cfg.max_q_uniq9 = 1;
    if (cfg.max_q_uniq9 > MAX_Q_UNIQ9_HARD_MAX) cfg.max_q_uniq9 = MAX_Q_UNIQ9_HARD_MAX;

    if (cfg.max_sum_df_seeds > MAX_SUM_DF_HARD_MAX) cfg.max_sum_df_seeds = MAX_SUM_DF_HARD_MAX;

    return cfg;
}

#pragma pack(push, 1)
struct HeaderV2 {
    char     magic[4];      // "PLAG"
    std::uint32_t version;  // 2
    std::uint32_t N_docs;
    std::uint64_t uniq9_cnt;
    std::uint64_t did9_cnt;
    std::uint64_t reserved0;
    std::uint64_t reserved1;
    // далее:
    // [N_docs * (u32 tok_len, u64 hi, u64 lo)]
    // [uniq9_cnt * u64 uniq_hash]
    // [uniq9_cnt+1 * u64 off]
    // [did9_cnt * u32 did]
};
#pragma pack(pop)

struct Index {
    Config cfg;

    std::vector<DocMeta> docs;
    std::vector<std::string> doc_ids;

    // CSR
    bool mmap_on = false;

#if defined(__linux__)
    int fd = -1;
    void* map = nullptr;
    std::size_t map_size = 0;
#endif

    const std::uint64_t* uniq9 = nullptr;
    const std::uint64_t* off9  = nullptr;
    const std::uint32_t* did9  = nullptr;
    std::uint64_t uniqN = 0;
    std::uint64_t didN  = 0;

    // v1 fallback storage (RAM)
    std::vector<std::uint64_t> uniq9_mem;
    std::vector<std::uint64_t> off9_mem;
    std::vector<std::uint32_t> did9_mem;

    ~Index() {
#if defined(__linux__)
        if (map && map != MAP_FAILED) {
            munmap(map, map_size);
            map = nullptr;
            map_size = 0;
        }
        if (fd != -1) {
            close(fd);
            fd = -1;
        }
#endif
    }

    inline bool get_postings(std::uint64_t h, std::uint64_t& L, std::uint64_t& R) const {
        const std::uint64_t* u = mmap_on ? uniq9 : (uniq9_mem.empty() ? nullptr : uniq9_mem.data());
        const std::uint64_t* o = mmap_on ? off9  : (off9_mem.empty()  ? nullptr : off9_mem.data());
        std::uint64_t n = mmap_on ? uniqN : (std::uint64_t)uniq9_mem.size();
        if (!u || !o || n == 0) return false;

        auto it = std::lower_bound(u, u + n, h);
        if (it == u + n || *it != h) return false;

        std::uint64_t idx = (std::uint64_t)(it - u);
        L = o[idx];
        R = o[idx + 1];
        return (L < R);
    }

    inline std::uint32_t did_at(std::uint64_t pos) const {
        return mmap_on ? did9[pos] : did9_mem[(std::size_t)pos];
    }
};

static std::atomic<std::shared_ptr<const Index>> g_index{nullptr};

static bool validate_v2_csr(const Index& idx, std::string* err) {
    if (!idx.uniq9 || !idx.off9 || !idx.did9) { if (err) *err = "null CSR pointers"; return false; }
    if (idx.uniqN == 0) { if (err) *err = "uniqN==0"; return false; }

    if (idx.off9[0] != 0) { if (err) *err = "off[0]!=0"; return false; }
    if (idx.off9[idx.uniqN] != idx.didN) { if (err) *err = "off[uniqN]!=didN"; return false; }

    for (std::uint64_t i = 0; i < idx.uniqN; i++) {
        if (idx.off9[i] > idx.off9[i + 1]) { if (err) *err = "off not monotonic"; return false; }
        if (i + 1 < idx.uniqN && idx.uniq9[i] > idx.uniq9[i + 1]) { if (err) *err = "uniq not sorted"; return false; }
    }

    // Full scan is safest; if it becomes too slow at load, change to sampling behind a flag.
    const std::uint32_t N = (std::uint32_t)idx.docs.size();
    for (std::uint64_t i = 0; i < idx.didN; i++) {
        if (idx.did9[i] >= N) { if (err) *err = "did out of range"; return false; }
    }

    return true;
}

static bool load_v2_mmap(Index& idx, const std::string& bin_path, std::string* err) {
#if !defined(__linux__)
    (void)idx; (void)bin_path; if (err) *err = "mmap not supported on this platform";
    return false;
#else
    idx.mmap_on = false;
    idx.uniq9 = idx.off9 = nullptr;
    idx.did9 = nullptr;
    idx.uniqN = idx.didN = 0;

    if (idx.map && idx.map != MAP_FAILED) {
        munmap(idx.map, idx.map_size);
        idx.map = nullptr;
        idx.map_size = 0;
    }
    if (idx.fd != -1) {
        close(idx.fd);
        idx.fd = -1;
    }

    idx.fd = open(bin_path.c_str(), O_RDONLY);
    if (idx.fd < 0) { if (err) *err = "open failed"; return false; }

    struct stat st{};
    if (fstat(idx.fd, &st) != 0) { if (err) *err = "fstat failed"; return false; }
    if (st.st_size < (off_t)sizeof(HeaderV2)) { if (err) *err = "file too small"; return false; }

    idx.map_size = (std::size_t)st.st_size;
    idx.map = mmap(nullptr, idx.map_size, PROT_READ, MAP_PRIVATE, idx.fd, 0);
    if (idx.map == MAP_FAILED) { idx.map = nullptr; if (err) *err = "mmap failed"; return false; }

#if defined(__linux__)
    // access hint: postings access is effectively random due to lower_bound + segments
    madvise(idx.map, idx.map_size, MADV_RANDOM);
#endif

    const auto* base = (const std::uint8_t*)idx.map;
    const auto* hdr = (const HeaderV2*)base;

    if (std::memcmp(hdr->magic, "PLAG", 4) != 0 || hdr->version != 2) { if (err) *err = "bad header"; return false; }

    const std::uint32_t N_docs = hdr->N_docs;
    const std::uint64_t U = hdr->uniq9_cnt;
    const std::uint64_t D = hdr->did9_cnt;

    const std::uint8_t* p = base + sizeof(HeaderV2);

    auto need = [&](std::size_t bytes) -> bool {
        std::size_t off = (std::size_t)(p - base);
        return off + bytes <= idx.map_size;
    };
    auto take_ptr = [&](std::size_t bytes) -> const std::uint8_t* {
        if (!need(bytes)) return nullptr;
        const std::uint8_t* out = p;
        p += bytes;
        return out;
    };

    // docs meta (copy to RAM)
    idx.docs.resize(N_docs);
    for (std::uint32_t i = 0; i < N_docs; i++) {
        const std::uint8_t* ptok = take_ptr(sizeof(std::uint32_t));
        const std::uint8_t* phi  = take_ptr(sizeof(std::uint64_t));
        const std::uint8_t* plo  = take_ptr(sizeof(std::uint64_t));
        if (!ptok || !phi || !plo) { if (err) *err = "truncated docs meta"; return false; }

        std::uint32_t tok = 0; std::uint64_t hi = 0, lo = 0;
        std::memcpy(&tok, ptok, sizeof(tok));
        std::memcpy(&hi,  phi,  sizeof(hi));
        std::memcpy(&lo,  plo,  sizeof(lo));

        DocMeta dm{};
        dm.tok_len = tok;
        dm.bm25_len = tok;
        dm.simhash_hi = hi;
        dm.simhash_lo = lo;
        idx.docs[i] = dm;
    }

    const std::uint8_t* uniq_ptr = take_ptr((std::size_t)U * sizeof(std::uint64_t));
    if (!uniq_ptr) { if (err) *err = "truncated uniq9"; return false; }
    idx.uniq9 = (const std::uint64_t*)uniq_ptr;

    const std::uint8_t* off_ptr = take_ptr((std::size_t)(U + 1) * sizeof(std::uint64_t));
    if (!off_ptr) { if (err) *err = "truncated off9"; return false; }
    idx.off9 = (const std::uint64_t*)off_ptr;

    const std::uint8_t* did_ptr = take_ptr((std::size_t)D * sizeof(std::uint32_t));
    if (!did_ptr) { if (err) *err = "truncated did9"; return false; }
    idx.did9 = (const std::uint32_t*)did_ptr;

    idx.uniqN = U;
    idx.didN  = D;
    idx.mmap_on = true;

    std::string verr;
    if (!validate_v2_csr(idx, &verr)) {
        if (err) *err = "CSR validation failed: " + verr;
        return false;
    }

    return true;
#endif
}

static bool load_v1_build_csr(Index& idx, std::ifstream& bin, std::string* err) {
    // WARNING: for big indexes this is not viable. Kept for small / legacy only.
    std::uint32_t N_docs = 0;
    std::uint64_t N_post9 = 0, N_post13 = 0;

    bin.seekg(4, std::ios::beg);
    std::uint32_t version = 0;
    bin.read((char*)&version, sizeof(version));
    bin.read((char*)&N_docs, sizeof(N_docs));
    bin.read((char*)&N_post9, sizeof(N_post9));
    bin.read((char*)&N_post13, sizeof(N_post13));
    if (!bin || version != 1) { if (err) *err = "bad v1 header"; return false; }

    // v1 guardrail
    const std::uint64_t V1_POSTINGS_MAX = 50'000'000ULL; // adjust as you like
    if (N_post9 > V1_POSTINGS_MAX) { if (err) *err = "v1 too large; require v2"; return false; }

    idx.docs.resize(N_docs);
    for (std::uint32_t i = 0; i < N_docs; i++) {
        std::uint32_t tok = 0; std::uint64_t hi = 0, lo = 0;
        bin.read((char*)&tok, sizeof(tok));
        bin.read((char*)&hi,  sizeof(hi));
        bin.read((char*)&lo,  sizeof(lo));
        if (!bin) { if (err) *err = "truncated docs meta v1"; return false; }

        DocMeta dm{};
        dm.tok_len = tok;
        dm.bm25_len = tok;
        dm.simhash_hi = hi;
        dm.simhash_lo = lo;
        idx.docs[i] = dm;
    }

    std::vector<std::pair<std::uint64_t, std::uint32_t>> tmp;
    tmp.reserve((std::size_t)N_post9);

    for (std::uint64_t i = 0; i < N_post9; i++) {
        std::uint64_t h = 0;
        std::uint32_t did = 0;
        bin.read((char*)&h, sizeof(h));
        bin.read((char*)&did, sizeof(did));
        if (!bin) { if (err) *err = "truncated post9 v1"; return false; }
        tmp.emplace_back(h, did);
    }

    // skip post13
    for (std::uint64_t i = 0; i < N_post13; i++) {
        std::uint64_t h = 0;
        std::uint32_t did = 0;
        bin.read((char*)&h, sizeof(h));
        bin.read((char*)&did, sizeof(did));
        if (!bin) { if (err) *err = "truncated post13 v1"; return false; }
    }

    std::sort(tmp.begin(), tmp.end());

    idx.uniq9_mem.clear(); idx.off9_mem.clear(); idx.did9_mem.clear();
    idx.uniq9_mem.reserve(tmp.size() / 4 + 1);
    idx.off9_mem.reserve(tmp.size() / 4 + 2);
    idx.did9_mem.reserve(tmp.size());

    for (std::size_t i = 0; i < tmp.size();) {
        std::uint64_t h = tmp[i].first;
        idx.uniq9_mem.push_back(h);
        idx.off9_mem.push_back((std::uint64_t)idx.did9_mem.size());
        while (i < tmp.size() && tmp[i].first == h) {
            idx.did9_mem.push_back(tmp[i].second);
            ++i;
        }
    }
    idx.off9_mem.push_back((std::uint64_t)idx.did9_mem.size());

    idx.mmap_on = false;
    idx.uniq9 = idx.uniq9_mem.data();
    idx.off9  = idx.off9_mem.data();
    idx.did9  = idx.did9_mem.data();
    idx.uniqN = (std::uint64_t)idx.uniq9_mem.size();
    idx.didN  = (std::uint64_t)idx.did9_mem.size();

    return true;
}

// TLS buffers for hot-path (no unordered_map)
struct CandHit {
    std::uint32_t did;
    std::uint16_t hits; // <= fetch_per_k (seed_hits)
};

struct TLS {
    std::vector<std::uint32_t> raw;
    std::vector<CandHit> cand;        // sorted by did for intersection
    std::vector<std::uint16_t> inter; // same size as cand
    std::vector<std::uint64_t> q_sh;  // reused buffer
};

static thread_local TLS g_tls;

} // namespace

// ============================================================================
// API: LOAD
// ============================================================================

extern "C" int se_load_index(const char* dir_utf8) {
    std::string dir = dir_utf8 ? dir_utf8 : ".";

    auto idx = std::make_shared<Index>();
    idx->cfg = load_config_from_json(dir);

    // docids
    {
        std::ifstream dj(dir + "/index_native_docids.json");
        if (!dj) return -1;
        json j; try { dj >> j; } catch (...) { return -1; }
        if (!j.is_array()) return -1;

        idx->doc_ids.reserve(j.size());
        for (auto& x : j) idx->doc_ids.push_back(x.get<std::string>());
    }

    const std::string bin_path = dir + "/index_native.bin";

    // try v2
    {
        std::string err;
        if (load_v2_mmap(*idx, bin_path, &err)) {
            if (idx->doc_ids.size() != idx->docs.size()) {
                if (idx->doc_ids.size() > idx->docs.size()) idx->doc_ids.resize(idx->docs.size());
            }
            g_index.store(idx, std::memory_order_release);
            std::cerr << "[se_load_index] v2 mmap loaded docs=" << idx->docs.size()
                      << " uniq9=" << idx->uniqN << " did9=" << idx->didN << "\n";
            return 0;
        }
    }

    // v1 fallback
    std::ifstream bin(bin_path, std::ios::binary);
    if (!bin) return -1;

    char magic[4]{};
    bin.read(magic, 4);
    if (!bin || std::memcmp(magic, "PLAG", 4) != 0) return -1;

    std::uint32_t version = 0;
    bin.read((char*)&version, sizeof(version));
    if (!bin || version != 1) return -1;

    bin.seekg(0, std::ios::beg);

    std::string err;
    if (!load_v1_build_csr(*idx, bin, &err)) {
        std::cerr << "[se_load_index] v1 load failed: " << err << "\n";
        return -1;
    }

    if (idx->doc_ids.size() != idx->docs.size()) {
        if (idx->doc_ids.size() > idx->docs.size()) idx->doc_ids.resize(idx->docs.size());
    }

    g_index.store(idx, std::memory_order_release);
    std::cerr << "[se_load_index] v1 loaded docs=" << idx->docs.size()
              << " uniq9=" << idx->uniqN << " did9=" << idx->didN << "\n";
    return 0;
}

// ============================================================================
// API: SEARCH
// ============================================================================

extern "C" SeSearchResult se_search_text(
    const char* text_utf8,
    int top_k,
    SeHit* out,
    int max_hits
) {
    SeSearchResult res{0};

    if (!text_utf8 || !out || top_k <= 0 || max_hits <= 0) return res;

    auto idx = g_index.load(std::memory_order_acquire);
    if (!idx) return res;

    int want = std::min(top_k, max_hits);
    want = std::min(want, TOPK_HARD_MAX);
    if (want <= 0) return res;

    std::string norm = normalize_for_shingles_simple(std::string(text_utf8));
    auto toks = simple_tokens(norm);
    if ((int)toks.size() < K) return res;

    auto q_sh_tmp = build_shingles(toks, K);
    if (q_sh_tmp.empty()) return res;

    TLS& tls = g_tls;

    tls.q_sh.clear();
    tls.q_sh.reserve(q_sh_tmp.size());
    for (auto h : q_sh_tmp) tls.q_sh.push_back(h);

    std::sort(tls.q_sh.begin(), tls.q_sh.end());
    tls.q_sh.erase(std::unique(tls.q_sh.begin(), tls.q_sh.end()), tls.q_sh.end());

    // budget: max_q_uniq9
    if ((int)tls.q_sh.size() > idx->cfg.max_q_uniq9) {
        tls.q_sh.resize((std::size_t)idx->cfg.max_q_uniq9);
    }

    const int q_size = (int)tls.q_sh.size();
    if (q_size <= 0) return res;

    // seeds: df-min with cutoff
    struct Seed { std::uint64_t df; std::uint64_t h; };
    std::vector<Seed> seeds;
    seeds.reserve(tls.q_sh.size());

    for (std::uint64_t h : tls.q_sh) {
        std::uint64_t L = 0, R = 0;
        if (!idx->get_postings(h, L, R)) continue;
        std::uint64_t df = R - L;
        if (df == 0) continue;
        if (df > (std::uint64_t)idx->cfg.max_df_for_seed) continue;
        seeds.push_back({df, h});
    }
    if (seeds.empty()) return res;

    std::sort(seeds.begin(), seeds.end(),
              [](const Seed& a, const Seed& b) { return a.df < b.df; });

    int take = std::min((int)seeds.size(), idx->cfg.fetch_per_k);
    take = std::min(take, FETCH_PER_K_HARD_MAX);

    // sum_df budget (0 = unlimited, but raw is still clamped by RAW_HARD_MAX)
    std::uint64_t sum_df = 0;
    int take2 = 0;
    const std::uint64_t sum_budget = idx->cfg.max_sum_df_seeds;
    for (; take2 < take; take2++) {
        std::uint64_t df = seeds[take2].df;
        if (sum_budget != 0 && sum_df + df > sum_budget) break;
        sum_df += df;
    }
    take = std::max(1, take2);

    // choose shingles for intersection: rarest first (from seeds)
    std::vector<std::uint64_t> inter_sh;
    inter_sh.reserve(std::min((int)seeds.size(), INTERSECT_SHINGLES_MAX));
    for (int i = 0; i < (int)seeds.size() && i < INTERSECT_SHINGLES_MAX; i++) {
        inter_sh.push_back(seeds[i].h);
    }

    // raw candidates from seed postings (OOM-safe)
    const std::uint64_t raw_cap = std::min((sum_df > 0 ? sum_df : RAW_HARD_MAX), RAW_HARD_MAX);

    tls.raw.clear();
    tls.raw.reserve((std::size_t)raw_cap);

    for (int i = 0; i < take; i++) {
        std::uint64_t L = 0, R = 0;
        if (!idx->get_postings(seeds[i].h, L, R)) continue;
        if (idx->mmap_on && R > idx->didN) R = idx->didN;

        for (std::uint64_t p = L; p < R; p++) {
            if (tls.raw.size() >= raw_cap) break;
            tls.raw.push_back(idx->did_at(p));
        }
        if (tls.raw.size() >= raw_cap) break;
    }
    if (tls.raw.empty()) return res;

    std::sort(tls.raw.begin(), tls.raw.end());

    // RLE -> cand (did, seed_hits)
    tls.cand.clear();
    tls.cand.reserve(std::min<std::size_t>(tls.raw.size(), 1'000'000));

    for (std::size_t i = 0; i < tls.raw.size();) {
        std::uint32_t did = tls.raw[i];
        std::uint32_t cnt = 1;
        i++;
        while (i < tls.raw.size() && tls.raw[i] == did) { cnt++; i++; }
        std::uint16_t hcnt = (cnt > 65535u) ? 65535u : (std::uint16_t)cnt;
        tls.cand.push_back(CandHit{did, hcnt});
    }
    if (tls.cand.empty()) return res;

    // cap candidates by seed_hits (UB-safe nth_element), then sort by did for intersection
    const int keep = idx->cfg.max_cands_doc;
    if ((int)tls.cand.size() > keep) {
        std::nth_element(
            tls.cand.begin(),
            tls.cand.begin() + (keep - 1),
            tls.cand.end(),
            [](const CandHit& a, const CandHit& b) {
                return a.hits > b.hits;
            }
        );
        tls.cand.resize((std::size_t)keep);
    }
    std::sort(tls.cand.begin(), tls.cand.end(),
              [](const CandHit& a, const CandHit& b) { return a.did < b.did; });

    // intersections: two-pointer postings vs cand (both sorted by did)
    if (tls.inter.size() < tls.cand.size())
        tls.inter.resize(tls.cand.size());
    std::fill(tls.inter.begin(), tls.inter.begin() + tls.cand.size(), 0);

    for (std::uint64_t h : inter_sh) {
        std::uint64_t L = 0, R = 0;
        if (!idx->get_postings(h, L, R)) continue;
        if (idx->mmap_on && R > idx->didN) R = idx->didN;

        std::size_t i = 0;
        std::uint64_t p = L;

        while (p < R && i < tls.cand.size()) {
            std::uint32_t did_p = idx->did_at(p);
            std::uint32_t did_c = tls.cand[i].did;
            if (did_p < did_c) {
                p++;
            } else if (did_p > did_c) {
                i++;
            } else {
                if (tls.inter[i] < 65535u) tls.inter[i] += 1;
                p++;
                i++;
            }
        }
    }

    struct Scored {
        std::uint32_t did;
        double score;
        double J;
        double C;
        int seed_hits;
        int inter_hits; // intersection count over inter_sh
    };

    std::vector<Scored> scored;
    scored.reserve(tls.cand.size());

    const double alpha = clamp01(idx->cfg.alpha);
    const double w9    = clamp01(idx->cfg.w9);

    const int q_used = (int)inter_sh.size(); // note: J/C computed vs q_used, not full q_size

    for (std::size_t i = 0; i < tls.cand.size(); i++) {
        std::uint32_t did = tls.cand[i].did;
        int seed_hits = (int)tls.cand[i].hits;
        int i9 = (int)tls.inter[i];
        if (i9 <= 0) continue;

        if (did >= idx->docs.size()) continue;
        if ((int)idx->docs[did].tok_len < idx->cfg.w_min_doc) continue;

        int t_size = (int)idx->docs[did].tok_len - K + 1;
        if (t_size <= 0) continue;

        double J = 0, C = 0;
        jc_compute(i9, q_used, t_size, J, C);

        double s = w9 * (alpha * J + (1.0 - alpha) * C);
        scored.push_back(Scored{did, s, J, C, seed_hits, i9});
    }

    if (scored.empty()) return res;

    // TopK without full sort
    if ((int)scored.size() > want) {
        std::nth_element(
            scored.begin(),
            scored.begin() + want,
            scored.end(),
            [](const Scored& a, const Scored& b) { return a.score > b.score; }
        );
        scored.resize((std::size_t)want);
    }
    std::sort(scored.begin(), scored.end(),
              [](const Scored& a, const Scored& b) { return a.score > b.score; });

    const int out_n = std::min(want, (int)scored.size());
    for (int i = 0; i < out_n; i++) {
        out[i].doc_id_int = (int)scored[i].did;
        out[i].score      = scored[i].score;
        out[i].j9         = scored[i].J;
        out[i].c9         = scored[i].C;
        out[i].j13        = 0.0;
        out[i].c13        = 0.0;
        out[i].cand_hits  = scored[i].seed_hits; // seed_hits (not real intersections)
    }

    res.count = out_n;
    return res;
}
