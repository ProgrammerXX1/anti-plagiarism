#include "search_segments.h"
#include "search_engine.h"

#include <algorithm>
#include <atomic>
#include <cctype>   // std::tolower
#include <chrono>   // steady_clock
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace {

// ==================== hard safety limits ====================
constexpr int TOPK_HARD_MAX    = 2000;
constexpr int LOCAL_K_HARD_MAX = 8000;
constexpr int ND_DIRS_HARD_MAX = 20000;
constexpr std::size_t ERR_SNIP_MAX = 512;

// ==================== time helper ====================
static std::uint64_t now_ms() {
    using clock = std::chrono::steady_clock;
    const auto t = clock::now().time_since_epoch();
    return (std::uint64_t)std::chrono::duration_cast<std::chrono::milliseconds>(t).count();
}

// ==================== env helpers ====================
static int env_int(const char* name, int defv) {
    const char* v = std::getenv(name);
    if (!v || !*v) return defv;
    char* end = nullptr;
    long x = std::strtol(v, &end, 10);
    if (end == v) return defv;
    if (x < 0) x = 0;
    if (x > 1'000'000) x = 1'000'000;
    return (int)x;
}

static bool env_bool01(const char* name, bool defv) {
    const char* v = std::getenv(name);
    if (!v || !*v) return defv;
    if (v[0] == '1') return true;
    if (v[0] == '0') return false;
    std::string s(v);
    for (auto& c : s) c = (char)std::tolower((unsigned char)c);
    if (s == "true" || s == "yes" || s == "on") return true;
    if (s == "false" || s == "no" || s == "off") return false;
    return defv;
}

static std::string safe_snip(std::string s) {
    if (s.size() > ERR_SNIP_MAX) s.resize(ERR_SNIP_MAX);
    return s;
}

static char* malloc_cstr(const std::string& s) {
    char* p = (char*)std::malloc(s.size() + 1);
    if (!p) return nullptr;
    std::memcpy(p, s.data(), s.size());
    p[s.size()] = '\0';
    return p;
}

static json make_error_json(const std::string& code, const std::string& msg) {
    json j;
    j["ok"] = false;
    j["error"] = {{"code", code}, {"message", msg}};
    j["count"] = 0;
    j["hits"] = json::array();
    return j;
}

static int clamp_topk(int k) {
    if (k <= 0) return 0;
    if (k > TOPK_HARD_MAX) return TOPK_HARD_MAX;
    return k;
}

// ==================== local_k policy ====================
static int choose_local_k(int k, int n_dirs) {
    int lk = k;
    if (n_dirs <= 8)        lk = k * 4;
    else if (n_dirs <= 64)  lk = k * 3;
    else if (n_dirs <= 512) lk = k * 2;
    else                    lk = k;

    if (lk < k) lk = k;
    if (lk > LOCAL_K_HARD_MAX) lk = LOCAL_K_HARD_MAX;
    return lk;
}

// ==================== bounded LRU cache with pinning + retry ====================
struct CacheEntry {
    std::mutex mu;
    bool loaded = false;
    bool ok = false;
    std::string err;
    std::shared_ptr<SearchEngine> se;

    // pinning
    std::atomic<int> pins{0};

    // retry/backoff for failed loads
    std::uint64_t last_attempt_ms = 0;

    std::list<std::string>::iterator lru_it;
};

static std::mutex g_cache_mu;
static std::unordered_map<std::string, std::shared_ptr<CacheEntry>> g_map;
static std::list<std::string> g_lru; // front=MRU, back=LRU

static void touch_lru_nolock(const std::string& dir, const std::shared_ptr<CacheEntry>& e) {
    g_lru.erase(e->lru_it);
    g_lru.push_front(dir);
    e->lru_it = g_lru.begin();
}

static void evict_if_needed_nolock(int max_entries) {
    if (max_entries <= 0) return;
    if ((int)g_map.size() <= max_entries) return;

    const std::size_t max_attempts = g_lru.size();
    std::size_t attempts = 0;

    while ((int)g_map.size() > max_entries &&
           !g_lru.empty() &&
           attempts < max_attempts)
    {
        ++attempts;
        const std::string victim = g_lru.back();
        auto it = g_map.find(victim);
        if (it == g_map.end()) {
            g_lru.pop_back();
            continue;
        }
        if (it->second->pins.load(std::memory_order_relaxed) == 0) {
            g_lru.pop_back();
            g_map.erase(it);
            continue;
        }
        // pinned -> rotate once; bounded attempts prevent cycles
        g_lru.pop_back();
        g_lru.push_front(victim);
        it->second->lru_it = g_lru.begin();
    }
}

static std::shared_ptr<CacheEntry> get_or_create_entry(const std::string& dir, int cache_max) {
    std::lock_guard<std::mutex> lk(g_cache_mu);
    auto it = g_map.find(dir);
    if (it != g_map.end()) return it->second;

    auto e = std::make_shared<CacheEntry>();
    g_lru.push_front(dir);
    e->lru_it = g_lru.begin();
    g_map.emplace(dir, e);

    evict_if_needed_nolock(cache_max);
    return e;
}

// touch only on successful use (avoid “spam-hot” misses)
static void touch_after_success(const std::string& dir, const std::shared_ptr<CacheEntry>& e) {
    std::lock_guard<std::mutex> lk(g_cache_mu);
    auto it = g_map.find(dir);
    if (it != g_map.end() && it->second == e) {
        touch_lru_nolock(dir, e);
    }
}

static void ensure_loaded_with_retry(const std::string& dir, CacheEntry& e, std::uint64_t retry_ms) {
    std::lock_guard<std::mutex> lk(e.mu);

    const std::uint64_t now = now_ms();

    if (e.loaded) {
        if (e.ok) return;
        // failed previously: allow retry after backoff
        if (retry_ms == 0) return;
        if (now < e.last_attempt_ms) return; // monotonic, but be defensive
        if (now - e.last_attempt_ms < retry_ms) return;
        // retry allowed
        e.loaded = false;
    }

    e.last_attempt_ms = now;
    e.loaded = true;
    e.ok = false;
    e.err.clear();
    e.se.reset();

    try {
        auto se = std::make_shared<SearchEngine>();
        if (!se->load(dir)) {
            e.ok = false;
            e.err = "load_failed";
            return;
        }
        e.se = std::move(se);
        e.ok = true;
    } catch (const std::exception& ex) {
        e.ok = false;
        e.err = ex.what();
    } catch (...) {
        e.ok = false;
        e.err = "unknown";
    }
}

struct PinGuard {
    CacheEntry* e;
    explicit PinGuard(CacheEntry* p) : e(p) {
        if (e) e->pins.fetch_add(1, std::memory_order_relaxed);
    }
    ~PinGuard() {
        if (e) e->pins.fetch_sub(1, std::memory_order_relaxed);
    }
};

// ==================== aggregation ====================
struct AggHit {
    std::string best_index_dir;

    double score = 0.0;
    double j9 = 0.0;
    double c9 = 0.0;
    int cand_hits = 0;

    int found_in = 0;      // number of indexes where doc appeared (max 1 per index)
    int last_seen_dir = -1;

    bool is_fallback = false; // key is dir:did
    std::uint32_t did = 0;    // fallback display
};

// heap item stores pointer + key (no O(k*|agg|) scan)
struct HeapItem {
    AggHit* hit;
    const std::string* key;
};

struct HeapCmp {
    bool operator()(const HeapItem& a, const HeapItem& b) const noexcept {
        return a.hit->score > b.hit->score; // min-heap by score
    }
};

} // namespace

extern "C" char* seg_search_many_json(
    const char* query_utf8,
    int top_k,
    const char** index_dirs_utf8,
    int n_dirs
) {
    try {
        if (!query_utf8 || !index_dirs_utf8 || n_dirs <= 0)
            return malloc_cstr(make_error_json("bad_request", "invalid arguments").dump());
        if (n_dirs > ND_DIRS_HARD_MAX)
            return malloc_cstr(make_error_json("bad_request", "too many dirs").dump());

        const int k = clamp_topk(top_k);
        if (k <= 0)
            return malloc_cstr(make_error_json("bad_request", "top_k<=0").dump());

        const std::string query(query_utf8);
        if (query.empty())
            return malloc_cstr(make_error_json("bad_request", "empty query").dump());

        const bool debug = env_bool01("SEG_DEBUG", false);
        const int cache_max = env_int("SEG_CACHE_MAX", 256);
        const std::uint64_t retry_ms = (std::uint64_t)env_int("SEG_LOAD_RETRY_MS", 3000);

        const int local_k = choose_local_k(k, n_dirs);

        std::unordered_map<std::string, AggHit> agg;
        agg.reserve(std::min<std::size_t>(1'000'000,
            (std::size_t)std::min(n_dirs, 1024) * (std::size_t)std::min(local_k, 128)));

        json stats_by_index = json::array();
        int dirs_ok = 0, dirs_failed = 0;

        for (int di = 0; di < n_dirs; ++di) {
            const char* d = index_dirs_utf8[di];
            if (!d || !*d) { ++dirs_failed; continue; }

            const std::string dir(d);
            auto entry = get_or_create_entry(dir, cache_max);

            ensure_loaded_with_retry(dir, *entry, retry_ms);

            std::shared_ptr<SearchEngine> se;
            {
                std::lock_guard<std::mutex> lk(entry->mu);
                if (!entry->ok || !entry->se) {
                    ++dirs_failed;
                    if (debug) {
                        stats_by_index.push_back({
                            {"index_dir", dir},
                            {"ok", false},
                            {"error", safe_snip(entry->err.empty() ? "load_failed" : entry->err)}
                        });
                    }
                    continue;
                }
                se = entry->se;
            }

            // pin before touch/search to avoid eviction window
            PinGuard pin(entry.get());
            touch_after_success(dir, entry);

            ++dirs_ok;

            std::vector<SeHitLite> local_hits;
            local_hits.reserve((std::size_t)local_k);

            SearchStats st{};
            const int got = se->search_text(query, local_k, local_hits, debug ? &st : nullptr);

            if (debug) {
                stats_by_index.push_back({
                    {"index_dir", dir},
                    {"ok", true},
                    {"got", got},
                    {"local_k", local_k},
                    {"stats", {
                        {"q_uniq_shingles", st.q_uniq_shingles},
                        {"seeds_total", st.seeds_total},
                        {"seeds_used", st.seeds_used},
                        {"cand_total_before_cap", st.cand_total_before_cap},
                        {"cand_after_cap", st.cand_after_cap},
                        {"inter_scanned_shingles", st.inter_scanned_shingles},
                        {"scored", st.scored},
                        {"index_version", st.index_version},
                        {"mmap_on", st.mmap_on}
                    }}
                });
            }

            if (got <= 0) continue;

            const auto& ids = se->doc_ids();

            for (const auto& h : local_hits) {
                const std::uint32_t did = h.doc_id_int;
                const bool has_real = (did < ids.size() && !ids[did].empty());

                if (has_real) {
                    // P1 FIX: no pre-copy; find by reference; copy only on insert
                    const std::string& real_id = ids[did];
                    auto it = agg.find(real_id);
                    if (it == agg.end()) {
                        AggHit ah;
                        ah.best_index_dir = dir;
                        ah.score = h.score;
                        ah.j9 = h.j9;
                        ah.c9 = h.c9;
                        ah.cand_hits = h.cand_hits;
                        ah.found_in = 1;
                        ah.last_seen_dir = di;
                        ah.is_fallback = false;
                        ah.did = did;
                        agg.emplace(real_id, std::move(ah)); // copies key once (into map)
                    } else {
                        AggHit& ah = it->second;
                        if (ah.last_seen_dir != di) {
                            ah.found_in++;
                            ah.last_seen_dir = di;
                        }
                        if (h.score > ah.score) {
                            ah.score = h.score;
                            ah.j9 = h.j9;
                            ah.c9 = h.c9;
                            ah.cand_hits = h.cand_hits;
                            ah.best_index_dir = dir;
                            ah.is_fallback = false;
                            ah.did = did;
                        } else if (h.cand_hits > ah.cand_hits) {
                            ah.cand_hits = h.cand_hits;
                        }
                    }
                } else {
                    // fallback: key = dir:did (avoid cross-index collisions)
                    std::string key;
                    key.reserve(dir.size() + 1 + 12);
                    key += dir;
                    key.push_back(':');
                    key += std::to_string(did);

                    auto it = agg.find(key);
                    if (it == agg.end()) {
                        AggHit ah;
                        ah.best_index_dir = dir;
                        ah.score = h.score;
                        ah.j9 = h.j9;
                        ah.c9 = h.c9;
                        ah.cand_hits = h.cand_hits;
                        ah.found_in = 1;
                        ah.last_seen_dir = di;
                        ah.is_fallback = true;
                        ah.did = did;
                        agg.emplace(std::move(key), std::move(ah));
                    } else {
                        AggHit& ah = it->second;
                        if (ah.last_seen_dir != di) {
                            ah.found_in++;
                            ah.last_seen_dir = di;
                        }
                        if (h.score > ah.score) {
                            ah.score = h.score;
                            ah.j9 = h.j9;
                            ah.c9 = h.c9;
                            ah.cand_hits = h.cand_hits;
                            ah.best_index_dir = dir;
                            ah.is_fallback = true;
                            ah.did = did;
                        } else if (h.cand_hits > ah.cand_hits) {
                            ah.cand_hits = h.cand_hits;
                        }
                    }
                }
            }
        }

        // global top-k, store key pointer to avoid any scan later
        std::vector<HeapItem> heap;
        heap.reserve((std::size_t)k);

        for (auto& kv : agg) {
            HeapItem hi{ &kv.second, &kv.first };
            if ((int)heap.size() < k) {
                heap.push_back(hi);
                std::push_heap(heap.begin(), heap.end(), HeapCmp{});
            } else if (hi.hit->score > heap.front().hit->score) {
                std::pop_heap(heap.begin(), heap.end(), HeapCmp{});
                heap.back() = hi;
                std::push_heap(heap.begin(), heap.end(), HeapCmp{});
            }
        }

        std::sort(heap.begin(), heap.end(),
                  [](const HeapItem& a, const HeapItem& b){
                      return a.hit->score > b.hit->score;
                  });

        json out;
        out["ok"] = true;
        out["top_k"] = k;
        out["local_k"] = local_k;
        out["dirs_ok"] = dirs_ok;
        out["dirs_failed"] = dirs_failed;
        out["unique_docs_considered"] = (int)agg.size();
        if (debug) out["stats_by_index"] = std::move(stats_by_index);

        json hits = json::array();
        hits.reserve(heap.size());

        for (const auto& hi : heap) {
            const AggHit& h = *hi.hit;
            const std::string& uid = *hi.key;
            hits.push_back({
                {"doc_id", h.is_fallback ? std::to_string(h.did) : uid},
                {"doc_uid", uid},
                {"best_index_dir", h.best_index_dir},
                {"score", h.score},
                {"j9", h.j9},
                {"c9", h.c9},
                {"cand_hits", h.cand_hits},
                {"found_in", h.found_in}
            });
        }

        out["count"] = (int)hits.size();
        out["hits"] = std::move(hits);

        return malloc_cstr(out.dump());
    } catch (const std::exception& ex) {
        return malloc_cstr(make_error_json("exception", safe_snip(ex.what())).dump());
    } catch (...) {
        return malloc_cstr(make_error_json("exception", "unknown").dump());
    }
}

extern "C" void seg_free(void* p) {
    if (p) std::free(p);
}
