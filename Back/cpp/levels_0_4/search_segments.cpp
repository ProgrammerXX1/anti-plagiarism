// cpp/common/search_segments.cpp
// FIXES (thread-safe cache):
//  - bounded LRU cache
//  - per-dir loading coordination: one loader, others wait
//  - publish engine to cache ONLY after successful load
//
// Works with thread_local SearchEngine::search_text.

#include "search_segments.h"
#include "search_engine.h"

#include <algorithm>
#include <condition_variable>
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

// ------------------------
// LRU cache entries
// ------------------------

struct CacheEntry {
    std::shared_ptr<const SearchEngine> eng;
    std::list<std::string>::iterator lru_it;
};

// global cache
static std::mutex g_mx;
static std::unordered_map<std::string, CacheEntry> g_cache;
static std::list<std::string> g_lru; // front=MRU, back=LRU

// loading coordination per dir
struct LoadingState {
    bool loading = false;
    bool done = false;
    bool ok = false;
    std::shared_ptr<const SearchEngine> eng;
    std::condition_variable cv;
};

static std::unordered_map<std::string, std::shared_ptr<LoadingState>> g_loading;

static std::size_t cache_capacity() {
    const char* s = std::getenv("PLAGIO_INDEX_CACHE_MAX");
    if (!s || !*s) return 64;
    long v = std::strtol(s, nullptr, 10);
    if (v <= 0) return 64;
    return (std::size_t)v;
}

static void touch_lru_locked(const std::string& key, CacheEntry& e) {
    g_lru.erase(e.lru_it);
    g_lru.push_front(key);
    e.lru_it = g_lru.begin();
}

static void evict_if_needed_locked() {
    const std::size_t cap = cache_capacity();
    while (g_cache.size() > cap && !g_lru.empty()) {
        const std::string victim = g_lru.back();
        g_lru.pop_back();
        g_cache.erase(victim);
    }
}

// ------------------------
// index loader with wait
// ------------------------

static std::shared_ptr<const SearchEngine> get_or_load(const std::string& dir) {
    // 1) cache fast path
    {
        std::lock_guard<std::mutex> lk(g_mx);
        auto it = g_cache.find(dir);
        if (it != g_cache.end()) {
            touch_lru_locked(dir, it->second);
            return it->second.eng;
        }
    }

    // 2) loading coordination:
    // if someone is loading, wait; else become the loader
    std::shared_ptr<LoadingState> st;
    bool i_am_loader = false;

    {
        std::unique_lock<std::mutex> lk(g_mx);
        auto itL = g_loading.find(dir);
        if (itL == g_loading.end()) {
            st = std::make_shared<LoadingState>();
            st->loading = true;
            st->done = false;
            st->ok = false;
            g_loading.emplace(dir, st);
            i_am_loader = true;
        } else {
            st = itL->second;
            // if already done, just return result (should be rare)
            if (st->done) return st->ok ? st->eng : std::shared_ptr<const SearchEngine>{};
        }
        if (!i_am_loader) {
            st->cv.wait(lk, [&]() { return st->done; });
            return st->ok ? st->eng : std::shared_ptr<const SearchEngine>{};
        }
    }

    // 3) actual load outside lock
    std::shared_ptr<const SearchEngine> eng;
    {
        auto e = std::make_shared<SearchEngine>();
        if (e->load(dir)) {
            eng = e;
        } else {
            eng.reset();
        }
    }

    // 4) publish result + fill cache + notify waiters
    {
        std::unique_lock<std::mutex> lk(g_mx);

        // set loading state result
        st->eng = eng;
        st->ok = (bool)eng;
        st->loading = false;
        st->done = true;

        // if loaded ok, insert into LRU cache
        if (eng) {
            // another thread might have inserted while we loaded (rare but possible)
            auto it = g_cache.find(dir);
            if (it == g_cache.end()) {
                g_lru.push_front(dir);
                CacheEntry ce;
                ce.eng = eng;
                ce.lru_it = g_lru.begin();
                g_cache.emplace(dir, std::move(ce));
                evict_if_needed_locked();
            } else {
                touch_lru_locked(dir, it->second);
                // prefer existing entry to avoid churn
                eng = it->second.eng;
                st->eng = eng;
            }
        }

        // remove loading state entry (optional)
        g_loading.erase(dir);

        st->cv.notify_all();
        return eng;
    }
}

// ------------------------
// json malloc helpers
// ------------------------

static char* malloc_json(const json& j) {
    std::string s = j.dump();
    char* out = (char*)std::malloc(s.size() + 1);
    if (!out) return nullptr;
    std::memcpy(out, s.c_str(), s.size() + 1);
    return out;
}

static char* malloc_cstr(const std::string& s) {
    char* out = (char*)std::malloc(s.size() + 1);
    if (!out) return nullptr;
    std::memcpy(out, s.c_str(), s.size() + 1);
    return out;
}

static char* mk_empty_hits() {
    return malloc_cstr("{\"hits\":[],\"count\":0}");
}

static char* mk_error(const char* msg) {
    json j;
    j["count"] = 0;
    j["hits"] = json::array();
    j["error"] = (msg ? msg : "error");
    return malloc_json(j);
}

struct OutHit {
    std::string doc_id;
    double score;
    double j9;
    double c9;
    int cand_hits;
    std::string index_dir;

    std::vector<MatchPair> matches;
    std::vector<MatchSpan> spans;
};

} // namespace

extern "C" char* seg_search_many_json_v3(
    const char* query_utf8,
    int top_k,
    const char** index_dirs_utf8,
    int n_dirs,
    int normalize_query,
    int include_matches
) {
    try {
        if (!query_utf8 || !index_dirs_utf8 || n_dirs <= 0 || top_k <= 0) {
            return mk_empty_hits();
        }

        const bool do_norm = (normalize_query != 0);
        const bool want_matches = (include_matches != 0);
        std::string q(query_utf8);

        std::vector<OutHit> all;
        all.reserve((std::size_t)top_k * (std::size_t)n_dirs);

        for (int i = 0; i < n_dirs; ++i) {
            const char* cdir = index_dirs_utf8[i];
            if (!cdir || !cdir[0]) continue;
            std::string dir(cdir);

            auto eng = get_or_load(dir);
            if (!eng) continue;

            std::vector<SeHitLite> tmp;
            tmp.reserve((std::size_t)top_k);

            int got = eng->search_text(q, top_k, tmp, do_norm);
            if (got <= 0) continue;

            std::vector<std::vector<MatchSpan>> tmp_spans;
            eng->collect_spans_for_hits(q, tmp, tmp_spans, do_norm);

            std::vector<std::vector<MatchPair>> tmp_matches;
            if (want_matches) {
                eng->collect_matches_for_hits(q, tmp, tmp_matches, do_norm);
            }

            const auto& docids = eng->doc_ids();
            for (int k = 0; k < got; ++k) {
                std::uint32_t did = tmp[k].doc_id_int;
                if (did >= docids.size()) continue;

                OutHit oh;
                oh.doc_id = docids[did];
                oh.score = tmp[k].score;
                oh.j9 = tmp[k].j9;
                oh.c9 = tmp[k].c9;
                oh.cand_hits = tmp[k].cand_hits;
                oh.index_dir = dir;

                if ((std::size_t)k < tmp_spans.size()) {
                    oh.spans = std::move(tmp_spans[(std::size_t)k]);
                }
                if (want_matches && (std::size_t)k < tmp_matches.size()) {
                    oh.matches = std::move(tmp_matches[(std::size_t)k]);
                }

                all.push_back(std::move(oh));
            }
        }

        if (all.empty()) return mk_empty_hits();

        std::sort(all.begin(), all.end(), [](const OutHit& a, const OutHit& b) {
            return a.score > b.score;
        });
        if ((int)all.size() > top_k) all.resize((std::size_t)top_k);

        json j;
        j["count"] = (int)all.size();
        j["hits"] = json::array();

        for (auto& h : all) {
            json x;
            x["doc_id"] = h.doc_id;
            x["score"] = h.score;
            x["j9"] = h.j9;
            x["c9"] = h.c9;
            x["cand_hits"] = h.cand_hits;
            x["index_dir"] = h.index_dir;

            json sp = json::array();
            for (const auto& s : h.spans) {
                json z;
                z["q_from"] = s.q_from;
                z["q_to"] = s.q_to;
                z["d_from"] = s.d_from;
                z["d_to"] = s.d_to;
                z["length"] = s.length;
                sp.push_back(std::move(z));
            }
            x["spans"] = std::move(sp);

            if (want_matches) {
                json m;
                m["q_pos"] = json::array();
                m["d_pos"] = json::array();
                m["h"]     = json::array();
                for (const auto& p : h.matches) {
                    m["q_pos"].push_back(p.qpos);
                    m["d_pos"].push_back(p.dpos);
                    m["h"].push_back(p.h);
                }
                x["matches"] = std::move(m);
            }

            j["hits"].push_back(std::move(x));
        }

        return malloc_json(j);
    } catch (...) {
        return mk_error("seg_search_many_json_v3_exception");
    }
}

extern "C" char* seg_search_many_json_v2(
    const char* query_utf8,
    int top_k,
    const char** index_dirs_utf8,
    int n_dirs,
    int normalize_query
) {
    return seg_search_many_json_v3(query_utf8, top_k, index_dirs_utf8, n_dirs, normalize_query, 1);
}

extern "C" char* seg_search_many_json(
    const char* query_utf8,
    int top_k,
    const char** index_dirs_utf8,
    int n_dirs
) {
    return seg_search_many_json_v2(query_utf8, top_k, index_dirs_utf8, n_dirs, 1);
}

// excerpt + normalize endpoints + seg_free остаются как у тебя в текущем search_segments.cpp
// (их можно оставить без изменений, они не держат shared state)

extern "C" void seg_free(void* p) {
    std::free(p);
}
