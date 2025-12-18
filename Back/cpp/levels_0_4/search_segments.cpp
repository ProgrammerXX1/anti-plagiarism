// cpp/levels_0_4/search_segments.cpp
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

#include "text_common.h"

using json = nlohmann::json;

namespace {

// ------------------------
// LRU cache + per-dir load wait
// ------------------------

struct CacheEntry {
    std::shared_ptr<const SearchEngine> eng;
    std::list<std::string>::iterator lru_it;
};

static std::mutex g_mx;
static std::unordered_map<std::string, CacheEntry> g_cache;
static std::list<std::string> g_lru;

struct LoadingState {
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

static std::shared_ptr<const SearchEngine> get_or_load(const std::string& dir) {
    // cache hit
    {
        std::lock_guard<std::mutex> lk(g_mx);
        auto it = g_cache.find(dir);
        if (it != g_cache.end()) {
            touch_lru_locked(dir, it->second);
            return it->second.eng;
        }
    }

    // per-dir loading coordination
    std::shared_ptr<LoadingState> st;
    bool i_am_loader = false;

    {
        std::unique_lock<std::mutex> lk(g_mx);
        auto itL = g_loading.find(dir);
        if (itL == g_loading.end()) {
            st = std::make_shared<LoadingState>();
            g_loading.emplace(dir, st);
            i_am_loader = true;
        } else {
            st = itL->second;
        }

        if (!i_am_loader) {
            st->cv.wait(lk, [&]() { return st->done; });
            return st->ok ? st->eng : std::shared_ptr<const SearchEngine>{};
        }
    }

    // load outside lock
    std::shared_ptr<const SearchEngine> eng;
    {
        auto e = std::make_shared<SearchEngine>();
        if (e->load(dir)) eng = e;
        else eng.reset();
    }

    // publish
    {
        std::unique_lock<std::mutex> lk(g_mx);

        st->eng = eng;
        st->ok = (bool)eng;
        st->done = true;

        if (eng) {
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
                eng = it->second.eng;
                st->eng = eng;
            }
        }

        g_loading.erase(dir);
        st->cv.notify_all();
        return eng;
    }
}

// ------------------------
// JSON malloc helpers
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

// ------------------------
// response struct
// ------------------------

struct OutHit {
    std::string doc_id;
    double score = 0.0;
    double j9 = 0.0;
    double c9 = 0.0;
    int cand_hits = 0;
    std::string index_dir;

    std::vector<MatchPair> matches;
    std::vector<MatchSpan> spans;
};

// ------------------------
// diversified merge across dirs
// ------------------------

static bool env_bool(const char* key, bool defv) {
    const char* s = std::getenv(key);
    if (!s || !*s) return defv;
    if (std::strcmp(s, "1") == 0) return true;
    if (std::strcmp(s, "0") == 0) return false;
    if (std::strcmp(s, "true") == 0 || std::strcmp(s, "TRUE") == 0) return true;
    if (std::strcmp(s, "false") == 0 || std::strcmp(s, "FALSE") == 0) return false;
    return defv;
}

static std::vector<OutHit> diversified_topk(
    const std::vector<OutHit>& all,
    int top_k
) {
    if (top_k <= 0 || all.empty()) return {};
    if ((int)all.size() <= top_k) return all;

    // group by dir -> indices in `all`
    std::unordered_map<std::string, std::vector<int>> by_dir;
    by_dir.reserve(32);
    for (int i = 0; i < (int)all.size(); ++i) {
        by_dir[all[i].index_dir].push_back(i);
    }

    // sort each dir bucket by score desc
    for (auto& kv : by_dir) {
        auto& idxs = kv.second;
        std::sort(idxs.begin(), idxs.end(), [&](int a, int b) {
            const auto& A = all[a];
            const auto& B = all[b];
            if (A.score != B.score) return A.score > B.score;
            if (A.cand_hits != B.cand_hits) return A.cand_hits > B.cand_hits;
            return A.doc_id < B.doc_id;
        });
    }

    std::vector<OutHit> out;
    out.reserve((std::size_t)top_k);

    // 1) take one best from each dir (ordered by that best score)
    struct Head { std::string dir; int idx; double score; };
    std::vector<Head> heads;
    heads.reserve(by_dir.size());
    for (auto& kv : by_dir) {
        if (!kv.second.empty()) {
            int idx = kv.second[0];
            heads.push_back(Head{kv.first, idx, all[idx].score});
        }
    }
    std::sort(heads.begin(), heads.end(), [](const Head& a, const Head& b) {
        return a.score > b.score;
    });

    std::unordered_map<std::string, int> cursor;
    cursor.reserve(by_dir.size());

    for (const auto& h : heads) {
        if ((int)out.size() >= top_k) break;
        out.push_back(all[h.idx]);
        cursor[h.dir] = 1;
    }
    if ((int)out.size() >= top_k) return out;

    // 2) fill remaining by best next across dirs
    while ((int)out.size() < top_k) {
        int best_idx = -1;
        double best_score = -1.0;

        for (auto& kv : by_dir) {
            const std::string& dir = kv.first;
            const auto& idxs = kv.second;
            int cur = 0;
            auto itc = cursor.find(dir);
            if (itc != cursor.end()) cur = itc->second;
            if (cur >= (int)idxs.size()) continue;

            int idx = idxs[cur];
            const auto& cand = all[idx];
            if (cand.score > best_score) {
                best_score = cand.score;
                best_idx = idx;
            }
        }

        if (best_idx < 0) break;

        out.push_back(all[best_idx]);
        cursor[all[best_idx].index_dir] += 1;
    }

    return out;
}

} // namespace

// ─────────────────────────────────────────────
// Search API
// ─────────────────────────────────────────────

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

        const int top_k_per_dir = top_k;

        for (int i = 0; i < n_dirs; ++i) {
            const char* cdir = index_dirs_utf8[i];
            if (!cdir || !cdir[0]) continue;
            std::string dir(cdir);

            auto eng = get_or_load(dir);
            if (!eng) continue;

            std::vector<SeHitLite> tmp;
            tmp.reserve((std::size_t)top_k_per_dir);

            int got = eng->search_text(q, top_k_per_dir, tmp, do_norm);
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

                if ((std::size_t)k < tmp_spans.size()) oh.spans = std::move(tmp_spans[(std::size_t)k]);
                if (want_matches && (std::size_t)k < tmp_matches.size())
                    oh.matches = std::move(tmp_matches[(std::size_t)k]);

                all.push_back(std::move(oh));
            }
        }

        if (all.empty()) return mk_empty_hits();

        std::sort(all.begin(), all.end(), [](const OutHit& a, const OutHit& b) {
            if (a.score != b.score) return a.score > b.score;
            if (a.cand_hits != b.cand_hits) return a.cand_hits > b.cand_hits;
            if (a.index_dir != b.index_dir) return a.index_dir < b.index_dir;
            return a.doc_id < b.doc_id;
        });

        const bool do_diverse = env_bool("PLAGIO_DIVERSIFY_MERGE", true);

        std::vector<OutHit> final_hits;
        if (do_diverse) {
            final_hits = diversified_topk(all, top_k);
        } else {
            final_hits = all;
            if ((int)final_hits.size() > top_k) final_hits.resize((std::size_t)top_k);
        }

        json j;
        j["count"] = (int)final_hits.size();
        j["hits"] = json::array();

        for (auto& h : final_hits) {
            json x;
            x["doc_id"] = h.doc_id;
            x["score"] = h.score;
            x["j9"] = h.j9;
            x["c9"] = h.c9;
            x["cand_hits"] = h.cand_hits;
            x["index_dir"] = h.index_dir;

            // spans WITHOUT delta
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

// ─────────────────────────────────────────────
// Excerpt API (REQUIRED BY PYTHON): seg_excerpt_for_span_json_v2
// d_from/d_to are SHINGLE indexes. Convert to token window: [d_from .. d_to+k-1].
// ─────────────────────────────────────────────

extern "C" char* seg_excerpt_for_span_json_v2(
    const char* text_utf8,
    int d_from,
    int d_to,
    int k_shingle,
    int max_chars,
    int normalize_text
) {
    try {
        json j;
        j["ok"] = false;
        j["excerpt"] = "";
        j["char_from"] = 0;
        j["char_to"] = 0;
        j["tok_from"] = nullptr;
        j["tok_to"] = nullptr;
        j["k"] = k_shingle;
        j["norm_len"] = 0;

        if (!text_utf8 || k_shingle <= 0) return malloc_json(j);
        if (d_from < 0 || d_to < d_from) return malloc_json(j);

        std::string norm;
        if (normalize_text != 0) norm = normalize_for_shingles_simple(std::string(text_utf8));
        else norm = std::string(text_utf8);

        j["norm_len"] = (int)norm.size();

        std::vector<TokenSpan> spans;
        spans.reserve(256);
        tokenize_spans(norm, spans);
        if (spans.empty()) return malloc_json(j);

        const int tok_from = d_from;
        const int tok_to_raw = d_to + (k_shingle - 1);

        if (tok_from < 0 || tok_from >= (int)spans.size()) return malloc_json(j);
        if (tok_to_raw < 0) return malloc_json(j);

        const int tok_to = std::min(tok_to_raw, (int)spans.size() - 1);

        int char_from = (int)spans[(std::size_t)tok_from].off;
        int char_to   = (int)(spans[(std::size_t)tok_to].off + spans[(std::size_t)tok_to].len);

        if (char_from < 0) char_from = 0;
        if (char_to < char_from) char_to = char_from;
        if (char_to > (int)norm.size()) char_to = (int)norm.size();

        if (max_chars > 0) {
            int want = char_to - char_from;
            if (want > max_chars) {
                char_to = char_from + max_chars;
                if (char_to > (int)norm.size()) char_to = (int)norm.size();
            }
        }

        std::string excerpt = norm.substr((std::size_t)char_from, (std::size_t)(char_to - char_from));

        j["ok"] = true;
        j["excerpt"] = excerpt;
        j["char_from"] = char_from;
        j["char_to"] = char_to;
        j["tok_from"] = tok_from;
        j["tok_to"] = tok_to;
        j["k"] = k_shingle;

        return malloc_json(j);
    } catch (...) {
        return mk_error("seg_excerpt_for_span_json_v2_exception");
    }
}

extern "C" char* seg_excerpt_for_span_json(
    const char* text_utf8,
    int d_from,
    int d_to,
    int k_shingle,
    int max_chars
) {
    return seg_excerpt_for_span_json_v2(text_utf8, d_from, d_to, k_shingle, max_chars, 1);
}

// ─────────────────────────────────────────────
// Normalize API
// ─────────────────────────────────────────────

extern "C" char* seg_normalize_json_v1(const char* text_utf8) {
    try {
        json j;
        j["ok"] = false;
        j["text"] = "";
        j["norm_len"] = 0;

        if (!text_utf8) return malloc_json(j);

        std::string norm = normalize_for_shingles_simple(std::string(text_utf8));
        j["ok"] = true;
        j["text"] = norm;
        j["norm_len"] = (int)norm.size();

        return malloc_json(j);
    } catch (...) {
        return mk_error("seg_normalize_json_v1_exception");
    }
}

extern "C" void seg_free(void* p) {
    std::free(p);
}
