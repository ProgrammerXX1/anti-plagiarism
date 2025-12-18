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

constexpr int K = 9;
constexpr int MIN_SPAN_LEN_SHINGLES = 6;

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

static bool env_bool(const char* key, bool defv) {
    const char* s = std::getenv(key);
    if (!s || !*s) return defv;
    if (std::strcmp(s, "1") == 0) return true;
    if (std::strcmp(s, "0") == 0) return false;
    if (std::strcmp(s, "true") == 0 || std::strcmp(s, "TRUE") == 0) return true;
    if (std::strcmp(s, "false") == 0 || std::strcmp(s, "FALSE") == 0) return false;
    return defv;
}

static std::vector<OutHit> diversified_topk(const std::vector<OutHit>& all, int top_k) {
    if (top_k <= 0 || all.empty()) return {};
    if ((int)all.size() <= top_k) return all;

    std::unordered_map<std::string, std::vector<int>> by_dir;
    by_dir.reserve(32);
    for (int i = 0; i < (int)all.size(); ++i) {
        by_dir[all[i].index_dir].push_back(i);
    }

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

static bool build_full_norm_and_spans(
    const std::string& q_utf8,
    bool do_norm,
    std::string& norm_out,
    std::vector<TokenSpan>& toks_out
) {
    norm_out = do_norm ? normalize_for_shingles_simple(q_utf8) : q_utf8;
    toks_out.clear();
    toks_out.reserve(256);
    tokenize_spans(norm_out, toks_out);
    return !toks_out.empty();
}

static std::string slice_by_token_range(
    const std::string& norm,
    const std::vector<TokenSpan>& toks,
    int tok_from,
    int tok_to_inclusive
) {
    if (toks.empty()) return "";
    if (tok_from < 0) tok_from = 0;
    if (tok_to_inclusive < tok_from) return "";

    if (tok_from >= (int)toks.size()) return "";
    if (tok_to_inclusive >= (int)toks.size()) tok_to_inclusive = (int)toks.size() - 1;

    const std::size_t b0 = (std::size_t)toks[(std::size_t)tok_from].off;
    const TokenSpan& last = toks[(std::size_t)tok_to_inclusive];
    const std::size_t b1 = (std::size_t)last.off + (std::size_t)last.len;

    if (b0 >= norm.size() || b1 <= b0) return "";
    const std::size_t end = std::min<std::size_t>(b1, norm.size());
    return norm.substr(b0, end - b0);
}

} // namespace

// ─────────────────────────────────────────────
// Flat Search API
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
        if (do_diverse) final_hits = diversified_topk(all, top_k);
        else {
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
// Windowed Sources API -> returns segments + token-based C
// ─────────────────────────────────────────────

extern "C" char* seg_search_windowed_json_v1(
    const char* query_utf8,
    int top_k,
    const char** index_dirs_utf8,
    int n_dirs,
    int normalize_query,
    int include_matches,
    int win_tokens,
    int stride_tokens
) {
    try {
        json out;
        out["segments"] = json::array();
        out["C"] = 0.0;

        if (!query_utf8 || !index_dirs_utf8 || n_dirs <= 0 || top_k <= 0) {
            return malloc_json(out);
        }

        const bool do_norm = (normalize_query != 0);
        const bool want_matches = (include_matches != 0);

        if (win_tokens <= 0) win_tokens = 120;
        if (stride_tokens <= 0) stride_tokens = win_tokens;
        if (stride_tokens > win_tokens) stride_tokens = win_tokens;

        std::string q(query_utf8);
        std::string norm_full;
        std::vector<TokenSpan> toks_full;

        if (!build_full_norm_and_spans(q, do_norm, norm_full, toks_full)) {
            return malloc_json(out);
        }

        const int N = (int)toks_full.size();
        if (N < K) {
            return malloc_json(out);
        }

        // total query shingles in qpos space
        const int q_shingles = N - K + 1;
        if (q_shingles <= 0) return malloc_json(out);

        const bool do_diverse = env_bool("PLAGIO_DIVERSIFY_MERGE", true);
        const int top_k_per_dir = top_k;

        // doc_id -> list of intervals in GLOBAL qpos space
        std::unordered_map<std::string, std::vector<std::pair<int,int>>> by_doc;
        by_doc.reserve(64);

        auto add_interval = [&](const std::string& doc_id, int L, int R) {
            if (R < L) return;
            if (L < 0) L = 0;
            if (R >= q_shingles) R = q_shingles - 1;
            if (R < L) return;
            by_doc[doc_id].push_back({L, R});
        };

        // iterate windows
        for (int start_tok = 0; start_tok < N; start_tok += stride_tokens) {
            int end_tok = start_tok + win_tokens - 1;
            if (end_tok >= N) end_tok = N - 1;

            if ((end_tok - start_tok + 1) < K) break;

            std::string w_text = slice_by_token_range(norm_full, toks_full, start_tok, end_tok);
            if (w_text.empty()) {
                if (end_tok == N - 1) break;
                continue;
            }

            // window qpos range: [start_tok .. end_tok - K + 1]
            const int win_q_from = start_tok;
            const int win_q_to = end_tok - K + 1;
            if (win_q_to < win_q_from) {
                if (end_tok == N - 1) break;
                continue;
            }

            std::vector<OutHit> all;
            all.reserve((std::size_t)top_k * (std::size_t)n_dirs);

            for (int i = 0; i < n_dirs; ++i) {
                const char* cdir = index_dirs_utf8[i];
                if (!cdir || !cdir[0]) continue;
                std::string dir(cdir);

                auto eng = get_or_load(dir);
                if (!eng) continue;

                std::vector<SeHitLite> tmp;
                tmp.reserve((std::size_t)top_k_per_dir);

                // w_text is normalized slice -> normalize_input=false
                int got = eng->search_text(w_text, top_k_per_dir, tmp, false);
                if (got <= 0) continue;

                std::vector<std::vector<MatchSpan>> tmp_spans;
                eng->collect_spans_for_hits(w_text, tmp, tmp_spans, false);

                std::vector<std::vector<MatchPair>> tmp_matches;
                if (want_matches) {
                    eng->collect_matches_for_hits(w_text, tmp, tmp_matches, false);
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

            if (all.empty()) {
                if (end_tok == N - 1) break;
                continue;
            }

            std::sort(all.begin(), all.end(), [](const OutHit& a, const OutHit& b) {
                if (a.score != b.score) return a.score > b.score;
                if (a.cand_hits != b.cand_hits) return a.cand_hits > b.cand_hits;
                if (a.index_dir != b.index_dir) return a.index_dir < b.index_dir;
                return a.doc_id < b.doc_id;
            });

            std::vector<OutHit> final_hits;
            if (do_diverse) final_hits = diversified_topk(all, top_k);
            else {
                final_hits = all;
                if ((int)final_hits.size() > top_k) final_hits.resize((std::size_t)top_k);
            }

            // include all participating docs by spans
            for (auto& h : final_hits) {
                for (const auto& s : h.spans) {
                    int L = (int)s.q_from + win_q_from;
                    int R = (int)s.q_to + win_q_from;

                    if (R < win_q_from || L > win_q_to) continue;
                    if (L < win_q_from) L = win_q_from;
                    if (R > win_q_to) R = win_q_to;

                    if ((R - L + 1) < MIN_SPAN_LEN_SHINGLES) continue;

                    add_interval(h.doc_id, L, R);
                }
            }

            if (end_tok == N - 1) break;
        }

        if (by_doc.empty()) return malloc_json(out);

        // merge per-doc qpos intervals into segments
        struct Seg { std::string doc; int L; int R; }; // qpos interval
        std::vector<Seg> segs;
        segs.reserve(128);

        for (auto& kv : by_doc) {
            auto& iv = kv.second;
            if (iv.empty()) continue;

            std::sort(iv.begin(), iv.end());
            int curL = iv[0].first;
            int curR = iv[0].second;

            for (std::size_t i = 1; i < iv.size(); ++i) {
                int L = iv[i].first;
                int R = iv[i].second;
                if (L <= curR + 1) {
                    if (R > curR) curR = R;
                } else {
                    segs.push_back(Seg{kv.first, curL, curR});
                    curL = L;
                    curR = R;
                }
            }
            segs.push_back(Seg{kv.first, curL, curR});
        }

        if (segs.empty()) return malloc_json(out);

        // convert to TOKEN intervals and compute token coverage union (C_tokens)
        std::vector<std::pair<int,int>> tok_iv;
        tok_iv.reserve(segs.size());

        int max_tok = -1;
        for (const auto& s : segs) {
            int tok_from = s.L;
            int tok_to   = s.R + (K - 1);

            if (tok_from < 0) tok_from = 0;
            if (tok_to < tok_from) tok_to = tok_from;
            if (tok_from > (N - 1)) tok_from = (N - 1);
            if (tok_to   > (N - 1)) tok_to   = (N - 1);

            tok_iv.push_back({tok_from, tok_to});
            if (tok_to > max_tok) max_tok = tok_to;
        }

        double C_tokens = 0.0;
        if (!tok_iv.empty() && max_tok >= 0) {
            std::sort(tok_iv.begin(), tok_iv.end());
            long long covered = 0;
            int curL = tok_iv[0].first;
            int curR = tok_iv[0].second;

            for (std::size_t i = 1; i < tok_iv.size(); ++i) {
                int L = tok_iv[i].first;
                int R = tok_iv[i].second;
                if (L <= curR + 1) {
                    if (R > curR) curR = R;
                } else {
                    covered += (long long)(curR - curL + 1);
                    curL = L;
                    curR = R;
                }
            }
            covered += (long long)(curR - curL + 1);

            const long long total = (long long)max_tok + 1;
            if (total > 0) {
                C_tokens = (double)covered / (double)total;
                if (C_tokens < 0.0) C_tokens = 0.0;
                if (C_tokens > 1.0) C_tokens = 1.0;
            }
        }
        out["C"] = C_tokens;

        // stable sort by token start then doc_id
        struct TokSeg { std::string doc; int tok_from; int tok_to; };
        std::vector<TokSeg> tok_segs;
        tok_segs.reserve(segs.size());

        for (const auto& s : segs) {
            int tok_from = s.L;
            int tok_to   = s.R + (K - 1);

            if (tok_from < 0) tok_from = 0;
            if (tok_to < tok_from) tok_to = tok_from;
            if (tok_from > (N - 1)) tok_from = (N - 1);
            if (tok_to   > (N - 1)) tok_to   = (N - 1);

            tok_segs.push_back(TokSeg{s.doc, tok_from, tok_to});
        }

        std::sort(tok_segs.begin(), tok_segs.end(), [](const TokSeg& a, const TokSeg& b) {
            if (a.tok_from != b.tok_from) return a.tok_from < b.tok_from;
            if (a.tok_to != b.tok_to) return a.tok_to < b.tok_to;
            return a.doc < b.doc;
        });

        for (auto& s : tok_segs) {
            json js;
            js["source_doc_id"] = s.doc;
            js["q_tok_from"] = s.tok_from;
            js["q_tok_to"]   = s.tok_to;
            out["segments"].push_back(std::move(js));
        }

        return malloc_json(out);
    } catch (...) {
        return mk_error("seg_search_windowed_json_v1_exception");
    }
}

// ─────────────────────────────────────────────
// Excerpt API
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
