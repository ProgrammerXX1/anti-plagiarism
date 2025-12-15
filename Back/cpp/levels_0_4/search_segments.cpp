#include "search_segments.h"
#include "search_engine.h"

#include <unordered_map>
#include <mutex>
#include <string>
#include <vector>
#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <memory>

#include <nlohmann/json.hpp>

#include "text_common.h"  // normalize_for_shingles_simple, tokenize_spans, TokenSpan

using json = nlohmann::json;

namespace {

struct CachedIndex {
    std::shared_ptr<const SearchEngine> eng;
};

static std::mutex g_cache_mx;
static std::unordered_map<std::string, CachedIndex> g_cache;

// Load outside lock, then insert
static std::shared_ptr<const SearchEngine> get_or_load(const std::string& dir) {
    {
        std::lock_guard<std::mutex> lk(g_cache_mx);
        auto it = g_cache.find(dir);
        if (it != g_cache.end()) return it->second.eng;
    }

    auto eng = std::make_shared<SearchEngine>();
    if (!eng->load(dir)) return {};

    std::lock_guard<std::mutex> lk(g_cache_mx);
    auto it = g_cache.find(dir);
    if (it != g_cache.end()) return it->second.eng;

    g_cache.emplace(dir, CachedIndex{eng});
    return eng;
}

static char* malloc_json(const json& j) {
    std::string s = j.dump();
    char* out = (char*)std::malloc(s.size() + 1);
    if (!out) return nullptr;
    std::memcpy(out, s.c_str(), s.size() + 1);
    return out;
}

struct OutHit {
    std::string doc_id;
    double score;
    double j9;
    double c9;
    int cand_hits;
    std::string index_dir;
    std::vector<MatchPair> matches;
};

} // namespace

extern "C" char* seg_search_many_json(
    const char* query_utf8,
    int top_k,
    const char** index_dirs_utf8,
    int n_dirs
) {
    auto mk_empty = []() -> char* {
        std::string s = "{\"hits\":[],\"count\":0}";
        char* out = (char*)std::malloc(s.size() + 1);
        if (!out) return nullptr;
        std::memcpy(out, s.c_str(), s.size() + 1);
        return out;
    };

    if (!query_utf8 || !index_dirs_utf8 || n_dirs <= 0 || top_k <= 0) {
        return mk_empty();
    }

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

        int got = eng->search_text(q, top_k, tmp);
        if (got <= 0) continue;

        std::vector<std::vector<MatchPair>> tmp_matches;
        eng->collect_matches_for_hits(q, tmp, tmp_matches);

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

            if ((std::size_t)k < tmp_matches.size()) {
                oh.matches = std::move(tmp_matches[(std::size_t)k]);
            }

            all.push_back(std::move(oh));
        }
    }

    if (all.empty()) return mk_empty();

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
        j["hits"].push_back(std::move(x));
    }

    return malloc_json(j);
}

extern "C" char* seg_excerpt_for_span_json(
    const char* text_utf8,
    int d_from,
    int d_to,
    int k_shingle,
    int max_chars
) {
    json j;
    j["ok"] = false;
    j["excerpt"] = "";
    j["char_from"] = 0;
    j["char_to"] = 0;
    j["tok_from"] = 0;
    j["tok_to"] = 0;
    j["k"] = k_shingle;
    j["norm_len"] = 0;

    if (!text_utf8 || k_shingle <= 0) return malloc_json(j);
    if (d_from < 0 || d_to < d_from) return malloc_json(j);

    std::string norm = normalize_for_shingles_simple(std::string(text_utf8));
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

    // TokenSpan = { off, len } over `norm`
    int char_from = (int)spans[(std::size_t)tok_from].off;
    int char_to   = (int)(spans[(std::size_t)tok_to].off + spans[(std::size_t)tok_to].len);

    if (char_from < 0) char_from = 0;
    if (char_to < char_from) char_to = char_from;
    if (char_to > (int)norm.size()) char_to = (int)norm.size();

    // safety cap by chars
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
}

extern "C" void seg_free(void* p) {
    std::free(p);
}
