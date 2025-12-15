// cpp/common/search_segments.cpp
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

using json = nlohmann::json;

namespace {

struct CachedIndex {
    std::shared_ptr<const SearchEngine> eng;
};

static std::mutex g_cache_mx;
static std::unordered_map<std::string, CachedIndex> g_cache;

// Load outside lock, then insert (prevents global stall)
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

struct OutHit {
    std::string doc_id;
    double score;
    double j9;
    double c9;
    int cand_hits;
    std::string index_dir;

    // offsets
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

        // Phase 2: collect offsets for those hits
        std::vector<std::vector<MatchPair>> tmp_matches;
        eng->collect_matches_for_hits(q, tmp, tmp_matches);

        const auto& docids = eng->doc_ids();
        for (int k = 0; k < got; ++k) {
            auto did = tmp[k].doc_id_int;
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

        // offsets arrays
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

    std::string s = j.dump();
    char* out = (char*)std::malloc(s.size() + 1);
    if (!out) return nullptr;
    std::memcpy(out, s.c_str(), s.size() + 1);
    return out;
}

extern "C" void seg_free(void* p) {
    std::free(p);
}
