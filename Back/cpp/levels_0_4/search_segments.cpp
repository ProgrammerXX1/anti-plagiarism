#include "search_segments.h"
#include "search_engine.h"

#include <unordered_map>
#include <mutex>
#include <string>
#include <vector>
#include <algorithm>
#include <cstdlib>   // malloc/free
#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace {

struct CachedIndex {
    SearchEngine eng;
    // можно добавить last_access для LRU
};

static std::mutex g_cache_mx;
static std::unordered_map<std::string, CachedIndex> g_cache;

// получить engine из кеша или загрузить
static SearchEngine* get_or_load(const std::string& dir) {
    std::lock_guard<std::mutex> lk(g_cache_mx);

    auto it = g_cache.find(dir);
    if (it != g_cache.end()) return &it->second.eng;

    CachedIndex ci;
    if (!ci.eng.load(dir)) {
        return nullptr;
    }
    auto [ins_it, ok] = g_cache.emplace(dir, std::move(ci));
    if (!ok) return nullptr;
    return &ins_it->second.eng;
}

struct OutHit {
    std::string doc_id;
    double score;
    double j9;
    double c9;
    int cand_hits;
    std::string index_dir;
};

} // namespace

extern "C" char* seg_search_many_json(
    const char* query_utf8,
    int top_k,
    const char** index_dirs_utf8,
    int n_dirs
) {
    if (!query_utf8 || !index_dirs_utf8 || n_dirs <= 0 || top_k <= 0) {
        auto s = std::string("{\"hits\":[],\"count\":0}");
        char* out = (char*)std::malloc(s.size()+1);
        std::memcpy(out, s.c_str(), s.size()+1);
        return out;
    }

    std::string q(query_utf8);

    std::vector<OutHit> all;
    all.reserve((size_t)top_k * (size_t)n_dirs);

    for (int i = 0; i < n_dirs; ++i) {
        const char* cdir = index_dirs_utf8[i];
        if (!cdir || !cdir[0]) continue;
        std::string dir(cdir);

        SearchEngine* eng = get_or_load(dir);
        if (!eng) continue;

        std::vector<SeHitLite> tmp;
        tmp.reserve((size_t)top_k);
        int got = eng->search_text(q, top_k, tmp);
        if (got <= 0) continue;

        const auto& docids = eng->doc_ids();
        for (int k = 0; k < got; ++k) {
            auto did = tmp[k].doc_id_int;
            if (did >= docids.size()) continue;
            all.push_back(OutHit{
                docids[did],
                tmp[k].score,
                tmp[k].j9,
                tmp[k].c9,
                tmp[k].cand_hits,
                dir
            });
        }
    }

    std::sort(all.begin(), all.end(), [](const OutHit& a, const OutHit& b){
        return a.score > b.score;
    });
    if ((int)all.size() > top_k) all.resize((size_t)top_k);

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
        j["hits"].push_back(std::move(x));
    }

    std::string s = j.dump();
    char* out = (char*)std::malloc(s.size()+1);
    std::memcpy(out, s.c_str(), s.size()+1);
    return out;
}

extern "C" void seg_free(void* p) {
    std::free(p);
}
