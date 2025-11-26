// cpp/search_core.cpp
#include "search_core.h"

#include <vector>
#include <unordered_map>
#include <string>
#include <mutex>
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <fstream>
#include <iostream>

#include <nlohmann/json.hpp>
#include "text_common.h"

using json = nlohmann::json;

namespace {

struct DocMeta {
    std::uint32_t tok_len;
    std::uint64_t simhash_hi;
    std::uint64_t simhash_lo;
};

struct Config {
    int  w_min_doc   = 8;
    int  w_min_query = 9;

    double alpha = 0.60;
    double w13   = 0.85;
    double w9    = 0.90;

    double plag_thr    = 0.70;
    double partial_thr = 0.30;

    double simhash_bonus = 0.0;  // пока не используем simhash bonus
    int    fetch_per_k   = 64;
    int    max_cands_doc = 1000;
};

std::once_flag g_init_flag;
bool g_index_loaded = false;

Config g_cfg;

// doc_id_int → Meta
std::vector<DocMeta> g_docs;
// hash -> [doc_id_int]
std::unordered_map<std::uint64_t, std::vector<std::uint32_t>> g_inv9;
std::unordered_map<std::uint64_t, std::vector<std::uint32_t>> g_inv13;
// doc_id_int → doc_id (строка)
std::vector<std::string> g_doc_ids;

// ── служебка ─────────────────────────────────────────────────────────

static inline void jc_compute(
    int inter,
    int q_size,
    int t_size,
    double& J,
    double& C
) {
    if (q_size <= 0) {
        J = 0.0;
        C = 0.0;
        return;
    }
    int u = q_size + t_size - inter;
    if (u <= 0) u = 1;
    J = static_cast<double>(inter) / static_cast<double>(u);
    C = static_cast<double>(inter) / static_cast<double>(q_size);
}

// читаем index_config.json из каталога индекса
static Config load_config_from_json(const std::string& index_dir) {
    Config cfg; // дефолты как выше

    std::string path = index_dir + "/index_config.json";
    std::ifstream in(path);
    if (!in) {
        std::cerr << "[config] no index_config.json, using defaults\n";
        return cfg;
    }
    json j;
    try {
        in >> j;
    } catch (...) {
        std::cerr << "[config] bad json in " << path << ", using defaults\n";
        return cfg;
    }

    if (j.contains("w_min_doc"))   cfg.w_min_doc   = j["w_min_doc"].get<int>();
    if (j.contains("w_min_query")) cfg.w_min_query = j["w_min_query"].get<int>();

    if (j.contains("weights")) {
        auto w = j["weights"];
        if (w.contains("alpha")) cfg.alpha = w["alpha"].get<double>();
        if (w.contains("w13"))   cfg.w13   = w["w13"].get<double>();
        if (w.contains("w9"))    cfg.w9    = w["w9"].get<double>();
    }
    if (j.contains("thresholds")) {
        auto t = j["thresholds"];
        if (t.contains("plag_thr"))    cfg.plag_thr    = t["plag_thr"].get<double>();
        if (t.contains("partial_thr")) cfg.partial_thr = t["partial_thr"].get<double>();
    }
    if (j.contains("fetch_per_k_doc"))
        cfg.fetch_per_k = j["fetch_per_k_doc"].get<int>();
    if (j.contains("max_cands_doc"))
        cfg.max_cands_doc = j["max_cands_doc"].get<int>();

    return cfg;
}

} // namespace

// ── Загрузка индекса ──────────────────────────────────────────────────

extern "C" int se_load_index(const char* index_dir_utf8) {
    std::call_once(g_init_flag, [&]() {
        g_index_loaded = false;
        g_docs.clear();
        g_inv9.clear();
        g_inv13.clear();
        g_doc_ids.clear();

        std::string dir = index_dir_utf8 ? std::string(index_dir_utf8) : std::string(".");
        std::string bin_path    = dir + "/index_native.bin";
        std::string docids_path = dir + "/index_native_docids.json";

        std::ifstream bin(bin_path, std::ios::binary);
        if (!bin) {
            std::cerr << "[se_load_index] cannot open " << bin_path << "\n";
            return;
        }

        char magic[4];
        bin.read(magic, 4);
        if (!bin || magic[0] != 'P' || magic[1] != 'L' || magic[2] != 'A' || magic[3] != 'G') {
            std::cerr << "[se_load_index] bad magic\n";
            return;
        }

        std::uint32_t version = 0;
        std::uint32_t N_docs = 0;
        std::uint64_t N_post9 = 0;
        std::uint64_t N_post13 = 0;

        bin.read(reinterpret_cast<char*>(&version), sizeof(version));
        bin.read(reinterpret_cast<char*>(&N_docs), sizeof(N_docs));
        bin.read(reinterpret_cast<char*>(&N_post9), sizeof(N_post9));
        bin.read(reinterpret_cast<char*>(&N_post13), sizeof(N_post13));
        if (!bin || version != 1) {
            std::cerr << "[se_load_index] bad header or version\n";
            return;
        }

        g_docs.resize(N_docs);
        for (std::uint32_t i = 0; i < N_docs; ++i) {
            DocMeta dm{};
            bin.read(reinterpret_cast<char*>(&dm.tok_len),    sizeof(dm.tok_len));
            bin.read(reinterpret_cast<char*>(&dm.simhash_hi), sizeof(dm.simhash_hi));
            bin.read(reinterpret_cast<char*>(&dm.simhash_lo), sizeof(dm.simhash_lo));
            if (!bin) {
                std::cerr << "[se_load_index] truncated docs_meta\n";
                return;
            }
            g_docs[i] = dm;
        }

        g_inv9.clear();
        g_inv13.clear();
        g_inv9.reserve((std::size_t)N_post9 / 4 + 1);
        g_inv13.reserve((std::size_t)N_post13 / 4 + 1);

        for (std::uint64_t i = 0; i < N_post9; ++i) {
            std::uint64_t h;
            std::uint32_t did;
            bin.read(reinterpret_cast<char*>(&h), sizeof(h));
            bin.read(reinterpret_cast<char*>(&did), sizeof(did));
            if (!bin) {
                std::cerr << "[se_load_index] truncated postings9\n";
                return;
            }
            g_inv9[h].push_back(did);
        }

        for (std::uint64_t i = 0; i < N_post13; ++i) {
            std::uint64_t h;
            std::uint32_t did;
            bin.read(reinterpret_cast<char*>(&h), sizeof(h));
            bin.read(reinterpret_cast<char*>(&did), sizeof(did));
            if (!bin) {
                std::cerr << "[se_load_index] truncated postings13\n";
                return;
            }
            g_inv13[h].push_back(did);
        }

        bin.close();

        std::ifstream dj(docids_path);
        if (!dj) {
            std::cerr << "[se_load_index] cannot open " << docids_path << "\n";
            return;
        }
        json j;
        dj >> j;
        if (!j.is_array()) {
            std::cerr << "[se_load_index] docids json must be array\n";
            return;
        }
        g_doc_ids.clear();
        for (auto& v : j) {
            g_doc_ids.push_back(v.get<std::string>());
        }
        if (g_doc_ids.size() != g_docs.size()) {
            std::cerr << "[se_load_index] docids size mismatch\n";
            return;
        }

        // грузим конфиг индекса
        g_cfg = load_config_from_json(dir);

        g_index_loaded = true;
        std::cerr << "[se_load_index] loaded: docs=" << g_docs.size()
                  << " inv9=" << g_inv9.size()
                  << " inv13=" << g_inv13.size() << "\n";
    });

    return g_index_loaded ? 0 : -1;
}

// ── Поиск по тексту ───────────────────────────────────────────────────

extern "C" SeSearchResult se_search_text(
    const char* text_utf8,
    int top_k,
    SeHit* out_hits,
    int max_hits
) {
    SeSearchResult result{0};
    if (!g_index_loaded || !text_utf8 || !out_hits || max_hits <= 0 || top_k <= 0) {
        return result;
    }

    const std::string qraw(text_utf8);
    // нормализация/токенизация/шинглы — из text_common.h
    std::string qnorm = normalize_for_shingles_simple(qraw);
    auto qtoks = simple_tokens(qnorm);

    const Config& cfg = g_cfg;
    if ((int)qtoks.size() < cfg.w_min_query) {
        return result;
    }

    auto s9  = build_shingles(qtoks, 9);
    auto s13 = build_shingles(qtoks, 13);

    const int qS9  = (int)s9.size();
    const int qS13 = (int)s13.size();

    if (qS9 <= 0 && qS13 <= 0) {
        return result;
    }

    const int N_docs = (int)g_docs.size();
    if (N_docs == 0) {
        return result;
    }

    const int min_inter9  = (qS9 <= 8 ? 1 : 2);
    const int min_inter13 = 1;

    // cand_hits + флаг кандидата
    std::vector<int> cand_hits(N_docs, 0);
    std::vector<std::uint8_t> cand_mask(N_docs, 0);

    const int fetch9  = std::min(cfg.fetch_per_k, qS9);
    const int fetch13 = std::min(cfg.fetch_per_k, qS13);

    // кандидаты по k9
    for (int i = 0; i < fetch9; ++i) {
        auto h = s9[i];
        auto it = g_inv9.find(h);
        if (it == g_inv9.end()) continue;
        const auto& lst = it->second;
        for (auto did : lst) {
            if ((int)did >= N_docs) continue;
            cand_hits[did] += 1;
            cand_mask[did] = 1;
        }
    }
    // кандидаты по k13
    for (int i = 0; i < fetch13; ++i) {
        auto h = s13[i];
        auto it = g_inv13.find(h);
        if (it == g_inv13.end()) continue;
        const auto& lst = it->second;
        for (auto did : lst) {
            if ((int)did >= N_docs) continue;
            cand_hits[did] += 1;
            cand_mask[did] = 1;
        }
    }

    int cand_count = 0;
    for (int i = 0; i < N_docs; ++i) if (cand_mask[i]) ++cand_count;
    if (cand_count == 0) {
        return result;
    }

    struct CandTmp {
        std::uint32_t did;
        int hits;
    };
    std::vector<CandTmp> cand_list;
    cand_list.reserve(cand_count);
    for (int i = 0; i < N_docs; ++i) {
        if (cand_mask[i]) {
            cand_list.push_back(CandTmp{(std::uint32_t)i, cand_hits[i]});
        }
    }

    if ((int)cand_list.size() > cfg.max_cands_doc) {
        std::nth_element(
            cand_list.begin(),
            cand_list.begin() + cfg.max_cands_doc,
            cand_list.end(),
            [](const CandTmp& a, const CandTmp& b) {
                return a.hits > b.hits;
            }
        );
        cand_list.resize(cfg.max_cands_doc);
    }

    // пересечения: hash -> docs → inter9/inter13 по кандидатам
    std::vector<int> inter9(N_docs, 0);
    std::vector<int> inter13(N_docs, 0);

    // маска кандидатов
    std::vector<std::uint8_t> is_cand(N_docs, 0);
    for (auto& c : cand_list) {
        if ((int)c.did < N_docs) is_cand[c.did] = 1;
    }

    // уникальные шинглы запроса k9
    {
        std::unordered_map<std::uint64_t, bool> seen;
        seen.reserve(s9.size() * 2);
        for (auto h : s9) {
            if (seen[h]) continue;
            seen[h] = true;
            auto it = g_inv9.find(h);
            if (it == g_inv9.end()) continue;
            for (auto did : it->second) {
                if ((int)did < N_docs && is_cand[did]) {
                    inter9[did] += 1;
                }
            }
        }
    }

    // уникальные шинглы запроса k13
    {
        std::unordered_map<std::uint64_t, bool> seen;
        seen.reserve(s13.size() * 2);
        for (auto h : s13) {
            if (seen[h]) continue;
            seen[h] = true;
            auto it = g_inv13.find(h);
            if (it == g_inv13.end()) continue;
            for (auto did : it->second) {
                if ((int)did < N_docs && is_cand[did]) {
                    inter13[did] += 1;
                }
            }
        }
    }

    // скоринг
    struct Scored {
        std::uint32_t did;
        double score;
        double j9, c9, j13, c13;
        int    hits;
    };
    std::vector<Scored> scored;
    scored.reserve(cand_list.size());

    const double alpha = cfg.alpha;
    const double w13   = cfg.w13;
    const double w9    = cfg.w9;

    const int tQ9  = qS9;
    const int tQ13 = qS13;

    for (const auto& c : cand_list) {
        int did = (int)c.did;
        const DocMeta& dm = g_docs[did];
        if ((int)dm.tok_len < cfg.w_min_doc) continue;

        int tlen = (int)dm.tok_len;
        int T9   = std::max(0, tlen - 9  + 1);
        int T13  = std::max(0, tlen - 13 + 1);

        int i9  = inter9[did];
        int i13 = inter13[did];

        if (i9 < min_inter9 && i13 < min_inter13) {
            continue;
        }

        double J9 = 0.0, C9 = 0.0;
        double J13 = 0.0, C13 = 0.0;

        if (tQ9 > 0 && T9 > 0) {
            jc_compute(i9, tQ9, T9, J9, C9);
        }
        if (tQ13 > 0 && T13 > 0) {
            jc_compute(i13, tQ13, T13, J13, C13);
        }

        double s13 = w13 * (alpha * J13 + (1.0 - alpha) * C13);
        double s9  = w9  * (alpha * J9  + (1.0 - alpha) * C9);
        double score = s13 > s9 ? s13 : s9;

        scored.push_back(Scored{
            (std::uint32_t)did,
            score,
            J9, C9,
            J13, C13,
            c.hits
        });
    }

    if (scored.empty()) {
        return result;
    }

    std::sort(scored.begin(), scored.end(), [](const Scored& a, const Scored& b) {
        return a.score > b.score;
    });

    int keep = std::min(top_k, std::min(max_hits, (int)scored.size()));
    for (int i = 0; i < keep; ++i) {
        const auto& s = scored[i];
        out_hits[i].doc_id_int = (int)s.did;
        out_hits[i].score      = s.score;
        out_hits[i].j9         = s.j9;
        out_hits[i].c9         = s.c9;
        out_hits[i].j13        = s.j13;
        out_hits[i].c13        = s.c13;
        out_hits[i].cand_hits  = s.hits;
    }

    result.count = keep;
    return result;
}
