// cpp/search_core.cpp
#include "search_core.h"

#include <vector>
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
    std::uint32_t bm25_len;   // пока не используем, но оставим на будущее под BM25
    std::uint64_t simhash_hi;
    std::uint64_t simhash_lo;
};

struct Config {
    int  w_min_doc   = 8;
    int  w_min_query = 9;

    double alpha = 0.60;
    double w13   = 0.85;
    double w9    = 0.90;  // чисто для совместимости с config, в расчётах не участвует

    double plag_thr    = 0.70;
    double partial_thr = 0.30;

    double simhash_bonus = 0.0;  // не используем пока
    int    fetch_per_k   = 64;
    int    max_cands_doc = 1000;
};

// компактная структура postings k=13
struct Posting13 {
    std::uint64_t h;   // hash шингла k=13
    std::uint32_t did; // doc_id_int
};

std::once_flag g_init_flag;
bool g_index_loaded = false;

Config g_cfg;

// doc_id_int → Meta
std::vector<DocMeta> g_docs;
// ПЛОСКИЙ массив postings k=13, отсортированный по h (и did)
std::vector<Posting13> g_post13;
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

// поиск диапазона postings для данного hash13 через lower/upper_bound
static inline std::pair<std::size_t, std::size_t> find_postings13_range(std::uint64_t h) {
    if (g_post13.empty()) return {0, 0};

    auto lower = std::lower_bound(
        g_post13.begin(),
        g_post13.end(),
        h,
        [](const Posting13& p, std::uint64_t value) {
            return p.h < value;
        }
    );
    if (lower == g_post13.end() || lower->h != h) {
        return {0, 0};
    }
    auto upper = std::upper_bound(
        lower,
        g_post13.end(),
        h,
        [](std::uint64_t value, const Posting13& p) {
            return value < p.h;
        }
    );
    return {static_cast<std::size_t>(lower - g_post13.begin()),
            static_cast<std::size_t>(upper - g_post13.begin())};
}

// читаем index_config.json из каталога индекса
static Config load_config_from_json(const std::string& index_dir) {
    Config cfg; // дефолты

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
        if (w.contains("w9"))    cfg.w9    = w["w9"].get<double>(); // не используем
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

// ── Загрузка индекса ─────────────────────────────────────────────────-
//
// Формат index_native.bin (k=13-only, совместим с твоим index_builder):
//   magic[4] = "PLAG"
//   u32 version = 1
//   u32 N_docs
//   u64 N_post9   (у тебя всегда 0)
//   u64 N_post13
//   [N_docs * (u32 tok_len, u64 simhash_hi, u64 simhash_lo)]
//   [N_post9 * (u64 hash9, u32 doc_id_int)]         // 0 записей
//   [N_post13 * (u64 hash13, u32 doc_id_int)]       // читаем в g_post13, потом sort
//
extern "C" int se_load_index(const char* index_dir_utf8) {
    std::call_once(g_init_flag, [&]() {
        g_index_loaded = false;
        g_docs.clear();
        g_post13.clear();
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
        std::uint32_t N_docs  = 0;
        std::uint64_t N_post9  = 0;
        std::uint64_t N_post13 = 0;

        bin.read(reinterpret_cast<char*>(&version),  sizeof(version));
        bin.read(reinterpret_cast<char*>(&N_docs),   sizeof(N_docs));
        bin.read(reinterpret_cast<char*>(&N_post9),  sizeof(N_post9));
        bin.read(reinterpret_cast<char*>(&N_post13), sizeof(N_post13));
        if (!bin || version != 1) {
            std::cerr << "[se_load_index] bad header or version\n";
            return;
        }

        const std::uint64_t MAX_DOCS     = 100000000ULL;   // 1e8
        const std::uint64_t MAX_POSTINGS = 5000000000ULL;  // 5e9

        if (N_docs == 0 || N_docs > MAX_DOCS) {
            std::cerr << "[se_load_index] suspicious N_docs=" << N_docs << ", abort\n";
            return;
        }
        if (N_post13 > MAX_POSTINGS || N_post9 > MAX_POSTINGS) {
            std::cerr << "[se_load_index] suspicious postings counts: "
                      << "N_post9=" << N_post9 << " N_post13=" << N_post13 << ", abort\n";
            return;
        }

        // docs_meta
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
            dm.bm25_len = dm.tok_len; // пока копируем, под BM25 пригодится
            g_docs[i] = dm;
        }

        // postings k9 — просто пропускаем, их нет (N_post9=0), но код оставим
        for (std::uint64_t i = 0; i < N_post9; ++i) {
            std::uint64_t h;
            std::uint32_t did;
            bin.read(reinterpret_cast<char*>(&h),   sizeof(h));
            bin.read(reinterpret_cast<char*>(&did), sizeof(did));
            if (!bin) {
                std::cerr << "[se_load_index] truncated postings9\n";
                return;
            }
        }

        // postings k13 — читаем в плоский массив
        g_post13.clear();
        g_post13.reserve(static_cast<std::size_t>(N_post13));

        for (std::uint64_t i = 0; i < N_post13; ++i) {
            std::uint64_t h;
            std::uint32_t did;
            bin.read(reinterpret_cast<char*>(&h),   sizeof(h));
            bin.read(reinterpret_cast<char*>(&did), sizeof(did));
            if (!bin) {
                std::cerr << "[se_load_index] truncated postings13\n";
                return;
            }
            if (did >= N_docs) {
                // защита от битых doc_id_int
                continue;
            }
            g_post13.push_back(Posting13{h, did});
        }

        bin.close();

        // сортируем postings по hash, потом по doc_id_int
        std::sort(
            g_post13.begin(),
            g_post13.end(),
            [](const Posting13& a, const Posting13& b) {
                if (a.h != b.h) return a.h < b.h;
                return a.did < b.did;
            }
        );

        // doc_ids
        std::ifstream dj(docids_path);
        if (!dj) {
            std::cerr << "[se_load_index] cannot open " << docids_path << "\n";
            return;
        }
        json j;
        try {
            dj >> j;
        } catch (...) {
            std::cerr << "[se_load_index] bad json in docids\n";
            return;
        }
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

        // конфиг индекса
        g_cfg = load_config_from_json(dir);

        g_index_loaded = true;
        std::cerr << "[se_load_index] loaded: docs=" << g_docs.size()
                  << " post13=" << g_post13.size()
                  << " (flat postings, no unordered_map)\n";
    });

    return g_index_loaded ? 0 : -1;
}

// ── Поиск по тексту (ТОЛЬКО k=13, postings плоские) ─────────────────────────

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
    if (static_cast<int>(qtoks.size()) < cfg.w_min_query) {
        return result;
    }

    // ТОЛЬКО k=13
    auto s13 = build_shingles(qtoks, 13);
    const int qS13 = static_cast<int>(s13.size());
    if (qS13 <= 0) {
        return result;
    }

    const int N_docs = static_cast<int>(g_docs.size());
    if (N_docs == 0) {
        return result;
    }

    std::vector<int>          cand_hits(N_docs, 0);
    std::vector<std::uint8_t> cand_mask(N_docs, 0);

    const int fetch13 = std::min(cfg.fetch_per_k, qS13);

    // кандидаты по k13: для первых fetch13 шинглов
    for (int i = 0; i < fetch13; ++i) {
        std::uint64_t h = s13[i];
        auto [beg, end] = find_postings13_range(h);
        if (beg == end) continue;
        for (std::size_t idx = beg; idx < end; ++idx) {
            std::uint32_t did = g_post13[idx].did;
            if (static_cast<int>(did) >= N_docs) continue;
            cand_hits[did] += 1;
            cand_mask[did] = 1;
        }
    }

    int cand_count = 0;
    for (int i = 0; i < N_docs; ++i) {
        if (cand_mask[i]) ++cand_count;
    }
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
            cand_list.push_back(CandTmp{
                static_cast<std::uint32_t>(i),
                cand_hits[i]
            });
        }
    }

    if (static_cast<int>(cand_list.size()) > cfg.max_cands_doc) {
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

    std::vector<int>          inter13(N_docs, 0);
    std::vector<std::uint8_t> is_cand(N_docs, 0);
    for (auto& c : cand_list) {
        if (static_cast<int>(c.did) < N_docs) {
            is_cand[c.did] = 1;
        }
    }

    // уникальные шинглы запроса k13 → пересечения по всем postings
    {
        std::vector<std::uint64_t> uniq = s13;
        std::sort(uniq.begin(), uniq.end());
        uniq.erase(std::unique(uniq.begin(), uniq.end()), uniq.end());

        for (auto h : uniq) {
            auto [beg, end] = find_postings13_range(h);
            if (beg == end) continue;
            for (std::size_t idx = beg; idx < end; ++idx) {
                std::uint32_t did = g_post13[idx].did;
                if (static_cast<int>(did) < N_docs && is_cand[did]) {
                    inter13[did] += 1;
                }
            }
        }
    }

    struct Scored {
        std::uint32_t did;
        double score;
        double j13;
        double c13;
        int    hits;
    };
    std::vector<Scored> scored;
    scored.reserve(cand_list.size());

    const double alpha = cfg.alpha;
    const double w13   = cfg.w13;

    const int tQ13 = qS13;

    for (const auto& c : cand_list) {
        int did = static_cast<int>(c.did);
        const DocMeta& dm = g_docs[did];
        if (static_cast<int>(dm.tok_len) < cfg.w_min_doc) continue;

        int tlen = static_cast<int>(dm.tok_len);
        int T13  = std::max(0, tlen - 13 + 1);

        int i13 = inter13[did];
        if (i13 < 1) {
            // минимум 1 пересекающийся шингл k=13
            continue;
        }

        double J13 = 0.0, C13 = 0.0;
        if (tQ13 > 0 && T13 > 0) {
            jc_compute(i13, tQ13, T13, J13, C13);
        }

        double score = w13 * (alpha * J13 + (1.0 - alpha) * C13);

        scored.push_back(Scored{
            static_cast<std::uint32_t>(did),
            score,
            J13,
            C13,
            c.hits
        });
    }

    if (scored.empty()) {
        return result;
    }

    std::sort(
        scored.begin(),
        scored.end(),
        [](const Scored& a, const Scored& b) {
            return a.score > b.score;
        }
    );

    int keep = std::min(top_k, std::min(max_hits, static_cast<int>(scored.size())));
    for (int i = 0; i < keep; ++i) {
        const auto& s = scored[i];
        out_hits[i].doc_id_int = static_cast<int>(s.did);
        out_hits[i].score      = s.score;
        // k=9 больше не считаем — нули для ABI с SeHit
        out_hits[i].j9         = 0.0;
        out_hits[i].c9         = 0.0;
        out_hits[i].j13        = s.j13;
        out_hits[i].c13        = s.c13;
        out_hits[i].cand_hits  = s.hits;
    }

    result.count = keep;
    return result;
}
