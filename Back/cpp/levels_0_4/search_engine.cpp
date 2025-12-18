// cpp/common/search_engine.cpp
// THREAD_SAFE (parallel) via thread_local counters:
//  - NO mutex
//  - NO shared mutable stamp arrays inside SearchEngine
//  - Each OS thread gets its own seen_stamp/hit_cnt buffers
//
// FIXES vs older stamp version:
//  - hit_cnt is uint32_t (no overflow at 65535)
//  - w_min_query enforced
//  - keeps your scoring + spans/matches logic unchanged

#include "search_engine.h"

#include <unordered_set>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <limits>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include "text_common.h"

using json = nlohmann::json;

namespace {

constexpr int K = 9;

struct QSh {
    std::uint64_t h;
    std::uint32_t qpos; // shingle position in query (0..q_sh-1)
};

struct Pt {
    std::uint32_t q; // shingle pos in query
    std::uint32_t d; // shingle pos in doc
};

// ---------------------------
// thread_local counters
// ---------------------------

struct TlCounters {
    std::vector<std::uint32_t> seen_stamp; // size = N_docs
    std::vector<std::uint32_t> hit_cnt;    // size = N_docs (uint32_t to avoid overflow)
    std::uint32_t stamp = 1;

    void ensure_size(std::size_t n) {
        if (seen_stamp.size() != n) {
            seen_stamp.assign(n, 0);
            hit_cnt.assign(n, 0);
            stamp = 1;
        }
    }

    void bump_stamp() {
        stamp++;
        if (stamp == 0) {
            std::fill(seen_stamp.begin(), seen_stamp.end(), 0);
            stamp = 1;
        }
    }
};

static thread_local TlCounters TLS;

// ---------------------------
// IO helpers
// ---------------------------

static bool read_all_text(const std::string& path, std::string& out) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return false;
    in.seekg(0, std::ios::end);
    std::streamoff n = in.tellg();
    in.seekg(0, std::ios::beg);
    if (n < 0) n = 0;
    out.resize((std::size_t)n);
    if (!out.empty()) in.read(out.data(), (std::streamsize)out.size());
    return true;
}

static bool read_u32(std::ifstream& in, std::uint32_t& v) {
    in.read(reinterpret_cast<char*>(&v), sizeof(v));
    return (bool)in;
}

static bool read_u64(std::ifstream& in, std::uint64_t& v) {
    in.read(reinterpret_cast<char*>(&v), sizeof(v));
    return (bool)in;
}

// ---------------------------
// query shingles
// ---------------------------

static bool build_query_shingles(
    const std::string& text_utf8,
    std::string& norm_out,
    std::vector<TokenSpan>& spans_out,
    std::vector<QSh>& qsh_out,
    bool normalize_input
) {
    if (normalize_input) {
        norm_out = normalize_for_shingles_simple(text_utf8);
    } else {
        norm_out = text_utf8; // already normalized
    }

    spans_out.clear();
    tokenize_spans(norm_out, spans_out);
    if ((int)spans_out.size() < K) return false;

    const int q_tok = (int)spans_out.size();
    const int q_sh  = q_tok - K + 1;
    if (q_sh <= 0) return false;

    qsh_out.clear();
    qsh_out.reserve((std::size_t)q_sh);

    for (int pos = 0; pos < q_sh; ++pos) {
        std::uint64_t h = hash_shingle_tokens_spans(norm_out, spans_out, pos, K);
        qsh_out.push_back(QSh{h, (std::uint32_t)pos});
    }

    std::sort(qsh_out.begin(), qsh_out.end(), [](const QSh& a, const QSh& b) {
        if (a.h != b.h) return a.h < b.h;
        return a.qpos < b.qpos;
    });

    return true;
}

} // namespace


// ─────────────────────────────────────────────
// helpers
// ─────────────────────────────────────────────

double SearchEngine::clamp01(double x) {
    if (x < 0.0) return 0.0;
    if (x > 1.0) return 1.0;
    return x;
}

int SearchEngine::clamp_int(int x, int lo, int hi) {
    if (x < lo) return lo;
    if (x > hi) return hi;
    return x;
}

void SearchEngine::jc_compute(int inter, int q_size, int t_size, double& J, double& C) {
    if (inter <= 0 || q_size <= 0 || t_size <= 0) {
        J = 0.0;
        C = 0.0;
        return;
    }
    const int uni = q_size + t_size - inter;
    J = (uni > 0) ? (double)inter / (double)uni : 0.0;
    C = (q_size > 0) ? (double)inter / (double)q_size : 0.0;
}

std::pair<std::size_t, std::size_t>
SearchEngine::find_postings9_range(std::uint64_t h) const {
    auto lb = std::lower_bound(
        post9_.begin(), post9_.end(), h,
        [](const Posting9& p, std::uint64_t key) { return p.h < key; }
    );
    auto ub = std::upper_bound(
        post9_.begin(), post9_.end(), h,
        [](std::uint64_t key, const Posting9& p) { return key < p.h; }
    );
    return {
        (std::size_t)std::distance(post9_.begin(), lb),
        (std::size_t)std::distance(post9_.begin(), ub)
    };
}

IndexConfig SearchEngine::load_config_from_json(const std::string& index_dir) {
    IndexConfig cfg;
    std::string meta_txt;
    const std::string meta_path = index_dir + "/index_native_meta.json";
    if (!read_all_text(meta_path, meta_txt)) return cfg;

    try {
        auto j = json::parse(meta_txt);
        if (j.contains("config") && j["config"].is_object()) {
            auto& c = j["config"];
            if (c.contains("w_min_doc"))        cfg.w_min_doc = c["w_min_doc"].get<int>();
            if (c.contains("w_min_query"))      cfg.w_min_query = c["w_min_query"].get<int>();
            if (c.contains("alpha"))            cfg.alpha = c["alpha"].get<double>();
            if (c.contains("max_matches_per_doc"))
                cfg.max_matches_per_doc = c["max_matches_per_doc"].get<int>();
            if (c.contains("span_min_len"))     cfg.span_min_len = c["span_min_len"].get<int>();
            if (c.contains("span_gap"))         cfg.span_gap = c["span_gap"].get<int>();
            if (c.contains("max_spans_per_doc"))
                cfg.max_spans_per_doc = c["max_spans_per_doc"].get<int>();
        }
    } catch (...) {
        return cfg;
    }
    return cfg;
}


// ─────────────────────────────────────────────
// load index
// ─────────────────────────────────────────────

bool SearchEngine::load(const std::string& index_dir) {
    loaded_ = false;
    docs_.clear();
    post9_.clear();
    doc_ids_.clear();

    cfg_ = load_config_from_json(index_dir);

    // docids
    {
        std::string txt;
        const std::string p = index_dir + "/index_native_docids.json";
        if (!read_all_text(p, txt)) return false;

        try {
            auto j = json::parse(txt);
            if (!j.is_array()) return false;

            doc_ids_.reserve(j.size());
            for (auto& x : j) {
                if (x.is_string()) doc_ids_.push_back(x.get<std::string>());
                else doc_ids_.push_back(x.dump());
            }
        } catch (...) {
            return false;
        }
    }

    // binary
    const std::string bin_path = index_dir + "/index_native.bin";
    std::ifstream in(bin_path, std::ios::binary);
    if (!in) return false;

    char magic[4] = {0,0,0,0};
    in.read(magic, 4);
    if (!in || magic[0] != 'P' || magic[1] != 'L' || magic[2] != 'A' || magic[3] != 'G')
        return false;

    std::uint32_t version = 0;
    if (!read_u32(in, version) || (version != 1 && version != 2)) return false;

    std::uint32_t N_docs = 0;
    std::uint64_t N_post9 = 0, N_post13 = 0;
    if (!read_u32(in, N_docs) || !read_u64(in, N_post9) || !read_u64(in, N_post13))
        return false;

    if (N_docs == 0) return false;

    if (doc_ids_.size() > (std::size_t)N_docs)
        doc_ids_.resize((std::size_t)N_docs);

    docs_.resize((std::size_t)N_docs);
    for (std::size_t i = 0; i < (std::size_t)N_docs; ++i) {
        std::uint32_t tok_len = 0;
        std::uint64_t hi = 0, lo = 0;
        if (!read_u32(in, tok_len) || !read_u64(in, hi) || !read_u64(in, lo))
            return false;
        docs_[i] = DocMeta{tok_len, tok_len, hi, lo};
    }

    post9_.resize((std::size_t)N_post9);
    if (version == 1) {
        for (std::size_t i = 0; i < (std::size_t)N_post9; ++i) {
            std::uint64_t h = 0;
            std::uint32_t did = 0;
            if (!read_u64(in, h) || !read_u32(in, did)) return false;
            post9_[i] = Posting9{h, did, 0};
        }
    } else {
        for (std::size_t i = 0; i < (std::size_t)N_post9; ++i) {
            std::uint64_t h = 0;
            std::uint32_t did = 0, pos = 0;
            if (!read_u64(in, h) || !read_u32(in, did) || !read_u32(in, pos))
                return false;
            post9_[i] = Posting9{h, did, pos};
        }
    }

    std::sort(post9_.begin(), post9_.end(), [](const Posting9& a, const Posting9& b) {
        if (a.h != b.h) return a.h < b.h;
        if (a.did != b.did) return a.did < b.did;
        return a.pos < b.pos;
    });

    loaded_ = true;
    return true;
}


// ─────────────────────────────────────────────
// search
// ─────────────────────────────────────────────

int SearchEngine::search_text(
    const std::string& text_utf8,
    int top_k,
    std::vector<SeHitLite>& out,
    bool normalize_input
) const {
    out.clear();
    if (!loaded_ || top_k <= 0) return 0;

    std::string norm;
    std::vector<TokenSpan> spans;
    std::vector<QSh> qsh;
    spans.reserve(256);

    if (!build_query_shingles(text_utf8, norm, spans, qsh, normalize_input))
        return 0;

    // enforce min query length
    if ((int)spans.size() < cfg_.w_min_query)
        return 0;

    // unique query shingles
    std::vector<std::uint64_t> q_uniq;
    q_uniq.reserve(qsh.size());
    for (auto& it : qsh) q_uniq.push_back(it.h);
    std::sort(q_uniq.begin(), q_uniq.end());
    q_uniq.erase(std::unique(q_uniq.begin(), q_uniq.end()), q_uniq.end());

    const int q_size = (int)q_uniq.size();
    if (q_size <= 0) return 0;

    TLS.ensure_size(docs_.size());
    TLS.bump_stamp();

    std::vector<std::uint32_t> touched;
    touched.reserve((std::size_t)q_size * 8);

    // accumulate intersections:
    // inter = number of UNIQUE query shingles that appear in doc
    for (std::uint64_t h : q_uniq) {
        auto [L, R] = find_postings9_range(h);

        // postings sorted by (h, did, pos), so did repeats consecutively in [L, R)
        std::uint32_t prev_did = std::numeric_limits<std::uint32_t>::max();

        for (std::size_t i = L; i < R; ++i) {
            const std::uint32_t did = post9_[i].did;
            if (did >= docs_.size()) continue;
            if (did == prev_did) continue;
            prev_did = did;

            if (TLS.seen_stamp[did] != TLS.stamp) {
                TLS.seen_stamp[did] = TLS.stamp;
                TLS.hit_cnt[did] = 1;
                touched.push_back(did);
            } else {
                TLS.hit_cnt[did] += 1;
            }
        }
    }

    if (touched.empty()) return 0;

    struct Cand {
        std::uint32_t did;
        int inter;
        double score;
        double J;
        double C;
    };

    std::vector<Cand> cands;
    cands.reserve(touched.size());

    for (std::uint32_t did : touched) {
        const int inter = (int)TLS.hit_cnt[did];
        TLS.hit_cnt[did] = 0;

        const auto& dm = docs_[did];
        if ((int)dm.tok_len < cfg_.w_min_doc) continue;

        int t_size = 0;
        if ((int)dm.tok_len >= K)
            t_size = (int)dm.tok_len - K + 1;
        if (t_size <= 0) continue;

        double J = 0.0, C = 0.0;
        jc_compute(inter, q_size, t_size, J, C);

        double s = cfg_.alpha * J + (1.0 - cfg_.alpha) * C;
        s = clamp01(s);

        cands.push_back(Cand{did, inter, s, J, C});
    }

    if (cands.empty()) return 0;

    const int keep = std::min<int>((int)cands.size(), top_k);
    std::partial_sort(
        cands.begin(),
        cands.begin() + keep,
        cands.end(),
        [](const Cand& a, const Cand& b) { return a.score > b.score; }
    );

    out.reserve((std::size_t)keep);
    for (int i = 0; i < keep; ++i) {
        SeHitLite h{};
        h.doc_id_int = cands[i].did;
        h.score = cands[i].score;
        h.j9 = cands[i].J;
        h.c9 = cands[i].C;
        h.cand_hits = cands[i].inter;
        out.push_back(h);
    }

    return keep;
}

double SearchEngine::coverage_union_for_hits(
    const std::string& text_utf8,
    const std::vector<SeHitLite>& hits,
    bool normalize_input,
    int* out_q_size,
    int* out_covered
) const {
    if (out_q_size) *out_q_size = 0;
    if (out_covered) *out_covered = 0;

    if (!loaded_ || hits.empty()) return 0.0;

    // Build query shingles (qsh has duplicates by qpos; we make unique set of hashes)
    std::string norm;
    std::vector<TokenSpan> spans;
    std::vector<QSh> qsh;
    spans.reserve(256);

    if (!build_query_shingles(text_utf8, norm, spans, qsh, normalize_input))
        return 0.0;

    if ((int)spans.size() < cfg_.w_min_query)
        return 0.0;

    std::vector<std::uint64_t> q_uniq;
    q_uniq.reserve(qsh.size());
    for (auto& it : qsh) q_uniq.push_back(it.h);
    std::sort(q_uniq.begin(), q_uniq.end());
    q_uniq.erase(std::unique(q_uniq.begin(), q_uniq.end()), q_uniq.end());

    const int q_size = (int)q_uniq.size();
    if (out_q_size) *out_q_size = q_size;
    if (q_size <= 0) return 0.0;

    // Candidate docs set (top-K)
    std::unordered_set<std::uint32_t> did_set;
    did_set.reserve(hits.size() * 2);
    for (const auto& h : hits) {
        if (h.doc_id_int < docs_.size()) did_set.insert(h.doc_id_int);
    }
    if (did_set.empty()) return 0.0;

    int covered = 0;

    // For each unique query shingle hash:
    // check postings range; if any posting belongs to did_set => covered++.
    for (std::uint64_t h : q_uniq) {
        auto [L, R] = find_postings9_range(h);
        if (L == R) continue;

        std::uint32_t prev_did = std::numeric_limits<std::uint32_t>::max();
        bool ok = false;

        for (std::size_t i = L; i < R; ++i) {
            const std::uint32_t did = post9_[i].did;
            if (did == prev_did) continue; // postings sorted by (h,did,pos)
            prev_did = did;

            if (did_set.find(did) != did_set.end()) {
                ok = true;
                break;
            }
        }

        if (ok) {
            covered += 1;
            if (covered == q_size) break;
        }
    }

    if (out_covered) *out_covered = covered;
    return (q_size > 0) ? (double)covered / (double)q_size : 0.0;
}


// ─────────────────────────────────────────────
// collect matches
// ─────────────────────────────────────────────

void SearchEngine::collect_matches_for_hits(
    const std::string& text_utf8,
    const std::vector<SeHitLite>& hits,
    std::vector<std::vector<MatchPair>>& out_matches,
    bool normalize_input
) const {
    out_matches.clear();
    out_matches.resize(hits.size());
    if (!loaded_ || hits.empty()) return;

    std::string norm;
    std::vector<TokenSpan> spans;
    std::vector<QSh> qsh;
    spans.reserve(256);

    if (!build_query_shingles(text_utf8, norm, spans, qsh, normalize_input))
        return;

    std::unordered_map<std::uint32_t, std::size_t> did2idx;
    did2idx.reserve(hits.size() * 2);
    for (std::size_t i = 0; i < hits.size(); ++i)
        did2idx[hits[i].doc_id_int] = i;

    std::unordered_map<std::uint32_t, int> used;
    used.reserve(hits.size() * 2);

    for (const auto& qs : qsh) {
        auto [L, R] = find_postings9_range(qs.h);
        if (L == R) continue;

        for (std::size_t i = L; i < R; ++i) {
            const auto& p = post9_[i];
            auto it = did2idx.find(p.did);
            if (it == did2idx.end()) continue;

            int& cnt = used[p.did];
            if (cfg_.max_matches_per_doc > 0 && cnt >= cfg_.max_matches_per_doc)
                continue;
            cnt += 1;

            out_matches[it->second].push_back(MatchPair{qs.qpos, p.pos, qs.h});
        }
    }

    for (auto& v : out_matches) {
        std::sort(v.begin(), v.end(), [](const MatchPair& a, const MatchPair& b) {
            if (a.qpos != b.qpos) return a.qpos < b.qpos;
            if (a.dpos != b.dpos) return a.dpos < b.dpos;
            return a.h < b.h;
        });
    }
}


// ─────────────────────────────────────────────
// collect spans
// ─────────────────────────────────────────────

void SearchEngine::collect_spans_for_hits(
    const std::string& text_utf8,
    const std::vector<SeHitLite>& hits,
    std::vector<std::vector<MatchSpan>>& out_spans,
    bool normalize_input
) const {
    out_spans.clear();
    out_spans.resize(hits.size());
    if (!loaded_ || hits.empty()) return;

    std::string norm;
    std::vector<TokenSpan> spans;
    std::vector<QSh> qsh;
    spans.reserve(256);

    if (!build_query_shingles(text_utf8, norm, spans, qsh, normalize_input))
        return;

    std::unordered_map<std::uint32_t, std::size_t> did2idx;
    did2idx.reserve(hits.size() * 2);
    for (std::size_t i = 0; i < hits.size(); ++i)
        did2idx[hits[i].doc_id_int] = i;

    std::unordered_map<std::uint32_t, int> used;
    used.reserve(hits.size() * 2);

    std::vector<std::unordered_map<int, std::vector<Pt>>> by_delta(hits.size());
    for (auto& m : by_delta) m.reserve(64);

    const int max_pts = (cfg_.max_matches_per_doc > 0 ? cfg_.max_matches_per_doc : 0);

    for (const auto& qs : qsh) {
        auto [L, R] = find_postings9_range(qs.h);
        if (L == R) continue;

        for (std::size_t i = L; i < R; ++i) {
            const auto& p = post9_[i];
            auto it = did2idx.find(p.did);
            if (it == did2idx.end()) continue;

            int& cnt = used[p.did];
            if (max_pts > 0 && cnt >= max_pts) continue;
            cnt += 1;

            std::size_t hit_idx = it->second;
            int delta = int(qs.qpos) - int(p.pos);
            by_delta[hit_idx][delta].push_back(Pt{qs.qpos, p.pos});
        }
    }

    const int min_len = (cfg_.span_min_len > 0 ? cfg_.span_min_len : 1);
    const int gap = (cfg_.span_gap >= 0 ? cfg_.span_gap : 0);
    const int max_spans = (cfg_.max_spans_per_doc > 0 ? cfg_.max_spans_per_doc : 0);

    for (std::size_t hi = 0; hi < hits.size(); ++hi) {
        std::vector<MatchSpan> all_sp;

        for (auto& kv : by_delta[hi]) {
            int delta = kv.first;
            auto& pts = kv.second;
            if (pts.empty()) continue;

            std::sort(pts.begin(), pts.end(), [](const Pt& a, const Pt& b) {
                if (a.q != b.q) return a.q < b.q;
                return a.d < b.d;
            });

            pts.erase(std::unique(pts.begin(), pts.end(), [](const Pt& a, const Pt& b) {
                return a.q == b.q && a.d == b.d;
            }), pts.end());

            std::uint32_t q0 = pts[0].q, d0 = pts[0].d;
            std::uint32_t q1 = pts[0].q, d1 = pts[0].d;

            for (std::size_t i = 1; i < pts.size(); ++i) {
                const auto& cur = pts[i];

                bool cont =
                    (cur.q > q1) && (cur.d > d1) &&
                    (cur.q <= q1 + 1u + (unsigned)gap) &&
                    (cur.d <= d1 + 1u + (unsigned)gap);

                if (cont) {
                    q1 = cur.q;
                    d1 = cur.d;
                    continue;
                }

                std::uint32_t len = (q1 >= q0) ? (q1 - q0 + 1) : 1;
                if ((int)len >= min_len) {
                    all_sp.push_back(MatchSpan{q0, q1, d0, d1, len, delta});
                }

                q0 = q1 = cur.q;
                d0 = d1 = cur.d;
            }

            std::uint32_t len = (q1 >= q0) ? (q1 - q0 + 1) : 1;
            if ((int)len >= min_len) {
                all_sp.push_back(MatchSpan{q0, q1, d0, d1, len, delta});
            }
        }

        if (all_sp.empty()) {
            out_spans[hi] = {};
            continue;
        }

        std::sort(all_sp.begin(), all_sp.end(), [](const MatchSpan& a, const MatchSpan& b) {
            if (a.length != b.length) return a.length > b.length;
            if (a.d_from != b.d_from) return a.d_from < b.d_from;
            return a.q_from < b.q_from;
        });

        if (max_spans > 0 && (int)all_sp.size() > max_spans) {
            all_sp.resize((std::size_t)max_spans);
        }

        std::sort(all_sp.begin(), all_sp.end(), [](const MatchSpan& a, const MatchSpan& b) {
            if (a.d_from != b.d_from) return a.d_from < b.d_from;
            return a.d_to < b.d_to;
        });

        out_spans[hi] = std::move(all_sp);
    }
}
