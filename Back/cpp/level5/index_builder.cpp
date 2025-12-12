// app/services/level5/index_builder.cpp
// VERSION 2.2 (10/10 target): STREAMING + RUNS ON DISK + (OPTIONAL) MULTI-PASS BATCH MERGE + CSR v2 (MATCHES search_core HeaderV2)
// + per-doc shingle dedup + global (h,doc) dedup + fixed binary layouts + soft-cap buffers + atomic replace.
//
// Build:
//   g++ -O3 -march=native -std=c++20 index_builder.cpp -pthread -o index_builder
//
// Usage:
//   index_builder <corpus_jsonl> <out_dir>
//
// Input JSONL (each line):
//   {"doc_id":"...", "text":"..."}
//
// Output:
//   <out_dir>/index_native.bin              (CSR v2, HeaderV2-compatible)
//   <out_dir>/index_native_docids.json      (array of doc_id strings)
//   <out_dir>/index_native_meta.json        (stats/config; docs_meta mapping disabled by default)
//
// Env knobs:
//   PLAGIO_THREADS            (int)  override worker count
//   PLAGIO_RUN_MAX_PAIRS      (int)  flush threshold per run (default 2,000,000)
//   PLAGIO_MERGE_MAX_WAY      (int)  max open runs in final merge (default 64)
//   PLAGIO_META_DOCS_MAP      (0/1)  include docs_meta as doc_id->meta map (default 0; huge on millions)
//   PLAGIO_TMP_KEEP           (0/1)  keep _runs/_tmp for debugging (default 0)

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <fstream>
#include <iostream>
#include <limits>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <filesystem>

#include <simdjson.h>
#include <nlohmann/json.hpp>

#include "text_common.h" // normalize_for_shingles_simple, tokenize_spans, hash_shingle_tokens_spans, simhash128_spans

using json = nlohmann::json;
namespace fs = std::filesystem;

namespace {

// ==================== constants ====================
constexpr int K = 9;

constexpr std::uint32_t MAX_TOKENS_PER_DOC   = 100000; // 0 = no limit
constexpr std::uint32_t MAX_SHINGLES_PER_DOC = 50000;  // 0 = no limit
constexpr int           SHINGLE_STRIDE       = 1;      // 1 = every shingle

constexpr std::size_t   LINES_BATCH     = 2048;
constexpr std::size_t   QUEUE_MAX_BATCH = 32;
constexpr std::size_t   MERGE_BUF_RECS  = 1 << 16; // per run read buffer

constexpr std::uint32_t BIN_VERSION_V2 = 2;

// ==================== env helpers ====================
static long env_long(const char* name, long defv) {
    const char* v = std::getenv(name);
    if (!v || !*v) return defv;
    char* end = nullptr;
    long x = std::strtol(v, &end, 10);
    if (end == v) return defv;
    return x;
}
static int env_int(const char* name, int defv) {
    long x = env_long(name, defv);
    if (x < std::numeric_limits<int>::min()) x = std::numeric_limits<int>::min();
    if (x > std::numeric_limits<int>::max()) x = std::numeric_limits<int>::max();
    return static_cast<int>(x);
}
static bool env_bool(const char* name, bool defv) {
    const char* v = std::getenv(name);
    if (!v || !*v) return defv;
    if (std::strcmp(v, "1") == 0 || std::strcmp(v, "true") == 0 || std::strcmp(v, "TRUE") == 0) return true;
    if (std::strcmp(v, "0") == 0 || std::strcmp(v, "false") == 0 || std::strcmp(v, "FALSE") == 0) return false;
    return defv;
}

// ==================== endianness ====================
static bool is_little_endian() {
    std::uint32_t x = 1;
    return *reinterpret_cast<std::uint8_t*>(&x) == 1;
}

// ==================== data ====================
struct DocMeta {
    std::uint32_t tok_len;
    std::uint64_t simhash_hi;
    std::uint64_t simhash_lo;
};

// Fixed on-disk record: (u64 hash, u32 doc) = 12 bytes exactly.
#pragma pack(push, 1)
struct PairRec {
    std::uint64_t h;
    std::uint32_t doc;
};
#pragma pack(pop)
static_assert(sizeof(PairRec) == 12, "PairRec must be 12 bytes");

// For sorting in-memory buffers.
static inline bool pair_less(const PairRec& a, const PairRec& b) {
    if (a.h < b.h) return true;
    if (a.h > b.h) return false;
    return a.doc < b.doc;
}

// ==================== HeaderV2 (MATCH search_core expectation) ====================
// As per your search_core: magic[4]="PLAG", version=2,
// then N_docs, uniq9_cnt, did9_cnt, reserved0, reserved1,
// then docs_meta[N_docs], uniq9[uniq9_cnt], off[uniq9_cnt+1], did[did9_cnt]
#pragma pack(push, 1)
struct HeaderV2 {
    char          magic[4];      // "PLAG"
    std::uint32_t version;       // 2
    std::uint32_t N_docs;        // docs count
    std::uint64_t uniq9_cnt;     // number of unique hashes
    std::uint64_t did9_cnt;      // number of docids in CSR payload
    std::uint64_t reserved0;     // can store flags later (0 for now)
    std::uint64_t reserved1;     // 0
};
#pragma pack(pop)
static_assert(sizeof(HeaderV2) == 4 + 4 + 4 + 8 + 8 + 8 + 8, "HeaderV2 size mismatch");

// ==================== bounded queue ====================
struct Batch {
    std::vector<std::string> lines;
};

class BoundedQueue {
public:
    explicit BoundedQueue(std::size_t cap) : cap_(cap) {}

    void push(Batch&& b) {
        std::unique_lock<std::mutex> lk(m_);
        cv_push_.wait(lk, [&] { return q_.size() < cap_ || closed_; });
        if (closed_) return;
        q_.emplace_back(std::move(b));
        cv_pop_.notify_one();
    }

    bool pop(Batch& out) {
        std::unique_lock<std::mutex> lk(m_);
        cv_pop_.wait(lk, [&] { return !q_.empty() || closed_; });
        if (q_.empty()) return false;
        out = std::move(q_.front());
        q_.pop_front();
        cv_push_.notify_one();
        return true;
    }

    void close() {
        std::lock_guard<std::mutex> lk(m_);
        closed_ = true;
        cv_pop_.notify_all();
        cv_push_.notify_all();
    }

private:
    std::mutex m_;
    std::condition_variable cv_push_;
    std::condition_variable cv_pop_;
    std::deque<Batch> q_;
    std::size_t cap_;
    bool closed_ = false;
};

// ==================== atomic replace ====================
static void atomic_replace(const fs::path& tmp, const fs::path& final) {
    std::error_code ec;
    if (fs::exists(final, ec)) {
        fs::remove(final, ec); // ignore error
    }
    fs::rename(tmp, final); // atomic on same filesystem
}

// ==================== run format ====================
// magic "RUN1" + u32 run_kind + u32 tid + u64 count + records (PairRec)
// run_kind:
//   1 = LOCAL docids (doc is local within worker tid)
//   2 = GLOBAL docids (doc is already global)
#pragma pack(push, 1)
struct RunHeader {
    char          magic[4];   // "RUN1"
    std::uint32_t kind;       // 1 local, 2 global
    std::uint32_t tid;        // worker id for local runs; 0 for global runs
    std::uint64_t count;      // number of PairRec
};
#pragma pack(pop)
static_assert(sizeof(RunHeader) == 4 + 4 + 4 + 8, "RunHeader size mismatch");

// ==================== run writer ====================
static void write_run_file(const fs::path& path, std::uint32_t kind, std::uint32_t tid, std::vector<PairRec>& recs) {
    std::sort(recs.begin(), recs.end(), pair_less);
    recs.erase(std::unique(recs.begin(), recs.end(), [](const PairRec& a, const PairRec& b) {
        return a.h == b.h && a.doc == b.doc;
    }), recs.end());

    std::ofstream out(path, std::ios::binary);
    if (!out) throw std::runtime_error("cannot open run for write: " + path.string());

    RunHeader hdr;
    hdr.magic[0] = 'R'; hdr.magic[1] = 'U'; hdr.magic[2] = 'N'; hdr.magic[3] = '1';
    hdr.kind = kind;
    hdr.tid  = tid;
    hdr.count = static_cast<std::uint64_t>(recs.size());

    out.write(reinterpret_cast<const char*>(&hdr), sizeof(hdr));
    if (!recs.empty()) {
        out.write(reinterpret_cast<const char*>(recs.data()),
                  static_cast<std::streamsize>(recs.size() * sizeof(PairRec)));
    }
    out.close();
}

// ==================== run reader ====================
struct RunReader {
    fs::path path;
    std::ifstream in;
    RunHeader hdr{};
    std::uint64_t read = 0;

    std::vector<PairRec> buf;
    std::size_t idx = 0;

    explicit RunReader(const fs::path& p) : path(p), in(p, std::ios::binary) {
        if (!in) throw std::runtime_error("cannot open run for read: " + p.string());
        in.read(reinterpret_cast<char*>(&hdr), sizeof(hdr));
        if (in.gcount() != static_cast<std::streamsize>(sizeof(hdr))) throw std::runtime_error("bad run header: " + p.string());
        if (std::memcmp(hdr.magic, "RUN1", 4) != 0) throw std::runtime_error("bad run magic: " + p.string());
        buf.reserve(MERGE_BUF_RECS);
    }

    bool refill() {
        if (read >= hdr.count) return false;
        buf.clear();
        idx = 0;

        std::uint64_t left = hdr.count - read;
        std::size_t want = static_cast<std::size_t>(std::min<std::uint64_t>(left, MERGE_BUF_RECS));
        buf.resize(want);

        in.read(reinterpret_cast<char*>(buf.data()),
                static_cast<std::streamsize>(want * sizeof(PairRec)));
        std::size_t got = static_cast<std::size_t>(in.gcount() / static_cast<std::streamsize>(sizeof(PairRec)));
        buf.resize(got);
        read += got;
        return !buf.empty();
    }

    const PairRec* peek() const {
        if (idx >= buf.size()) return nullptr;
        return &buf[idx];
    }
    void pop() { ++idx; }
};

// ==================== worker context (NO global mutex) ====================
struct WorkerCtx {
    int tid = 0;
    fs::path runs_dir;

    // local docs store (no lock)
    std::vector<std::string> doc_ids;
    std::vector<DocMeta>     docs_meta;

    // per-doc work buffers
    std::vector<TokenSpan> spans;
    std::vector<std::uint64_t> doc_hashes;

    // run buffer
    std::vector<PairRec> run_recs;
    std::vector<fs::path> run_paths;
    std::uint32_t run_seq = 0;

    simdjson::dom::parser parser;

    std::uint64_t docs_ok = 0;
    std::uint64_t docs_bad = 0;
    std::uint64_t pairs_emitted = 0;

    std::size_t run_max_pairs = RUN_MAX_PAIRS_DEFAULT();

    static std::size_t RUN_MAX_PAIRS_DEFAULT() {
        long v = env_long("PLAGIO_RUN_MAX_PAIRS", 2'000'000);
        if (v < 1000) v = 1000;
        if (v > 50'000'000) v = 50'000'000;
        return static_cast<std::size_t>(v);
    }

    WorkerCtx(int t, fs::path rd) : tid(t), runs_dir(std::move(rd)) {
        doc_ids.reserve(1024);
        docs_meta.reserve(1024);
        spans.reserve(256);
        doc_hashes.reserve(4096);
        run_recs.reserve(run_max_pairs);
    }

    void flush_run_softcap() {
        if (run_recs.empty()) return;
        fs::path rp = runs_dir / ("run_local_t" + std::to_string(tid) + "_" + std::to_string(run_seq++) + ".bin");
        write_run_file(rp, /*kind=*/1u, /*tid=*/static_cast<std::uint32_t>(tid), run_recs);
        run_paths.push_back(rp);

        run_recs.clear();
        // soft cap: avoid thrashing
        const std::size_t cap = run_recs.capacity();
        if (cap > run_max_pairs * 2) {
            std::vector<PairRec>().swap(run_recs);
            run_recs.reserve(run_max_pairs);
        }
    }

    std::uint32_t add_local_doc(std::string&& did, const DocMeta& dm) {
        std::uint32_t id = static_cast<std::uint32_t>(doc_ids.size());
        doc_ids.push_back(std::move(did));
        docs_meta.push_back(dm);
        return id;
    }
};

// ==================== per-batch processing ====================
static void process_batch(const Batch& b, WorkerCtx& ctx) {
    for (const std::string& line : b.lines) {
        if (line.empty()) continue;

        simdjson::dom::element doc;
        auto perr = ctx.parser.parse(line).get(doc);
        if (perr) { ctx.docs_bad++; continue; }

        std::string_view did_sv;
        auto err = doc["doc_id"].get(did_sv);
        if (err || did_sv.empty()) { ctx.docs_bad++; continue; }

        std::string_view text_sv;
        err = doc["text"].get(text_sv);
        if (err || text_sv.empty()) { ctx.docs_bad++; continue; }

        std::string did{did_sv};
        std::string text{text_sv};

        std::string norm = normalize_for_shingles_simple(text);

        ctx.spans.clear();
        tokenize_spans(norm, ctx.spans);
        if (ctx.spans.empty()) { ctx.docs_bad++; continue; }

        if (MAX_TOKENS_PER_DOC > 0 && ctx.spans.size() > static_cast<std::size_t>(MAX_TOKENS_PER_DOC)) {
            ctx.spans.resize(MAX_TOKENS_PER_DOC);
        }
        if (ctx.spans.size() < static_cast<std::size_t>(K)) { ctx.docs_bad++; continue; }

        const int n   = static_cast<int>(ctx.spans.size());
        const int cnt = n - K + 1;
        if (cnt <= 0) { ctx.docs_bad++; continue; }

        auto [hi, lo] = simhash128_spans(norm, ctx.spans);

        DocMeta dm{};
        dm.tok_len    = static_cast<std::uint32_t>(ctx.spans.size());
        dm.simhash_hi = hi;
        dm.simhash_lo = lo;

        const std::uint32_t ldoc = ctx.add_local_doc(std::move(did), dm);
        ctx.docs_ok++;

        const int step = (SHINGLE_STRIDE > 0 ? SHINGLE_STRIDE : 1);
        const std::uint32_t max_sh =
            (MAX_SHINGLES_PER_DOC > 0) ? MAX_SHINGLES_PER_DOC : static_cast<std::uint32_t>(cnt);

        ctx.doc_hashes.clear();
        std::uint32_t produced = 0;
        for (int pos = 0; pos < cnt && produced < max_sh; pos += step) {
            std::uint64_t h = hash_shingle_tokens_spans(norm, ctx.spans, pos, K);
            ctx.doc_hashes.push_back(h);
            ++produced;
        }

        // per-doc dedup (fast enough, saves a lot downstream)
        std::sort(ctx.doc_hashes.begin(), ctx.doc_hashes.end());
        ctx.doc_hashes.erase(std::unique(ctx.doc_hashes.begin(), ctx.doc_hashes.end()), ctx.doc_hashes.end());

        for (std::uint64_t h : ctx.doc_hashes) {
            ctx.run_recs.push_back(PairRec{h, ldoc});
        }
        ctx.pairs_emitted += ctx.doc_hashes.size();

        if (ctx.run_recs.size() >= ctx.run_max_pairs) {
            ctx.flush_run_softcap();
        }
    }
}

// ==================== heap merge helpers ====================
struct HeapItem {
    PairRec p;       // p.doc is already GLOBAL when used in some merges
    int run_idx;
};
struct HeapCmp {
    bool operator()(const HeapItem& a, const HeapItem& b) const {
        // min-heap (invert)
        if (a.p.h != b.p.h) return a.p.h > b.p.h;
        return a.p.doc > b.p.doc;
    }
};

// Read next record from reader into p_out; returns false if exhausted.
static bool reader_next_global(RunReader& rr, const std::vector<std::uint32_t>* doc_offsets, PairRec& out) {
    const PairRec* pp = rr.peek();
    if (!pp) {
        if (!rr.refill()) return false;
        pp = rr.peek();
        if (!pp) return false;
    }

    PairRec p = *pp;
    rr.pop();

    if (rr.hdr.kind == 1u) {
        // local -> global by adding offset[tid]
        if (!doc_offsets) throw std::runtime_error("local run without doc_offsets");
        std::uint32_t tid = rr.hdr.tid;
        if (tid >= doc_offsets->size()) throw std::runtime_error("bad tid in run: " + rr.path.string());
        p.doc = (*doc_offsets)[tid] + p.doc;
    } else if (rr.hdr.kind == 2u) {
        // already global
    } else {
        throw std::runtime_error("unknown run kind in: " + rr.path.string());
    }

    out = p;
    return true;
}

// Merge many runs into ONE GLOBAL run (kind=2). This is used for multi-pass batching when too many runs.
static fs::path merge_runs_to_global_run(
    const std::vector<fs::path>& inputs,
    const fs::path& out_path,
    const std::vector<std::uint32_t>* doc_offsets // required if inputs may be local runs
) {
    std::vector<std::unique_ptr<RunReader>> readers;
    readers.reserve(inputs.size());
    for (const auto& p : inputs) readers.emplace_back(std::make_unique<RunReader>(p));

    // init
    std::priority_queue<HeapItem, std::vector<HeapItem>, HeapCmp> heap;
    std::vector<PairRec> cur; // will be streamed, but buffer for write batching
    cur.reserve(1 << 20);

    // prime heap
    for (int i = 0; i < static_cast<int>(readers.size()); ++i) {
        PairRec p{};
        if (reader_next_global(*readers[i], doc_offsets, p)) {
            heap.push(HeapItem{p, i});
        }
    }

    std::ofstream out(out_path, std::ios::binary);
    if (!out) throw std::runtime_error("cannot open merged run for write: " + out_path.string());

    RunHeader hdr{};
    hdr.magic[0] = 'R'; hdr.magic[1] = 'U'; hdr.magic[2] = 'N'; hdr.magic[3] = '1';
    hdr.kind = 2u;
    hdr.tid  = 0u;
    hdr.count = 0u; // unknown now; we will patch later
    out.write(reinterpret_cast<const char*>(&hdr), sizeof(hdr));

    std::uint64_t written = 0;

    bool have_last = false;
    PairRec last{};

    auto flush_buf = [&]() {
        if (cur.empty()) return;
        out.write(reinterpret_cast<const char*>(cur.data()),
                  static_cast<std::streamsize>(cur.size() * sizeof(PairRec)));
        written += static_cast<std::uint64_t>(cur.size());
        cur.clear();
    };

    while (!heap.empty()) {
        HeapItem it = heap.top();
        heap.pop();

        const PairRec p = it.p;

        // advance that reader
        {
            PairRec nxt{};
            if (reader_next_global(*readers[it.run_idx], doc_offsets, nxt)) {
                heap.push(HeapItem{nxt, it.run_idx});
            }
        }

        // global dedup (h,doc)
        if (!have_last || p.h != last.h || p.doc != last.doc) {
            cur.push_back(p);
            last = p;
            have_last = true;
            if (cur.size() >= (1 << 20)) flush_buf();
        }
    }

    flush_buf();
    out.close();

    // patch header.count
    {
        std::fstream patch(out_path, std::ios::in | std::ios::out | std::ios::binary);
        if (!patch) throw std::runtime_error("cannot reopen merged run to patch header");
        RunHeader ph = hdr;
        ph.count = written;
        patch.write(reinterpret_cast<const char*>(&ph), sizeof(ph));
        patch.close();
    }

    return out_path;
}

// Multi-pass batching to reduce number of run files (max-way merge).
static std::vector<fs::path> reduce_runs_multipass(
    std::vector<fs::path> runs,
    const fs::path& tmp_dir,
    int max_way,
    const std::vector<std::uint32_t>* doc_offsets
) {
    if (max_way < 8) max_way = 8;
    if (runs.size() <= static_cast<std::size_t>(max_way)) return runs;

    std::vector<fs::path> cur = std::move(runs);
    int pass = 0;

    while (cur.size() > static_cast<std::size_t>(max_way)) {
        std::vector<fs::path> next;
        next.reserve((cur.size() + max_way - 1) / max_way);

        for (std::size_t i = 0; i < cur.size(); i += static_cast<std::size_t>(max_way)) {
            std::size_t j = std::min<std::size_t>(i + static_cast<std::size_t>(max_way), cur.size());
            std::vector<fs::path> group;
            group.reserve(j - i);
            for (std::size_t k = i; k < j; ++k) group.push_back(cur[k]);

            fs::path outp = tmp_dir / ("run_global_p" + std::to_string(pass) + "_g" + std::to_string(next.size()) + ".bin");
            merge_runs_to_global_run(group, outp, doc_offsets);
            next.push_back(outp);
        }

        // old runs can be deleted now (they are either local or previous globals)
        for (const auto& p : cur) {
            std::error_code ec;
            fs::remove(p, ec);
        }

        cur = std::move(next);
        ++pass;

        // after first pass, all runs are kind=2 (global), so doc_offsets no longer needed.
        doc_offsets = nullptr;
    }

    return cur;
}

// Final merge: runs -> CSR tmp files (hashes, offsets, docids). Inputs can be local or global (handled).
static void merge_runs_to_csr(
    const std::vector<fs::path>& run_paths,
    const std::vector<std::uint32_t>* doc_offsets,
    const fs::path& hashes_tmp,
    const fs::path& offsets_tmp,
    const fs::path& docids_tmp,
    std::uint64_t& out_uniq_hashes,
    std::uint64_t& out_pairs
) {
    std::vector<std::unique_ptr<RunReader>> readers;
    readers.reserve(run_paths.size());
    for (const auto& p : run_paths) readers.emplace_back(std::make_unique<RunReader>(p));

    std::ofstream f_hash(hashes_tmp, std::ios::binary);
    std::ofstream f_off(offsets_tmp, std::ios::binary);
    std::ofstream f_doc(docids_tmp, std::ios::binary);
    if (!f_hash || !f_off || !f_doc) throw std::runtime_error("cannot open csr tmp outputs");

    std::priority_queue<HeapItem, std::vector<HeapItem>, HeapCmp> heap;

    // prime heap
    for (int i = 0; i < static_cast<int>(readers.size()); ++i) {
        PairRec p{};
        if (reader_next_global(*readers[i], doc_offsets, p)) {
            heap.push(HeapItem{p, i});
        }
    }

    std::uint64_t pairs_written = 0;
    std::uint64_t hashes_written = 0;

    bool have_hash = false;
    std::uint64_t cur_h = 0;
    std::uint32_t last_doc_for_hash = std::numeric_limits<std::uint32_t>::max();

    auto start_new_hash = [&](std::uint64_t h) {
        // write hash
        f_hash.write(reinterpret_cast<const char*>(&h), sizeof(h));
        // write offset for this hash
        f_off.write(reinterpret_cast<const char*>(&pairs_written), sizeof(pairs_written));
        ++hashes_written;

        cur_h = h;
        have_hash = true;
        last_doc_for_hash = std::numeric_limits<std::uint32_t>::max();
    };

    while (!heap.empty()) {
        HeapItem it = heap.top();
        heap.pop();

        const PairRec p = it.p;

        // advance that reader
        {
            PairRec nxt{};
            if (reader_next_global(*readers[it.run_idx], doc_offsets, nxt)) {
                heap.push(HeapItem{nxt, it.run_idx});
            }
        }

        if (!have_hash || p.h != cur_h) {
            start_new_hash(p.h);
        }

        // global dedup within hash (h,doc)
        if (p.doc != last_doc_for_hash) {
            f_doc.write(reinterpret_cast<const char*>(&p.doc), sizeof(p.doc));
            ++pairs_written;
            last_doc_for_hash = p.doc;
        }
    }

    // final offset (uniq+1)
    f_off.write(reinterpret_cast<const char*>(&pairs_written), sizeof(pairs_written));

    f_hash.close();
    f_off.close();
    f_doc.close();

    out_uniq_hashes = hashes_written;
    out_pairs = pairs_written;
}

// ==================== file copy ====================
static void copy_stream(std::ifstream& in, std::ofstream& out) {
    constexpr std::size_t BUF = 1 << 20;
    std::vector<char> buf(BUF);
    while (in) {
        in.read(buf.data(), static_cast<std::streamsize>(buf.size()));
        std::streamsize got = in.gcount();
        if (got > 0) out.write(buf.data(), got);
    }
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: index_builder <corpus_jsonl> <out_dir>\n";
        return 1;
    }

    if (!is_little_endian()) {
        std::cerr << "Little-endian only. Refusing to write raw u32/u64 on big-endian.\n";
        return 1;
    }

    const fs::path corpus_path = argv[1];
    const fs::path out_dir     = argv[2];

    std::error_code ec;
    fs::create_directories(out_dir, ec);
    if (ec) {
        std::cerr << "cannot create out_dir: " << out_dir.string() << "\n";
        return 1;
    }

    std::ifstream in(corpus_path);
    if (!in) {
        std::cerr << "cannot open " << corpus_path.string() << "\n";
        return 1;
    }

    std::ios::sync_with_stdio(false);
    std::cin.tie(nullptr);

    // threads
    unsigned hw = std::thread::hardware_concurrency();
    if (hw == 0) hw = 4;
    int threads_env = env_int("PLAGIO_THREADS", 0);
    unsigned num_workers = (threads_env > 0) ? static_cast<unsigned>(threads_env) : std::min<unsigned>(hw, 16u);
    if (num_workers == 0) num_workers = 1;

    // dirs
    fs::path runs_dir = out_dir / "_runs";
    fs::path tmp_dir  = out_dir / "_tmp";
    fs::create_directories(runs_dir, ec);
    fs::create_directories(tmp_dir, ec);

    const int merge_max_way = std::max(8, env_int("PLAGIO_MERGE_MAX_WAY", 64));
    const bool meta_docs_map = env_bool("PLAGIO_META_DOCS_MAP", false);
    const bool keep_tmp = env_bool("PLAGIO_TMP_KEEP", false);

    BoundedQueue q(QUEUE_MAX_BATCH);

    std::vector<std::unique_ptr<WorkerCtx>> wctx;
    wctx.reserve(num_workers);
    for (unsigned t = 0; t < num_workers; ++t) {
        wctx.emplace_back(std::make_unique<WorkerCtx>(static_cast<int>(t), runs_dir));
    }

    // workers
    std::vector<std::thread> workers;
    workers.reserve(num_workers);
    for (unsigned t = 0; t < num_workers; ++t) {
        workers.emplace_back([&, t]() {
            Batch b;
            while (q.pop(b)) {
                process_batch(b, *wctx[t]);
            }
            wctx[t]->flush_run_softcap();
        });
    }

    // producer
    std::uint64_t total_lines = 0;
    {
        Batch cur;
        cur.lines.reserve(LINES_BATCH);

        std::string line;
        while (std::getline(in, line)) {
            if (line.empty()) continue;
            cur.lines.push_back(std::move(line));
            ++total_lines;

            if (cur.lines.size() >= LINES_BATCH) {
                q.push(std::move(cur));
                cur = Batch{};
                cur.lines.reserve(LINES_BATCH);
            }
        }
        if (!cur.lines.empty()) q.push(std::move(cur));
    }

    q.close();
    for (auto& th : workers) th.join();

    // build global doc arrays using prefix sums (NO mutex bottleneck)
    std::uint64_t docs_bad = 0;
    std::uint64_t docs_ok  = 0;
    std::uint64_t pairs_emitted = 0;

    std::vector<std::uint32_t> doc_offsets(num_workers, 0);
    {
        std::uint32_t acc = 0;
        for (unsigned t = 0; t < num_workers; ++t) {
            doc_offsets[t] = acc;
            std::uint32_t add = static_cast<std::uint32_t>(wctx[t]->doc_ids.size());
            acc += add;
        }
    }

    std::vector<std::string> doc_ids;
    std::vector<DocMeta> docs_meta;

    {
        std::uint64_t total_docs = 0;
        for (unsigned t = 0; t < num_workers; ++t) total_docs += wctx[t]->doc_ids.size();
        if (total_docs == 0) {
            std::cerr << "no valid docs in corpus. lines=" << total_lines << "\n";
            return 1;
        }

        doc_ids.reserve(static_cast<std::size_t>(total_docs));
        docs_meta.reserve(static_cast<std::size_t>(total_docs));

        for (unsigned t = 0; t < num_workers; ++t) {
            docs_bad += wctx[t]->docs_bad;
            docs_ok  += wctx[t]->docs_ok;
            pairs_emitted += wctx[t]->pairs_emitted;

            // concat in worker order (doc_offsets matches this)
            for (auto& s : wctx[t]->doc_ids) doc_ids.push_back(std::move(s));
            for (auto& m : wctx[t]->docs_meta) docs_meta.push_back(m);
        }
    }

    // collect run paths
    std::vector<fs::path> run_paths;
    for (unsigned t = 0; t < num_workers; ++t) {
        for (const auto& p : wctx[t]->run_paths) run_paths.push_back(p);
    }
    if (run_paths.empty()) {
        std::cerr << "no runs produced (unexpected). docs=" << doc_ids.size() << "\n";
        return 1;
    }

    // if too many runs, reduce via multi-pass merges (64-way by default)
    // First pass may read LOCAL runs using doc_offsets, outputs GLOBAL runs (doc_offsets becomes null after pass 0 inside).
    std::vector<fs::path> reduced_runs = reduce_runs_multipass(run_paths, tmp_dir, merge_max_way, &doc_offsets);

    // final merge to CSR tmp
    fs::path hashes_tmp  = tmp_dir / "hashes.bin";
    fs::path offsets_tmp = tmp_dir / "offsets.bin";
    fs::path docids_tmp  = tmp_dir / "docids.bin";

    std::uint64_t uniq9_cnt = 0;
    std::uint64_t did9_cnt  = 0;

    // reduced_runs are GLOBAL (kind=2), but even if count <= max_way initially, they may still be LOCAL.
    // So we pass doc_offsets only if any run is local; simplest: pass &doc_offsets always, reader ignores for globals.
    merge_runs_to_csr(reduced_runs, &doc_offsets, hashes_tmp, offsets_tmp, docids_tmp, uniq9_cnt, did9_cnt);

    // write index_native.bin.tmp then atomic replace
    const fs::path bin_final = out_dir / "index_native.bin";
    const fs::path bin_tmp   = tmp_dir / "index_native.bin.tmp";

    {
        std::ofstream bout(bin_tmp, std::ios::binary);
        if (!bout) {
            std::cerr << "cannot open " << bin_tmp.string() << " for write\n";
            return 1;
        }

        HeaderV2 hdr{};
        hdr.magic[0] = 'P'; hdr.magic[1] = 'L'; hdr.magic[2] = 'A'; hdr.magic[3] = 'G';
        hdr.version  = BIN_VERSION_V2;
        hdr.N_docs   = static_cast<std::uint32_t>(doc_ids.size());
        hdr.uniq9_cnt = uniq9_cnt;
        hdr.did9_cnt  = did9_cnt;
        hdr.reserved0 = 0;
        hdr.reserved1 = 0;

        bout.write(reinterpret_cast<const char*>(&hdr), sizeof(hdr));

        // docs_meta
        for (const auto& dm : docs_meta) {
            bout.write(reinterpret_cast<const char*>(&dm.tok_len),    sizeof(dm.tok_len));
            bout.write(reinterpret_cast<const char*>(&dm.simhash_hi), sizeof(dm.simhash_hi));
            bout.write(reinterpret_cast<const char*>(&dm.simhash_lo), sizeof(dm.simhash_lo));
        }

        // uniq9 (hashes)
        {
            std::ifstream fin(hashes_tmp, std::ios::binary);
            if (!fin) throw std::runtime_error("cannot open hashes tmp");
            copy_stream(fin, bout);
        }
        // off (uniq+1)
        {
            std::ifstream fin(offsets_tmp, std::ios::binary);
            if (!fin) throw std::runtime_error("cannot open offsets tmp");
            copy_stream(fin, bout);
        }
        // did (docids payload)
        {
            std::ifstream fin(docids_tmp, std::ios::binary);
            if (!fin) throw std::runtime_error("cannot open docids tmp");
            copy_stream(fin, bout);
        }

        bout.close();
    }

    atomic_replace(bin_tmp, bin_final);

    // write docids json (tmp + atomic replace)
    const fs::path docids_final = out_dir / "index_native_docids.json";
    const fs::path docids_tmpj  = tmp_dir / "index_native_docids.json.tmp";
    {
        std::ofstream dout(docids_tmpj);
        if (!dout) {
            std::cerr << "cannot open " << docids_tmpj.string() << " for write\n";
            return 1;
        }
        json j(doc_ids);
        dout << j.dump();
        dout.close();
    }
    atomic_replace(docids_tmpj, docids_final);

    // meta json (tmp + atomic replace)
    const fs::path meta_final = out_dir / "index_native_meta.json";
    const fs::path meta_tmpj  = tmp_dir / "index_native_meta.json.tmp";
    {
        json j_cfg;
        {
            json thr;
            thr["plag_thr"] = 0.7;
            thr["partial_thr"] = 0.3;
            j_cfg["thresholds"] = std::move(thr);
            j_cfg["k"] = K;
            j_cfg["stride"] = SHINGLE_STRIDE;
            j_cfg["max_tokens"] = MAX_TOKENS_PER_DOC;
            j_cfg["max_shingles"] = MAX_SHINGLES_PER_DOC;
            j_cfg["bin_version"] = BIN_VERSION_V2;
            j_cfg["merge_max_way"] = merge_max_way;
        }

        json j_stats;
        j_stats["lines_total"] = total_lines;
        j_stats["docs_ok"] = static_cast<std::uint64_t>(doc_ids.size());
        j_stats["docs_bad"] = docs_bad;
        j_stats["pairs_emitted_pre_dedup"] = pairs_emitted;
        j_stats["uniq9_cnt"] = uniq9_cnt;
        j_stats["did9_cnt"] = did9_cnt;
        j_stats["workers"] = num_workers;
        j_stats["runs_final"] = reduced_runs.size();

        json j_meta;
        j_meta["config"] = std::move(j_cfg);
        j_meta["stats"]  = std::move(j_stats);

        if (meta_docs_map) {
            // WARNING: huge on millions. Default OFF.
            json j_docs_meta = json::object();
            for (std::size_t i = 0; i < doc_ids.size(); ++i) {
                json m;
                m["tok_len"] = docs_meta[i].tok_len;
                m["simhash_hi"] = docs_meta[i].simhash_hi;
                m["simhash_lo"] = docs_meta[i].simhash_lo;
                j_docs_meta[doc_ids[i]] = std::move(m);
            }
            j_meta["docs_meta"] = std::move(j_docs_meta);
        }

        std::ofstream mout(meta_tmpj);
        if (!mout) {
            std::cerr << "cannot open " << meta_tmpj.string() << " for write\n";
            return 1;
        }
        mout << j_meta.dump();
        mout.close();
    }
    atomic_replace(meta_tmpj, meta_final);

    // cleanup
    if (!keep_tmp) {
        std::error_code ec2;
        fs::remove_all(tmp_dir, ec2);
        fs::remove_all(runs_dir, ec2);
    }

    std::cout
        << "[index_builder] built index_native.bin (CSR v2/HeaderV2): "
        << "docs=" << doc_ids.size()
        << " uniq9=" << uniq9_cnt
        << " did9=" << did9_cnt
        << " lines=" << total_lines
        << " bad_docs=" << docs_bad
        << " workers=" << num_workers
        << " merge_max_way=" << merge_max_way
        << "\n";

    return 0;
}
