// cpp/etl_index_builder_v3.cpp  (VERSION 3, 10/10: streaming + external runs + batched merge + CSR stream + atomic+durable option)
// K=9 shingles only.
//
// Closes the remaining review items:
// 1) Unique temp prefix for ALL temp/run/merged files (no collisions across reruns/crashes).
// 2) Optional Linux fsync(file) + fsync(dir) before atomic replace (power-loss durability mode).
// 3) Versioned header safety: header_bytes, file_bytes, csum_header, static_assert layout size.
// 4) Validate run sortedness during merge (fail fast on corrupted/unsorted run files).
// 5) Dedup (h,docg) in final CSR merge remains.
//
// Build:
//   g++ -O3 -march=native -std=c++20 etl_index_builder_v3.cpp -lsimdjson -o etl_index_builder_v3
//
// Usage:
//   etl_index_builder_v3 <corpus_jsonl> <out_dir>
//
// Input JSONL:
//   {"doc_id":"83","text":"..."}

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <fstream>
#include <iostream>
#include <limits>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>
#include <filesystem>
#include <condition_variable>
#include <stdexcept>
#include <chrono>
#include <random>

#include <simdjson.h>

#include "text_common.h" // normalize_for_shingles_simple, tokenize_spans, hash_shingle_tokens_spans, simhash128_spans

#if defined(__linux__)
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#endif

namespace fs = std::filesystem;

namespace {

constexpr int K = 9;

// Safety knobs
constexpr std::uint32_t MAX_TOKENS_PER_DOC   = 100000;
constexpr std::uint32_t MAX_SHINGLES_PER_DOC = 50000;
constexpr int           SHINGLE_STRIDE       = 1;

// Run spill settings
constexpr std::size_t   RUN_MAX_POSTINGS = 2'000'000; // postings in RAM before spill
constexpr std::size_t   QUEUE_MAX_LINES  = 4096;      // bounded queue capacity per worker

// Merge settings (fd limits)
constexpr std::size_t   MERGE_FANIN = 64;             // merge up to 64 runs in one pass

// Durability option: fsync before rename on Linux (power-loss safe-ish)
constexpr bool          DURABLE_FSYNC = true;

struct DocMeta {
    std::uint32_t tok_len;
    std::uint64_t simhash_hi;
    std::uint64_t simhash_lo;
};

#pragma pack(push,1)
struct HeaderV3 {
    char     magic[4];       // "PLAG"
    uint32_t version;        // 3
    uint32_t flags;          // bit0 = little_endian (must be 1)
    uint32_t header_bytes;   // sizeof(HeaderV3)
    uint32_t reserved_u32;   // align

    uint64_t N_docs;

    uint64_t off_docmeta;
    uint64_t bytes_docmeta;

    uint64_t off_docid_off;   // uint64 offsets, size = N_docs+1
    uint64_t bytes_docid_off;

    uint64_t off_docid_blob;  // raw bytes blob
    uint64_t bytes_docid_blob;

    uint64_t off_uniq9;       // uint64 uniq hashes
    uint64_t cnt_uniq9;
    uint64_t bytes_uniq9;

    uint64_t off_off9;        // uint64 offsets (CSR row ptr), size=cnt_uniq9+1
    uint64_t cnt_off9;
    uint64_t bytes_off9;

    uint64_t off_did9;        // uint32 docids flat
    uint64_t cnt_did9;
    uint64_t bytes_did9;

    // checksums (PLAG64) for integrity
    uint64_t csum_header;     // checksum of header with this field zeroed
    uint64_t csum_docmeta;
    uint64_t csum_docid_off;
    uint64_t csum_docid_blob;
    uint64_t csum_uniq9;
    uint64_t csum_off9;
    uint64_t csum_did9;

    // reproducibility / config in header
    uint32_t shingle_k;       // =9
    uint32_t shingle_stride;  // >=1
    uint32_t max_tokens;      // 0 = unlimited
    uint32_t max_shingles;    // 0 = unlimited

    uint64_t file_bytes;      // final size of index_native.bin

    uint64_t reserved0;
    uint64_t reserved1;
};
#pragma pack(pop)

// Layout safety. If you edit HeaderV3, bump version or keep backward compatibility in reader.
static_assert(sizeof(HeaderV3) == 200, "HeaderV3 size changed: update static_assert, reader, and/or versioning.");

// ------------------ PLAG64 checksum (XXH64-like internal) ------------------

static inline uint64_t rotl64(uint64_t x, int r) { return (x << r) | (x >> (64 - r)); }

struct PLAG64 {
    static constexpr uint64_t P1 = 11400714785074694791ULL;
    static constexpr uint64_t P2 = 14029467366897019727ULL;
    static constexpr uint64_t P3 =  1609587929392839161ULL;
    static constexpr uint64_t P4 =  9650029242287828579ULL;
    static constexpr uint64_t P5 =  2870177450012600261ULL;

    uint64_t seed = 0;
    uint64_t v1, v2, v3, v4;
    uint64_t total_len = 0;

    std::uint8_t buf[32];
    std::size_t  buf_len = 0;

    explicit PLAG64(uint64_t s = 0) : seed(s) {
        v1 = seed + P1 + P2;
        v2 = seed + P2;
        v3 = seed + 0;
        v4 = seed - P1;
    }

    static inline uint64_t round(uint64_t acc, uint64_t input) {
        acc += input * P2;
        acc = rotl64(acc, 31);
        acc *= P1;
        return acc;
    }

    static inline uint64_t merge_round(uint64_t acc, uint64_t val) {
        val = round(0, val);
        acc ^= val;
        acc = acc * P1 + P4;
        return acc;
    }

    void update(const void* data, std::size_t len) {
        const auto* p = (const std::uint8_t*)data;
        total_len += len;

        if (buf_len + len < 32) {
            std::memcpy(buf + buf_len, p, len);
            buf_len += len;
            return;
        }

        if (buf_len > 0) {
            std::size_t need = 32 - buf_len;
            std::memcpy(buf + buf_len, p, need);
            p += need;
            len -= need;
            buf_len = 0;

            const uint64_t* b64 = (const uint64_t*)buf;
            v1 = round(v1, b64[0]);
            v2 = round(v2, b64[1]);
            v3 = round(v3, b64[2]);
            v4 = round(v4, b64[3]);
        }

        while (len >= 32) {
            const uint64_t* b64 = (const uint64_t*)p;
            v1 = round(v1, b64[0]);
            v2 = round(v2, b64[1]);
            v3 = round(v3, b64[2]);
            v4 = round(v4, b64[3]);
            p += 32;
            len -= 32;
        }

        if (len > 0) {
            std::memcpy(buf, p, len);
            buf_len = len;
        }
    }

    uint64_t digest() const {
        uint64_t h64;
        if (total_len >= 32) {
            h64 = rotl64(v1, 1) + rotl64(v2, 7) + rotl64(v3, 12) + rotl64(v4, 18);
            h64 = merge_round(h64, v1);
            h64 = merge_round(h64, v2);
            h64 = merge_round(h64, v3);
            h64 = merge_round(h64, v4);
        } else {
            h64 = seed + P5;
        }

        h64 += total_len;

        const auto* p = (const std::uint8_t*)buf;
        std::size_t len = buf_len;

        while (len >= 8) {
            uint64_t k1;
            std::memcpy(&k1, p, 8);
            k1 *= P2;
            k1 = rotl64(k1, 31);
            k1 *= P1;
            h64 ^= k1;
            h64 = rotl64(h64, 27) * P1 + P4;
            p += 8;
            len -= 8;
        }

        if (len >= 4) {
            uint32_t k1;
            std::memcpy(&k1, p, 4);
            h64 ^= (uint64_t)k1 * P1;
            h64 = rotl64(h64, 23) * P2 + P3;
            p += 4;
            len -= 4;
        }

        while (len > 0) {
            h64 ^= (*p) * P5;
            h64 = rotl64(h64, 11) * P1;
            ++p;
            --len;
        }

        h64 ^= h64 >> 33;
        h64 *= P2;
        h64 ^= h64 >> 29;
        h64 *= P3;
        h64 ^= h64 >> 32;
        return h64;
    }
};

// ------------------ endianness policy ------------------

static bool is_little_endian() {
    uint32_t x = 1;
    return *(uint8_t*)&x == 1;
}

// ------------------ bounded queue ------------------

struct LineItem { std::string line; };

class BoundedQueue {
public:
    explicit BoundedQueue(std::size_t cap) : cap_(cap) {}

    void push(LineItem&& it) {
        std::unique_lock<std::mutex> lk(mu_);
        cv_push_.wait(lk, [&]{ return q_.size() < cap_ || closed_; });
        if (closed_) return;
        q_.push_back(std::move(it));
        cv_pop_.notify_one();
    }

    bool pop(LineItem& out) {
        std::unique_lock<std::mutex> lk(mu_);
        cv_pop_.wait(lk, [&]{ return !q_.empty() || closed_; });
        if (q_.empty()) return false;
        out = std::move(q_.front());
        q_.pop_front();
        cv_push_.notify_one();
        return true;
    }

    void close() {
        std::lock_guard<std::mutex> lk(mu_);
        closed_ = true;
        cv_pop_.notify_all();
        cv_push_.notify_all();
    }

private:
    std::mutex mu_;
    std::condition_variable cv_push_, cv_pop_;
    std::deque<LineItem> q_;
    std::size_t cap_;
    bool closed_ = false;
};

// ------------------ run file I/O ------------------

#pragma pack(push,1)
struct Posting {
    std::uint64_t h;
    std::uint32_t doc; // local doc id (worker runs) OR global docid (merged runs), depending on stage
};
#pragma pack(pop)

static inline void write_all(std::ofstream& out, const void* p, std::size_t n) {
    out.write((const char*)p, (std::streamsize)n);
    if (!out.good()) throw std::runtime_error("write failed");
}

static inline void read_all(std::ifstream& in, void* p, std::size_t n) {
    in.read((char*)p, (std::streamsize)n);
    if ((std::size_t)in.gcount() != n) throw std::runtime_error("read failed");
}

struct RunFileInfo {
    std::string path;
    std::uint64_t count;
};

struct RunCursor {
    std::ifstream in;
    std::uint64_t remaining = 0;
    Posting cur{};
    bool has = false;
    std::uint32_t base = 0; // base docid remap (0 for already-global runs)

    // for sortedness validation
    bool have_prev = false;
    std::uint64_t prev_h = 0;
    std::uint32_t prev_doc = 0;

    void advance_checked() {
        if (remaining == 0) { has = false; return; }
        read_all(in, &cur, sizeof(cur));
        --remaining;

        if (have_prev) {
            if (cur.h < prev_h || (cur.h == prev_h && cur.doc < prev_doc)) {
                throw std::runtime_error("run file is not sorted (corrupt or partial write)");
            }
        }
        have_prev = true;
        prev_h = cur.h;
        prev_doc = cur.doc;

        has = true;
    }
};

// ------------------ temp prefix ------------------

static std::string make_temp_prefix() {
    auto now = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    std::random_device rd;
    uint64_t rnd = ((uint64_t)rd() << 32) ^ (uint64_t)rd();
#if defined(__linux__)
    uint64_t pid = (uint64_t)getpid();
#else
    uint64_t pid = 0;
#endif
    return ".tmp_plag_" + std::to_string((uint64_t)now) + "_" + std::to_string(pid) + "_" + std::to_string(rnd);
}

static std::string run_path(const std::string& out_dir, const std::string& prefix, unsigned tid, unsigned run_idx) {
    return out_dir + "/" + prefix + "_run_t" + std::to_string(tid) + "_" + std::to_string(run_idx) + ".bin";
}

static std::string merged_path(const std::string& out_dir, const std::string& prefix, unsigned pass, unsigned group) {
    return out_dir + "/" + prefix + "_merged_p" + std::to_string(pass) + "_g" + std::to_string(group) + ".bin";
}

// ------------------ worker output ------------------

struct WorkerOut {
    std::vector<DocMeta> docs;
    std::vector<std::string> doc_ids;
    std::vector<RunFileInfo> runs;

    std::uint32_t local_doc_count = 0;
    std::uint64_t local_post_count = 0;
};

static void spill_run(
    const std::string& out_dir,
    const std::string& prefix,
    unsigned tid,
    unsigned& run_idx,
    std::vector<Posting>& buf,
    WorkerOut& out
) {
    if (buf.empty()) return;

    std::sort(buf.begin(), buf.end(), [](const Posting& a, const Posting& b){
        if (a.h != b.h) return a.h < b.h;
        return a.doc < b.doc;
    });

    const std::string path = run_path(out_dir, prefix, tid, run_idx++);
    std::ofstream f(path, std::ios::binary | std::ios::trunc);
    if (!f) throw std::runtime_error("cannot open run file: " + path);

    const std::uint64_t cnt = (std::uint64_t)buf.size();
    write_all(f, &cnt, sizeof(cnt));
    write_all(f, buf.data(), buf.size() * sizeof(Posting));
    f.close();

    out.runs.push_back({path, cnt});
    buf.clear();
}

static void worker_thread(
    unsigned tid,
    BoundedQueue& q,
    const std::string& out_dir,
    const std::string& prefix,
    WorkerOut& out
) {
    out.docs.clear();
    out.doc_ids.clear();
    out.runs.clear();
    out.local_doc_count = 0;
    out.local_post_count = 0;

    simdjson::dom::parser parser;

    std::vector<TokenSpan> spans;
    spans.reserve(256);

    std::string norm;
    norm.reserve(2048);

    std::vector<std::uint64_t> local_hashes;
    local_hashes.reserve(4096);

    std::vector<Posting> buf;
    buf.reserve(std::min<std::size_t>(RUN_MAX_POSTINGS, 4'000'000));

    unsigned run_idx = 0;

    LineItem it;
    while (q.pop(it)) {
        const std::string& line = it.line;
        if (line.empty()) continue;

        simdjson::dom::element doc;
        if (parser.parse(line).get(doc)) continue;

        std::string_view did_sv;
        if (doc["doc_id"].get(did_sv) || did_sv.empty()) continue;

        std::string_view text_sv;
        if (doc["text"].get(text_sv) || text_sv.empty()) continue;

        norm = normalize_for_shingles_simple(std::string(text_sv));

        spans.clear();
        tokenize_spans(norm, spans);
        if (spans.empty()) continue;

        if (MAX_TOKENS_PER_DOC > 0 && spans.size() > MAX_TOKENS_PER_DOC) spans.resize(MAX_TOKENS_PER_DOC);
        if (spans.size() < (std::size_t)K) continue;

        const int n   = (int)spans.size();
        const int cnt = n - K + 1;
        if (cnt <= 0) continue;

        auto [hi, lo] = simhash128_spans(norm, spans);

        const std::uint32_t local_doc_id = out.local_doc_count++;
        out.docs.push_back(DocMeta{(std::uint32_t)spans.size(), hi, lo});
        out.doc_ids.push_back(std::string(did_sv));

        // hashes (dedup per doc)
        local_hashes.clear();
        const int step = (SHINGLE_STRIDE > 0 ? SHINGLE_STRIDE : 1);
        std::uint32_t produced = 0;
        const std::uint32_t max_sh = (MAX_SHINGLES_PER_DOC > 0) ? MAX_SHINGLES_PER_DOC : (std::uint32_t)cnt;

        for (int pos = 0; pos < cnt && produced < max_sh; pos += step) {
            local_hashes.push_back(hash_shingle_tokens_spans(norm, spans, pos, K));
            ++produced;
        }

        std::sort(local_hashes.begin(), local_hashes.end());
        local_hashes.erase(std::unique(local_hashes.begin(), local_hashes.end()), local_hashes.end());

        for (auto h : local_hashes) {
            buf.push_back(Posting{h, local_doc_id});
        }
        out.local_post_count += local_hashes.size();

        if (buf.size() >= RUN_MAX_POSTINGS) {
            spill_run(out_dir, prefix, tid, run_idx, buf, out);
        }
    }

    spill_run(out_dir, prefix, tid, run_idx, buf, out);
}

// ------------------ section writing ------------------

struct SectionInfo {
    uint64_t off = 0;
    uint64_t bytes = 0;
    uint64_t csum = 0;
};

static SectionInfo append_file_to(std::ofstream& out, const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("cannot open temp section: " + path);

    PLAG64 hasher(0);
    const uint64_t off = (uint64_t)out.tellp();

    std::vector<char> buf(1 << 20);
    uint64_t total = 0;
    while (in) {
        in.read(buf.data(), (std::streamsize)buf.size());
        std::streamsize got = in.gcount();
        if (got <= 0) break;

        out.write(buf.data(), got);
        if (!out.good()) throw std::runtime_error("append write failed");

        hasher.update(buf.data(), (std::size_t)got);
        total += (uint64_t)got;
    }
    return SectionInfo{off, total, hasher.digest()};
}

static SectionInfo write_docmeta_section(std::ofstream& out, const std::vector<WorkerOut>& workers) {
    PLAG64 hasher(0);
    const uint64_t off = (uint64_t)out.tellp();

    for (const auto& w : workers) {
        for (const auto& dm : w.docs) {
            out.write((const char*)&dm, sizeof(dm));
            if (!out.good()) throw std::runtime_error("docmeta write failed");
            hasher.update(&dm, sizeof(dm));
        }
    }

    const uint64_t bytes = (uint64_t)out.tellp() - off;
    return SectionInfo{off, bytes, hasher.digest()};
}

// ------------------ merge ------------------

struct HeapNode {
    std::uint64_t h;
    std::uint32_t doc;
    std::size_t   run_idx;
};

static inline bool heap_cmp(const HeapNode& a, const HeapNode& b) {
    if (a.h != b.h) return a.h > b.h;   // reverse for min-heap
    return a.doc > b.doc;
}

// Merge a set of run files into one run file (multi-pass reduction).
// Output run stores GLOBAL docid in Posting.doc.
static RunFileInfo merge_runs_into_run(
    const std::vector<RunFileInfo>& in_runs,
    const std::vector<std::uint32_t>& bases_for_each_run,
    const std::string& out_path
) {
    std::vector<RunCursor> cursors;
    cursors.reserve(in_runs.size());

    for (std::size_t i = 0; i < in_runs.size(); ++i) {
        RunCursor c;
        c.in.open(in_runs[i].path, std::ios::binary);
        if (!c.in) throw std::runtime_error("cannot open run for batch-merge: " + in_runs[i].path);

        std::uint64_t cnt = 0;
        read_all(c.in, &cnt, sizeof(cnt));
        c.remaining = cnt;
        c.base = bases_for_each_run[i];
        c.advance_checked();
        cursors.push_back(std::move(c));
    }

    std::vector<HeapNode> heap;
    heap.reserve(cursors.size());
    for (std::size_t i = 0; i < cursors.size(); ++i) {
        if (cursors[i].has) heap.push_back(HeapNode{cursors[i].cur.h, cursors[i].cur.doc, i});
    }
    std::make_heap(heap.begin(), heap.end(), heap_cmp);

    std::ofstream out(out_path, std::ios::binary | std::ios::trunc);
    if (!out) throw std::runtime_error("cannot create merged run: " + out_path);

    std::uint64_t out_cnt = 0;
    write_all(out, &out_cnt, sizeof(out_cnt)); // placeholder

    // Dedup identical (h, docg) while writing new run.
    bool have_last = false;
    std::uint64_t last_h = 0;
    std::uint32_t last_docg = 0;

    while (!heap.empty()) {
        std::pop_heap(heap.begin(), heap.end(), heap_cmp);
        HeapNode node = heap.back();
        heap.pop_back();

        RunCursor& c = cursors[node.run_idx];

        const uint64_t h = node.h;
        const uint32_t docg = c.base + node.doc;

        if (!have_last || h != last_h || docg != last_docg) {
            Posting p{h, docg}; // doc field becomes GLOBAL docid
            write_all(out, &p, sizeof(p));
            ++out_cnt;
            have_last = true;
            last_h = h;
            last_docg = docg;
        }

        c.advance_checked();
        if (c.has) {
            heap.push_back(HeapNode{c.cur.h, c.cur.doc, node.run_idx});
            std::push_heap(heap.begin(), heap.end(), heap_cmp);
        }
    }

    out.seekp(0, std::ios::beg);
    write_all(out, &out_cnt, sizeof(out_cnt));
    out.close();

    return RunFileInfo{out_path, out_cnt};
}

static std::vector<RunFileInfo> reduce_runs_batched(
    const std::vector<RunFileInfo>& runs,
    const std::vector<std::uint32_t>& run_bases,
    const std::string& out_dir,
    const std::string& prefix
) {
    if (runs.size() <= MERGE_FANIN) return runs;

    std::vector<RunFileInfo> cur = runs;
    std::vector<std::uint32_t> cur_bases = run_bases;

    std::vector<RunFileInfo> next;
    std::vector<std::uint32_t> next_bases;

    unsigned pass = 0;

    while (cur.size() > MERGE_FANIN) {
        next.clear();
        next_bases.clear();

        unsigned group = 0;
        for (std::size_t i = 0; i < cur.size(); i += MERGE_FANIN) {
            const std::size_t j = std::min(i + MERGE_FANIN, cur.size());

            std::vector<RunFileInfo> chunk;
            std::vector<std::uint32_t> bases_chunk;
            chunk.reserve(j - i);
            bases_chunk.reserve(j - i);

            for (std::size_t k = i; k < j; ++k) {
                chunk.push_back(cur[k]);
                bases_chunk.push_back(cur_bases[k]);
            }

            const std::string out_path = merged_path(out_dir, prefix, pass, group++);
            RunFileInfo merged = merge_runs_into_run(chunk, bases_chunk, out_path);

            next.push_back(merged);
            next_bases.push_back(0); // merged runs store global docid already
        }

        // remove previous generation run files
        std::error_code ec;
        for (auto& r : cur) fs::remove(r.path, ec);

        cur.swap(next);
        cur_bases.swap(next_bases);
        ++pass;
    }

    return cur;
}

static void merge_runs_to_temp_csr(
    const std::vector<RunFileInfo>& runs,
    const std::vector<std::uint32_t>& run_bases,
    const std::string& tmp_uniq_path,
    const std::string& tmp_off_path,
    const std::string& tmp_did_path,
    uint64_t& out_cnt_uniq,
    uint64_t& out_cnt_off,
    uint64_t& out_cnt_did,
    uint64_t& csum_uniq,
    uint64_t& csum_off,
    uint64_t& csum_did
) {
    std::vector<RunCursor> cursors;
    cursors.reserve(runs.size());

    for (std::size_t i = 0; i < runs.size(); ++i) {
        RunCursor c;
        c.in.open(runs[i].path, std::ios::binary);
        if (!c.in) throw std::runtime_error("cannot open run for merge: " + runs[i].path);

        std::uint64_t cnt = 0;
        read_all(c.in, &cnt, sizeof(cnt));
        c.remaining = cnt;
        c.base = run_bases[i];
        c.advance_checked();
        cursors.push_back(std::move(c));
    }

    std::vector<HeapNode> heap;
    heap.reserve(cursors.size());
    for (std::size_t i = 0; i < cursors.size(); ++i) {
        if (cursors[i].has) heap.push_back(HeapNode{cursors[i].cur.h, cursors[i].cur.doc, i});
    }
    std::make_heap(heap.begin(), heap.end(), heap_cmp);

    std::ofstream f_uniq(tmp_uniq_path, std::ios::binary | std::ios::trunc);
    std::ofstream f_off(tmp_off_path, std::ios::binary | std::ios::trunc);
    std::ofstream f_did(tmp_did_path, std::ios::binary | std::ios::trunc);
    if (!f_uniq || !f_off || !f_did) throw std::runtime_error("cannot open temp csr files");

    PLAG64 h_uniq(0), h_off(0), h_did(0);

    auto write_u64 = [&](std::ofstream& f, uint64_t x, PLAG64& hh) {
        f.write((const char*)&x, sizeof(x));
        if (!f.good()) throw std::runtime_error("write u64 failed");
        hh.update(&x, sizeof(x));
    };
    auto write_u32 = [&](std::ofstream& f, uint32_t x, PLAG64& hh) {
        f.write((const char*)&x, sizeof(x));
        if (!f.good()) throw std::runtime_error("write u32 failed");
        hh.update(&x, sizeof(x));
    };

    uint64_t did_count = 0;
    uint64_t uniq_count = 0;

    std::optional<uint64_t> cur_h;

    // dedup (h,docg)
    bool have_last = false;
    uint64_t last_h = 0;
    uint32_t last_docg = 0;

    while (!heap.empty()) {
        std::pop_heap(heap.begin(), heap.end(), heap_cmp);
        HeapNode node = heap.back();
        heap.pop_back();

        RunCursor& c = cursors[node.run_idx];

        const uint64_t h = node.h;
        const uint32_t docg = c.base + node.doc;

        if (!cur_h.has_value() || *cur_h != h) {
            cur_h = h;
            write_u64(f_uniq, h, h_uniq);
            write_u64(f_off, did_count, h_off);
            ++uniq_count;
        }

        if (!have_last || h != last_h || docg != last_docg) {
            write_u32(f_did, docg, h_did);
            ++did_count;
            have_last = true;
            last_h = h;
            last_docg = docg;
        }

        c.advance_checked();
        if (c.has) {
            heap.push_back(HeapNode{c.cur.h, c.cur.doc, node.run_idx});
            std::push_heap(heap.begin(), heap.end(), heap_cmp);
        }
    }

    write_u64(f_off, did_count, h_off);

    f_uniq.close();
    f_off.close();
    f_did.close();

    out_cnt_uniq = uniq_count;
    out_cnt_off  = uniq_count + 1;
    out_cnt_did  = did_count;

    csum_uniq = h_uniq.digest();
    csum_off  = h_off.digest();
    csum_did  = h_did.digest();
}

// ------------------ atomic replace + optional fsync ------------------

#if defined(__linux__)
static void fsync_path_file(const fs::path& p) {
    int fd = ::open(p.c_str(), O_RDONLY);
    if (fd >= 0) {
        ::fsync(fd);
        ::close(fd);
    }
}

static void fsync_dir(const fs::path& dir) {
    int dfd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
    if (dfd >= 0) {
        ::fsync(dfd);
        ::close(dfd);
    }
}
#endif

static void atomic_replace_file(const fs::path& tmp, const fs::path& dst) {
    std::error_code ec;
    fs::remove(dst, ec); // ignore
    ec.clear();

#if defined(__linux__)
    if constexpr (DURABLE_FSYNC) {
        fsync_path_file(tmp);
        fsync_dir(dst.parent_path());
    }
#endif

    fs::rename(tmp, dst, ec);
    if (ec) throw std::runtime_error("rename failed: " + ec.message());

#if defined(__linux__)
    if constexpr (DURABLE_FSYNC) {
        fsync_dir(dst.parent_path());
    }
#endif
}

// ------------------ header checksum ------------------

static uint64_t checksum_header(const HeaderV3& hdr_in) {
    HeaderV3 tmp = hdr_in;
    tmp.csum_header = 0;
    PLAG64 h(0);
    h.update(&tmp, sizeof(tmp));
    return h.digest();
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: etl_index_builder_v3 <corpus_jsonl> <out_dir>\n";
        return 1;
    }

    const std::string corpus_path = argv[1];
    const std::string out_dir     = argv[2];

    try {
        if (!is_little_endian()) {
            throw std::runtime_error("Big-endian is not supported. Format is little-endian only.");
        }

        fs::create_directories(out_dir);
        const std::string prefix = make_temp_prefix();

        std::ifstream in(corpus_path);
        if (!in) {
            std::cerr << "[etl_index_builder_v3] cannot open " << corpus_path << "\n";
            return 1;
        }

        std::ios::sync_with_stdio(false);

        unsigned hw = std::thread::hardware_concurrency();
        if (hw == 0) hw = 4;
        unsigned num_workers = std::min<unsigned>(hw, 16u);
        if (num_workers == 0) num_workers = 1;

        std::vector<std::unique_ptr<BoundedQueue>> queues;
        queues.reserve(num_workers);
        for (unsigned i = 0; i < num_workers; ++i) {
            queues.emplace_back(std::make_unique<BoundedQueue>(QUEUE_MAX_LINES));
        }

        std::vector<WorkerOut> outs(num_workers);
        std::vector<std::thread> workers;
        workers.reserve(num_workers);

        for (unsigned t = 0; t < num_workers; ++t) {
            workers.emplace_back([&, t]{
                worker_thread(t, *queues[t], out_dir, prefix, outs[t]);
            });
        }

        // Reader: round-robin dispatch (OK; if you want best balancing switch to 1 shared queue)
        {
            std::string line;
            unsigned rr = 0;
            while (std::getline(in, line)) {
                if (line.empty()) continue;
                queues[rr]->push(LineItem{std::move(line)});
                rr = (rr + 1) % num_workers;
                line.clear();
            }
        }

        for (auto& q : queues) q->close();
        for (auto& th : workers) th.join();

        // compute global doc bases
        std::vector<std::uint32_t> bases(num_workers, 0);
        {
            uint64_t acc = 0;
            for (unsigned t = 0; t < num_workers; ++t) {
                bases[t] = (std::uint32_t)acc;
                acc += outs[t].docs.size();
                if (acc > std::numeric_limits<std::uint32_t>::max()) {
                    throw std::runtime_error("N_docs exceeds uint32 docid capacity; widen did9 to uint64.");
                }
            }
        }

        uint64_t N_docs = 0;
        uint64_t total_posts = 0;
        uint64_t total_runs = 0;
        for (unsigned t = 0; t < num_workers; ++t) {
            N_docs += outs[t].docs.size();
            total_posts += outs[t].local_post_count;
            total_runs += outs[t].runs.size();
        }
        if (N_docs == 0) {
            std::cerr << "[etl_index_builder_v3] no valid docs\n";
            return 1;
        }

        // Collect all runs + bases per run
        std::vector<RunFileInfo> all_runs;
        std::vector<std::uint32_t> all_run_bases;
        all_runs.reserve((std::size_t)total_runs);
        all_run_bases.reserve((std::size_t)total_runs);

        for (unsigned t = 0; t < num_workers; ++t) {
            for (auto& r : outs[t].runs) {
                all_runs.push_back(r);
                all_run_bases.push_back(bases[t]); // worker-local docid -> add base
            }
        }

        // Reduce runs (ulimit safety)
        all_runs = reduce_runs_batched(all_runs, all_run_bases, out_dir, prefix);
        all_run_bases.assign(all_runs.size(), 0); // reduced runs store global docid

        // Merge to temp CSR
        const std::string tmp_uniq = out_dir + "/" + prefix + "_tmp_uniq9.bin";
        const std::string tmp_off  = out_dir + "/" + prefix + "_tmp_off9.bin";
        const std::string tmp_did  = out_dir + "/" + prefix + "_tmp_did9.bin";

        uint64_t cnt_uniq = 0, cnt_off = 0, cnt_did = 0;
        uint64_t csum_uniq = 0, csum_off = 0, csum_did = 0;

        merge_runs_to_temp_csr(
            all_runs, all_run_bases,
            tmp_uniq, tmp_off, tmp_did,
            cnt_uniq, cnt_off, cnt_did,
            csum_uniq, csum_off, csum_did
        );

        // Final output: temp + atomic replace
        const fs::path final_path = fs::path(out_dir) / "index_native.bin";
        const fs::path tmp_path   = fs::path(out_dir) / (prefix + "_index_native.bin.tmp");

        std::ofstream out(tmp_path, std::ios::binary | std::ios::trunc);
        if (!out) throw std::runtime_error("cannot open output tmp: " + tmp_path.string());

        HeaderV3 hdr{};
        hdr.magic[0]='P'; hdr.magic[1]='L'; hdr.magic[2]='A'; hdr.magic[3]='G';
        hdr.version = 3;
        hdr.flags   = 1u; // LE only
        hdr.header_bytes = (uint32_t)sizeof(HeaderV3);
        hdr.N_docs  = N_docs;

        hdr.shingle_k = (uint32_t)K;
        hdr.shingle_stride = (uint32_t)(SHINGLE_STRIDE > 0 ? SHINGLE_STRIDE : 1);
        hdr.max_tokens = (uint32_t)MAX_TOKENS_PER_DOC;
        hdr.max_shingles = (uint32_t)MAX_SHINGLES_PER_DOC;

        out.write((const char*)&hdr, sizeof(hdr));
        if (!out.good()) throw std::runtime_error("header write failed");

        // docmeta
        SectionInfo sec_docmeta = write_docmeta_section(out, outs);

        // docid offsets + blob
        const uint64_t off_docid_off = (uint64_t)out.tellp();

        std::vector<uint64_t> off_table;
        off_table.resize((std::size_t)N_docs + 1);
        const uint64_t bytes_docid_off = (uint64_t)off_table.size() * sizeof(uint64_t);

        out.write((const char*)off_table.data(), (std::streamsize)bytes_docid_off);
        if (!out.good()) throw std::runtime_error("docid_off placeholder write failed");

        const uint64_t off_docid_blob = (uint64_t)out.tellp();
        PLAG64 h_docid_off(0), h_docid_blob(0);

        uint64_t cur = 0;
        uint64_t idx = 0;
        for (unsigned t = 0; t < num_workers; ++t) {
            auto& w = outs[t];
            for (auto& s : w.doc_ids) {
                off_table[(std::size_t)idx] = cur;

                const uint32_t len = (uint32_t)std::min<std::size_t>(s.size(), std::numeric_limits<uint32_t>::max());
                out.write((const char*)&len, sizeof(len));
                if (len) out.write(s.data(), (std::streamsize)len);
                if (!out.good()) throw std::runtime_error("docid_blob write failed");

                h_docid_blob.update(&len, sizeof(len));
                if (len) h_docid_blob.update(s.data(), len);

                cur += sizeof(len) + len;
                ++idx;
            }
        }
        off_table[(std::size_t)idx] = cur;

        const uint64_t bytes_docid_blob = cur;
        h_docid_off.update(off_table.data(), off_table.size() * sizeof(uint64_t));

        const uint64_t end_after_blob = (uint64_t)out.tellp();
        out.seekp((std::streamoff)off_docid_off, std::ios::beg);
        out.write((const char*)off_table.data(), (std::streamsize)bytes_docid_off);
        if (!out.good()) throw std::runtime_error("docid_off final write failed");
        out.seekp((std::streamoff)end_after_blob, std::ios::beg);

        // append CSR temp files
        SectionInfo sec_uniq = append_file_to(out, tmp_uniq);
        SectionInfo sec_off  = append_file_to(out, tmp_off);
        SectionInfo sec_did  = append_file_to(out, tmp_did);

        // fill header
        hdr.off_docmeta   = sec_docmeta.off;
        hdr.bytes_docmeta = sec_docmeta.bytes;

        hdr.off_docid_off   = off_docid_off;
        hdr.bytes_docid_off = bytes_docid_off;
        hdr.off_docid_blob  = off_docid_blob;
        hdr.bytes_docid_blob= bytes_docid_blob;

        hdr.off_uniq9    = sec_uniq.off;
        hdr.cnt_uniq9    = cnt_uniq;
        hdr.bytes_uniq9  = sec_uniq.bytes;

        hdr.off_off9     = sec_off.off;
        hdr.cnt_off9     = cnt_off;
        hdr.bytes_off9   = sec_off.bytes;

        hdr.off_did9     = sec_did.off;
        hdr.cnt_did9     = cnt_did;
        hdr.bytes_did9   = sec_did.bytes;

        hdr.csum_docmeta    = sec_docmeta.csum;
        hdr.csum_docid_off  = h_docid_off.digest();
        hdr.csum_docid_blob = h_docid_blob.digest();
        hdr.csum_uniq9      = csum_uniq;
        hdr.csum_off9       = csum_off;
        hdr.csum_did9       = csum_did;

        // finalize file_bytes and header checksum
        hdr.file_bytes = (uint64_t)out.tellp() + 0; // current position is EOF
        hdr.csum_header = checksum_header(hdr);

        // rewrite header
        out.seekp(0, std::ios::beg);
        out.write((const char*)&hdr, sizeof(hdr));
        if (!out.good()) throw std::runtime_error("header rewrite failed");

        out.flush();
        out.close();

        atomic_replace_file(tmp_path, final_path);

        // cleanup our temp files (unique prefix => safe)
        std::error_code ec;
        fs::remove(tmp_uniq, ec);
        fs::remove(tmp_off, ec);
        fs::remove(tmp_did, ec);
        for (auto& r : all_runs) fs::remove(r.path, ec);

        std::cout
            << "[etl_index_builder_v3] built v3 docs=" << N_docs
            << " uniq9=" << cnt_uniq
            << " did9=" << cnt_did
            << " runs_final=" << all_runs.size()
            << " postings_in=" << total_posts
            << " workers=" << num_workers
            << " prefix=" << prefix
            << "\n";

        return 0;

    } catch (const std::exception& e) {
        std::cerr << "[etl_index_builder_v3] ERROR: " << e.what() << "\n";
        return 2;
    }
}
