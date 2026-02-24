/*  convert_v2v3.cpp  –  offline converter: v2 index_native.bin → v3
 *
 *  v2 Posting = { uint64_t h; uint32_t did; uint32_t pos; } = 16 bytes
 *  v3 Posting = { uint32_t h; uint32_t did; uint32_t pos; } = 12 bytes
 *
 *  After truncating h64→h32 the sort order changes, so we re-sort.
 *
 *  Build:  g++ -O2 -std=c++17 -o convert_v2v3 convert_v2v3.cpp -lpthread
 *  Usage:  sudo ./convert_v2v3 <dir>          (recursively find index_native.bin)
 *          sudo ./convert_v2v3 <file.bin>     (convert single file)
 */
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>
#include <chrono>

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

namespace fs = std::filesystem;
using clk = std::chrono::steady_clock;

static constexpr uint32_t MAGIC = 0x47414C50; // "PLAG" little-endian
static constexpr size_t   HDR   = 28;
static constexpr size_t   DMETA = 20;

#pragma pack(push, 1)
struct Header {
    uint32_t magic;
    uint32_t version;
    uint32_t n_docs;
    uint64_t n_post9;
    uint64_t n_post13;
};
static_assert(sizeof(Header) == HDR);

struct PostV2 { uint64_t h; uint32_t did; uint32_t pos; };
static_assert(sizeof(PostV2) == 16);

struct PostV3 { uint32_t h; uint32_t did; uint32_t pos; };
static_assert(sizeof(PostV3) == 12);
#pragma pack(pop)

static double elapsed(clk::time_point t0) {
    return std::chrono::duration<double>(clk::now() - t0).count();
}

static bool convert_one(const fs::path& path) {
    // Read header
    std::ifstream fin(path, std::ios::binary);
    if (!fin) { fprintf(stderr, "  SKIP (cannot open): %s\n", path.c_str()); return false; }

    Header hdr{};
    fin.read(reinterpret_cast<char*>(&hdr), HDR);
    if (!fin || hdr.magic != MAGIC) { fprintf(stderr, "  SKIP (bad magic): %s\n", path.c_str()); return false; }
    if (hdr.version == 3) { printf("  SKIP (already v3): %s\n", path.c_str()); return false; }
    if (hdr.version != 2) { fprintf(stderr, "  SKIP (version %u): %s\n", hdr.version, path.c_str()); return false; }
    fin.close();

    const size_t n = (size_t)hdr.n_post9;
    const size_t file_exp = HDR + (size_t)hdr.n_docs * DMETA + n * 16;
    const size_t file_sz  = (size_t)fs::file_size(path);
    if (file_sz != file_exp) {
        fprintf(stderr, "  SKIP (size %zu vs expected %zu): %s\n", file_sz, file_exp, path.c_str());
        return false;
    }

    printf("  %s\n", path.c_str());
    printf("    n_docs=%u  n_post9=%zu  (%.2f GiB → ~%.2f GiB)\n",
           hdr.n_docs, n,
           (double)file_sz / (1ULL << 30),
           (double)(HDR + (size_t)hdr.n_docs * DMETA + n * 12) / (1ULL << 30));

    auto t0 = clk::now();

    // mmap the whole file read-only
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror("open"); return false; }
    void* base = mmap(nullptr, file_sz, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (base == MAP_FAILED) { perror("mmap"); return false; }
    madvise(base, file_sz, MADV_SEQUENTIAL);

    const auto* v2 = reinterpret_cast<const PostV2*>(
        static_cast<const char*>(base) + HDR + (size_t)hdr.n_docs * DMETA);

    // Convert to v3 vector
    std::vector<PostV3> v3(n);
    for (size_t i = 0; i < n; ++i) {
        v3[i] = PostV3{(uint32_t)v2[i].h, v2[i].did, v2[i].pos};
    }

    // Release mmap — we only need v3 from here
    munmap(base, file_sz);

    printf("    read+convert: %.1fs\n", elapsed(t0));
    auto t1 = clk::now();

    // Sort by (h, did, pos)
    std::sort(v3.begin(), v3.end(), [](const PostV3& a, const PostV3& b) {
        if (a.h != b.h) return a.h < b.h;
        if (a.did != b.did) return a.did < b.did;
        return a.pos < b.pos;
    });

    printf("    sort: %.1fs\n", elapsed(t1));
    auto t2 = clk::now();

    // Write to temp file then atomic rename
    const auto tmp = path.string() + ".v3tmp";
    {
        std::ofstream fout(tmp, std::ios::binary);
        if (!fout) { perror("open tmp"); return false; }

        // Write v3 header
        Header hdr3 = hdr;
        hdr3.version = 3;
        fout.write(reinterpret_cast<const char*>(&hdr3), HDR);

        // Copy docmeta from original (re-read from disk since mmap is gone)
        {
            std::ifstream orig(path, std::ios::binary);
            orig.seekg(HDR);
            std::vector<char> dm((size_t)hdr.n_docs * DMETA);
            orig.read(dm.data(), (std::streamsize)dm.size());
            fout.write(dm.data(), (std::streamsize)dm.size());
        }

        // Write postings
        fout.write(reinterpret_cast<const char*>(v3.data()),
                    (std::streamsize)(n * sizeof(PostV3)));
    }

    // Atomic rename
    fs::rename(tmp, path);

    const size_t new_sz = (size_t)fs::file_size(path);
    const size_t exp_v3 = HDR + (size_t)hdr.n_docs * DMETA + n * 12;
    const char* status = (new_sz == exp_v3) ? "OK" : "SIZE MISMATCH";

    printf("    write+rename: %.1fs  total: %.1fs  → %.2f GiB  [%s]\n",
           elapsed(t2), elapsed(t0),
           (double)new_sz / (1ULL << 30), status);
    return (new_sz == exp_v3);
}

int main(int argc, char** argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <dir-or-file> [--dry-run]\n", argv[0]);
        return 1;
    }
    const bool dry_run = (argc >= 3 && std::string(argv[2]) == "--dry-run");

    fs::path target(argv[1]);
    std::vector<fs::path> files;

    if (fs::is_directory(target)) {
        for (auto& e : fs::recursive_directory_iterator(target)) {
            if (e.is_regular_file() && e.path().filename() == "index_native.bin")
                files.push_back(e.path());
        }
        std::sort(files.begin(), files.end());
    } else {
        files.push_back(target);
    }

    printf("Found %zu index_native.bin files\n", files.size());
    if (dry_run) printf("(DRY RUN)\n");

    int ok = 0, skip = 0, fail = 0;
    auto t_all = clk::now();

    for (size_t i = 0; i < files.size(); ++i) {
        printf("\n[%zu/%zu]\n", i + 1, files.size());
        if (dry_run) {
            printf("  %s\n", files[i].c_str());
            continue;
        }
        if (convert_one(files[i]))
            ++ok;
        else
            ++skip; // might be skip or fail, rough count
    }

    printf("\n=== DONE in %.0fs ===\n", elapsed(t_all));
    printf("  converted: %d  other: %d\n", ok, skip);
    return 0;
}
