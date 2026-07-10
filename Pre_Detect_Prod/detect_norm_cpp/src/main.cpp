/*
 * detect_norm C++ — Anti-Fraud Detection Microservice
 *
 * High-performance reimplementation of the Python detect_norm service.
 * Uses cpp-httplib for HTTP, nlohmann/json for JSON, ICU for Unicode,
 * libzip + pugixml for DOCX parsing.
 *
 * Endpoints (same API as the Python version):
 *   GET  /health
 *   GET  /formats
 *   POST /detect          (full detection + paragraphs)
 *   POST /detect/symbols
 *   POST /detect/spaces
 *   POST /detect/hidden
 *   GET  /tests/list
 *   POST /tests/run/{file}
 *   GET  /tests/file/{file}
 *   GET  /                (serves static HTML)
 */

#include <httplib.h>
#include <nlohmann/json.hpp>

#include "models.h"
#include "detectors/symbol_substitution.h"
#include "detectors/spacing_detector.h"
#include "detectors/hidden_text_detector.h"
#include "parsers/txt_parser.h"
#include "parsers/rtf_parser.h"
#include "parsers/doc_parser.h"
#include "parsers/docx_parser.h"
#include "parsers/pptx_parser.h"
#include "parsers/odt_parser.h"
#include "parsers/pdf_parser.h"

#include <chrono>
#include <fstream>
#include <sstream>
#include <filesystem>
#include <algorithm>
#include <iostream>
#include <map>
#include <functional>
#include <stdexcept>
#include <mutex>
#include <ctime>
#include <cstring>
#include <iomanip>

namespace fs = std::filesystem;
using json = nlohmann::json;

static const std::string VERSION = "1.0.0-cpp";
static const int PORT = 8001;

// ── Request logging ──
static const std::string LOG_DIR = "/tmp/detect-norm-log";
static const int LOG_RETENTION_DAYS = 30;
static std::mutex log_mutex;

static std::string current_date_str() {
    auto now = std::chrono::system_clock::now();
    auto t = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
    localtime_r(&t, &tm);
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
    return buf;
}

static std::string current_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto t = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()) % 1000;
    std::tm tm{};
    localtime_r(&t, &tm);
    char buf[64];
    std::snprintf(buf, sizeof(buf), "%04d-%02d-%02dT%02d:%02d:%02d.%03d",
        tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
        tm.tm_hour, tm.tm_min, tm.tm_sec, static_cast<int>(ms.count()));
    return buf;
}

static void cleanup_old_logs() {
    try {
        if (!fs::exists(LOG_DIR)) return;
        auto now = std::chrono::system_clock::now();
        for (auto& entry : fs::directory_iterator(LOG_DIR)) {
            if (!entry.is_regular_file()) continue;
            auto ftime = fs::last_write_time(entry);
            auto sctp = decltype(ftime)::clock::now() - ftime;
            auto days = std::chrono::duration_cast<std::chrono::hours>(sctp).count() / 24;
            if (days > LOG_RETENTION_DAYS) {
                fs::remove(entry);
            }
        }
    } catch (...) {}
}

static void log_request(const std::string& endpoint, const std::string& filename,
                        const std::string& format, int findings_count,
                        double elapsed_sec, int text_chars, int file_bytes,
                        int status, const std::string& client_ip,
                        const std::string& user_agent,
                        const std::string& error = "")
{
    try {
        std::lock_guard<std::mutex> lock(log_mutex);
        fs::create_directories(LOG_DIR);

        std::string date = current_date_str();
        std::string log_path = LOG_DIR + "/" + date + ".jsonl";

        json entry = {
            {"timestamp", current_timestamp() + "Z"},
            {"endpoint", endpoint},
            {"filename", filename},
            {"preview", filename},
            {"format", format},
            {"findings", findings_count},
            {"time_sec", elapsed_sec},
            {"text_chars", text_chars},
            {"file_bytes", file_bytes},
            {"status", status},
            {"client_ip", client_ip},
            {"user_agent", user_agent},
            {"error", error.empty() ? json(nullptr) : json(error)},
        };

        std::ofstream ofs(log_path, std::ios::app);
        ofs << entry.dump() << '\n';
    } catch (...) {}
}

// ── Supported extensions ──
static const std::vector<std::string> SUPPORTED_EXTS = {
    ".txt", ".docx", ".doc", ".pdf", ".odt", ".pptx", ".rtf"
};

// ── Merge runs by paragraph (for symbol/space detectors) ──
static std::vector<dn::TextRun> merge_runs_by_paragraph(const std::vector<dn::TextRun>& runs) {
    // Merge runs per paragraph, keeping formula and non-formula runs separate
    // so that detectors can skip formula text (Latin variable names are normal there).
    // Key: (page, paragraph, is_formula)
    std::map<std::tuple<int,int,bool>, dn::TextRun> merged;
    for (auto& r : runs) {
        auto key = std::make_tuple(r.page, r.paragraph, r.is_formula);
        auto it = merged.find(key);
        if (it == merged.end()) {
            dn::TextRun m = r;
            m.offset = 0;
            merged[key] = std::move(m);
        } else {
            it->second.text += r.text;
        }
    }
    std::vector<dn::TextRun> result;
    result.reserve(merged.size());
    for (auto& [k, v] : merged) result.push_back(std::move(v));
    return result;
}

// ── OLE2 (legacy .doc) detection ──
static bool is_ole2(const std::string& data) {
    // OLE2 Compound Document magic: D0 CF 11 E0 A1 B1 1A E1
    static const unsigned char ole2_magic[] = {0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1};
    return data.size() >= 8 &&
           std::memcmp(data.data(), ole2_magic, 8) == 0;
}

// ── Parse file by extension ──
// Returns ParsedDocument; sets error string if parsing fails.
static dn::ParsedDocument parse_file(const std::string& filename, const std::string& content,
                                     std::string& error_out) {
    error_out.clear();
    std::string ext;
    auto dot = filename.rfind('.');
    if (dot != std::string::npos) {
        ext = filename.substr(dot);
        std::transform(ext.begin(), ext.end(), ext.begin(), ::tolower);
    }

    // Detect legacy OLE2 binary format (.doc)
    if (is_ole2(content)) {
        if (ext == ".pptx" || ext == ".odt") {
            error_out = "Файл '" + filename + "' имеет бинарный формат OLE2 (старый .doc/.ppt). "
                        "Сохраните документ в современном формате и загрузите повторно.";
            dn::ParsedDocument doc;
            doc.filename = filename;
            return doc;
        }
        // .doc / .docx with OLE2 magic → parse as legacy .doc via antiword
        return dn::parse_doc(filename, content, error_out);
    }

    if (ext == ".txt") {
        return dn::parse_txt(filename, content);
    } else if (ext == ".rtf") {
        return dn::parse_rtf(filename, content);
    } else if (ext == ".docx" || ext == ".doc") {
        auto doc = dn::parse_docx(filename, content);
        if (doc.runs.empty()) {
            error_out = "Не удалось извлечь текст из '" + filename + "'. "
                        "Файл повреждён или не является валидным DOCX (ZIP/OOXML).";
        }
        return doc;
    } else if (ext == ".pptx") {
        auto doc = dn::parse_pptx(filename, content);
        if (doc.runs.empty()) {
            error_out = "Не удалось извлечь текст из '" + filename + "'. "
                        "Файл повреждён или не является валидным PPTX.";
        }
        return doc;
    } else if (ext == ".odt") {
        auto doc = dn::parse_odt(filename, content);
        if (doc.runs.empty()) {
            error_out = "Не удалось извлечь текст из '" + filename + "'. "
                        "Файл повреждён или не является валидным ODT.";
        }
        return doc;
    } else if (ext == ".pdf") {
        return dn::parse_pdf(filename, content);
    } else {
        return dn::parse_txt(filename, content);
    }
}

// ── Run detection ──
struct DetectOptions {
    bool check_symbols = true;
    bool check_spaces  = true;
    bool check_hidden  = true;
};

static dn::DetectionReport run_detection(
    const std::string& filename, const std::string& content, const DetectOptions& opts)
{
    auto t0 = std::chrono::high_resolution_clock::now();

    std::string parse_error;
    auto doc = parse_file(filename, content, parse_error);

    dn::DetectionReport report;
    report.filename = filename;

    if (!parse_error.empty()) {
        report.errors.push_back(parse_error);
    }

    // Build full text from parsed runs (exclude vanish runs — hidden fraud text
    // like bidi control chars that split words; detected separately by hidden detector)
    {
        std::string full_text;
        int last_para = -1;
        for (auto& r : doc.runs) {
            if (r.is_vanish) continue;
            if (r.paragraph != last_para) {
                if (last_para >= 0) full_text += '\n';
                last_para = r.paragraph;
            }
            full_text += r.text;
        }
        report.text = std::move(full_text);
    }

    // Merged runs for text-based detectors (exclude vanish runs)
    // Visible runs for text-based detectors (symbol substitution, spaces)
    std::vector<dn::TextRun> visible_runs;
    visible_runs.reserve(doc.runs.size());
    for (auto& r : doc.runs) {
        if (!r.is_vanish) visible_runs.push_back(r);
    }
    auto merged = merge_runs_by_paragraph(visible_runs);

    // 1) change_word (symbol substitution) — always first
    if (opts.check_symbols) {
        auto ff = dn::detect_symbols(merged);
        report.findings.insert(report.findings.end(),
            std::make_move_iterator(ff.begin()), std::make_move_iterator(ff.end()));
    }

    // 2) spaces — second
    if (opts.check_spaces) {
        auto ff = dn::detect_spaces(merged);
        report.findings.insert(report.findings.end(),
            std::make_move_iterator(ff.begin()), std::make_move_iterator(ff.end()));
    }

    // 3) hidden_symbols — last (uses original runs including vanish for detection)
    if (opts.check_hidden) {
        auto ff = dn::detect_hidden(doc.runs);

        // Vanish runs were excluded from report.text so their content doesn't
        // exist in the output.  Adjust offsets and set limit=0 so downstream
        // consumers (dashboard) don't try to highlight non-existent characters.
        if (!ff.empty()) {
            // Build per-paragraph vanish byte accumulator
            std::map<int, std::vector<std::pair<int,int>>> vanish_adj;
            // Track which (paragraph, offset) pairs are vanish runs
            std::set<std::pair<int,int>> vanish_positions;
            {
                std::map<int, int> accum;
                for (auto& r : doc.runs) {
                    if (accum.find(r.paragraph) == accum.end()) accum[r.paragraph] = 0;
                    vanish_adj[r.paragraph].push_back({r.offset, accum[r.paragraph]});
                    if (r.is_vanish) {
                        vanish_positions.insert({r.paragraph, r.offset});
                        accum[r.paragraph] += static_cast<int>(r.text.size());
                    }
                }
            }

            for (auto& f : ff) {
                bool is_from_vanish = vanish_positions.count({f.paragraph, f.offset}) > 0;

                auto it = vanish_adj.find(f.paragraph);
                if (it != vanish_adj.end()) {
                    int adj = 0;
                    for (auto& [run_off, vb] : it->second) {
                        if (run_off <= f.offset) adj = vb;
                        else break;
                    }
                    f.offset -= adj;
                }

                // Vanish text doesn't exist in output — zero out limit
                // so dashboard won't highlight a visible character
                if (is_from_vanish) {
                    f.limit = 0;
                }
            }
        }

        report.findings.insert(report.findings.end(),
            std::make_move_iterator(ff.begin()), std::make_move_iterator(ff.end()));
    }

    // ── Convert byte offsets → global UTF-16 code-unit offsets ──
    // Detectors produce byte offsets relative to paragraph start.
    // Consumers index report.text with JavaScript string semantics
    // (UTF-16 code units), so astral codepoints (4-byte UTF-8) count as 2.
    {
        // Build paragraph char-start positions (global UTF-16 offset of each paragraph)
        std::vector<int> para_char_starts;
        {
            int char_pos = 0;
            size_t i = 0;
            int cur_para_start = 0;
            para_char_starts.push_back(0);
            while (i < report.text.size()) {
                if (report.text[i] == '\n') {
                    ++char_pos;
                    ++i;
                    para_char_starts.push_back(char_pos);
                } else {
                    auto c = static_cast<unsigned char>(report.text[i]);
                    i += (c >= 0xF0) ? 4 : (c >= 0xE0) ? 3 : (c >= 0xC0) ? 2 : 1;
                    char_pos += (c >= 0xF0) ? 2 : 1;  // astral = surrogate pair
                }
            }
        }

        // Build per-paragraph byte text for local byte→char conversion
        std::vector<std::string> para_texts;
        {
            size_t start = 0;
            for (size_t i = 0; i <= report.text.size(); ++i) {
                if (i == report.text.size() || report.text[i] == '\n') {
                    para_texts.emplace_back(report.text, start, i - start);
                    start = i + 1;
                }
            }
        }

        for (auto& f : report.findings) {
            int p = f.paragraph;
            if (p < 0 || p >= static_cast<int>(para_texts.size())) continue;

            // Convert local byte offset → local char offset
            const auto& pt = para_texts[p];
            int byte_off = f.offset;
            int local_char = 0, b = 0;
            int pt_len = static_cast<int>(pt.size());
            while (b < byte_off && b < pt_len) {
                auto c = static_cast<unsigned char>(pt[b]);
                b += (c >= 0xF0) ? 4 : (c >= 0xE0) ? 3 : (c >= 0xC0) ? 2 : 1;
                local_char += (c >= 0xF0) ? 2 : 1;  // astral = UTF-16 surrogate pair
            }

            // Convert to global char offset
            int global_start = (p < static_cast<int>(para_char_starts.size()))
                                   ? para_char_starts[p] : 0;
            f.offset = global_start + local_char;
        }
    }

    report.total_findings = static_cast<int>(report.findings.size());

    auto t1 = std::chrono::high_resolution_clock::now();
    report.elapsed_sec = std::chrono::duration<double>(t1 - t0).count();

    return report;
}

// ── Extract multipart file ──
struct UploadedFile {
    std::string filename;
    std::string content;
};

static UploadedFile extract_file(const httplib::Request& req) {
    UploadedFile uf;
    if (req.has_file("file")) {
        auto file = req.get_file_value("file");
        uf.filename = file.filename;
        uf.content = file.content;
    }
    return uf;
}

// ── Pagination params ──
// (removed — limit always = 1)

static DetectOptions parse_options(const httplib::Request& req) {
    DetectOptions opts;
    auto get_bool = [&](const std::string& key, bool def) -> bool {
        if (req.has_param(key)) {
            auto v = req.get_param_value(key);
            return v == "true" || v == "True" || v == "1" || v == "on";
        }
        return def;
    };
    opts.check_symbols = get_bool("check_symbols", true);
    opts.check_spaces  = get_bool("check_spaces", true);
    opts.check_hidden  = get_bool("check_hidden", true);
    return opts;
}

// ── Get file extension ──
static std::string get_ext(const std::string& filename) {
    auto dot = filename.rfind('.');
    if (dot == std::string::npos) return "";
    std::string ext = filename.substr(dot);
    std::transform(ext.begin(), ext.end(), ext.begin(), ::tolower);
    return ext;
}

// ── Get client IP from request ──
static std::string get_client_ip(const httplib::Request& req) {
    // Check X-Forwarded-For first (behind proxy)
    auto it = req.headers.find("X-Forwarded-For");
    if (it != req.headers.end() && !it->second.empty()) {
        // Take first IP from comma-separated list
        auto comma = it->second.find(',');
        return (comma != std::string::npos) ? it->second.substr(0, comma) : it->second;
    }
    return req.remote_addr;
}

// ── Get User-Agent ──
static std::string get_user_agent(const httplib::Request& req) {
    auto it = req.headers.find("User-Agent");
    return (it != req.headers.end()) ? it->second : "";
}

// ── Path-traversal guard ──
static bool is_safe_filename(const std::string& name) {
    if (name.empty()) return false;
    if (name.find("..") != std::string::npos) return false;
    if (name.find('/') != std::string::npos) return false;
    if (name.find('\\') != std::string::npos) return false;
    if (name[0] == '.') return false;
    return true;
}

// ── Find test directory ──
static std::string find_tests_dir() {
    // Look in standard locations
    for (auto& p : {
        fs::path("/app/tests"),
        fs::path("tests"),
        fs::path("../detect_norm/tests"),
    }) {
        if (fs::exists(p) && fs::is_directory(p)) return p.string();
    }
    return "tests";
}

// ══════════════════════════════════════════════
int main(int argc, char** argv) {
    int port = PORT;
    if (argc > 1) port = std::atoi(argv[1]);

    // Cleanup old request logs on startup
    cleanup_old_logs();

    std::string tests_dir = find_tests_dir();
    std::string static_dir;

    // Find static dir
    for (auto& p : {
        fs::path("/app/app/static"),
        fs::path("app/static"),
        fs::path("../detect_norm/app/static"),
    }) {
        if (fs::exists(p)) { static_dir = p.string(); break; }
    }

    httplib::Server svr;
    svr.set_payload_max_length(50 * 1024 * 1024);  // 50 MB

    // ── Timeouts (fault tolerance under slow clients) ──
    svr.set_read_timeout(30);   // 30 sec
    svr.set_write_timeout(30);  // 30 sec
    svr.set_idle_interval(5);   // keep-alive check

    // ── Thread pool: 4 workers (matches Docker cpus=4), max queued = 32 ──
    svr.new_task_queue = [] {
        return new httplib::ThreadPool(4, /*max_queued_requests=*/32);
    };

    // ── Global exception handler — catches any unhandled exception ──
    svr.set_exception_handler([](const httplib::Request&, httplib::Response& res, std::exception_ptr ep) {
        std::string msg = "Internal server error";
        try {
            if (ep) std::rethrow_exception(ep);
        } catch (const std::exception& e) {
            msg = e.what();
        } catch (...) {}
        json j = {{"detail", msg}};
        res.status = 500;
        res.set_content(j.dump(), "application/json");
    });

    // ── CORS ──
    svr.set_pre_routing_handler([](const httplib::Request&, httplib::Response& res) {
        res.set_header("Access-Control-Allow-Origin", "*");
        res.set_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        res.set_header("Access-Control-Allow-Headers", "*");
        return httplib::Server::HandlerResponse::Unhandled;
    });

    // ── GET /health ──
    svr.Get("/health", [](const httplib::Request&, httplib::Response& res) {
        json j = {{"status", "ok"}, {"version", VERSION}, {"engine", "C++17"}};
        res.set_content(j.dump(), "application/json");
    });

    // ── GET /formats ──
    svr.Get("/formats", [](const httplib::Request&, httplib::Response& res) {
        json j = {{"formats", SUPPORTED_EXTS}};
        res.set_content(j.dump(), "application/json");
    });

    // ── POST /detect ──
    // Форматы: .docx .doc .pptx .odt .rtf .txt .pdf
    // Парсеры: DOCX/DOC → ZIP+XML (pugixml), PPTX → ZIP+XML, ODT → ZIP+XML,
    //          PDF → poppler-cpp (текстовый слой, без OCR, сканы пропускаются),
    //          RTF → полный парсер (strip markup), TXT → UTF-8 текст
    // Лимит: 50 МБ (set_payload_max_length)
    // Ответ содержит: text (полный текст), findings, total_findings, elapsed_sec, errors
    svr.Post("/detect", [](const httplib::Request& req, httplib::Response& res) {
        auto uf = extract_file(req);
        if (uf.filename.empty()) {
            res.status = 400;
            res.set_content(R"({"detail":"No file uploaded"})", "application/json");
            return;
        }

        std::string ext = get_ext(uf.filename);
        std::string ip = get_client_ip(req);
        std::string ua = get_user_agent(req);
        int file_bytes = static_cast<int>(uf.content.size());

        try {
            auto opts = parse_options(req);
            auto report = run_detection(uf.filename, uf.content, opts);
            int text_chars = static_cast<int>(report.text.size());
            log_request("/detect", uf.filename, ext, report.total_findings,
                        report.elapsed_sec, text_chars, file_bytes, 200, ip, ua);
            json j = report;
            res.set_content(j.dump(), "application/json");
        } catch (const std::exception& e) {
            log_request("/detect", uf.filename, ext, 0, 0, 0, file_bytes, 422, ip, ua, e.what());
            res.status = 422;
            json j = {{"detail", std::string("Processing error: ") + e.what()}};
            res.set_content(j.dump(), "application/json");
        }
    });

    // ── POST /detect/symbols ──
    svr.Post("/detect/symbols", [](const httplib::Request& req, httplib::Response& res) {
        auto uf = extract_file(req);
        if (uf.filename.empty()) { res.status = 400; res.set_content(R"({"detail":"No file"})", "application/json"); return; }
        std::string ext = get_ext(uf.filename);
        std::string ip = get_client_ip(req);
        std::string ua = get_user_agent(req);
        int file_bytes = static_cast<int>(uf.content.size());
        try {
            DetectOptions opts{true, false, false};
            auto report = run_detection(uf.filename, uf.content, opts);
            int text_chars = static_cast<int>(report.text.size());
            log_request("/detect/symbols", uf.filename, ext, report.total_findings,
                        report.elapsed_sec, text_chars, file_bytes, 200, ip, ua);
            json j = report;
            res.set_content(j.dump(), "application/json");
        } catch (const std::exception& e) {
            log_request("/detect/symbols", uf.filename, ext, 0, 0, 0, file_bytes, 422, ip, ua, e.what());
            res.status = 422;
            json j = {{"detail", std::string("Processing error: ") + e.what()}};
            res.set_content(j.dump(), "application/json");
        }
    });

    // ── POST /detect/spaces ──
    svr.Post("/detect/spaces", [](const httplib::Request& req, httplib::Response& res) {
        auto uf = extract_file(req);
        if (uf.filename.empty()) { res.status = 400; res.set_content(R"({"detail":"No file"})", "application/json"); return; }
        std::string ext = get_ext(uf.filename);
        std::string ip = get_client_ip(req);
        std::string ua = get_user_agent(req);
        int file_bytes = static_cast<int>(uf.content.size());
        try {
            DetectOptions opts{false, true, false};
            auto report = run_detection(uf.filename, uf.content, opts);
            int text_chars = static_cast<int>(report.text.size());
            log_request("/detect/spaces", uf.filename, ext, report.total_findings,
                        report.elapsed_sec, text_chars, file_bytes, 200, ip, ua);
            json j = report;
            res.set_content(j.dump(), "application/json");
        } catch (const std::exception& e) {
            log_request("/detect/spaces", uf.filename, ext, 0, 0, 0, file_bytes, 422, ip, ua, e.what());
            res.status = 422;
            json j = {{"detail", std::string("Processing error: ") + e.what()}};
            res.set_content(j.dump(), "application/json");
        }
    });

    // ── POST /detect/hidden ──
    svr.Post("/detect/hidden", [](const httplib::Request& req, httplib::Response& res) {
        auto uf = extract_file(req);
        if (uf.filename.empty()) { res.status = 400; res.set_content(R"({"detail":"No file"})", "application/json"); return; }
        std::string ext = get_ext(uf.filename);
        std::string ip = get_client_ip(req);
        std::string ua = get_user_agent(req);
        int file_bytes = static_cast<int>(uf.content.size());
        try {
            DetectOptions opts{false, false, true};
            auto report = run_detection(uf.filename, uf.content, opts);
            int text_chars = static_cast<int>(report.text.size());
            log_request("/detect/hidden", uf.filename, ext, report.total_findings,
                        report.elapsed_sec, text_chars, file_bytes, 200, ip, ua);
            json j = report;
            res.set_content(j.dump(), "application/json");
        } catch (const std::exception& e) {
            log_request("/detect/hidden", uf.filename, ext, 0, 0, 0, file_bytes, 422, ip, ua, e.what());
            res.status = 422;
            json j = {{"detail", std::string("Processing error: ") + e.what()}};
            res.set_content(j.dump(), "application/json");
        }
    });

    // ── GET /monitor/requests — read JSONL logs for dashboard ──
    svr.Get("/monitor/requests", [](const httplib::Request& req, httplib::Response& res) {
        int limit = 50, offset = 0;
        std::string sort_by = "timestamp", sort_order = "desc";
        if (req.has_param("limit"))      limit = std::atoi(req.get_param_value("limit").c_str());
        if (req.has_param("offset"))     offset = std::atoi(req.get_param_value("offset").c_str());
        if (req.has_param("sort_by"))    sort_by = req.get_param_value("sort_by");
        if (req.has_param("sort_order")) sort_order = req.get_param_value("sort_order");
        if (limit < 1) limit = 50;
        if (limit > 500) limit = 500;
        if (offset < 0) offset = 0;

        // Read all JSONL log files (most recent first)
        std::vector<json> all_entries;
        try {
            if (fs::exists(LOG_DIR) && fs::is_directory(LOG_DIR)) {
                std::vector<std::string> log_files;
                for (auto& entry : fs::directory_iterator(LOG_DIR)) {
                    if (entry.is_regular_file() && entry.path().extension() == ".jsonl")
                        log_files.push_back(entry.path().string());
                }
                std::sort(log_files.rbegin(), log_files.rend()); // newest first

                for (auto& lf : log_files) {
                    std::ifstream ifs(lf);
                    std::string line;
                    while (std::getline(ifs, line)) {
                        if (line.empty()) continue;
                        try { all_entries.push_back(json::parse(line)); } catch (...) {}
                    }
                }
            }
        } catch (...) {}

        // Sort
        if (sort_by == "timestamp") {
            std::sort(all_entries.begin(), all_entries.end(), [&](const json& a, const json& b) {
                auto ta = a.value("timestamp", ""), tb = b.value("timestamp", "");
                return sort_order == "desc" ? ta > tb : ta < tb;
            });
        } else if (sort_by == "time_sec") {
            std::sort(all_entries.begin(), all_entries.end(), [&](const json& a, const json& b) {
                auto ta = a.value("time_sec", 0.0), tb = b.value("time_sec", 0.0);
                return sort_order == "desc" ? ta > tb : ta < tb;
            });
        } else if (sort_by == "findings") {
            std::sort(all_entries.begin(), all_entries.end(), [&](const json& a, const json& b) {
                auto ta = a.value("findings", 0), tb = b.value("findings", 0);
                return sort_order == "desc" ? ta > tb : ta < tb;
            });
        } else if (sort_by == "file_bytes") {
            std::sort(all_entries.begin(), all_entries.end(), [&](const json& a, const json& b) {
                auto ta = a.value("file_bytes", 0), tb = b.value("file_bytes", 0);
                return sort_order == "desc" ? ta > tb : ta < tb;
            });
        }

        int total = static_cast<int>(all_entries.size());
        int total_errors = 0;
        double sum_time = 0;
        for (auto& e : all_entries) {
            if (e.value("status", 200) != 200) ++total_errors;
            sum_time += e.value("time_sec", 0.0);
        }
        double avg_time = total > 0 ? sum_time / total : 0;
        int total_pages = (total + limit - 1) / limit;
        int current_page = offset / limit + 1;

        // Paginate
        json page_entries = json::array();
        for (int i = offset; i < std::min(offset + limit, total); ++i) {
            page_entries.push_back(all_entries[i]);
        }

        json result = {
            {"total", total},
            {"total_errors", total_errors},
            {"avg_time_sec", avg_time},
            {"total_pages", total_pages},
            {"current_page", current_page},
            {"requests", page_entries}
        };
        res.set_content(result.dump(), "application/json");
    });

    // ── GET /tests/list ──
    svr.Get("/tests/list", [&tests_dir](const httplib::Request&, httplib::Response& res) {
        std::vector<std::string> files;
        try {
            for (auto& entry : fs::directory_iterator(tests_dir)) {
                if (entry.is_regular_file()) {
                    auto ext = entry.path().extension().string();
                    std::transform(ext.begin(), ext.end(), ext.begin(), ::tolower);
                    if (ext == ".docx" || ext == ".doc" || ext == ".pptx" ||
                        ext == ".odt" || ext == ".rtf" || ext == ".txt" || ext == ".pdf") {
                        files.push_back(entry.path().filename().string());
                    }
                }
            }
        } catch (...) {}
        std::sort(files.begin(), files.end());
        json j = {{"files", files}};
        res.set_content(j.dump(), "application/json");
    });

    // ── POST /tests/run/{file} ──
    svr.Post(R"(/tests/run/(.+))", [&tests_dir](const httplib::Request& req, httplib::Response& res) {
        auto filename = req.matches[1].str();

        // Path-traversal guard
        if (!is_safe_filename(filename)) {
            res.status = 400;
            res.set_content(R"({"detail":"Invalid filename"})", "application/json");
            return;
        }

        auto path = fs::path(tests_dir) / filename;

        if (!fs::exists(path)) {
            res.status = 404;
            res.set_content(R"({"detail":"Test file not found"})", "application/json");
            return;
        }

        try {
            // Read file
            std::ifstream ifs(path, std::ios::binary);
            std::string content((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());

            DetectOptions opts{true, true, true};
            auto report = run_detection(filename, content, opts);

            json j = report;
            res.set_content(j.dump(), "application/json");
        } catch (const std::exception& e) {
            res.status = 422;
            json j = {{"detail", std::string("Processing error: ") + e.what()}};
            res.set_content(j.dump(), "application/json");
        }
    });

    // ── GET /tests/file/{file} ──
    svr.Get(R"(/tests/file/(.+))", [&tests_dir](const httplib::Request& req, httplib::Response& res) {
        auto filename = req.matches[1].str();

        // Path-traversal guard
        if (!is_safe_filename(filename)) {
            res.status = 400;
            res.set_content(R"({"detail":"Invalid filename"})", "application/json");
            return;
        }

        auto path = fs::path(tests_dir) / filename;

        if (!fs::exists(path)) {
            res.status = 404;
            res.set_content(R"({"detail":"Not found"})", "application/json");
            return;
        }

        std::ifstream ifs(path, std::ios::binary);
        std::string content((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
        res.set_content(content, "application/octet-stream");
        res.set_header("Content-Disposition", "attachment; filename=\"" + filename + "\"");
    });

    // ── GET /openapi.json — OpenAPI 3.1 spec ──
    svr.Get("/openapi.json", [](const httplib::Request&, httplib::Response& res) {
        json spec = {
            {"openapi", "3.1.0"},
            {"info", {
                {"title", "detect_norm — Anti-Fraud Detection (C++)"},
                {"description", "Сервис обнаружения мошенничества в документах: подмена символов, аномальные пробелы, скрытый текст.\n\nФорматы: DOCX, DOC, PPTX, ODT, RTF, TXT, PDF (текстовый слой, без OCR).\nЛимит: 50 МБ на файл.\nEngine: C++17, ICU, libzip, pugixml."},
                {"version", VERSION}
            }},
            {"servers", json::array({{{"url", "/"}}})},
            {"paths", {
                {"/health", {
                    {"get", {
                        {"summary", "Health Check"},
                        {"operationId", "health"},
                        {"tags", json::array({"System"})},
                        {"responses", {{"200", {
                            {"description", "OK"},
                            {"content", {{"application/json", {{"schema", {
                                {"type", "object"},
                                {"properties", {
                                    {"status", {{"type", "string"}}},
                                    {"version", {{"type", "string"}}},
                                    {"engine", {{"type", "string"}}}
                                }}
                            }}}}}}
                        }}}}
                    }}
                }},
                {"/formats", {
                    {"get", {
                        {"summary", "Список поддерживаемых форматов"},
                        {"operationId", "formats"},
                        {"tags", json::array({"System"})},
                        {"responses", {{"200", {
                            {"description", "OK"},
                            {"content", {{"application/json", {{"schema", {
                                {"type", "object"},
                                {"properties", {{"formats", {{"type", "array"}, {"items", {{"type", "string"}}}}}}}
                            }}}}}}
                        }}}}
                    }}
                }},
                {"/detect", {
                    {"post", {
                        {"summary", "Полная проверка файла"},
                        {"description", "Проверка по 3 категориям: change_word, spaces, hidden_symbols. Порядок: change_word → spaces → hidden_symbols.\n\nФорматы: .docx, .doc, .pptx, .odt, .rtf, .txt, .pdf\nПарсеры: DOCX/DOC → ZIP+XML (pugixml), PPTX → ZIP+XML (слайды), ODT → ZIP+XML (стили), PDF → poppler-cpp (текстовый слой, без OCR, сканы пропускаются), TXT/RTF → UTF-8.\nЛимит: 50 МБ на файл.\nОтвет содержит поле text — полный извлечённый текст документа (сканы/изображения пропускаются)."},
                        {"operationId", "detect_all"},
                        {"tags", json::array({"Detection"})},
                        {"parameters", json::array({
                            {{"name", "check_symbols"}, {"in", "query"}, {"required", false}, {"schema", {{"type", "boolean"}, {"default", true}}}, {"description", "Проверять change_word"}},
                            {{"name", "check_spaces"}, {"in", "query"}, {"required", false}, {"schema", {{"type", "boolean"}, {"default", true}}}, {"description", "Проверять spaces"}},
                            {{"name", "check_hidden"}, {"in", "query"}, {"required", false}, {"schema", {{"type", "boolean"}, {"default", true}}}, {"description", "Проверять hidden_symbols"}}
                        })},
                        {"requestBody", {
                            {"required", true},
                            {"content", {{"multipart/form-data", {{"schema", {
                                {"type", "object"},
                                {"required", json::array({"file"})},
                                {"properties", {{"file", {{"type", "string"}, {"format", "binary"}, {"description", "Файл для проверки (.docx .doc .pptx .odt .rtf .txt .pdf). Лимит: 50 МБ"}}}}}
                            }}}}}}
                        }},
                        {"responses", {
                            {"200", {
                                {"description", "Результат проверки: text + findings"},
                                {"content", {{"application/json", {{"schema", {{"$ref", "#/components/schemas/DetectionReport"}}}}}}}
                            }},
                            {"400", {{"description", "Файл не загружен"}}}
                        }}
                    }}
                }},
                {"/detect/symbols", {
                    {"post", {
                        {"summary", "Только change_word (подмена символов)"},
                        {"operationId", "detect_symbols"},
                        {"tags", json::array({"Detection"})},
                        {"requestBody", {
                            {"required", true},
                            {"content", {{"multipart/form-data", {{"schema", {
                                {"type", "object"},
                                {"required", json::array({"file"})},
                                {"properties", {{"file", {{"type", "string"}, {"format", "binary"}}}}}
                            }}}}}}
                        }},
                        {"responses", {{"200", {{"description", "OK"}, {"content", {{"application/json", {{"schema", {{"$ref", "#/components/schemas/DetectionReport"}}}}}}}}}}}
                    }}
                }},
                {"/detect/spaces", {
                    {"post", {
                        {"summary", "Только spaces (аномальные пробелы)"},
                        {"operationId", "detect_spaces"},
                        {"tags", json::array({"Detection"})},
                        {"requestBody", {
                            {"required", true},
                            {"content", {{"multipart/form-data", {{"schema", {
                                {"type", "object"},
                                {"required", json::array({"file"})},
                                {"properties", {{"file", {{"type", "string"}, {"format", "binary"}}}}}
                            }}}}}}
                        }},
                        {"responses", {{"200", {{"description", "OK"}, {"content", {{"application/json", {{"schema", {{"$ref", "#/components/schemas/DetectionReport"}}}}}}}}}}}
                    }}
                }},
                {"/detect/hidden", {
                    {"post", {
                        {"summary", "Только hidden_symbols (скрытый текст)"},
                        {"operationId", "detect_hidden"},
                        {"tags", json::array({"Detection"})},
                        {"requestBody", {
                            {"required", true},
                            {"content", {{"multipart/form-data", {{"schema", {
                                {"type", "object"},
                                {"required", json::array({"file"})},
                                {"properties", {{"file", {{"type", "string"}, {"format", "binary"}}}}}
                            }}}}}}
                        }},
                        {"responses", {{"200", {{"description", "OK"}, {"content", {{"application/json", {{"schema", {{"$ref", "#/components/schemas/DetectionReport"}}}}}}}}}}}
                    }}
                }},
                {"/tests/list", {
                    {"get", {
                        {"summary", "Список тестовых файлов"},
                        {"operationId", "tests_list"},
                        {"tags", json::array({"Tests"})},
                        {"responses", {{"200", {{"description", "OK"}, {"content", {{"application/json", {{"schema", {
                            {"type", "object"},
                            {"properties", {{"files", {{"type", "array"}, {"items", {{"type", "string"}}}}}}}
                        }}}}}}}}}}
                    }}
                }},
                {"/tests/run/{filename}", {
                    {"post", {
                        {"summary", "Запустить тест на файле"},
                        {"operationId", "tests_run"},
                        {"tags", json::array({"Tests"})},
                        {"parameters", json::array({
                            {{"name", "filename"}, {"in", "path"}, {"required", true}, {"schema", {{"type", "string"}}}}
                        })},
                        {"responses", {
                            {"200", {{"description", "Результат"}, {"content", {{"application/json", {{"schema", {{"$ref", "#/components/schemas/DetectionReport"}}}}}}}}},
                            {"404", {{"description", "Файл не найден"}}}
                        }}
                    }}
                }},
                {"/tests/file/{filename}", {
                    {"get", {
                        {"summary", "Скачать тестовый файл"},
                        {"operationId", "tests_download"},
                        {"tags", json::array({"Tests"})},
                        {"parameters", json::array({{{"name", "filename"}, {"in", "path"}, {"required", true}, {"schema", {{"type", "string"}}}}})},
                        {"responses", {
                            {"200", {{"description", "Файл"}, {"content", {{"application/octet-stream", {{"schema", {{"type", "string"}, {"format", "binary"}}}}}}}}},
                            {"404", {{"description", "Не найден"}}}
                        }}
                    }}
                }}
            }},
            {"components", {
                {"schemas", {
                    {"Finding", {
                        {"type", "object"},
                        {"properties", {
                            {"word", {{"type", "string"}, {"description", "Слово/текст с нарушением"}}},
                            {"offset", {{"type", "integer"}, {"description", "Точная позиция (байт) в документе"}}},
                            {"limit", {{"type", "integer"}, {"description", "Длина нарушения: кол-во символов (spaces), всегда 1 (change_word), длина текста (hidden_symbols)"}}},
                            {"fraud_type", {{"type", "string"}, {"enum", json::array({"change_word", "spaces", "hidden_symbols"})}, {"description", "change_word → spaces → hidden_symbols"}}}
                        }}
                    }},
                    {"DetectionReport", {
                        {"type", "object"},
                        {"properties", {
                            {"filename", {{"type", "string"}}},
                            {"text", {{"type", "string"}, {"description", "Полный извлечённый текст документа. Абзацы разделены \\n. Сканы/изображения пропускаются"}}},
                            {"total_findings", {{"type", "integer"}}},
                            {"findings", {{"type", "array"}, {"items", {{"$ref", "#/components/schemas/Finding"}}}}},
                            {"elapsed_sec", {{"type", "number"}}},
                            {"errors", {{"type", "array"}, {"items", {{"type", "string"}}}}}
                        }}
                    }}
                }}
            }}
        };
        res.set_content(spec.dump(), "application/json");
    });

    // ── GET /docs — Swagger UI ──
    svr.Get("/docs", [](const httplib::Request&, httplib::Response& res) {
        std::string html = R"(<!DOCTYPE html>
<html>
<head>
<title>detect_norm C++ — Swagger UI</title>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
<div id="swagger-ui"></div>
<script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
<script>
SwaggerUIBundle({
    url: window.location.origin + '/openapi.json',
    dom_id: '#swagger-ui',
    presets: [SwaggerUIBundle.presets.apis, SwaggerUIBundle.SwaggerUIStandalonePreset],
    layout: 'BaseLayout',
    deepLinking: true,
    defaultModelsExpandDepth: 1,
    defaultModelExpandDepth: 1
});
</script>
</body>
</html>)";
        res.set_content(html, "text/html; charset=utf-8");
    });

    // ── GET /redoc — ReDoc alternative ──
    svr.Get("/redoc", [](const httplib::Request&, httplib::Response& res) {
        std::string html = R"(<!DOCTYPE html>
<html>
<head>
<title>detect_norm C++ — ReDoc</title>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1">
</head>
<body>
<redoc spec-url='/openapi.json'></redoc>
<script src="https://cdn.jsdelivr.net/npm/redoc@latest/bundles/redoc.standalone.js"></script>
</body>
</html>)";
        res.set_content(html, "text/html; charset=utf-8");
    });

    // ── GET / — serve static HTML ──
    svr.Get("/", [&static_dir](const httplib::Request&, httplib::Response& res) {
        auto path = fs::path(static_dir) / "index.html";
        if (!fs::exists(path)) {
            res.set_content("<h1>detect_norm C++</h1><p>No index.html found</p>", "text/html");
            return;
        }
        std::ifstream ifs(path);
        std::string html((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
        res.set_content(html, "text/html; charset=utf-8");
    });

    std::cout << "detect_norm C++ listening on port " << port
              << " (tests: " << tests_dir << ", static: " << static_dir << ")" << std::endl;

    svr.listen("0.0.0.0", port);
    return 0;
}
