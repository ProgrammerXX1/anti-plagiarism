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

namespace fs = std::filesystem;
using json = nlohmann::json;

static const std::string VERSION = "1.0.0-cpp";
static const int PORT = 8001;

// ── Supported extensions ──
static const std::vector<std::string> SUPPORTED_EXTS = {
    ".txt", ".docx", ".doc", ".pdf", ".odt", ".pptx", ".rtf"
};

// ── Merge runs by paragraph (for symbol/space detectors) ──
static std::vector<dn::TextRun> merge_runs_by_paragraph(const std::vector<dn::TextRun>& runs) {
    std::map<std::pair<int,int>, dn::TextRun> merged;
    for (auto& r : runs) {
        auto key = std::make_pair(r.page, r.paragraph);
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

// ── Parse file by extension ──
static dn::ParsedDocument parse_file(const std::string& filename, const std::string& content) {
    std::string ext;
    auto dot = filename.rfind('.');
    if (dot != std::string::npos) {
        ext = filename.substr(dot);
        std::transform(ext.begin(), ext.end(), ext.begin(), ::tolower);
    }

    if (ext == ".txt" || ext == ".rtf") {
        return dn::parse_txt(filename, content);
    } else if (ext == ".docx" || ext == ".doc") {
        return dn::parse_docx(filename, content);
    } else if (ext == ".pptx") {
        return dn::parse_pptx(filename, content);
    } else if (ext == ".odt") {
        return dn::parse_odt(filename, content);
    } else if (ext == ".pdf") {
        // PDF: extract text layer via poppler (no OCR, scans skipped)
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

    auto doc = parse_file(filename, content);

    dn::DetectionReport report;
    report.filename = filename;

    // Build full text from parsed runs
    {
        std::string full_text;
        int last_para = -1;
        for (auto& r : doc.runs) {
            if (r.paragraph != last_para) {
                if (last_para >= 0) full_text += '\n';
                last_para = r.paragraph;
            }
            full_text += r.text;
        }
        report.text = std::move(full_text);
    }

    // Merged runs for text-based detectors
    auto merged = merge_runs_by_paragraph(doc.runs);

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

    // 3) hidden_symbols — last (uses original runs for color info)
    if (opts.check_hidden) {
        auto ff = dn::detect_hidden(doc.runs);
        report.findings.insert(report.findings.end(),
            std::make_move_iterator(ff.begin()), std::make_move_iterator(ff.end()));
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

    // ── Thread pool: 2 workers (matches Docker cpus=2), max queued = 32 ──
    svr.new_task_queue = [] {
        return new httplib::ThreadPool(2, /*max_queued_requests=*/32);
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
    //          TXT/RTF → UTF-8 текст
    // Лимит: 50 МБ (set_payload_max_length)
    // Ответ содержит: text (полный текст), findings, total_findings, elapsed_sec, errors
    svr.Post("/detect", [](const httplib::Request& req, httplib::Response& res) {
        auto uf = extract_file(req);
        if (uf.filename.empty()) {
            res.status = 400;
            res.set_content(R"({"detail":"No file uploaded"})", "application/json");
            return;
        }

        try {
            auto opts = parse_options(req);
            auto report = run_detection(uf.filename, uf.content, opts);
            json j = report;
            res.set_content(j.dump(), "application/json");
        } catch (const std::exception& e) {
            res.status = 422;
            json j = {{"detail", std::string("Processing error: ") + e.what()}};
            res.set_content(j.dump(), "application/json");
        }
    });

    // ── POST /detect/symbols ──
    svr.Post("/detect/symbols", [](const httplib::Request& req, httplib::Response& res) {
        auto uf = extract_file(req);
        if (uf.filename.empty()) { res.status = 400; res.set_content(R"({"detail":"No file"})", "application/json"); return; }
        try {
            DetectOptions opts{true, false, false};
            auto report = run_detection(uf.filename, uf.content, opts);
            json j = report;
            res.set_content(j.dump(), "application/json");
        } catch (const std::exception& e) {
            res.status = 422;
            json j = {{"detail", std::string("Processing error: ") + e.what()}};
            res.set_content(j.dump(), "application/json");
        }
    });

    // ── POST /detect/spaces ──
    svr.Post("/detect/spaces", [](const httplib::Request& req, httplib::Response& res) {
        auto uf = extract_file(req);
        if (uf.filename.empty()) { res.status = 400; res.set_content(R"({"detail":"No file"})", "application/json"); return; }
        try {
            DetectOptions opts{false, true, false};
            auto report = run_detection(uf.filename, uf.content, opts);
            json j = report;
            res.set_content(j.dump(), "application/json");
        } catch (const std::exception& e) {
            res.status = 422;
            json j = {{"detail", std::string("Processing error: ") + e.what()}};
            res.set_content(j.dump(), "application/json");
        }
    });

    // ── POST /detect/hidden ──
    svr.Post("/detect/hidden", [](const httplib::Request& req, httplib::Response& res) {
        auto uf = extract_file(req);
        if (uf.filename.empty()) { res.status = 400; res.set_content(R"({"detail":"No file"})", "application/json"); return; }
        try {
            DetectOptions opts{false, false, true};
            auto report = run_detection(uf.filename, uf.content, opts);
            json j = report;
            res.set_content(j.dump(), "application/json");
        } catch (const std::exception& e) {
            res.status = 422;
            json j = {{"detail", std::string("Processing error: ") + e.what()}};
            res.set_content(j.dump(), "application/json");
        }
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
