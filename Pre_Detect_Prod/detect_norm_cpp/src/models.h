#pragma once
#include <string>
#include <vector>
#include <cstdint>
#include <nlohmann/json.hpp>

namespace dn {

// ── Text run (from parser) ──
struct TextRun {
    std::string text;
    int page = 1;
    int paragraph = 0;
    int offset = 0;
    std::string font_name;
    double font_size = 12.0;
    std::vector<int> color_rgb;       // [r,g,b] or empty
    std::vector<int> background_rgb;  // [r,g,b] or empty
    bool is_formula = false;          // true for OMML math runs
    bool is_vanish = false;           // true for w:vanish (hidden text in Word)
};

struct ParsedDocument {
    std::string filename;
    std::vector<TextRun> runs;
};

// ── Finding ──
enum class FraudType { change_word, spaces, hidden_symbols };

std::string fraud_type_str(FraudType ft);

struct Finding {
    FraudType fraud_type;
    std::string word;
    int paragraph = 0;
    int offset = 0;
    int limit = 1;
};

// ── Report ──
struct DetectionReport {
    std::string filename;
    std::string text;  // full extracted document text
    int total_findings = 0;
    std::vector<Finding> findings;
    double elapsed_sec = 0.0;
    std::vector<std::string> errors;
};

// JSON serialization
void to_json(nlohmann::json& j, const Finding& f);
void to_json(nlohmann::json& j, const DetectionReport& r);

} // namespace dn
