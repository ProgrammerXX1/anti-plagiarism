/*
 * PDF parser: extract text layer from PDF using poppler-cpp text_list() API.
 *
 * Micro-space detection strategy:
 * Uses per-LINE gap comparison to distinguish real double-spaces from justified
 * text alignment artifacts. In justified text, ALL gaps on a line are stretched
 * equally → per-line ratio ≈ 1.0. A genuine double-space makes ONE gap on the
 * line significantly wider than others → per-line ratio > 1.7.
 *
 * For short lines (< 3 gaps): uses page-level median with higher threshold (2.5x).
 */
#include "pdf_parser.h"

#include <poppler/cpp/poppler-document.h>
#include <poppler/cpp/poppler-page.h>

#include <limits>
#include <sstream>
#include <memory>
#include <algorithm>
#include <vector>
#include <cmath>
#include <signal.h>
#include <csetjmp>

namespace dn {

// Per-line threshold: gap must be this much wider than line median
static constexpr double LINE_RATIO_THRESHOLD = 2.0;
// Minimum gaps on a line for per-line analysis (need enough for reliable median)
static constexpr int MIN_LINE_GAPS = 4;
// For short lines, compare against page median with higher threshold
static constexpr double PAGE_RATIO_THRESHOLD = 3.0;
// Minimum samples for reliable page median
static constexpr size_t MIN_GAPS_FOR_MEDIAN = 15;
// Upper bound: gaps wider than this are table/column layout
static constexpr double MAX_RATIO_CAP = 6.0;

// Check if two bounding boxes are on the same text line
static bool same_line(const poppler::rectf& a, const poppler::rectf& b) {
    double mid_a = (a.top() + a.bottom()) / 2.0;
    double mid_b = (b.top() + b.bottom()) / 2.0;
    double h_a = std::fabs(a.height());
    double h_b = std::fabs(b.height());
    double tolerance = std::min(h_a, h_b) * 0.5;
    if (tolerance < 0.1) tolerance = 0.1;
    return std::fabs(mid_a - mid_b) < tolerance;
}

static double compute_median(std::vector<double>& v) {
    if (v.empty()) return 0.0;
    size_t n = v.size();
    auto mid = v.begin() + n / 2;
    std::nth_element(v.begin(), mid, v.end());
    return *mid;
}

// Info about a gap between two text boxes
struct GapInfo {
    double norm_gap;   // gap / font_size (font-size-normalized)
    size_t box_index;  // index of the box AFTER this gap
};

// A line is a sequence of boxes at the same Y level
struct LineInfo {
    std::vector<size_t> box_indices;  // indices into the boxes array
    std::vector<GapInfo> gaps;        // gaps between boxes on this line
};

// Check if a line looks like table data or reference numbering.
// Table lines: high ratio of digits/punctuation vs letters.
// Reference lines: "123  Author Name..." pattern.
// Returns true if the line should have double-spaces collapsed (not fraud).
static bool is_table_or_ref_line(const std::string& line) {
    if (line.empty()) return false;

    int letters = 0, digits = 0, punct = 0;
    for (unsigned char c : line) {
        if (c >= 0x80) {
            letters++;  // UTF-8 continuation / Cyrillic = letter
        } else if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')) {
            letters++;
        } else if (c >= '0' && c <= '9') {
            digits++;
        } else if (c == ',' || c == '.' || c == '-' || c == '/' || c == ':' || c == ';') {
            punct++;
        }
    }

    int total = letters + digits + punct;
    if (total == 0) return false;

    // Table data: >40% digits+punctuation
    double digit_punct_ratio = static_cast<double>(digits + punct) / total;
    if (digit_punct_ratio > 0.40) return true;

    // Reference numbering: starts with "123  " pattern (number + double space)
    size_t dbl = line.find("  ");
    if (dbl != std::string::npos && dbl <= 5) {
        bool prefix_digits = true;
        for (size_t i = 0; i < dbl; ++i) {
            if (line[i] < '0' || line[i] > '9') { prefix_digits = false; break; }
        }
        if (prefix_digits) return true;
    }

    // Table row pattern: "text  number" — double space before a numeric value
    // Common in PDF tables: "Лютесценс1148  1,0"
    if (dbl != std::string::npos) {
        // Check if text after "  " starts with digit or is mostly numeric
        size_t after = dbl + 2;
        if (after < line.size()) {
            int after_digits = 0, after_total = 0;
            for (size_t i = after; i < line.size() && after_total < 20; ++i) {
                unsigned char c = line[i];
                if (c >= '0' && c <= '9') { after_digits++; after_total++; }
                else if (c == ',' || c == '.' || c == '-') { after_total++; }
                else if (c == ' ') { /* skip */ }
                else { after_total++; }
            }
            // If part after double-space is >60% numeric → table cell
            if (after_total > 0 && static_cast<double>(after_digits) / after_total > 0.60)
                return true;
        }
    }

    return false;
}

// Collapse multiple spaces to single space in a string
static std::string collapse_spaces(const std::string& s) {
    std::string result;
    result.reserve(s.size());
    bool prev_sp = false;
    for (char c : s) {
        if (c == ' ') {
            if (!prev_sp) result += c;
            prev_sp = true;
        } else {
            result += c;
            prev_sp = false;
        }
    }
    return result;
}

// SIGSEGV guard: catch poppler segfaults in text_list() without fork().
// Uses sigsetjmp/siglongjmp — thread-safe via thread_local storage.
// If text_list() crashes, we fall back to text() for remaining pages.
static thread_local sigjmp_buf s_jmpbuf;
static thread_local volatile sig_atomic_t s_guarded = 0;

static void segv_guard_handler(int sig) {
    if (s_guarded) {
        siglongjmp(s_jmpbuf, 1);
    }
    // Not in guarded section — restore default and re-raise
    signal(sig, SIG_DFL);
    raise(sig);
}

ParsedDocument parse_pdf(const std::string& filename, const std::string& data) {
    ParsedDocument doc;
    doc.filename = filename;

    if (data.empty() || data.size() > static_cast<size_t>(std::numeric_limits<int>::max()))
        return doc;

    std::unique_ptr<poppler::document> pdf_doc(
        poppler::document::load_from_raw_data(data.data(), static_cast<int>(data.size())));
    if (!pdf_doc) return doc;

    int num_pages = pdf_doc->pages();
    int para_idx = 0;

    bool text_list_failed = false;

    // Install SIGSEGV/SIGBUS guard for poppler text_list() calls
    struct sigaction sa_new{}, sa_old_segv{}, sa_old_bus{};
    sa_new.sa_handler = segv_guard_handler;
    sigemptyset(&sa_new.sa_mask);
    sa_new.sa_flags = 0;
    sigaction(SIGSEGV, &sa_new, &sa_old_segv);
    sigaction(SIGBUS,  &sa_new, &sa_old_bus);

    for (int pi = 0; pi < num_pages; ++pi) {
        poppler::page* raw_pg = pdf_doc->create_page(pi);
        if (!raw_pg) continue;

        std::vector<poppler::text_box> boxes;
        if (!text_list_failed) {
            s_guarded = 1;
            if (sigsetjmp(s_jmpbuf, 1) == 0) {
                boxes = raw_pg->text_list(
                    static_cast<int>(poppler::page::text_list_include_font));
            } else {
                // SIGSEGV/SIGBUS caught — poppler crashed on this page
                text_list_failed = true;
                boxes.clear();
                // raw_pg is corrupted — leak it, reload document for fallback
                raw_pg = nullptr;
                pdf_doc.reset(poppler::document::load_from_raw_data(
                    data.data(), static_cast<int>(data.size())));
                if (!pdf_doc) { s_guarded = 0; break; }
                raw_pg = pdf_doc->create_page(pi);
            }
            s_guarded = 0;
        }
        std::unique_ptr<poppler::page> pg(raw_pg);

        if (boxes.empty()) {
            // Fallback: text() with normalization (no micro-space detection)
            poppler::ustring utext = pg->text();
            poppler::byte_array bytes = utext.to_utf8();
            std::string page_text(bytes.begin(), bytes.end());
            if (page_text.empty()) continue;

            std::istringstream iss(page_text);
            std::string line;
            while (std::getline(iss, line)) {
                if (line.empty()) continue;
                std::string normalized;
                normalized.reserve(line.size());
                bool prev_space = false;
                for (char c : line) {
                    if (c == ' ') {
                        if (!prev_space) normalized += c;
                        prev_space = true;
                    } else {
                        normalized += c;
                        prev_space = false;
                    }
                }
                while (!normalized.empty() && normalized.front() == ' ')
                    normalized.erase(normalized.begin());
                while (!normalized.empty() && normalized.back() == ' ')
                    normalized.pop_back();
                if (normalized.empty()) continue;

                TextRun run;
                run.text = std::move(normalized);
                run.page = pi + 1;
                run.paragraph = para_idx++;
                run.offset = 0;
                run.font_size = 12.0;
                doc.runs.push_back(std::move(run));
            }
            continue;
        }

        // ─── Phase 1: Group boxes into lines ───
        std::vector<LineInfo> lines;
        {
            LineInfo current_line;
            current_line.box_indices.push_back(0);

            for (size_t i = 1; i < boxes.size(); ++i) {
                auto r_prev = boxes[i - 1].bbox();
                auto r_curr = boxes[i].bbox();

                if (same_line(r_prev, r_curr)) {
                    // Same line — record gap (use physical gap, not just has_space_after)
                    double gap = r_curr.left() - r_prev.right();
                    double font_sz = boxes[i - 1].get_font_size();
                    if (font_sz <= 1.0) font_sz = 12.0;
                    double norm_gap = gap / font_sz;
                    if (norm_gap > 0.01) {
                        current_line.gaps.push_back({norm_gap, i});
                    }
                    current_line.box_indices.push_back(i);
                } else {
                    // New line
                    lines.push_back(std::move(current_line));
                    current_line = LineInfo{};
                    current_line.box_indices.push_back(i);
                }
            }
            if (!current_line.box_indices.empty()) {
                lines.push_back(std::move(current_line));
            }
        }

        // ─── Phase 2: Compute page median gap ───
        std::vector<double> all_page_gaps;
        for (auto& ln : lines) {
            for (auto& g : ln.gaps) {
                all_page_gaps.push_back(g.norm_gap);
            }
        }
        double page_median = 0.25;  // default
        if (all_page_gaps.size() >= MIN_GAPS_FOR_MEDIAN) {
            page_median = compute_median(all_page_gaps);
        }

        // ─── Phase 3: Identify anomalous gaps ───
        // Set of box indices AFTER which we should insert double-space
        std::vector<bool> is_micro_space(boxes.size(), false);

        for (auto& ln : lines) {
            if (ln.gaps.empty()) continue;

            if (static_cast<int>(ln.gaps.size()) >= MIN_LINE_GAPS) {
                // Per-line analysis: compare each gap against LINE median
                std::vector<double> line_gap_values;
                line_gap_values.reserve(ln.gaps.size());
                for (auto& g : ln.gaps) {
                    line_gap_values.push_back(g.norm_gap);
                }
                double line_med = compute_median(line_gap_values);

                if (line_med > 0.001) {
                    for (auto& g : ln.gaps) {
                        double ratio = g.norm_gap / line_med;
                        if (ratio > LINE_RATIO_THRESHOLD && ratio < MAX_RATIO_CAP) {
                            is_micro_space[g.box_index] = true;
                        }
                    }
                }
            } else {
                // Short line: use page median with stricter threshold
                if (page_median > 0.001) {
                    for (auto& g : ln.gaps) {
                        double ratio = g.norm_gap / page_median;
                        if (ratio > PAGE_RATIO_THRESHOLD && ratio < MAX_RATIO_CAP) {
                            is_micro_space[g.box_index] = true;
                        }
                    }
                }
            }
        }

        // ─── Phase 4: Reconstruct text, track per-line font size ───
        std::string page_text;
        page_text.reserve(boxes.size() * 8);
        std::vector<double> line_font_sizes;  // min font size per line
        double cur_line_min_fs = 99.0;

        for (size_t i = 0; i < boxes.size(); ++i) {
            auto utext = boxes[i].text();
            poppler::byte_array bytes = utext.to_utf8();
            std::string word(bytes.begin(), bytes.end());

            double fs = boxes[i].get_font_size();
            if (fs > 0 && fs < cur_line_min_fs) cur_line_min_fs = fs;

            if (i > 0) {
                auto r_prev = boxes[i - 1].bbox();
                auto r_curr = boxes[i].bbox();

                if (same_line(r_prev, r_curr)) {
                    if (boxes[i - 1].has_space_after()) {
                        if (is_micro_space[i]) {
                            page_text += "  ";  // double space → caught by detector
                        } else {
                            page_text += ' ';
                        }
                    } else {
                        // has_space_after() is false — check physical gap as fallback.
                        // Some PDFs don't set the space flag but have real gaps between words.
                        double gap = r_curr.left() - r_prev.right();
                        double font_sz = boxes[i - 1].get_font_size();
                        if (font_sz <= 1.0) font_sz = 12.0;
                        if (gap > font_sz * 0.15) {
                            page_text += ' ';
                        }
                    }
                } else {
                    page_text += '\n';
                    line_font_sizes.push_back(cur_line_min_fs);
                    cur_line_min_fs = (fs > 0) ? fs : 99.0;
                }
            }

            page_text += word;
        }
        line_font_sizes.push_back(cur_line_min_fs);  // last line

        // ─── Phase 5: Split into lines → TextRuns ───
        // For table/reference lines, collapse double spaces (they are layout artifacts)
        std::istringstream iss(page_text);
        std::string line_str;
        size_t line_idx = 0;
        while (std::getline(iss, line_str)) {
            if (line_str.empty()) { ++line_idx; continue; }
            auto start = line_str.find_first_not_of(' ');
            if (start == std::string::npos) { ++line_idx; continue; }
            auto end = line_str.find_last_not_of(' ');
            line_str = line_str.substr(start, end - start + 1);
            if (line_str.empty()) { ++line_idx; continue; }

            // Filter: if line looks like table data or reference numbering,
            // collapse double spaces (they're column gaps, not fraud)
            if (is_table_or_ref_line(line_str)) {
                line_str = collapse_spaces(line_str);
            }

            double fs = (line_idx < line_font_sizes.size()) ? line_font_sizes[line_idx] : 12.0;
            if (fs <= 0 || fs >= 99.0) fs = 12.0;

            TextRun run;
            run.text = std::move(line_str);
            run.page = pi + 1;
            run.paragraph = para_idx++;
            run.offset = 0;
            run.font_size = fs;
            doc.runs.push_back(std::move(run));
            ++line_idx;
        }
    }

    // Restore original signal handlers
    sigaction(SIGSEGV, &sa_old_segv, nullptr);
    sigaction(SIGBUS,  &sa_old_bus, nullptr);

    return doc;
}

} // namespace dn
