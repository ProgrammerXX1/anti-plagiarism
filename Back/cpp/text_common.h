// cpp/text_common.h
#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <functional>

// Простая нормализация под шинглы:
// - ASCII lower-case
// - всё, что не [a-z0-9_], превращаем в пробел
// - схлопываем пробелы
inline std::string normalize_for_shingles_simple(const std::string& in) {
    std::string out;
    out.reserve(in.size());
    bool prev_space = false;
    for (unsigned char c : in) {
        unsigned char cc = c;
        if (cc >= 'A' && cc <= 'Z') {
            cc = static_cast<unsigned char>(cc - 'A' + 'a');
        }
        bool is_word = (cc == '_') ||
                       (cc >= 'a' && cc <= 'z') ||
                       (cc >= '0' && cc <= '9');
        if (is_word) {
            out.push_back(static_cast<char>(cc));
            prev_space = false;
        } else {
            if (!prev_space) {
                out.push_back(' ');
                prev_space = true;
            }
        }
    }
    while (!out.empty() && out.back() == ' ') out.pop_back();
    if (!out.empty() && out.front() == ' ') out.erase(out.begin());
    return out;
}

inline std::vector<std::string> simple_tokens(const std::string& text) {
    std::vector<std::string> toks;
    std::string cur;
    for (unsigned char c : text) {
        if (c == ' ') {
            if (!cur.empty()) {
                toks.push_back(cur);
                cur.clear();
            }
        } else {
            cur.push_back(static_cast<char>(c));
        }
    }
    if (!cur.empty()) toks.push_back(cur);
    return toks;
}

inline std::uint64_t hash_shingle(const std::string& s) {
    return std::hash<std::string>{}(s);
}

inline std::vector<std::uint64_t> build_shingles(
    const std::vector<std::string>& toks,
    int k
) {
    std::vector<std::uint64_t> out;
    if ((int)toks.size() < k) return out;
    std::string buf;
    for (int i = 0; i <= (int)toks.size() - k; ++i) {
        buf.clear();
        for (int j = 0; j < k; ++j) {
            if (j) buf.push_back(' ');
            buf += toks[i + j];
        }
        out.push_back(hash_shingle(buf));
    }
    return out;
}
