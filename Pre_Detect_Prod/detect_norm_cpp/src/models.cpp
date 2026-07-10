#include "models.h"
#include <map>
using json = nlohmann::json;

namespace dn {

std::string fraud_type_str(FraudType ft) {
    switch (ft) {
        case FraudType::change_word:   return "change_word";
        case FraudType::spaces:          return "spaces";
        case FraudType::hidden_symbols:  return "hidden_symbols";
    }
    return "unknown";
}

void to_json(json& j, const Finding& f) {
    j = json{
        {"word", f.word},
        {"paragraph", f.paragraph},
        {"offset", f.offset},
        {"limit", f.limit},
        {"fraud_type", fraud_type_str(f.fraud_type)}
    };
}

void to_json(json& j, const DetectionReport& r) {
    j = json{
        {"filename", r.filename},
        {"text", r.text},
        {"total_findings", r.total_findings},
        {"findings", r.findings},
        {"elapsed_sec", r.elapsed_sec},
        {"errors", r.errors}
    };
}

} // namespace dn
