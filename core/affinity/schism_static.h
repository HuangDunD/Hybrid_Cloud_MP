#pragma once

#include <cstdint>
#include <cstdlib>
#include <string>

namespace affinity {

inline bool EnvFlagEnabled(const char* name) {
    const char* raw = std::getenv(name);
    if (raw == nullptr) return false;
    const std::string value(raw);
    return value != "0" && value != "false" && value != "FALSE";
}

inline bool IsSchismStaticEnabled() {
    return EnvFlagEnabled("SCHISM_STATIC");
}

inline std::string SchismStaticCsvPath() {
    const char* raw = std::getenv("SCHISM_STATIC_CSV");
    return raw == nullptr ? std::string() : std::string(raw);
}

inline int SchismStaticApplyMs() {
    const char* raw = std::getenv("SCHISM_STATIC_APPLY_MS");
    if (raw == nullptr) return 60000;
    const int parsed = std::atoi(raw);
    return parsed > 0 ? parsed : 60000;
}

inline bool SchismApplyConverged(uint64_t planned,
                                 uint64_t done,
                                 uint64_t failed) {
    if (planned == 0) return false;
    return (done * 100 >= planned * 99) && (failed * 100 <= planned);
}

}  // namespace affinity
