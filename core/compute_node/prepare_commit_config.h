#pragma once

#include <stdexcept>
#include "util/json_config.h"

// Read once at compute-server startup. Missing keys preserve the enabled default.
inline bool ReadCornusPrepareCommit(const JsonConfig& local_compute_node) {
    const auto value = local_compute_node.get("cornus_prepare_commit");
    if (!value.exists()) return true;
    if (!value.is_int64() || (value.get_int64() != 0 && value.get_int64() != 1)) {
        throw std::invalid_argument("local_compute_node.cornus_prepare_commit must be integer 0 or 1");
    }
    return value.get_int64() == 1;
}

// Named normal-path variants; the old switch remains readable for direct starts.
inline std::string ReadTwoPCVariant(const JsonConfig& local_compute_node) {
    const auto value = local_compute_node.get("two_pc_variant");
    if (!value.exists()) {
        return ReadCornusPrepareCommit(local_compute_node) ? "2pc-cornus-opt" : "2pc";
    }
    const auto variant = value.get_str();
    if (variant != "2pc" && variant != "2pc-cornus" && variant != "2pc-cornus-opt") {
        throw std::invalid_argument("two_pc_variant must be 2pc, 2pc-cornus or 2pc-cornus-opt");
    }
    return variant;
}
