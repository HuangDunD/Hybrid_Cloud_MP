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
