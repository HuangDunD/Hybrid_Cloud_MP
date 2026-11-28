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

// True when the operator pinned the variant instead of relying on a default.
inline bool TwoPCVariantConfigured(const JsonConfig& local_compute_node) {
    return local_compute_node.get("two_pc_variant").exists();
}

// Named normal-path variants; the old switch remains readable for direct starts.
// MIX's distributed leg defaults to the storage-notified Cornus variant and is
// not redirected by the legacy cornus_prepare_commit switch. An explicit
// two_pc_variant always wins, so the baseline stays selectable for comparison
// and for the cost model.
inline std::string ReadTwoPCVariant(const JsonConfig& local_compute_node, bool mix_mode = false) {
    const auto value = local_compute_node.get("two_pc_variant");
    if (!value.exists()) {
        if (mix_mode) return "2pc-cornus-opt";
        return ReadCornusPrepareCommit(local_compute_node) ? "2pc-cornus-opt" : "2pc";
    }
    const auto variant = value.get_str();
    if (variant != "2pc" && variant != "2pc-cornus" && variant != "2pc-cornus-opt") {
        throw std::invalid_argument("two_pc_variant must be 2pc, 2pc-cornus or 2pc-cornus-opt");
    }
    return variant;
}
