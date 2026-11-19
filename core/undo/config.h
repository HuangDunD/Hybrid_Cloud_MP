#pragma once
#include <limits>
#include <butil/logging.h>
#include "util/json_config.h"
namespace undo {
constexpr size_t kDefaultLockTableInitialCapacity = 10000;

// Number of mapped page-ID slots, independent of the undo buffer frame budget.
// Zero leaves the initial array empty; all page locks are then created on demand.
inline size_t LockTableInitialCapacity(const JsonConfig& config) {
    auto setting = config.get("undo_lock_table_initial_capacity");
    if (!setting.exists()) return kDefaultLockTableInitialCapacity;
    CHECK(setting.is_int64() && setting.get_int64() >= 0 &&
          setting.get_int64() <= std::numeric_limits<int32_t>::max());
    return size_t(setting.get_int64());
}

// Append-only undo has no page reuse; this is an admission limit for new write
// transactions, not a GC or a hard cap on in-flight transactions. Zero means
// unlimited.
inline uint64_t Capacity(const JsonConfig& config) {
    auto setting = config.get("undo_capacity_pages");
    if (!setting.exists()) return 0;
    CHECK(setting.is_int64() && setting.get_int64() >= 0);
    return uint64_t(setting.get_int64());
}

inline size_t BufferSize(const JsonConfig& config) {
    auto setting = config.get("undo_buffer_pool_size_pages");
    if (!setting.exists()) return 10000;
    CHECK(setting.is_int64() && setting.get_int64() > 0 &&
          setting.get_int64() <= std::numeric_limits<int32_t>::max());
    return size_t(setting.get_int64());
}
}  // namespace undo
