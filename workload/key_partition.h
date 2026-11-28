#pragma once

#include <cassert>
#include <cstdint>

namespace key_partition {
struct Layout {
    int nodes = 0; // Page-routed workloads do not configure a key owner.
    int pages = 0;
    bool ByKey() const { return nodes > 0; }
};

inline int KeyOwner(uint64_t key, int nodes) {
    assert(nodes > 0);
    return key % nodes;
}
}  // namespace key_partition
