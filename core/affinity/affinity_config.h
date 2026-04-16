// Affinity-driven repartitioning — common types & helpers.
// 论文实验代码：performance > industrialization.
#pragma once

#include <cstddef>
#include <cstdint>

#include "config.h"

namespace affinity {

// One TxnSample carries up to this many co-accessed tuples.
// SmallBank typical=4, TPCC NewOrder ~10. Cap at 32.
constexpr size_t MAX_TUPLES_PER_SAMPLE = 32;

// Pack (table_id, item_key) into a single uint64_t.
// table_id occupies high 16 bits; item_key the low 48 bits.
// Workload keys (YCSB key, SmallBank acct_id, TPCC composite key) all fit in 48 bits.
inline uint64_t pack_tuple_id(uint32_t table_id, uint64_t item_key) {
    return (static_cast<uint64_t>(table_id & 0xFFFFu) << 48) | (item_key & 0x0000FFFFFFFFFFFFull);
}

inline uint32_t unpack_table_id(uint64_t tuple_id) {
    return static_cast<uint32_t>((tuple_id >> 48) & 0xFFFFu);
}

inline uint64_t unpack_item_key(uint64_t tuple_id) {
    return tuple_id & 0x0000FFFFFFFFFFFFull;
}

// Stable hash for sharding tuple_id across sidecar ranks (vtxdist owner_rank).
// FNV-1a 64-bit on 8 bytes — cheap, stable across processes.
inline uint64_t stable_hash_u64(uint64_t v) {
    uint64_t h = 1469598103934665603ull;
    for (int i = 0; i < 8; ++i) {
        h ^= (v >> (i * 8)) & 0xFFull;
        h *= 1099511628211ull;
    }
    return h;
}

inline int owner_rank(uint64_t tuple_id, int n_ranks) {
    return static_cast<int>(stable_hash_u64(tuple_id) % static_cast<uint64_t>(n_ranks));
}

}  // namespace affinity
