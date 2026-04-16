// Affinity counters — surfaced into result.txt and the per-second time-series CSV.
#pragma once

#include <atomic>
#include <cstdint>

namespace affinity {

struct AffinityStats {
    std::atomic<uint64_t> samples_pushed{0};
    std::atomic<uint64_t> samples_dropped{0};   // ring full
    std::atomic<uint64_t> samples_consumed{0};

    std::atomic<uint64_t> graph_vertices{0};
    std::atomic<uint64_t> graph_edges{0};
    std::atomic<uint64_t> last_edgecut{0};

    std::atomic<uint64_t> partition_runs{0};
    std::atomic<uint64_t> partition_skipped{0};
    std::atomic<uint64_t> partition_total_ms{0};

    std::atomic<uint64_t> migrations_planned{0};
    std::atomic<uint64_t> migrations_done{0};
    std::atomic<uint64_t> migrations_failed{0};
};

// Single global instance.
extern AffinityStats stats;

}  // namespace affinity
