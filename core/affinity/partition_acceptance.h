// Small helper for deciding whether a ParMETIS result should replace the
// visible assignment table. Keeping the predicate isolated makes the
// anti-thrashing policy easy to test without standing up MPI/BRPC.
#pragma once

#include <cstdint>

namespace affinity {

struct PartitionAcceptanceInput {
    uint64_t current_assignment_size = 0;
    uint64_t changed_vertices = 0;
    uint64_t owned_vertices = 0;
    double max_changed_vertices_ratio = 1.0;
};

inline double ChangedVerticesRatio(uint64_t changed_vertices,
                                   uint64_t owned_vertices) {
    return owned_vertices > 0
               ? static_cast<double>(changed_vertices) /
                     static_cast<double>(owned_vertices)
               : 0.0;
}

inline bool ShouldAcceptPartition(const PartitionAcceptanceInput& in) {
    // Always accept the initial assignment. There is no prior placement to
    // preserve, and blocking it would leave affinity disabled in practice.
    if (in.current_assignment_size == 0) return true;
    if (in.owned_vertices == 0) return true;

    // Values >= 1.0 preserve the historical behavior. Values <= 0.0 are
    // treated as disabled as well to avoid accidental "reject everything".
    if (in.max_changed_vertices_ratio <= 0.0 ||
        in.max_changed_vertices_ratio >= 1.0) {
        return true;
    }

    return ChangedVerticesRatio(in.changed_vertices, in.owned_vertices) <=
           in.max_changed_vertices_ratio;
}

}  // namespace affinity
