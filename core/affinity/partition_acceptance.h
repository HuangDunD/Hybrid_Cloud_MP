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

struct PartitionAcceptanceDecision {
    bool accept_all = true;
    bool clip_changed_vertices = false;
    uint64_t changed_vertices_budget = 0;
};

inline double ChangedVerticesRatio(uint64_t changed_vertices,
                                   uint64_t owned_vertices) {
    return owned_vertices > 0
               ? static_cast<double>(changed_vertices) /
                     static_cast<double>(owned_vertices)
               : 0.0;
}

inline uint64_t ChangedVerticesBudget(uint64_t owned_vertices,
                                      double max_changed_vertices_ratio) {
    if (owned_vertices == 0) return 0;
    if (max_changed_vertices_ratio <= 0.0 ||
        max_changed_vertices_ratio >= 1.0) {
        return owned_vertices;
    }
    const double budget =
        static_cast<double>(owned_vertices) * max_changed_vertices_ratio;
    return static_cast<uint64_t>(budget + 1e-9);
}

inline PartitionAcceptanceDecision DecidePartitionAcceptance(
    const PartitionAcceptanceInput& in) {
    PartitionAcceptanceDecision out{};
    out.changed_vertices_budget =
        ChangedVerticesBudget(in.owned_vertices,
                              in.max_changed_vertices_ratio);

    // Always accept the initial assignment. There is no prior placement to
    // preserve, and blocking it would leave affinity disabled in practice.
    if (in.current_assignment_size == 0 || in.owned_vertices == 0) {
        out.accept_all = true;
        out.clip_changed_vertices = false;
        out.changed_vertices_budget = in.changed_vertices;
        return out;
    }

    // Values >= 1.0 preserve the historical behavior. Values <= 0.0 are
    // treated as disabled as well to avoid accidental "reject everything".
    if (in.max_changed_vertices_ratio <= 0.0 ||
        in.max_changed_vertices_ratio >= 1.0) {
        out.accept_all = true;
        out.clip_changed_vertices = false;
        out.changed_vertices_budget = in.changed_vertices;
        return out;
    }

    out.accept_all = in.changed_vertices <= out.changed_vertices_budget;
    out.clip_changed_vertices = !out.accept_all;
    return out;
}

inline bool ShouldAcceptChangedVertex(
    const PartitionAcceptanceDecision& decision,
    uint64_t accepted_changed_vertices) {
    return !decision.clip_changed_vertices ||
           accepted_changed_vertices < decision.changed_vertices_budget;
}

inline bool ShouldAcceptPartition(const PartitionAcceptanceInput& in) {
    return DecidePartitionAcceptance(in).accept_all;
}

}  // namespace affinity
