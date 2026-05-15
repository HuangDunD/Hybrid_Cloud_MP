// Migration planner policy helpers.
#pragma once

#include <cstdint>
#include <cstddef>

namespace affinity {

// The assignment target selector already filters noisy one-off moves. Requiring
// two consecutive partition epochs here is too strict for large assignment
// tables because the planner cursor may not revisit a tuple every epoch.
constexpr uint32_t kRequiredStableTargetObservations = 1;
constexpr size_t kMaxPlansPerSourcePageDestPerSweep = 2;
constexpr uint64_t kFreshMigrationAssignmentEpochs = 1;
constexpr uint32_t kMigrationSuccessCooldownEpochs = 3;
constexpr uint32_t kMigrationFailureCooldownEpochs = 1;
constexpr uint32_t kMigrationFailureCooldownMaxEpochs = 16;
constexpr size_t kMigrationPlannerPriorityWindowMin = 1024;
constexpr size_t kMigrationPlannerPriorityWindowMultiplier = 16;

inline uint32_t UpdateSameTargetObservations(
    int previous_dst_node,
    uint32_t previous_observations,
    int next_dst_node) {
    return previous_dst_node == next_dst_node ? previous_observations + 1 : 1;
}

inline bool MigrationTargetObservedOftenEnough(uint32_t observations) {
    return observations >= kRequiredStableTargetObservations;
}

inline size_t MaxPlansPerSourcePageDestPerSweep() {
    return kMaxPlansPerSourcePageDestPerSweep;
}

inline bool IsFreshMigrationAssignment(
    uint64_t assignment_last_seen_version,
    uint64_t snapshot_version) {
    return assignment_last_seen_version + kFreshMigrationAssignmentEpochs >=
           snapshot_version;
}

inline uint32_t MigrationSuccessCooldownEpochs() {
    return kMigrationSuccessCooldownEpochs;
}

inline uint32_t MigrationFailureCooldownEpochs(uint32_t consecutive_failures) {
    if (consecutive_failures == 0) {
        return kMigrationFailureCooldownEpochs;
    }
    uint32_t epochs = kMigrationFailureCooldownEpochs;
    for (uint32_t i = 1; i < consecutive_failures; ++i) {
        if (epochs >= kMigrationFailureCooldownMaxEpochs / 2) {
            return kMigrationFailureCooldownMaxEpochs;
        }
        epochs *= 2;
    }
    return epochs > kMigrationFailureCooldownMaxEpochs
               ? kMigrationFailureCooldownMaxEpochs
               : epochs;
}

inline size_t MigrationPlannerPriorityWindow(
    size_t remaining_plan_cap,
    size_t remaining_entries) {
    if (remaining_plan_cap == 0 || remaining_entries == 0) {
        return 0;
    }
    const size_t scaled =
        remaining_plan_cap * kMigrationPlannerPriorityWindowMultiplier;
    const size_t target =
        scaled > kMigrationPlannerPriorityWindowMin
            ? scaled
            : kMigrationPlannerPriorityWindowMin;
    return target < remaining_entries ? target : remaining_entries;
}

}  // namespace affinity
