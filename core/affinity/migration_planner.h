// Phase 5 — MigrationPlanner. Walks the AssignmentTable each tick and turns
// (tuple_id_owned_here -> target!=self) deltas into MigrationPlan entries
// that the MigrationWorker drains in batches.
//
// Dedup: we keep an in-flight set keyed by tuple_id so the same tuple isn't
// queued twice across ticks. Once MigrationWorker reports done/failed it
// removes the key.
//
// Throughput is intentionally bounded by `affinity_migration_batch` to keep
// the migration thread from stealing too many storage RPCs from the hot path.
#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <mutex>
#include <unordered_set>

namespace affinity {

struct MigrationPlan {
    uint64_t tuple_id;   // packed (table_id, item_key)
    int      src_node;   // expected current arithmetic owner == self
    int      dst_node;   // target node from AssignmentTable
    uint32_t epoch;      // assignment table version that produced this plan
};

class MigrationQueue {
public:
    static MigrationQueue& Instance();

    // Producer side — called by the planner sweep. Returns false if the tuple
    // is already pending (deduplicated).
    bool Enqueue(const MigrationPlan& plan);

    // Consumer side — pops up to `max` plans into `out`.
    size_t Drain(std::vector<MigrationPlan>& out, size_t max);

    // Called by MigrationWorker after MigrateOne completes (success or fail).
    void MarkDone(uint64_t tuple_id);

    // For metrics.
    size_t PendingCount() const;

private:
    MigrationQueue() = default;

    mutable std::mutex mtx_;
    std::deque<MigrationPlan> queue_;
    std::unordered_set<uint64_t> in_flight_;
};

}  // namespace affinity
