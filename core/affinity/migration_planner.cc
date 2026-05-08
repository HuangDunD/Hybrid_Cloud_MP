#include "migration_planner.h"

#include <vector>

namespace affinity {

namespace {
constexpr uint32_t kMigrationCooldownEpochs = 3;
constexpr uint32_t kMigrationFailureCooldownEpochs = 1;
}

MigrationQueue& MigrationQueue::Instance() {
    static MigrationQueue g;
    return g;
}

bool MigrationQueue::Enqueue(const MigrationPlan& plan) {
    std::lock_guard<std::mutex> lk(mtx_);
    if (in_flight_.count(plan.tuple_id)) return false;
    in_flight_.insert(plan.tuple_id);
    queue_.push_back(plan);
    return true;
}

bool MigrationQueue::InCooldown(uint64_t tuple_id, uint32_t current_epoch) {
    std::lock_guard<std::mutex> lk(mtx_);
    auto it = cooldown_until_epoch_.find(tuple_id);
    if (it == cooldown_until_epoch_.end()) return false;
    if (current_epoch < it->second) return true;
    cooldown_until_epoch_.erase(it);
    return false;
}

size_t MigrationQueue::Drain(std::vector<MigrationPlan>& out, size_t max) {
    std::lock_guard<std::mutex> lk(mtx_);
    size_t n = 0;
    while (!queue_.empty() && n < max) {
        out.push_back(queue_.front());
        queue_.pop_front();
        ++n;
    }
    return n;
}

void MigrationQueue::MarkDone(uint64_t tuple_id, uint32_t completed_epoch,
                              bool migrated) {
    std::lock_guard<std::mutex> lk(mtx_);
    in_flight_.erase(tuple_id);
    // Failed migrations (typically: source tuple is X-locked by a concurrent
    // workload txn) used to be re-enqueued the very next planner tick, which
    // produced a retry storm on hot tuples — exactly the tuples the partitioner
    // most wanted to move. Park them for one epoch so the workload has a chance
    // to release the lock before we try again.
    cooldown_until_epoch_[tuple_id] =
        completed_epoch + (migrated ? kMigrationCooldownEpochs
                                    : kMigrationFailureCooldownEpochs);
}

size_t MigrationQueue::PendingCount() const {
    std::lock_guard<std::mutex> lk(mtx_);
    return queue_.size();
}

}  // namespace affinity
