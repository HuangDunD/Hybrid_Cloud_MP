#include "migration_planner.h"

#include <vector>

namespace affinity {

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

void MigrationQueue::MarkDone(uint64_t tuple_id) {
    std::lock_guard<std::mutex> lk(mtx_);
    in_flight_.erase(tuple_id);
}

size_t MigrationQueue::PendingCount() const {
    std::lock_guard<std::mutex> lk(mtx_);
    return queue_.size();
}

}  // namespace affinity
