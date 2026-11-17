#pragma once

#include <algorithm>
#include <cstdint>
#include <vector>
#include "common.h"
#include "util/fast_random.h"

// One window per worker. IDs are workload objects, not physical pages.
// Select against the old window; Remember only after all txn targets are chosen.
class IdReuseWindow {
 public:
    explicit IdReuseWindow(uint64_t worker_seed)
        : seed_(worker_seed ^ 0x4944524555534521ULL) {}

    template <typename Eligible>
    bool Select(double probability, Eligible eligible, itemkey_t& id) {
        if (probability == 0.0 || FastRand(&seed_) / 4294967296.0 >= probability)
            return false;
        std::vector<itemkey_t> candidates;
        for (auto previous : ids_) {
            if (eligible(previous)) candidates.push_back(previous);
        }
        if (candidates.empty()) return false;
        id = candidates[FastRand(&seed_) % candidates.size()];
        return true;
    }

    void Remember(itemkey_t id, size_t capacity) {
        auto old = std::find(ids_.begin(), ids_.end(), id);
        if (old != ids_.end()) ids_.erase(old);
        else if (ids_.size() == capacity) ids_.erase(ids_.begin());
        ids_.push_back(id);
    }

 private:
    uint64_t seed_;
    std::vector<itemkey_t> ids_; // distinct IDs, LRU -> MRU
};
