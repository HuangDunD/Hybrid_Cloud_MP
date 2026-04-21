// Tuple_id -> node_id mapping, RCU-style: readers never block, writers swap
// a fresh snapshot atomically. The migration worker reads this on every tuple
// to decide whether to ship it; lookups are on every read on the hot path
// (eventually — Phase 5 wires that in), so reads must be lock-free.
//
// Why std::shared_ptr atomics rather than std::atomic<std::shared_ptr> (C++20):
//   GCC < 12 / Clang < 15 don't ship the C++20 atomic specialization. We use
//   the deprecated-in-C++20-but-still-present std::atomic_load/store overloads
//   which compile everywhere this project supports.
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <unordered_map>

namespace affinity {

class AssignmentTable {
public:
    struct Snapshot {
        std::unordered_map<uint64_t, int> map;  // tuple_id -> node_id
        uint64_t version = 0;
    };

    AssignmentTable() {
        // Empty initial snapshot so Lookup() never returns from a null pointer.
        std::atomic_store(&current_, std::make_shared<const Snapshot>());
    }

    // Hot path. Returns the assigned node_id, or `fallback` if the tuple is
    // unknown (cold tuple — let the legacy arithmetic owner handle it).
    int Lookup(uint64_t tuple_id, int fallback) const {
        auto snap = std::atomic_load(&current_);
        auto it = snap->map.find(tuple_id);
        return it == snap->map.end() ? fallback : it->second;
    }

    // Replace the visible snapshot. Old readers keep their pointer alive via
    // shared_ptr refcount; once they release it the old snapshot is collected.
    void Replace(std::shared_ptr<const Snapshot> snap) {
        std::atomic_store(&current_, std::move(snap));
    }

    // Merge a per-epoch delta into the visible snapshot: entries in `delta`
    // overwrite, entries only in the current snapshot survive. Required
    // because the partitioner only reports tuples the aggregator observed
    // in the most recent cycle, while ParMETIS_V3_AdaptiveRepart's
    // prev_part contract demands we remember what we told it last time —
    // otherwise each epoch loses the previous decision for untouched
    // tuples and AdaptiveRepart re-solves from scratch. The merged map
    // grows monotonically; bound its size externally if the workload's
    // hot set grows without bound. Version tracks `delta->version` so
    // timeseries / stability checks see forward progress.
    void Merge(std::shared_ptr<const Snapshot> delta) {
        auto cur = std::atomic_load(&current_);
        auto merged = std::make_shared<Snapshot>();
        merged->map = cur->map;
        for (const auto& kv : delta->map) {
            merged->map[kv.first] = kv.second;
        }
        merged->version = delta->version;
        std::atomic_store(&current_,
                          std::shared_ptr<const Snapshot>(std::move(merged)));
    }

    std::shared_ptr<const Snapshot> Current() const {
        return std::atomic_load(&current_);
    }

    uint64_t CurrentVersion() const {
        auto snap = std::atomic_load(&current_);
        return snap->version;
    }

    size_t Size() const {
        auto snap = std::atomic_load(&current_);
        return snap->map.size();
    }

private:
    std::shared_ptr<const Snapshot> current_;
};

// Process-wide singleton. Phase 5 migration worker + the eventual hot-path
// hook in dtx_exe both look the routing here.
AssignmentTable& GetAssignmentTable();

}  // namespace affinity
