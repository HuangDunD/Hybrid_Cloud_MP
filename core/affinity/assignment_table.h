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
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

namespace affinity {

class AssignmentTable {
public:
    // Per-entry payload. `last_seen_version` = the most recent partition epoch
    // whose delta included this tuple (either as fresh input from the
    // aggregator or re-emitted by a peer's slice). Used for TTL pruning so
    // Merge doesn't let the table grow unbounded on long runs.
    struct Entry {
        int      node_id;
        uint64_t last_seen_version;
        uint32_t migration_priority = 0;
    };

    struct Snapshot {
        std::unordered_map<uint64_t, Entry> map;
        uint64_t version = 0;
    };

    struct LoadResult {
        bool ok = false;
        size_t rows_loaded = 0;
        std::string error;
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
        return it == snap->map.end() ? fallback : it->second.node_id;
    }

    // Replace the visible snapshot. Old readers keep their pointer alive via
    // shared_ptr refcount; once they release it the old snapshot is collected.
    // Kept as an escape hatch; the partitioner uses Merge.
    void Replace(std::shared_ptr<const Snapshot> snap) {
        std::atomic_store(&current_, std::move(snap));
    }

    LoadResult LoadFromCsv(const std::string& path, int compute_node_count);

    // Merge a per-epoch delta into the visible snapshot with TTL pruning:
    //   - Entries in `delta` overwrite and refresh last_seen_version to
    //     delta->version (every merge counts as "still relevant").
    //   - Entries only in the current snapshot survive if
    //     (delta->version - last_seen_version) <= ttl_epochs.
    //   - ttl_epochs == 0 disables TTL entirely (monotone growth — the
    //     pre-TTL behaviour; use with max_vertices as a hard cap).
    //
    // Rationale: the aggregator only reports tuples observed in the most
    // recent cycle, while ParMETIS_V3_AdaptiveRepart's prev_part contract
    // demands we remember what we told it last time. Without any merge,
    // AdaptiveRepart re-solves from scratch every epoch. Without TTL, the
    // merged map grows forever as fresh tuples drift through the workload.
    // TTL gives cold tuples a finite grace period (`ttl_epochs * partition
    // cycle`) before we forget them and fall back to physical-page routing.
    void Merge(std::shared_ptr<const Snapshot> delta, uint64_t ttl_epochs) {
        auto cur = std::atomic_load(&current_);
        auto merged = std::make_shared<Snapshot>();
        merged->version = delta->version;

        const uint64_t cutoff =
            (ttl_epochs > 0 && merged->version > ttl_epochs)
                ? merged->version - ttl_epochs
                : 0;

        merged->map.reserve(cur->map.size() + delta->map.size());
        for (const auto& kv : cur->map) {
            if (ttl_epochs == 0 || kv.second.last_seen_version >= cutoff) {
                merged->map.emplace(kv.first, kv.second);
            }
        }
        for (const auto& kv : delta->map) {
            merged->map[kv.first] =
                Entry{kv.second.node_id, merged->version,
                      kv.second.migration_priority};
        }
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
