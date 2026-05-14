// Phase 4 — Partitioner: drives one partition epoch end-to-end.
//
// Per epoch:
//   1. WaitAndTake the merged accumulator from EdgeShuffler.
//   2. Allgather owned-tuple inventories with all peers (PushVertexInventory RPC).
//   3. Build a global tuple_id -> dense vid map; compute vtxdist[].
//   4. Serialize the local CSR (xadj/adjncy/vwgt/vsize/adjwgt) and send via UDS
//      to the local parmetis_sidecar.
//   5. Receive part_local[] from the sidecar.
//   6. Broadcast (tuple_id -> node_id) slice to all peers via PushAssignmentSlice.
//   7. Once all N slices are in, swap them into the global AssignmentTable.
#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "graph.h"

class ComputeServer;

namespace affinity {

struct PendingAssignmentEntry {
    int node_id = -1;
    uint32_t migration_priority = 0;
};

// Per-epoch coordination state for the inventory + assignment broadcast steps.
// One instance per in-flight epoch; PartitionerLoop creates / destroys them.
struct EpochCoord {
    uint32_t epoch = 0;

    std::mutex mtx;
    std::condition_variable cv;

    // ----- Vertex inventory exchange -----
    // owned_tuples_by_rank[r] == sorted owned tuple_ids reported by rank r.
    std::vector<std::vector<uint64_t>> owned_tuples_by_rank;
    std::unordered_set<int> inventory_seen;
    bool inventory_ready = false;

    // ----- Assignment slice exchange -----
    // pending[tuple_id] == assignment entry (from any rank's slice).
    std::unordered_map<uint64_t, PendingAssignmentEntry> pending_assignment;
    std::unordered_set<int> slice_seen;
    bool assignment_ready = false;
};

class PartitionCoordinator {
public:
    static PartitionCoordinator& Instance();

    // Receiver entry — handlers invoke these from the brpc service threads.
    void OnInventory(uint32_t epoch, int from_rank,
                     const uint64_t* owned, size_t n);
    void OnAssignmentSlice(uint32_t epoch, int from_rank,
                           const uint64_t* tuples, const int32_t* nodes,
                           const uint32_t* migration_priorities, size_t n,
                           bool final);

    // Producer entries — block until inventory / assignment are complete.
    bool WaitInventory(uint32_t epoch, int n_ranks, int timeout_ms);
    bool WaitAssignment(uint32_t epoch, int n_ranks, int timeout_ms);

    std::shared_ptr<EpochCoord> Get(uint32_t epoch);
    void Drop(uint32_t epoch);

    void Stop();

private:
    PartitionCoordinator() = default;

    std::mutex map_mtx_;
    std::unordered_map<uint32_t, std::shared_ptr<EpochCoord>> epochs_;
    std::atomic<bool> stopping_{false};
};

// Background thread body. Consumes EdgeShuffler output, drives one partition
// per partition_cycle_ms.
void PartitionerLoop(ComputeServer* cs);

// Shutdown hook for Phase 6.
void RequestPartitionerStop();

}  // namespace affinity
