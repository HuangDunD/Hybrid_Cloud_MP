// Co-access graph used by the affinity aggregator + ParMETIS partitioner.
// Phase 3: edges are accumulated in a hash-of-hash; freeze() snapshots into a
// CSR-friendly form that EdgeShuffler / Partitioner consume.
//
// All counts are local to this compute node — global aggregation happens via
// EdgeShuffler (push-by-owner-rank) before the partitioner runs.
#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "affinity_config.h"

namespace affinity {

// A bag of (u, v, w) edges keyed by 64-bit packed tuple_id.
// Used both for the live mutable accumulator and for "frozen" snapshots
// handed off to EdgeShuffler.
struct LocalGraph {
    // u -> { v -> co-access weight }  (always stored symmetrically: u<->v both inserted)
    std::unordered_map<uint64_t, std::unordered_map<uint64_t, uint32_t>> edges;

    // For migration cost hint (vsize in ParMETIS): how many times each tuple
    // was accessed locally by which compute node — Phase 4 sums these for vsize/vwgt.
    // tuple_id -> { node_id -> access_count }
    std::unordered_map<uint64_t, std::unordered_map<int, uint32_t>> node_access;

    uint64_t epoch = 0;          // Partition epoch this snapshot belongs to.
    uint64_t total_samples = 0;  // For metrics — # of TxnSamples folded in.

    // O(1) helpers used in the hot accumulator path.
    void AddEdge(uint64_t u, uint64_t v, uint32_t w = 1) {
        if (u == v) return;
        edges[u][v] += w;
        edges[v][u] += w;
    }
    void AddNodeAccess(uint64_t tuple_id, int node_id) {
        node_access[tuple_id][node_id] += 1;
    }

    void Clear() {
        edges.clear();
        node_access.clear();
        total_samples = 0;
    }

    size_t VertexCount() const { return edges.size(); }
    size_t EdgeCount() const {
        size_t n = 0;
        for (const auto& kv : edges) n += kv.second.size();
        return n / 2;  // each undirected edge stored twice
    }
};

// Lock-protected handoff slot between Aggregator (producer) and EdgeShuffler
// (consumer). Aggregator builds the accumulator in place; on tick boundary it
// swaps the accumulator pointer with this slot for the shuffler to pick up.
class GraphHandoff {
public:
    // Producer pushes a frozen snapshot. Returns previous slot value (usually nullptr).
    std::shared_ptr<LocalGraph> Publish(std::shared_ptr<LocalGraph> snap) {
        std::lock_guard<std::mutex> lk(mtx_);
        auto prev = std::move(slot_);
        slot_ = std::move(snap);
        return prev;
    }
    // Consumer takes the published snapshot (nullptr if none ready).
    std::shared_ptr<LocalGraph> Take() {
        std::lock_guard<std::mutex> lk(mtx_);
        auto out = std::move(slot_);
        slot_.reset();
        return out;
    }
private:
    std::mutex mtx_;
    std::shared_ptr<LocalGraph> slot_;
};

}  // namespace affinity
