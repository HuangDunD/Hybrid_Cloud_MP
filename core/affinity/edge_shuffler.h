// EdgeShuffler — routes locally-observed (u,v,w) edges to the sidecar rank
// that owns vertex u (and a second copy to owner of v) so that ParMETIS sees
// a globally consistent, symmetric adjacency on every rank.
//
// Phase 3 contains:
//   - The receiver-side state (a per-epoch accumulator of inbound edges)
//   - The barrier counter (used by PartitionerLoop to wait for all peers)
//   - The brpc service hooks live in affinity_service.cc
//
// The sender-side (network ship) is implemented in edge_shuffler.cc and is
// invoked by EdgeShufflerLoop after the aggregator publishes a frozen graph.
#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "graph.h"

class ComputeServer;

namespace affinity {

// One epoch's worth of shuffled edges, accumulated on the receiver.
// Producers: brpc handler threads (ShuffleEdges RPC).
// Consumer: PartitionerLoop on the same node, after the barrier closes.
struct EpochAccumulator {
    uint32_t epoch = 0;

    std::mutex mtx;
    std::condition_variable cv;

    // Same hash-of-hash shape as LocalGraph::edges, but contains only edges
    // for which this rank is the owner of u (the first endpoint).
    std::unordered_map<uint64_t, std::unordered_map<uint64_t, uint32_t>> owned_edges;
    // node_access aggregated for owned tuples only.
    std::unordered_map<uint64_t, std::unordered_map<int, uint32_t>> owned_node_access;

    // Set of ranks that have signaled "done shipping epoch=k".
    std::unordered_set<int> ranks_done;
    // Set of ranks whose ShuffleEdges arrived (used as a sanity check).
    std::unordered_set<int> ranks_seen;

    bool barrier_passed = false;
};

class EdgeShuffler {
public:
    static EdgeShuffler& Instance();

    // Receiver entry — called from the brpc service handler.
    // Each call is a partial batch; `final=true` means the sender is done for this epoch.
    void OnShuffleEdges(uint32_t epoch, int from_rank,
                        const uint64_t* us, const uint64_t* vs, const uint32_t* ws, size_t n,
                        const uint64_t* na_tuples, const int32_t* na_nodes,
                        const uint32_t* na_counts, size_t na_n,
                        bool final);

    // Receiver entry — called from the brpc Barrier service handler.
    void OnBarrier(uint32_t epoch, int from_rank);

    // Sender side: ship a frozen LocalGraph to peers, then call Barrier on each.
    // Blocks until all RPCs return (or fail).  Returns true if all sends succeeded.
    bool ShipAndBarrier(ComputeServer* cs, std::shared_ptr<const LocalGraph> graph,
                        uint32_t epoch);

    // Consumer: block until the barrier closes for `epoch` (or timeout fires),
    // then move out the EpochAccumulator. Returns nullptr on timeout.
    std::shared_ptr<EpochAccumulator> WaitAndTake(uint32_t epoch, int timeout_ms);

    // Cleanup when shutting down — wakes any blocked consumer.
    void Stop();

private:
    EdgeShuffler() = default;
    EdgeShuffler(const EdgeShuffler&) = delete;
    EdgeShuffler& operator=(const EdgeShuffler&) = delete;

    // Look up (creating if needed) the accumulator for `epoch`. Holds map_mtx_.
    std::shared_ptr<EpochAccumulator> Get(uint32_t epoch);

    std::mutex map_mtx_;
    std::unordered_map<uint32_t, std::shared_ptr<EpochAccumulator>> epochs_;

    std::atomic<bool> stopping_{false};
};

// EdgeShufflerLoop reads from the GraphHandoff that aggregator publishes,
// ships edges to peers, hits the barrier, hands the merged accumulator off
// to the partitioner via PartitionerLoop's queue. Phase 4 wires that final
// step in.
//
// `cs` is the local ComputeServer (used for peer brpc channels and to read
// the local rank id). Returns when stopping_ becomes true (Phase 3 has no
// stop signal yet — Shutdown() is a Phase 6 chore).
void EdgeShufflerLoop(ComputeServer* cs);

// Hand the published frozen graph from the aggregator into the shuffler's
// input queue. Called by AggregatorLoop on tick boundaries.
void EnqueueLocalGraph(std::shared_ptr<LocalGraph> snap);

// Signal the shuffler loop to exit; also wakes blocked WaitAndTake() callers.
void RequestShufflerStop();

}  // namespace affinity
