// Aggregator — drains every worker's SampleRing on a fixed tick, folds the
// (RO + RW)-cross-product into a LocalGraph, and on the partition cycle
// boundary publishes a frozen snapshot to the EdgeShuffler.
//
// One thread per compute_server. `cs` is used only to read the local node id
// (for node_access bookkeeping) — the aggregator does not touch any storage.
#pragma once

class ComputeServer;

namespace affinity {

// Body of the aggregator background thread. Returns when stopping_ becomes true.
void AggregatorLoop(ComputeServer* cs);

// Signal the aggregator loop to exit. Idempotent. Safe to call from any thread.
void RequestAggregatorStop();

}  // namespace affinity
