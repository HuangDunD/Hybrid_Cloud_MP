#include "edge_shuffler.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <thread>
#include <vector>

#include <brpc/channel.h>
#include <brpc/controller.h>

#include "affinity_config.h"
#include "affinity_metrics.h"
#include "affinity_service.pb.h"
#include "compute_server/server.h"

namespace affinity {

namespace {

// Producer queue between Aggregator and EdgeShufflerLoop. Keep it intentionally
// small: if shuffling falls behind partitioning for multiple cycles, old graphs
// are less useful than current workload shape.
std::mutex g_handoff_mtx;
std::deque<std::shared_ptr<LocalGraph>> g_handoff_queue;
constexpr size_t kHandoffQueueCap = 4;

// Cap edges per RPC to keep brpc message size reasonable. With 64-bit ids
// and 32-bit weights, 32 KiB ≈ 1.6K edges — generous for 1 GbE / RDMA both.
constexpr size_t kEdgesPerRpc = 4096;

}  // namespace

EdgeShuffler& EdgeShuffler::Instance() {
    static EdgeShuffler g;
    return g;
}

std::shared_ptr<EpochAccumulator> EdgeShuffler::Get(uint32_t epoch) {
    std::lock_guard<std::mutex> lk(map_mtx_);
    auto it = epochs_.find(epoch);
    if (it != epochs_.end()) return it->second;
    auto acc = std::make_shared<EpochAccumulator>();
    acc->epoch = epoch;
    epochs_[epoch] = acc;
    return acc;
}

void EdgeShuffler::OnShuffleEdges(uint32_t epoch, int from_rank,
                                  const uint64_t* us, const uint64_t* vs,
                                  const uint32_t* ws, size_t n,
                                  const uint64_t* na_tuples,
                                  const int32_t* na_nodes,
                                  const uint32_t* na_counts, size_t na_n,
                                  bool final) {
    auto acc = Get(epoch);
    std::lock_guard<std::mutex> lk(acc->mtx);
    acc->ranks_seen.insert(from_rank);
    for (size_t i = 0; i < n; ++i) {
        acc->owned_edges[us[i]][vs[i]] += ws[i];
    }
    for (size_t i = 0; i < na_n; ++i) {
        acc->owned_node_access[na_tuples[i]][na_nodes[i]] += na_counts[i];
    }
    if (final) {
        // No barrier change here — the explicit Barrier RPC closes the door.
    }
}

void EdgeShuffler::OnBarrier(uint32_t epoch, int from_rank) {
    auto acc = Get(epoch);
    std::lock_guard<std::mutex> lk(acc->mtx);
    acc->ranks_done.insert(from_rank);
    if (static_cast<int>(acc->ranks_done.size()) >= ComputeNodeCount) {
        acc->barrier_passed = true;
        acc->cv.notify_all();
    }
}

namespace {

// Build per-target buckets — one per peer rank. Edges are routed to owner_rank(u);
// because the graph is symmetric, also push (v, u, w) to owner_rank(v) so the
// receiver can reconstruct full adjacency for u.
struct Bucket {
    std::vector<uint64_t> us;
    std::vector<uint64_t> vs;
    std::vector<uint32_t> ws;
    std::vector<uint64_t> na_tuples;
    std::vector<int32_t>  na_nodes;
    std::vector<uint32_t> na_counts;
};

void BucketEdges(const LocalGraph& g, int n_ranks, std::vector<Bucket>& out) {
    out.assign(n_ranks, Bucket{});
    uint64_t pruned_min_weight = 0;
    for (const auto& [u, nbrs] : g.edges) {
        const int owner_u = owner_rank(u, n_ranks);
        for (const auto& [v, w] : nbrs) {
            // Skip the duplicate symmetric edge — only emit when u<v.
            if (u >= v) continue;
            // Min-weight prune to cut shuffle traffic.
            if (static_cast<double>(w) < affinity_edge_min_weight) {
                ++pruned_min_weight;
                continue;
            }
            const int owner_v = owner_rank(v, n_ranks);
            // Push (u,v,w) to owner_u — the receiver records it under u's adjacency.
            out[owner_u].us.push_back(u);
            out[owner_u].vs.push_back(v);
            out[owner_u].ws.push_back(w);
            // Push the mirror (v,u,w) to owner_v unless same rank.
            if (owner_v != owner_u) {
                out[owner_v].us.push_back(v);
                out[owner_v].vs.push_back(u);
                out[owner_v].ws.push_back(w);
            } else {
                // Same-owner case: still need both halves for adjacency symmetry
                // because owner_u stored only the (u,v) direction.
                out[owner_u].us.push_back(v);
                out[owner_u].vs.push_back(u);
                out[owner_u].ws.push_back(w);
            }
        }
    }
    // Route node_access by owner of the tuple.
    for (const auto& [tid, by_node] : g.node_access) {
        const int owner_t = owner_rank(tid, n_ranks);
        for (const auto& [nid, cnt] : by_node) {
            out[owner_t].na_tuples.push_back(tid);
            out[owner_t].na_nodes.push_back(nid);
            out[owner_t].na_counts.push_back(cnt);
        }
    }
    if (pruned_min_weight > 0) {
        stats.edges_pruned_min_weight.fetch_add(pruned_min_weight,
                                                std::memory_order_relaxed);
    }
}

bool ShipBucketTo(ComputeServer* cs, int target_rank, const Bucket& bucket,
                  uint32_t epoch, int self_rank) {
    if (target_rank == self_rank) {
        // Local short-circuit — feed directly into the receiver-side accumulator
        // without going through brpc.
        EdgeShuffler::Instance().OnShuffleEdges(
            epoch, self_rank,
            bucket.us.data(), bucket.vs.data(), bucket.ws.data(), bucket.us.size(),
            bucket.na_tuples.data(), bucket.na_nodes.data(),
            bucket.na_counts.data(), bucket.na_tuples.size(), true);
        return true;
    }
    auto* chan = cs->GetComputeChannel(target_rank);
    if (chan == nullptr) return false;
    affinity_proto::AffinityService_Stub stub(chan);

    // Chunk into kEdgesPerRpc-sized batches.
    size_t total = bucket.us.size();
    size_t off = 0;
    do {
        const size_t n = std::min(kEdgesPerRpc, total - off);
        affinity_proto::ShuffleEdgesRequest req;
        req.set_epoch(epoch);
        req.set_from_rank(self_rank);
        for (size_t i = 0; i < n; ++i) req.add_us(bucket.us[off + i]);
        for (size_t i = 0; i < n; ++i) req.add_vs(bucket.vs[off + i]);
        for (size_t i = 0; i < n; ++i) req.add_ws(bucket.ws[off + i]);
        const bool last = (off + n >= total);
        // Attach node_access only on the final chunk (it's small) to keep
        // earlier chunks pure edge data.
        if (last) {
            for (auto x : bucket.na_tuples) req.add_node_access_tuples(x);
            for (auto x : bucket.na_nodes)  req.add_node_access_nodes(x);
            for (auto x : bucket.na_counts) req.add_node_access_counts(x);
            req.set_final(true);
        }
        affinity_proto::ShuffleEdgesResponse resp;
        brpc::Controller cntl;
        stub.ShuffleEdges(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) return false;
        off += n;
    } while (off < total);

    // If the bucket was empty, still send a final marker so the peer knows we're done.
    if (total == 0) {
        affinity_proto::ShuffleEdgesRequest req;
        req.set_epoch(epoch);
        req.set_from_rank(self_rank);
        req.set_final(true);
        affinity_proto::ShuffleEdgesResponse resp;
        brpc::Controller cntl;
        stub.ShuffleEdges(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) return false;
    }

    // Now signal the per-epoch barrier on this peer.
    affinity_proto::BarrierRequest breq;
    breq.set_epoch(epoch);
    breq.set_from_rank(self_rank);
    affinity_proto::BarrierResponse bresp;
    brpc::Controller bcntl;
    stub.Barrier(&bcntl, &breq, &bresp, nullptr);
    return !bcntl.Failed();
}

}  // namespace

bool EdgeShuffler::ShipAndBarrier(ComputeServer* cs,
                                  std::shared_ptr<const LocalGraph> graph,
                                  uint32_t epoch) {
    const int n_ranks = ComputeNodeCount;
    const int self_rank = cs->GetNodeID();

    std::vector<Bucket> buckets;
    BucketEdges(*graph, n_ranks, buckets);

    // Parallelize across peers — each ShipBucketTo is a small chunk sequence
    // plus a Barrier, and peers are independent. The local bucket is handled
    // on this thread (it's a direct call into the local accumulator with no
    // IO). A thread per peer for a 5-second partition cycle is trivially
    // cheap compared to the per-peer RPC RTT we're amortizing.
    std::vector<char> oks(n_ranks, 1);
    if (self_rank >= 0 && self_rank < n_ranks) {
        if (!ShipBucketTo(cs, self_rank, buckets[self_rank], epoch, self_rank)) {
            oks[self_rank] = 0;
        }
    }
    std::vector<std::thread> ths;
    ths.reserve(n_ranks);
    for (int r = 0; r < n_ranks; ++r) {
        if (r == self_rank) continue;
        ths.emplace_back([cs, r, &buckets, epoch, self_rank, &oks]() {
            if (!ShipBucketTo(cs, r, buckets[r], epoch, self_rank)) {
                oks[r] = 0;
            }
        });
    }
    for (auto& t : ths) t.join();

    bool all_ok = true;
    for (char o : oks) if (!o) { all_ok = false; break; }
    // Local barrier signal — even if everything is local, OnBarrier needs to
    // see "self" report in. ShipBucketTo's local short-circuit doesn't barrier.
    OnBarrier(epoch, self_rank);
    return all_ok;
}

std::shared_ptr<EpochAccumulator> EdgeShuffler::WaitAndTake(uint32_t epoch,
                                                            int timeout_ms) {
    auto acc = Get(epoch);
    {
        std::unique_lock<std::mutex> lk(acc->mtx);
        const bool ok = acc->cv.wait_for(
            lk, std::chrono::milliseconds(timeout_ms),
            [&] { return acc->barrier_passed || stopping_.load(); });
        if (!ok || stopping_.load()) return nullptr;
    }
    // Remove from the map so memory can be reclaimed once last user drops it.
    {
        std::lock_guard<std::mutex> lk(map_mtx_);
        epochs_.erase(epoch);
    }
    return acc;
}

void EdgeShuffler::Stop() {
    stopping_.store(true);
    std::lock_guard<std::mutex> lk(map_mtx_);
    for (auto& kv : epochs_) {
        std::lock_guard<std::mutex> lk2(kv.second->mtx);
        kv.second->cv.notify_all();
    }
}

void EnqueueLocalGraph(std::shared_ptr<LocalGraph> snap) {
    std::lock_guard<std::mutex> lk(g_handoff_mtx);
    if (g_handoff_queue.size() >= kHandoffQueueCap) {
        g_handoff_queue.pop_front();
        stats.partition_skipped.fetch_add(1, std::memory_order_relaxed);
    }
    g_handoff_queue.push_back(std::move(snap));
}

namespace {
std::shared_ptr<LocalGraph> TakeLocalGraph() {
    std::lock_guard<std::mutex> lk(g_handoff_mtx);
    if (g_handoff_queue.empty()) return nullptr;
    auto out = std::move(g_handoff_queue.front());
    g_handoff_queue.pop_front();
    return out;
}
}  // namespace

static std::atomic<bool> g_shuffler_stop{false};

void RequestShufflerStop() { g_shuffler_stop.store(true); EdgeShuffler::Instance().Stop(); }

void EdgeShufflerLoop(ComputeServer* cs) {
    if (!enable_affinity) return;
    while (!g_shuffler_stop.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        auto snap = TakeLocalGraph();
        if (!snap) continue;

        const uint32_t epoch = static_cast<uint32_t>(snap->epoch);
        const bool ok = EdgeShuffler::Instance().ShipAndBarrier(cs, snap, epoch);
        if (!ok) {
            stats.partition_skipped.fetch_add(1, std::memory_order_relaxed);
            // Drop this epoch's accumulator so the next one starts clean.
            EdgeShuffler::Instance().WaitAndTake(epoch, 1);
            continue;
        }
        // The PartitionerLoop (Phase 4) calls WaitAndTake to consume the
        // accumulator. Phase 3 ends here — leave the accumulator parked.
    }
}

}  // namespace affinity
