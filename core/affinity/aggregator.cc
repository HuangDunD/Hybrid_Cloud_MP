#include "aggregator.h"

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include "affinity_config.h"
#include "affinity_metrics.h"
#include "edge_shuffler.h"
#include "graph.h"
#include "sample_buffer.h"

namespace affinity {

namespace {

// Drain budget per tick — generous; with kCap=4096 per ring & ~16 workers,
// 50ms tick × 16k samples/tick ≫ realistic txn rates per machine.
constexpr size_t kDrainBudgetPerRing = 4096;

// Number of accumulated samples that triggers a hand-off to the shuffler.
// Cuts both ways: too small → lots of small ships; too large → high latency
// before partition runs see the new edges. 50ms × ~5k txn = a few hundred K
// edges typical, well under the next partition cycle.
size_t FlushSampleThreshold() {
    return static_cast<size_t>(affinity_partition_cycle_ms) *
           1000ull /* assume up to 1 sample/us steady state */;
}

void FoldOneSample(LocalGraph& g, const TxnSample& s) {
    g.total_samples += 1;
    if (s.n_items < 2) return;
    // Quadratic in n_items, but n_items is bounded by MAX_TUPLES_PER_SAMPLE (32).
    // Worst case 32*31/2 = 496 increments per txn — cheap.
    for (uint16_t i = 0; i < s.n_items; ++i) {
        const uint64_t u = s.tuple_ids[i];
        g.AddNodeAccess(u, s.node_id);
        for (uint16_t j = i + 1; j < s.n_items; ++j) {
            g.AddEdge(u, s.tuple_ids[j], 1);
        }
    }
}

}  // namespace

// Module-private "stop" flag — Phase 6 will provide a real shutdown driver.
// Phase 3 just needs the loop to be killable from a signal handler if any.
static std::atomic<bool> g_aggr_stop{false};

void RequestAggregatorStop() { g_aggr_stop.store(true, std::memory_order_relaxed); }

void AggregatorLoop(ComputeServer* /*cs*/) {
    if (!enable_affinity) return;

    auto accumulator = std::make_shared<LocalGraph>();
    accumulator->epoch = 1;

    auto last_flush = std::chrono::steady_clock::now();
    const auto flush_interval =
        std::chrono::milliseconds(affinity_partition_cycle_ms);

    std::vector<SampleRing*> rings;
    std::vector<TxnSample> drained;
    drained.reserve(kDrainBudgetPerRing);

    while (!g_aggr_stop.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(
            std::chrono::milliseconds(affinity_aggregator_tick_ms));

        rings.clear();
        SnapshotRings(rings);

        size_t consumed_this_tick = 0;
        for (auto* r : rings) {
            drained.clear();
            const size_t got = r->drain(drained, kDrainBudgetPerRing);
            consumed_this_tick += got;
            for (const auto& s : drained) {
                FoldOneSample(*accumulator, s);
            }
        }
        if (consumed_this_tick > 0) {
            stats.samples_consumed.fetch_add(consumed_this_tick,
                                             std::memory_order_relaxed);
        }

        const auto now = std::chrono::steady_clock::now();
        if (now - last_flush >= flush_interval && accumulator->VertexCount() > 0) {
            // Update graph-size metrics before publishing.
            stats.graph_vertices.store(accumulator->VertexCount(),
                                       std::memory_order_relaxed);
            stats.graph_edges.store(accumulator->EdgeCount(),
                                    std::memory_order_relaxed);

            // Hand off the snapshot, start a fresh accumulator for the next epoch.
            const uint64_t next_epoch = accumulator->epoch + 1;
            EnqueueLocalGraph(accumulator);
            accumulator = std::make_shared<LocalGraph>();
            accumulator->epoch = next_epoch;
            last_flush = now;
        }

        // Safety hat — if a partitioner is wedged for many cycles and the
        // accumulator keeps growing past affinity_max_vertices, drop the
        // oldest data by resetting. Phase-experiment correctness is fine
        // either way; we just don't want to OOM the box.
        if (accumulator->VertexCount() >
            static_cast<size_t>(affinity_max_vertices)) {
            accumulator->Clear();
        }
    }
}

}  // namespace affinity
