#include "aggregator.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "affinity_config.h"
#include "affinity_metrics.h"
#include "edge_shuffler.h"
#include "graph.h"
#include "graph_dump.h"
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

std::string GraphDumpPathFromEnv() {
    const char* raw = std::getenv("AFFINITY_GRAPH_DUMP_PATH");
    return raw == nullptr ? std::string() : std::string(raw);
}

void MaybeDumpGraph(const std::string& path,
                    const LocalGraph& graph,
                    const char* phase) {
    if (path.empty() || graph.VertexCount() == 0) return;
    std::string error;
    if (!DumpLocalGraphCsv(graph, path, &error)) {
        std::fprintf(stderr,
                     "[affinity] graph dump failed during %s: %s\n",
                     phase,
                     error.c_str());
    }
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
    const std::string graph_dump_path = GraphDumpPathFromEnv();

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
            stats.graph_node_access_vertices.store(accumulator->node_access.size(),
                                                   std::memory_order_relaxed);

            // Hand off the snapshot, seed a fresh accumulator from the decayed
            // prior graph. EWMA-style: edges observed in earlier epochs keep
            // shrinking weight until they drop below 1 and get pruned, so
            // long-lived hot co-accesses remain visible to ParMETIS even on
            // epochs where the workload didn't touch them this cycle — without
            // this, the repartitioner's load-balance constraint would pick
            // unrelated partitions for those vertices every round, triggering
            // cascades of migration that our own AssignmentTable then dutifully
            // enforces. decay=1.0 = infinite memory (dangerous, will OOM);
            // decay=0.0 = the old reset-every-epoch behaviour.
            const uint64_t next_epoch = accumulator->epoch + 1;
            const double decay = affinity_edge_decay_factor;
            auto frozen = accumulator;
            EnqueueLocalGraph(accumulator);
            MaybeDumpGraph(graph_dump_path, *frozen, "publish");
            accumulator = std::make_shared<LocalGraph>();
            accumulator->epoch = next_epoch;
            if (decay > 0.0 && decay < 1.0) {
                for (const auto& kv_u : frozen->edges) {
                    auto& new_nbrs = accumulator->edges[kv_u.first];
                    for (const auto& kv_v : kv_u.second) {
                        const uint32_t w = static_cast<uint32_t>(
                            static_cast<double>(kv_v.second) * decay);
                        if (w >= 1) new_nbrs[kv_v.first] = w;
                    }
                    if (new_nbrs.empty()) {
                        accumulator->edges.erase(kv_u.first);
                    }
                }
                for (const auto& kv_t : frozen->node_access) {
                    auto& new_by_node = accumulator->node_access[kv_t.first];
                    for (const auto& kv_n : kv_t.second) {
                        const uint32_t c = static_cast<uint32_t>(
                            static_cast<double>(kv_n.second) * decay);
                        if (c >= 1) new_by_node[kv_n.first] = c;
                    }
                    if (new_by_node.empty()) {
                        accumulator->node_access.erase(kv_t.first);
                    }
                }
            } else if (decay >= 1.0) {
                // Monotone accumulation — copy whole. Mostly for debugging /
                // "does more history help?" experiments; expect slow memory
                // growth and bound via affinity_max_vertices.
                accumulator->edges = frozen->edges;
                accumulator->node_access = frozen->node_access;
            }
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

    MaybeDumpGraph(graph_dump_path, *accumulator, "shutdown");
}

}  // namespace affinity
