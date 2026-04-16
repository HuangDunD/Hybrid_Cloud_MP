#include "sample_buffer.h"

#include <mutex>
#include <shared_mutex>

#include "affinity_metrics.h"
#include "dtx/dtx.h"
#include "compute_server/server.h"

namespace affinity {

namespace {

// Global registry of per-thread rings. Aggregator (Phase 3) takes shared lock to drain.
std::shared_mutex g_rings_mtx;
std::vector<SampleRing*> g_rings;

thread_local SampleRing* tls_ring = nullptr;

}  // namespace

SampleRing* RegisterRing() {
    if (!enable_affinity) return nullptr;
    if (tls_ring) return tls_ring;
    auto* ring = new SampleRing();
    {
        std::unique_lock<std::shared_mutex> lk(g_rings_mtx);
        g_rings.push_back(ring);
    }
    tls_ring = ring;
    return ring;
}

void SnapshotRings(std::vector<SampleRing*>& out) {
    std::shared_lock<std::shared_mutex> lk(g_rings_mtx);
    out.assign(g_rings.begin(), g_rings.end());
}

void RecordTxn(DTX* dtx) {
    // Hot-path: must be cheap. The two early-returns are the only cost when the
    // experiment is off, and the branch predictor learns the disabled state fast.
    if (!enable_affinity) return;
    if (!tls_ring) return;
    if (dtx == nullptr) return;

    const auto& ro = dtx->read_only_set;
    const auto& rw = dtx->read_write_set;
    const size_t total = ro.size() + rw.size();
    // Tiny heuristic: 1-tuple txns can't form an edge (need >= 2 vertices).
    // Skip them to avoid burning ring slots on data the aggregator would discard.
    if (total < 2) return;

    TxnSample s;
    s.tx_id   = dtx->tx_id;
    s.node_id = dtx->compute_server ? dtx->compute_server->GetNodeID() : -1;
    s.n_items = 0;

    auto append = [&](const std::vector<std::pair<itemkey_t, DataSetItem>>& set) {
        for (const auto& kv : set) {
            if (s.n_items >= MAX_TUPLES_PER_SAMPLE) return;
            // table_id lives on the underlying DataItem; if missing, skip rather than
            // pack a meaningless tuple_id.
            if (!kv.second.item_ptr) continue;
            const uint32_t table_id = static_cast<uint32_t>(kv.second.item_ptr->table_id);
            s.tuple_ids[s.n_items++] = pack_tuple_id(table_id, kv.first);
        }
    };
    append(ro);
    append(rw);

    // n_items < 2 here means almost everything was filtered (e.g. items missing
    // their item_ptr); also discard.
    if (s.n_items < 2) return;

    if (!tls_ring->push(s)) {
        stats.samples_dropped.fetch_add(1, std::memory_order_relaxed);
    } else {
        stats.samples_pushed.fetch_add(1, std::memory_order_relaxed);
    }
}

void Init() {
    if (!enable_affinity) return;
    // Phase 3+ will spawn aggregator/shuffler/partitioner/migration threads here.
}

void Shutdown() {
    if (!enable_affinity) return;
    // Phase 3+ will signal threads to drain in-flight work and exit.
}

}  // namespace affinity
