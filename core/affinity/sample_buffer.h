// Per-thread MPSC sample ring + RecordTxn entry point on the dtx commit path.
// Hot-path requirement: MUST NOT block. Lossy by design — drops samples when full.
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "affinity_config.h"

class DTX;  // forward — defined in core/dtx/dtx.h

namespace affinity {

struct TxnSample {
    uint64_t  tx_id;
    int       node_id;       // compute node where the txn ran
    uint16_t  n_items;
    uint64_t  tuple_ids[MAX_TUPLES_PER_SAMPLE];
};

// Single-producer (the owning worker thread) / single-consumer (the aggregator) ring.
// kCap chosen so that one aggregator pass at 50ms can reasonably drain 16 worker
// threads each producing ~1k txns/s without dropping under load.
class SampleRing {
public:
    static constexpr size_t kCap = 4096;

    SampleRing() = default;
    SampleRing(const SampleRing&) = delete;
    SampleRing& operator=(const SampleRing&) = delete;

    // Producer side — never blocks. Returns false if full (sample dropped).
    bool push(const TxnSample& s) {
        uint64_t head = head_.load(std::memory_order_relaxed);
        uint64_t tail = tail_.load(std::memory_order_acquire);
        if (head - tail >= kCap) return false;
        slots_[head % kCap] = s;
        head_.store(head + 1, std::memory_order_release);
        return true;
    }

    // Consumer side — drains up to `max` samples into `out`.
    size_t drain(std::vector<TxnSample>& out, size_t max) {
        uint64_t head = head_.load(std::memory_order_acquire);
        uint64_t tail = tail_.load(std::memory_order_relaxed);
        size_t n = 0;
        while (tail < head && n < max) {
            out.push_back(slots_[tail % kCap]);
            ++tail;
            ++n;
        }
        tail_.store(tail, std::memory_order_release);
        return n;
    }

private:
    alignas(64) std::atomic<uint64_t> head_{0};
    alignas(64) std::atomic<uint64_t> tail_{0};
    TxnSample slots_[kCap];
};

// Register a per-worker-thread ring. Called once at thread init.
// Returns nullptr if affinity is disabled.
SampleRing* RegisterRing();

// Consumer-side: snapshot the current set of registered rings.
// The aggregator iterates over this list under a shared lock.
void SnapshotRings(std::vector<SampleRing*>& out);

// Hot-path entry point. Implementation lives in sample_buffer.cc and reads
// dtx->read_only_set + dtx->read_write_set. Safe to call from TxCommit after
// commit_status==true. Must be cheap when enable_affinity==false.
void RecordTxn(DTX* dtx);

// Process-wide init / shutdown helpers (called from compute_server bootstrap).
void Init();
void Shutdown();

}  // namespace affinity
