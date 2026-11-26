#pragma once
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <butil/logging.h>

// Optional per-process WAL diagnostics. Use a different tmpfs path for each process.
// Join compute and storage batches on (producer, first LSN), never across host clocks.
// batch: producer, first LSN, bytes, records, oldest enqueue, RPC start and end.
// sync: producer, first LSN, bytes, sync calls, first sync start, last sync end,
//       cumulative sync and write duration. All timestamps and durations use ns.
namespace wal_trace {
inline FILE* Output() {
    static FILE* output = []() -> FILE* {
        const char* path = std::getenv("HYBRID_WAL_BATCH_TRACE");
        if (!path || !*path) return nullptr;
        FILE* file = std::fopen(path, "wx");
        PCHECK(file);
        std::setvbuf(file, nullptr, _IOFBF, 65536);
        return file;
    }();
    return output;
}
inline uint64_t NowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}
}  // namespace wal_trace
