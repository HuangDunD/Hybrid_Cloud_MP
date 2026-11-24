#pragma once

#include <cstdint>
#include <mutex>
#include <string>

namespace mvcc {

// One remote_server owns this timestamp domain. A snapshot samples the last
// issued CTS. Unused slots in a persisted allocation block are skipped on boot.
class DurableTso {
public:
    explicit DurableTso(std::string directory);
    ~DurableTso();

    uint64_t Snapshot(uint64_t floor);
    uint64_t Commit(uint64_t floor);

private:
    void OpenLocked();
    void ReserveThroughLocked(uint64_t needed);

    const std::string directory_;
    std::mutex mutex_;
    int lock_fd_ = -1;
    uint64_t current_ = 0;
    uint64_t reserved_ = 0;
};

} // namespace mvcc
