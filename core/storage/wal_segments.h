#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

// Physical WAL storage only. Native transaction records remain unchanged.
namespace wal {
constexpr uint64_t kSegmentSize = 16 * 1024 * 1024;
constexpr size_t kFrameHeaderSize = 32;
struct Tail { uint64_t offset = 0, payload_bytes = 0; };
std::string SegmentPath(const std::string& stream, uint64_t segment);
bool IsSegment(const std::string& suffix, unsigned long& node);

class Reader {
public:
    explicit Reader(std::string stream) : stream_(std::move(stream)) {}
    ~Reader();
    // Returns false for an empty/torn tail. The caller supplies the durable or
    // physically available limit; a preallocated file's length is not WAL end.
    bool ReadBatch(uint64_t& offset, uint64_t limit, std::vector<char>& payload);
private:
    void Read(char* data, size_t size, uint64_t offset);
    std::string stream_;
    int fd_ = -1;
    uint64_t segment_ = UINT64_MAX;
};
Tail Recover(const std::string& stream);

class Writer {
public:
    Writer(std::string stream, uint64_t end);
    ~Writer();
    // Initialize capacity before accepting RPCs; does not advance the WAL end.
    void Preallocate(uint64_t bytes);
    struct Timing {
        uint64_t write_ns = 0, sync_ns = 0, sync_calls = 0;
        uint64_t first_sync_ns = 0, last_sync_ns = 0;
    };
    Timing Append(const char* data, size_t size);
    uint64_t end() const { return end_; }
    void ResetBenchmarkStats() {
        std::lock_guard<std::mutex> guard(prepare_mutex_);
        initialized_segments_ = 0; initialization_ns_ = 0;
    }
    uint64_t initialized_segments() const { return initialized_segments_.load(); }
    uint64_t initialization_ns() const { return initialization_ns_.load(); }
private:
    void Prepare(uint64_t segment);
    void PrepareLoop();
    void RequestNext(uint64_t segment);
    std::string stream_;
    uint64_t end_;
    int fd_ = -1;
    uint64_t open_segment_ = UINT64_MAX;
    std::mutex prepare_mutex_;
    std::mutex request_mutex_;
    std::condition_variable changed_;
    uint64_t requested_ = 0;
    bool stop_ = false;
    std::thread prepare_thread_;
    std::atomic<uint64_t> initialized_segments_{0}, initialization_ns_{0};
};
}
