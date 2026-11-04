#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <bthread/mutex.h>
#include "common.h"
#include "storage/disk_manager.h"
#include "logreplay.h"

// Each compute node owns one append stream on shared storage. Different streams
// have independent file descriptors and synchronization; batching is at compute.
class LogManager {
public:
    LogManager(DiskManager* disk_manager, LogReplay* log_replay,
               std::string file_prefix = LOG_FILE_NAME);
    ~LogManager();
    void write_batch_log_to_disk(const std::string& batch_log);
    void write_batch_log_to_disk(char* batch_log, size_t size);
    void write_raft_log_to_disk(const std::string& batch_log);
    int64_t get_lseek_time_ns() const;
    int64_t get_write_time_ns() const;
    int64_t get_fdatasync_time_ns() const;
    int64_t get_logwrite_count() const;
    int64_t get_fdatasync_count() const;
    int64_t get_group_commit_logwrite_sum() const;
    LogReplay* log_replay_;
private:
    struct Stream {
        int fd = -1;
        uint64_t end = 0;
        bthread::Mutex mutex;
        ~Stream() { if (fd >= 0) close(fd); }
    };
    Stream& GetStream(node_id_t node);
    void AppendAndSync(const char* data, size_t size);
    std::string file_prefix_;
    std::mutex streams_mutex_;
    std::unordered_map<node_id_t, std::unique_ptr<Stream>> streams_;
    std::atomic<int64_t> write_total_time_ns_{0};
    std::atomic<int64_t> fdatasync_total_time_ns_{0};
    std::atomic<int64_t> logwrite_count_{0};
    std::atomic<int64_t> fdatasync_count_{0};
};
