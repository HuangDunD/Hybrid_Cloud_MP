#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <unistd.h>
#include "sm_manager.h"
#include "common.h"
#include "log_record.h"
#include "disk_manager.h"
#include "base/data_item.h"
#include "util/bitmap.h"

// A shared replay scheduler reads independent node WALs. The ordering unit is
// (table, page): apply a record only after its prev_lsn, never by file arrival.
class LogReplay {
public:
    LogReplay(DiskManager* disk, SmManager* sm, const std::string mode,
              std::unordered_map<table_id_t, std::string> table_names = {});
    ~LogReplay();
    static std::string LogFilePath(node_id_t node);
    void RegisterStream(node_id_t node, uint64_t durable_end);
    void PublishDurable(node_id_t node, uint64_t durable_end);
    void Drain(); // called after compute nodes have flushed their final batches
    bool WaitUntilReplayed(std::chrono::milliseconds timeout);
    uint64_t RecordsRead() const { return records_read_.load(); }
    uint64_t RecordsReplayed() const { return records_replayed_.load(); }
    uint64_t PendingRecords() const { return pending_records_.load(); }
    uint64_t DurableBytes() const;
    uint64_t ReadBytes() const;
    void apply_undo_log(const LogRecord* log);
    void set_table_name_map(std::unordered_map<table_id_t, std::string> names);
private:
    struct Stream {
        node_id_t node;
        int fd = -1;
        std::atomic<uint64_t> durable_end{0};
        std::atomic<uint64_t> read_offset{0};
        std::vector<char> buffer;
        ~Stream() { if (fd >= 0) close(fd); }
    };
    using PageKey = std::pair<std::string, page_id_t>;
    using Pending = std::map<LLSN, std::shared_ptr<LogRecord>>;
    void replayFun();
    bool ReadStream(Stream& stream);
    void Dispatch(const std::shared_ptr<LogRecord>& log);
    void apply_single_log(LogRecord* log);
    bool PageForLog(const LogRecord& log, PageKey& key) const;
    bool CaughtUp() const; // streams_mutex_ held
    int ResolveTableFd(table_id_t table_id, const char* name, size_t size);
    std::string ResolveTableName(table_id_t table_id, const char* name, size_t size) const;

    DiskManager* disk_manager_;
    SmManager* sm_manager;
    std::string mode;
    std::unordered_map<table_id_t, std::string> table_name_map_;
    std::unordered_map<table_id_t, int> table_fd_cache_;
    std::mutex table_fd_mutex_;
    mutable std::mutex streams_mutex_;
    std::map<node_id_t, std::unique_ptr<Stream>> streams_;
    std::condition_variable cv_;
    std::atomic<bool> replay_stop_{false};
    std::thread replay_thread_;
    // Only the replay thread accesses page state and the dependency queues.
    std::map<PageKey, Pending> waiting_;
    std::set<int> replayed_files_;
    std::atomic<uint64_t> records_read_{0};
    std::atomic<uint64_t> records_replayed_{0};
    std::atomic<uint64_t> pending_records_{0};
};
