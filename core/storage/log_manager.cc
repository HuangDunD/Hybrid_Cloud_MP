#include "log_manager.h"
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <fcntl.h>
#include <sys/stat.h>

namespace {
void WalError(const char* message) {
    LOG(ERROR) << message << ": " << strerror(errno);
    std::abort();
}
}

LogManager::LogManager(DiskManager*, LogReplay* replay, std::string prefix, uint64_t preallocate_bytes)
    : log_replay_(replay), file_prefix_(std::move(prefix)), preallocate_bytes_(preallocate_bytes) {}
LogManager::~LogManager() = default;

void LogManager::ResetBenchmarkStats() {
    write_total_time_ns_ = 0; fdatasync_total_time_ns_ = 0;
    logwrite_count_ = 0; fdatasync_count_ = 0;
    std::lock_guard<std::mutex> guard(streams_mutex_);
    for (auto& entry : streams_)
        if (entry.second->segments) entry.second->segments->ResetBenchmarkStats();
}

void LogManager::InitializeStreams(int node_count) {
    for (int node = 0; node < node_count; ++node) {
        auto& stream = GetStream(node);
        if (stream.segments) stream.segments->Preallocate(preallocate_bytes_);
    }
}

LogManager::Stream& LogManager::GetStream(node_id_t node) {
    std::lock_guard<std::mutex> guard(streams_mutex_);
    auto& stream = streams_[node];
    if (!stream) {
        stream.reset(new Stream);
        const std::string path = file_prefix_ + "_" + std::to_string(node);
        if (access(path.c_str(), F_OK) == 0) {
            // Retained pre-segmentation WAL stays readable/appendable. Fresh
            // runs use segments; never silently reinterpret or erase old WAL.
            stream->fd = open(path.c_str(), O_RDWR | O_APPEND);
            if (stream->fd < 0) WalError("Open legacy node WAL failed");
            struct stat st;
            if (fstat(stream->fd, &st) != 0) WalError("Stat node WAL failed");
            stream->end = st.st_size;
            if (log_replay_) log_replay_->RegisterStream(node, stream->end);
        } else {
            const auto tail = log_replay_ ? log_replay_->StreamTail(node) : wal::Recover(path);
            stream->end = tail.offset;
            stream->segments.reset(new wal::Writer(path, stream->end));
            if (log_replay_) log_replay_->RegisterStream(node, tail.offset, true, tail.payload_bytes);
        }
    }
    return *stream;
}

void LogManager::write_batch_log_to_disk(const std::string& batch, WriteTiming* timing) {
    AppendAndSync(batch.data(), batch.size(), timing);
}
void LogManager::write_batch_log_to_disk(char* batch, size_t size) {
    AppendAndSync(batch, size);
}
void LogManager::write_raft_log_to_disk(const std::string& batch) {
    AppendAndSync(batch.data(), batch.size());
}

void LogManager::AppendAndSync(const char* data, size_t size, WriteTiming* timing) {
    if (size == 0) return;
    // The record header identifies the producer. A compute batch cannot mix
    // streams, including metadata records (Prepare / Backup / transaction end).
    if (size < LOG_HEADER_SIZE) WalError("Truncated WAL batch");
    node_id_t node;
    memcpy(&node, data + OFFSET_LOG_NODE_ID, sizeof(node));
    if (node < 0) WalError("Invalid WAL producer");
    size_t offset = 0;
    while (offset < size) {
        if (size - offset < LOG_HEADER_SIZE) WalError("Truncated WAL header");
        uint32_t length;
        node_id_t producer;
        memcpy(&length, data + offset + OFFSET_LOG_TOT_LEN, sizeof(length));
        memcpy(&producer, data + offset + OFFSET_LOG_NODE_ID, sizeof(producer));
        if (producer != node || length < LOG_HEADER_SIZE || length > size - offset)
            WalError("Invalid or mixed-stream WAL batch");
        offset += length;
    }
    Stream& stream = GetStream(node);
    // One in-flight batch per producer. Different nodes never take this lock.
    std::lock_guard<bthread::Mutex> guard(stream.mutex);
    if (stream.segments) {
        const auto cost = stream.segments->Append(data, size);
        write_total_time_ns_.fetch_add(cost.write_ns);
        fdatasync_total_time_ns_.fetch_add(cost.sync_ns);
        logwrite_count_.fetch_add(1);
        fdatasync_count_.fetch_add(cost.sync_calls);
        stream.end = stream.segments->end();
        if (timing) *timing = {cost.write_ns, cost.sync_ns};
        if (log_replay_) log_replay_->PublishDurable(node, stream.end, size);
        return;
    }
    auto begin = std::chrono::steady_clock::now();
    size_t written = 0;
    while (written < size) {
        const ssize_t n = write(stream.fd, data + written, size - written);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) WalError("WAL write failed");
        written += n;
    }
    auto sync_begin = std::chrono::steady_clock::now();
    const auto write_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(sync_begin - begin).count();
    write_total_time_ns_.fetch_add(write_ns);
    if (fdatasync(stream.fd) != 0) WalError("WAL fdatasync failed");
    const auto sync_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - sync_begin).count();
    fdatasync_total_time_ns_.fetch_add(sync_ns);
    if (timing) *timing = {static_cast<uint64_t>(write_ns), static_cast<uint64_t>(sync_ns)};
    stream.end += written;
    logwrite_count_.fetch_add(1);
    fdatasync_count_.fetch_add(1);
    // Replay may only consume bytes whose durability has been confirmed.
    if (log_replay_) log_replay_->PublishDurable(node, stream.end, size);
}

int64_t LogManager::get_lseek_time_ns() const { return 0; }
int64_t LogManager::get_write_time_ns() const { return write_total_time_ns_.load(); }
int64_t LogManager::get_fdatasync_time_ns() const { return fdatasync_total_time_ns_.load(); }
int64_t LogManager::get_logwrite_count() const { return logwrite_count_.load(); }
int64_t LogManager::get_fdatasync_count() const { return fdatasync_count_.load(); }
// Keep the existing report column: no cross-stream fdatasync coalescing now.
int64_t LogManager::get_group_commit_logwrite_sum() const { return logwrite_count_.load(); }
uint64_t LogManager::initialized_segments() const {
    std::lock_guard<std::mutex> lock(streams_mutex_);
    uint64_t total = 0;
    for (const auto& entry : streams_) if (entry.second->segments)
        total += entry.second->segments->initialized_segments();
    return total;
}
uint64_t LogManager::initialization_ns() const {
    std::lock_guard<std::mutex> lock(streams_mutex_);
    uint64_t total = 0;
    for (const auto& entry : streams_) if (entry.second->segments)
        total += entry.second->segments->initialization_ns();
    return total;
}
