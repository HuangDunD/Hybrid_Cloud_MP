#include "wal_segments.h"
#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <butil/crc32c.h>
#include <butil/logging.h>

namespace wal {
namespace {
constexpr uint64_t kMagic = 0x314c4157504d4348ULL; // HCMPWAL1
using Clock = std::chrono::steady_clock;
void Check(bool ok, const char* message) {
    if (!ok) { LOG(ERROR) << message; std::abort(); }
}
void CheckIO(bool ok, const char* message) {
    if (!ok) { PLOG(ERROR) << message; std::abort(); }
}
uint64_t Elapsed(Clock::time_point start) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count();
}
void Put(char* p, uint64_t value, size_t n) {
    for (size_t i = 0; i < n; ++i) p[i] = static_cast<char>(value >> (8 * i));
}
uint64_t Get(const char* p, size_t n) {
    uint64_t value = 0;
    for (size_t i = 0; i < n; ++i) value |= uint64_t(static_cast<unsigned char>(p[i])) << (8 * i);
    return value;
}
void WriteAt(int fd, const char* p, size_t size, uint64_t offset) {
    while (size) {
        const auto n = pwrite(fd, p, size, offset);
        if (n < 0 && errno == EINTR) continue;
        CheckIO(n > 0, "WAL pwrite");
        p += n; offset += n; size -= n;
    }
}
void Zero(int fd, uint64_t begin) {
    static const std::array<char, 256 * 1024> zeros{};
    while (begin < kSegmentSize) {
        const size_t n = std::min<uint64_t>(zeros.size(), kSegmentSize - begin);
        WriteAt(fd, zeros.data(), n, begin);
        begin += n;
    }
}
void SyncDirectory(const std::string& stream) {
    auto directory = std::filesystem::path(stream).parent_path();
    if (directory.empty()) directory = ".";
    const int fd = open(directory.c_str(), O_RDONLY | O_DIRECTORY);
    CheckIO(fd >= 0 && fsync(fd) == 0, "Sync WAL directory");
    close(fd);
}
}

std::string SegmentPath(const std::string& stream, uint64_t segment) {
    return stream + "." + std::to_string(segment);
}
bool IsSegment(const std::string& suffix, unsigned long& node) {
    const auto dot = suffix.find('.');
    if (dot == std::string::npos || dot == 0 || dot + 1 == suffix.size() ||
        suffix.substr(dot + 1).find_first_not_of("0123456789") != std::string::npos ||
        suffix.substr(0, dot).find_first_not_of("0123456789") != std::string::npos) return false;
    node = std::stoul(suffix.substr(0, dot));
    return true;
}
Reader::~Reader() { if (fd_ >= 0) close(fd_); }
void Reader::Read(char* data, size_t size, uint64_t offset) {
    while (size) {
        const uint64_t segment = offset / kSegmentSize;
        if (segment != segment_) {
            if (fd_ >= 0) close(fd_);
            fd_ = open(SegmentPath(stream_, segment).c_str(), O_RDONLY);
            segment_ = segment;
        }
        CheckIO(fd_ >= 0, "Open WAL segment for replay");
        const size_t chunk = std::min<uint64_t>(size, kSegmentSize - offset % kSegmentSize);
        const auto n = pread(fd_, data, chunk, offset % kSegmentSize);
        if (n < 0 && errno == EINTR) continue;
        CheckIO(n > 0, "Read WAL segment");
        data += n; size -= n; offset += n;
    }
}
bool Reader::ReadBatch(uint64_t& offset, uint64_t limit, std::vector<char>& payload) {
    char header[kFrameHeaderSize];
    if (offset > limit || limit - offset < sizeof(header)) return false;
    Read(header, sizeof(header), offset);
    if (Get(header, 8) != kMagic || Get(header + 8, 8) != offset ||
        Get(header + 28, 4) != butil::crc32c::Value(header, 28)) return false;
    const uint64_t size = Get(header + 16, 8);
    if (!size || size > limit - offset - sizeof(header)) return false;
    payload.resize(size);
    Read(payload.data(), size, offset + sizeof(header));
    if (Get(header + 24, 4) != butil::crc32c::Value(payload.data(), size)) return false;
    offset += sizeof(header) + size;
    return true;
}

Tail Recover(const std::string& stream) {
    uint64_t segments = 0;
    const auto stream_path = std::filesystem::path(stream);
    auto directory = stream_path.parent_path();
    if (directory.empty()) directory = ".";
    const std::string prefix = stream_path.filename().string() + ".";
    uint64_t files = 0;
    for (const auto& entry : std::filesystem::directory_iterator(directory)) {
        const std::string name = entry.path().filename().string();
        if (name.compare(0, prefix.size(), prefix) != 0) continue;
        const auto suffix = name.substr(prefix.size());
        if (!suffix.empty() && suffix.find_first_not_of("0123456789") == std::string::npos) ++files;
    }
    while (std::filesystem::exists(SegmentPath(stream, segments))) {
        Check(std::filesystem::file_size(SegmentPath(stream, segments)) == kSegmentSize,
              "Invalid WAL segment size");
        ++segments;
    }
    Check(files == segments, "Missing WAL segment; cannot recover a discontinuous stream");
    Tail tail;
    Reader reader(stream);
    std::vector<char> batch;
    while (reader.ReadBatch(tail.offset, segments * kSegmentSize, batch)) tail.payload_bytes += batch.size();
    // Discard the unsynced/torn suffix, keeping the allocation of the current
    // segment. No persistent replay cursor can skip previously acknowledged WAL.
    const uint64_t current = tail.offset / kSegmentSize;
    if (current < segments) {
        const int fd = open(SegmentPath(stream, current).c_str(), O_RDWR);
        CheckIO(fd >= 0, "Open WAL recovery tail");
        Zero(fd, tail.offset % kSegmentSize);
        CheckIO(fdatasync(fd) == 0, "Sync WAL recovery tail");
        close(fd);
    }
    for (uint64_t i = current + 1; i < segments; ++i)
        CheckIO(unlink(SegmentPath(stream, i).c_str()) == 0, "Remove incomplete WAL suffix");
    if (segments) SyncDirectory(stream);
    return tail;
}

Writer::Writer(std::string stream, uint64_t end) : stream_(std::move(stream)), end_(end) {
    const uint64_t current = end_ / kSegmentSize;
    Prepare(current);
    Prepare(current + 1);
    requested_ = current + 1;
    prepare_thread_ = std::thread(&Writer::PrepareLoop, this);
}
Writer::~Writer() {
    { std::lock_guard<std::mutex> lock(request_mutex_); stop_ = true; }
    changed_.notify_one();
    prepare_thread_.join();
    if (fd_ >= 0) close(fd_);
}
void Writer::Preallocate(uint64_t bytes) {
    const uint64_t segments = bytes / kSegmentSize + (bytes % kSegmentSize != 0);
    for (uint64_t segment = end_ / kSegmentSize; segment < segments; ++segment)
        Prepare(segment);
}
void Writer::Prepare(uint64_t segment) {
    std::lock_guard<std::mutex> lock(prepare_mutex_);
    const auto path = SegmentPath(stream_, segment);
    if (std::filesystem::exists(path)) return;
    const auto start = Clock::now();
    const auto temporary = path + ".init";
    const int fd = open(temporary.c_str(), O_CREAT | O_TRUNC | O_RDWR, 0644);
    CheckIO(fd >= 0, "Create WAL segment");
    // Actual zero writes, not just fallocate: initialize extents and metadata
    // before foreground overwrites, as PG/elog do for initialized WAL segments.
    Zero(fd, 0);
    CheckIO(fsync(fd) == 0, "Initialize WAL segment");
    close(fd);
    CheckIO(rename(temporary.c_str(), path.c_str()) == 0, "Install WAL segment");
    SyncDirectory(stream_);
    initialized_segments_.fetch_add(1);
    initialization_ns_.fetch_add(Elapsed(start));
}
void Writer::RequestNext(uint64_t segment) {
    {
        std::lock_guard<std::mutex> lock(request_mutex_);
        if (segment <= requested_) return;
        requested_ = segment;
    }
    changed_.notify_one();
}
void Writer::PrepareLoop() {
    std::unique_lock<std::mutex> lock(request_mutex_);
    uint64_t prepared = 0;
    while (true) {
        changed_.wait(lock, [&] { return stop_ || requested_ > prepared; });
        if (stop_) return;
        const auto next = requested_;
        lock.unlock();
        Prepare(next);
        lock.lock();
        prepared = next;
    }
}
Writer::Timing Writer::Append(const char* data, size_t size) {
    std::vector<char> frame(kFrameHeaderSize + size);
    Put(frame.data(), kMagic, 8);
    Put(frame.data() + 8, end_, 8);
    Put(frame.data() + 16, size, 8);
    Put(frame.data() + 24, butil::crc32c::Value(data, size), 4);
    Put(frame.data() + 28, butil::crc32c::Value(frame.data(), 28), 4);
    memcpy(frame.data() + kFrameHeaderSize, data, size);
    Timing timing;
    size_t written = 0;
    while (written < frame.size()) {
        const auto segment = end_ / kSegmentSize;
        if (segment != open_segment_) {
            Prepare(segment); // normally already ready; never overwrite uninitialized space
            if (fd_ >= 0) close(fd_);
            fd_ = open(SegmentPath(stream_, segment).c_str(), O_RDWR);
            CheckIO(fd_ >= 0, "Open WAL segment");
            open_segment_ = segment;
        }
        const size_t chunk = std::min<uint64_t>(frame.size() - written, kSegmentSize - end_ % kSegmentSize);
        auto start = Clock::now();
        WriteAt(fd_, frame.data() + written, chunk, end_ % kSegmentSize);
        timing.write_ns += Elapsed(start);
        start = Clock::now();
        CheckIO(fdatasync(fd_) == 0, "WAL fdatasync");
        timing.sync_ns += Elapsed(start);
        ++timing.sync_calls;
        end_ += chunk; written += chunk;
        if (end_ % kSegmentSize >= kSegmentSize / 2) RequestNext(end_ / kSegmentSize + 1);
    }
    return timing;
}
}
