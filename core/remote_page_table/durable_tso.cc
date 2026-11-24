#include "remote_page_table/durable_tso.h"

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>

#include <butil/logging.h>

namespace mvcc {
namespace {
constexpr uint64_t kBlock = 65536;
constexpr uint64_t kMaxCts = UINT64_MAX - 1;
struct Watermark {
    uint64_t end;
    uint64_t complement;
};
} // namespace

DurableTso::DurableTso(std::string directory) : directory_(std::move(directory)) {}

DurableTso::~DurableTso() {
    if (lock_fd_ >= 0) close(lock_fd_);
}

void DurableTso::OpenLocked() {
    if (lock_fd_ >= 0) return;
    lock_fd_ = open((directory_ + "/mvcc_tso.lock").c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0600);
    PCHECK(lock_fd_ >= 0);
    PCHECK(flock(lock_fd_, LOCK_EX | LOCK_NB) == 0) << "another oracle owns this timestamp directory";

    const int fd = open((directory_ + "/mvcc_tso.watermark").c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        PCHECK(errno == ENOENT);
        return;
    }
    struct stat info{};
    PCHECK(fstat(fd, &info) == 0);
    CHECK_EQ(info.st_size, sizeof(Watermark));
    Watermark w{};
    PCHECK(read(fd, &w, sizeof(w)) == sizeof(w));
    PCHECK(close(fd) == 0);
    CHECK(w.end > 0 && w.end <= kMaxCts && w.complement == ~w.end) << "invalid durable TSO watermark";
    current_ = reserved_ = w.end;
}

void DurableTso::ReserveThroughLocked(uint64_t needed) {
    if (needed <= reserved_) return;
    const uint64_t end = needed + std::min(kBlock - 1, kMaxCts - needed);
    const Watermark w{end, ~end};
    const auto path = directory_ + "/mvcc_tso.watermark";
    const auto tmp = path + ".tmp";
    const int fd = open(tmp.c_str(), O_CREAT | O_TRUNC | O_WRONLY | O_CLOEXEC, 0600);
    PCHECK(fd >= 0);
    PCHECK(write(fd, &w, sizeof(w)) == sizeof(w));
    PCHECK(fsync(fd) == 0);
    PCHECK(close(fd) == 0);
    PCHECK(rename(tmp.c_str(), path.c_str()) == 0);
    const int dir = open(directory_.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    PCHECK(dir >= 0 && fsync(dir) == 0);
    PCHECK(close(dir) == 0);
    reserved_ = end;
}

uint64_t DurableTso::Snapshot(uint64_t floor) {
    std::lock_guard<std::mutex> guard(mutex_);
    OpenLocked();
    CHECK_LE(floor, kMaxCts);
    ReserveThroughLocked(floor);
    current_ = std::max(current_, floor);
    return current_;
}

uint64_t DurableTso::Commit(uint64_t floor) {
    std::lock_guard<std::mutex> guard(mutex_);
    OpenLocked();
    const auto last = std::max(current_, floor);
    CHECK_LT(last, kMaxCts) << "commit timestamp space exhausted";
    ReserveThroughLocked(last + 1);
    return current_ = last + 1;
}

} // namespace mvcc
