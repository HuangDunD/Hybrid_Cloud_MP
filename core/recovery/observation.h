#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <fcntl.h>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <time.h>
#include <unistd.h>
#include <vector>
#include "core/recovery/fetch_error.h"

namespace recovery_observation {

inline uint64_t WallUs() {
    timespec ts{};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return uint64_t(ts.tv_sec) * 1000000 + ts.tv_nsec / 1000;
}
inline uint64_t CpuUs() {
    timespec ts{};
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
    return uint64_t(ts.tv_sec) * 1000000 + ts.tv_nsec / 1000;
}
inline uint64_t Mix(uint64_t x) {
    x += 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
}
inline uint64_t EnvUInt(const char* name, uint64_t fallback, uint64_t maximum) {
    const char* text = std::getenv(name);
    if (!text || !*text || *text == '-') return fallback;
    char* end = nullptr;
    auto value = std::strtoull(text, &end, 10);
    return end && !*end ? std::min<uint64_t>(value, maximum) : fallback;
}

struct Context {
    uint64_t request = 0, lookup = 0, key = 0, generation = 0;
    uint64_t logical = 0, attempt = 0, admitted_us = 0, deadline_us = 0;
    const std::atomic<bool>* cancelled = nullptr;
    bool sampled = false;
};
inline thread_local Context context;

class ContextScope {
public:
    explicit ContextScope(const Context& captured) : previous_(context) { context = captured; }
    ~ContextScope() { context = previous_; }
    ContextScope(const ContextScope&) = delete;
    ContextScope& operator=(const ContextScope&) = delete;
private:
    Context previous_;
};
inline bool IsCancelled() {
    return (context.cancelled && context.cancelled->load(std::memory_order_relaxed)) ||
           (context.deadline_us && WallUs() >= context.deadline_us);
}
inline void CheckCancelled() { if (IsCancelled()) throw recovery::RequestCancelled(); }

struct Event {
    uint64_t ts, request, lookup, epoch, key, generation, logical, attempt;
    int64_t table, page, a, b, c;
    const char* name;
    const char* reason;
};

// Observations are never a READY certificate. Names/reasons must be string literals.
class Recorder {
    static constexpr size_t kThreads = 32;
    struct Buffer {
        std::atomic<bool> leased{false};
        std::mutex mutex;
        std::unique_ptr<Event[]> events;
        size_t head = 0, size = 0;
    };
public:
    static Recorder& Get() { static Recorder recorder; return recorder; }
    bool Enabled() const { return enabled_; }
    uint64_t NextId() { return sequence_.fetch_add(1, std::memory_order_relaxed); }
    uint64_t Epoch() const { return epoch_.load(std::memory_order_relaxed); }
    void SetEpoch(uint64_t epoch) { epoch_.store(epoch, std::memory_order_relaxed); }
    bool Sample(uint64_t id) const {
        return Mix(id ^ seed_ ^ Mix(uint64_t(node_ + 1))) % 1000000 < sample_ppm_;
    }
    size_t Capacity() const { return per_thread_ * kThreads; }
    uint64_t Dropped() const { return dropped_.load(); }
    void Add(const char* name, int64_t table = -1, int64_t page = -1,
             int64_t a = 0, int64_t b = 0, int64_t c = 0, const char* reason = "") {
        if (!enabled_) return;
        auto* buffer = LocalBuffer();
        if (!buffer) { dropped_.fetch_add(1, std::memory_order_relaxed); return; }
        Event event{WallUs(), context.request, context.lookup, Epoch(), context.key,
                    context.generation, context.logical, context.attempt, table, page, a, b, c, name, reason};
        std::lock_guard<std::mutex> lock(buffer->mutex);
        if (buffer->size < per_thread_) {
            buffer->events[(buffer->head + buffer->size) % per_thread_] = event;
            ++buffer->size;
            auto count = buffered_.fetch_add(1, std::memory_order_relaxed) + 1;
            auto peak = peak_buffered_.load(std::memory_order_relaxed);
            while (count > peak && !peak_buffered_.compare_exchange_weak(peak, count, std::memory_order_relaxed)) {}
        } else dropped_.fetch_add(1, std::memory_order_relaxed);
    }
    void Flush() {
        if (!enabled_) return;
        std::lock_guard<std::mutex> output_lock(output_mutex_);
        uint64_t wall = WallUs(), cpu = CpuUs();
        std::array<Event, 128> chunk;
        for (auto& buffer : buffers_) {
            size_t remaining;
            { std::lock_guard<std::mutex> lock(buffer.mutex); remaining = buffer.size; }
            while (remaining) {
                size_t count = std::min(chunk.size(), remaining);
                {
                    std::lock_guard<std::mutex> lock(buffer.mutex);
                    for (size_t i = 0; i < count; ++i) chunk[i] = buffer.events[(buffer.head + i) % per_thread_];
                    buffer.head = (buffer.head + count) % per_thread_;
                    buffer.size -= count;
                    buffered_.fetch_sub(count, std::memory_order_relaxed);
                }
                remaining -= count;
                for (size_t i = 0; i < count; ++i) {
                    const auto& e = chunk[i];
                    std::fprintf(output_, "{\"event\":\"%s\",\"reason\":\"%s\",\"ts_us\":%llu,\"pid\":%d,\"node\":%d,\"request\":%llu,\"lookup\":%llu,\"epoch\":%llu,\"key\":%llu,\"generation\":%llu,\"logical\":%llu,\"attempt\":%llu,\"table\":%lld,\"page\":%lld,\"a\":%lld,\"b\":%lld,\"c\":%lld}\n",
                        e.name, e.reason, (unsigned long long)e.ts, int(getpid()), node_,
                        (unsigned long long)e.request, (unsigned long long)e.lookup,
                        (unsigned long long)e.epoch, (unsigned long long)e.key,
                        (unsigned long long)e.generation, (unsigned long long)e.logical,
                        (unsigned long long)e.attempt, (long long)e.table, (long long)e.page,
                        (long long)e.a, (long long)e.b, (long long)e.c);
                    ++written_;
                }
            }
        }
        std::fflush(output_);
        output_wall_us_ += WallUs() - wall;
        output_cpu_us_ += CpuUs() - cpu;
    }
private:
    Recorder() {
        const char* directory = std::getenv("HCM_TRACE_DIR");
        if (!directory || !*directory) return;
        per_thread_ = std::max<uint64_t>(1, EnvUInt("HCM_TRACE_CAPACITY", 32768, 1048576) / kThreads);
        seed_ = EnvUInt("HCM_TRACE_SEED", 20260915, UINT64_MAX);
        node_ = int(EnvUInt("HCM_TRACE_NODE", 0, 1024));
        double rate = 0.01;
        if (const char* text = std::getenv("HCM_TRACE_SAMPLE_RATE")) {
            char* end = nullptr;
            double parsed = std::strtod(text, &end);
            if (end && !*end && std::isfinite(parsed)) rate = std::max(0.0, std::min(1.0, parsed));
        }
        sample_ppm_ = uint64_t(rate * 1000000);
        std::string path = std::string(directory) + "/trace-" + std::to_string(getpid()) + ".jsonl";
        int fd = ::open(path.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0600);
        if (fd < 0) return;
        output_ = ::fdopen(fd, "w");
        if (!output_) { ::close(fd); return; }
        enabled_ = true;
        std::fprintf(output_, "{\"event\":\"trace_config\",\"schema\":2,\"pid\":%d,\"node\":%d,\"sample_ppm\":%llu,\"seed\":%llu,\"capacity\":%zu,\"event_bytes\":%zu,\"clock\":\"CLOCK_MONOTONIC\",\"safe_ready_supported\":false}\n",
            int(getpid()), node_, (unsigned long long)sample_ppm_, (unsigned long long)seed_, Capacity(), sizeof(Event));
        writer_ = std::thread([this] {
            std::unique_lock<std::mutex> lock(wake_mutex_);
            while (!stop_) {
                wake_.wait_for(lock, std::chrono::milliseconds(100), [this] { return stop_; });
                lock.unlock();
                Flush();
                lock.lock();
            }
        });
    }
    ~Recorder() {
        if (!enabled_) return;
        { std::lock_guard<std::mutex> lock(wake_mutex_); stop_ = true; }
        wake_.notify_all();
        writer_.join();
        Flush();
        std::fprintf(output_, "{\"event\":\"trace_summary\",\"dropped_events\":%llu,\"registered_threads\":%zu,\"buffer_capacity_bytes\":%zu,\"allocated_buffer_bytes\":%zu,\"peak_buffered_events\":%llu,\"written_events\":%llu,\"output_wall_us\":%llu,\"output_cpu_us\":%llu,\"io_error\":%s}\n",
            (unsigned long long)Dropped(), next_buffer_.load(), Capacity() * sizeof(Event), allocated_bytes_.load(),
            (unsigned long long)peak_buffered_.load(), (unsigned long long)written_,
            (unsigned long long)output_wall_us_, (unsigned long long)output_cpu_us_, std::ferror(output_) ? "true" : "false");
        std::fclose(output_);
    }
    Buffer* LocalBuffer() {
        struct Lease {
            Buffer* buffer = nullptr;
            ~Lease() { if (buffer) buffer->leased.store(false, std::memory_order_release); }
        };
        static thread_local Lease lease;
        if (lease.buffer) return lease.buffer;
        for (auto& buffer : buffers_) {
            bool expected = false;
            if (!buffer.leased.compare_exchange_strong(expected, true, std::memory_order_acquire)) continue;
            std::lock_guard<std::mutex> lock(buffer.mutex);
            if (!buffer.events) {
                buffer.events.reset(new Event[per_thread_]);
                allocated_bytes_.fetch_add(per_thread_ * sizeof(Event), std::memory_order_relaxed);
            }
            ++next_buffer_;
            lease.buffer = &buffer;
            return lease.buffer;
        }
        return nullptr;
    }
    bool enabled_ = false, stop_ = false;
    FILE* output_ = nullptr;
    size_t per_thread_ = 1;
    int node_ = 0;
    uint64_t sample_ppm_ = 10000, seed_ = 20260915;
    std::atomic<uint64_t> sequence_{1}, epoch_{0}, dropped_{0};
    std::atomic<size_t> next_buffer_{0}, allocated_bytes_{0};
    std::atomic<uint64_t> buffered_{0}, peak_buffered_{0};
    uint64_t written_ = 0, output_wall_us_ = 0, output_cpu_us_ = 0;
    std::array<Buffer, kThreads> buffers_;
    std::thread writer_;
    std::mutex output_mutex_, wake_mutex_;
    std::condition_variable wake_;
};
inline bool Enabled() { return Recorder::Get().Enabled(); }
inline void Emit(const char* name, int64_t table = -1, int64_t page = -1,
                 int64_t a = 0, int64_t b = 0, int64_t c = 0, const char* reason = "") {
    Recorder::Get().Add(name, table, page, a, b, c, reason);
}

class Span {
public:
    explicit Span(const char* name, int64_t table = -1, int64_t page = -1)
        : name_(name), table_(table), page_(page), active_(Enabled()) {
        if (active_) { wall_ = WallUs(); cpu_ = CpuUs(); }
    }
    ~Span() { Stop(); }
    void Stop(int64_t result = 0) {
        if (!active_) return;
        Emit("stage", table_, page_, WallUs() - wall_, CpuUs() - cpu_, result, name_);
        active_ = false;
    }
private:
    const char* name_;
    int64_t table_, page_;
    bool active_;
    uint64_t wall_ = 0, cpu_ = 0;
};

class RequestScope {
public:
    explicit RequestScope(uint64_t logical_id = 0, bool supported = true,
                          uint64_t admitted_us = 0, uint64_t attempt = 0,
                          const std::atomic<bool>* cancelled = nullptr, uint64_t deadline_us = 0)
        : active_(supported && Enabled()), installed_(supported), previous_(context) {
        if (!installed_) return;
        context = {};
        context.cancelled = cancelled;
        context.deadline_us = deadline_us;
        if (!active_) return;
        context.request = Recorder::Get().NextId();
        context.logical = logical_id ? logical_id : context.request;
        context.attempt = attempt;
        context.admitted_us = admitted_us ? admitted_us : WallUs();
        Emit("request_begin", -1, -1, context.logical, attempt, context.admitted_us);
    }
    ~RequestScope() {
        if (!installed_) return;
        if (active_) Emit("request_end", -1, -1,
            IsCancelled() ? 3 : (std::uncaught_exceptions() ? 2 : outcome_));
        context = previous_;
    }
    // 0=unverified execution, 1=external BLink+heap validation, 2=failure, 3=cancel.
    void Finish(int outcome = 0) { outcome_ = outcome; }
    RequestScope(const RequestScope&) = delete;
    RequestScope& operator=(const RequestScope&) = delete;
private:
    bool active_, installed_;
    Context previous_;
    int outcome_ = 2;
};

class LookupScope {
public:
    LookupScope(int64_t table, uint64_t key, uint64_t generation)
        : active_(Enabled()), previous_(context), table_(table) {
        if (!active_) return;
        owns_request_ = !context.request;
        if (owns_request_) {
            context.request = Recorder::Get().NextId();
            context.logical = context.request;
            context.admitted_us = WallUs();
            Emit("request_begin", -1, -1, context.logical, 0, context.admitted_us, "lookup_only_unverified");
        }
        context.lookup = Recorder::Get().NextId();
        context.key = key;
        context.generation = generation;
        context.sampled = Recorder::Get().Sample(context.lookup);
        Emit("lookup_begin", table_, -1, context.sampled);
    }
    ~LookupScope() {
        if (!active_) return;
        if (!finished_) Emit("lookup_cancel", table_);
        if (owns_request_) Emit("request_end", -1, -1, IsCancelled() ? 3 : (finished_ ? 0 : 2));
        context = previous_;
    }
    void Finish(bool found, int64_t data_page, int64_t slot, uint64_t end_generation = 0) {
        if (!active_) return;
        Emit("path_status", table_, -1, end_generation, end_generation == context.generation, 0, "local_generation_not_authoritative");
        if (found) Emit("lookup_target", table_ >= 10000 && table_ < 20000 ? table_ - 10000 : -1, data_page, slot);
        Emit("lookup_end", table_, found ? data_page : -1, found, found ? slot : -1, context.sampled);
        finished_ = true;
    }
private:
    bool active_, finished_ = false, owns_request_ = false;
    Context previous_;
    int64_t table_;
};

class FetchScope;
inline thread_local FetchScope* current_fetch = nullptr;
class FetchScope {
public:
    FetchScope(int64_t table, int64_t page)
        : active_(Enabled()), table_(table), page_(page), previous_(current_fetch), saved_(context) {
        current_fetch = this;
        if (!active_) return;
        if (!context.request) context.request = Recorder::Get().NextId();
        start_ = WallUs();
    }
    ~FetchScope() {
        Unblock(false);
        current_fetch = previous_;
        context = saved_;
    }
    void AuthorizeStorage(bool authorized) { storage_authorized_ = authorized; }
    bool StorageAuthorized() const { return storage_authorized_; }
    void Block(const char* reason) {
        if (!active_ || (reason_ && std::strcmp(reason_, reason) == 0)) return;
        Unblock(false);
        reason_ = reason;
        blocked_ = WallUs();
        Emit("block", table_, page_, 0, 0, 0, reason_);
    }
    void Unblock(bool completed = true) {
        if (!active_ || !reason_) return;
        Emit("unblock", table_, page_, WallUs() - blocked_, completed, 0, reason_);
        reason_ = nullptr;
    }
    void Complete(bool local = false) {
        if (!active_) return;
        Unblock();
        Emit("page_access", table_, page_, WallUs() - start_, local);
    }
private:
    bool active_;
    bool storage_authorized_ = false;
    int64_t table_, page_;
    FetchScope* previous_;
    Context saved_;
    const char* reason_ = nullptr;
    uint64_t start_ = 0, blocked_ = 0;
};
inline void Block(const char* reason) { if (current_fetch) current_fetch->Block(reason); }
inline void Unblock() { if (current_fetch) current_fetch->Unblock(); }
inline void RequireStorageSource() {
    CheckCancelled();
    if (current_fetch && !current_fetch->StorageAuthorized())
        throw recovery::PageUnavailable("unverified storage fallback after missing replica or interrupted page push");
}
inline void PathPage(int64_t table, int64_t page, int role) {
    if (context.lookup && context.sampled) Emit("path_page", table, page, role);
}

}  // namespace recovery_observation
