#pragma once

#include <array>
#include <atomic>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <ostream>
#include <map>
#include <string>
#include <stdexcept>

// Coordinator-side measurements. Each DTX owns its counters; workers merge them
// under the existing statistics mutex after execution, never on the hot path.
namespace benchmark_stats {
inline uint64_t NowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}
enum Stage { Begin, Execute, Commit, Abort, StageCount };
inline constexpr const char* stages[] = {"begin", "execute", "commit", "abort"};
inline constexpr const char* paths[] = {"eager", "lazy", "2pc", "single", "mix_lazy", "mix_2pc"};

// One measurement clock per compute node, started after all local workers initialize.
// Outcome counters are updated by workload loops, never by participant RPCs/TxExe.
struct Runtime {
    bool timed = false;
    double run_seconds = 60, prewarm_seconds = 10;
    enum Phase { Prewarm, ResetWorkers, Measure };
    std::atomic<Phase> phase{Measure};
    std::atomic<unsigned> paused{0}, reset_done{0};
    uint64_t prewarm_attempts = 0, prewarm_commits = 0;
    std::atomic<unsigned> ready{0};
    std::atomic<uint64_t> start_ns{0}, last_finish_ns{0};
    std::atomic<uint64_t> attempts{0}, commits{0}, aborts{0};
    std::atomic<uint64_t> window_attempts{0}, window_commits{0}, window_aborts{0};
    uint64_t end_ns = 0; // Written only after all workers have finished.
    void Configure(const std::string& mode, double seconds, double prewarm = 10) {
        if ((mode != "count" && mode != "time") || !(seconds > 0 && seconds <= 86400))
            throw std::invalid_argument("Invalid benchmark run mode/run time");
        if (!(prewarm >= 0 && prewarm <= 86400))
            throw std::invalid_argument("Invalid benchmark prewarm time");
        timed = mode == "time"; run_seconds = seconds; prewarm_seconds = prewarm;
        phase = prewarm > 0 ? Prewarm : Measure;
    }
    uint64_t Deadline() const { return start_ns.load() + static_cast<uint64_t>((phase.load() == Prewarm ? prewarm_seconds : run_seconds) * 1e9); }
    void Start(uint64_t now = NowNs()) { start_ns.store(now); }
    bool WithinWindow(uint64_t now) const { return (!timed && phase.load() != Prewarm) || now < Deadline(); }
    bool TryStart(uint64_t local_attempts, uint64_t limit, uint64_t now = NowNs()) {
        if ((timed || phase.load() == Prewarm) ? !WithinWindow(now) : local_attempts >= limit) return false;
        attempts.fetch_add(1, std::memory_order_relaxed);
        window_attempts.fetch_add(1, std::memory_order_relaxed);
        return true;
    }
    void Finish(bool success, uint64_t now = NowNs()) {
        (success ? commits : aborts).fetch_add(1, std::memory_order_relaxed);
        if (WithinWindow(now))
            (success ? window_commits : window_aborts).fetch_add(1, std::memory_order_relaxed);
        auto last = last_finish_ns.load();
        while (last < now && !last_finish_ns.compare_exchange_weak(last, now)) {}
    }
    void Stop() { end_ns = std::max(last_finish_ns.load(), timed ? Deadline() : start_ns.load()); }
    double MeasuredSeconds() const {
        return timed ? run_seconds : static_cast<double>(end_ns - start_ns.load()) / 1e9;
    }
    void Dump(std::ostream& out) const {
        const double measured = MeasuredSeconds();
        const double elapsed = static_cast<double>(end_ns - start_ns.load()) / 1e9;
        out << "benchmark_run_mode=" << (timed ? "time" : "count") << '\n'
            << "benchmark_prewarm_seconds=" << prewarm_seconds << '\n'
            << "benchmark_prewarm_attempts=" << prewarm_attempts << '\n'
            << "benchmark_prewarm_commits=" << prewarm_commits << '\n'
            << "benchmark_run_seconds=" << run_seconds << '\n'
            << "benchmark_measurement_seconds=" << measured << '\n'
            << "benchmark_elapsed_seconds=" << elapsed << '\n'
            << "benchmark_drain_seconds=" << std::max(0.0, elapsed - measured) << '\n'
            << "benchmark_window_attempts=" << window_attempts.load() << '\n'
            << "benchmark_window_commits=" << window_commits.load() << '\n'
            << "benchmark_window_aborts=" << window_aborts.load() << '\n'
            << "benchmark_final_attempts=" << attempts.load() << '\n'
            << "benchmark_final_commits=" << commits.load() << '\n'
            << "benchmark_final_aborts=" << aborts.load() << '\n'
            << "benchmark_committed_tps=" << (measured > 0 ? window_commits.load() / measured : 0) << '\n'
            << "benchmark_completed_tps=" << (measured > 0 ? (window_commits.load() + window_aborts.load()) / measured : 0) << '\n';
    }
};
inline Runtime runtime;

struct Row {
    uint64_t completed = 0, committed = 0, aborted = 0;
    uint64_t total_ns = 0, committed_ns = 0, aborted_ns = 0;
    std::array<uint64_t, StageCount> stage_ns{};
    void Merge(const Row& other) {
        completed += other.completed; committed += other.committed; aborted += other.aborted;
        total_ns += other.total_ns; committed_ns += other.committed_ns; aborted_ns += other.aborted_ns;
        for (int i = 0; i < StageCount; ++i) stage_ns[i] += other.stage_ns[i];
    }
    void Dump(std::ostream& out, const std::string& prefix) const {
        out << prefix << "completed=" << completed << '\n'
            << prefix << "committed=" << committed << '\n'
            << prefix << "aborted=" << aborted << '\n'
            << prefix << "total_ns=" << total_ns << '\n'
            << prefix << "committed_ns=" << committed_ns << '\n'
            << prefix << "aborted_ns=" << aborted_ns << '\n';
        for (int i = 0; i < StageCount; ++i)
            out << prefix << stages[i] << "_ns=" << stage_ns[i] << '\n';
    }
};
// A transaction's coordinator observes durable YES replies on one clock.
struct PrepareSample {
    uint64_t count = 0, first_ns = 0, tail_ns = 0, all_ns = 0;
    void Merge(const PrepareSample& other) {
        count += other.count; first_ns += other.first_ns;
        tail_ns += other.tail_ns; all_ns += other.all_ns;
    }
    void Dump(std::ostream& out, const std::string& prefix) const {
        out << prefix << "prepare_sample_count=" << count << '\n'
            << prefix << "prepare_first_ns=" << first_ns << '\n'
            << prefix << "prepare_tail_ns=" << tail_ns << '\n'
            << prefix << "prepare_all_ns=" << all_ns << '\n';
    }
};

// Request-side, mutually exclusive page acquisition paths, not RTT estimates.
// A pending release can contain concurrent revocations and asynchronous pushes.
enum PagePath { LocalReady, LocalManager, OwnerManager, ForwardedPage,
                LocalUpgrade, StoragePage, PagePathCount };
inline constexpr const char* page_paths[] = {"local_ready", "local_manager", "owner_manager",
    "forwarded_page", "local_upgrade", "storage_page"};
inline constexpr const char* page_phases[] = {"begin", "execute", "commit", "abort", "unscoped"};
struct PageRow {
    std::atomic<uint64_t> count{0}, total_ns{0}, local_lock_ns{0}, transfer_log_wait_ns{0};
    std::atomic<uint64_t> transfer_log_wait_count{0};
    void Add(uint64_t ns, uint64_t wait_ns, uint64_t log_wait_ns = 0) {
        count.fetch_add(1, std::memory_order_relaxed);
        total_ns.fetch_add(ns, std::memory_order_relaxed);
        local_lock_ns.fetch_add(wait_ns, std::memory_order_relaxed);
        transfer_log_wait_ns.fetch_add(log_wait_ns, std::memory_order_relaxed);
        if (log_wait_ns > 0) transfer_log_wait_count.fetch_add(1, std::memory_order_relaxed);
    }
};
inline std::array<std::array<PageRow, PagePathCount>, StageCount + 1> page_access;
inline void DumpPageAccess(std::ostream& out) {
    for (size_t phase = 0; phase < page_access.size(); ++phase)
        for (size_t path = 0; path < PagePathCount; ++path) {
            const auto& row = page_access[phase][path];
            const std::string prefix = std::string("stats_lazy_page_") + page_phases[phase] + "_" + page_paths[path] + "_";
            out << prefix << "count=" << row.count.load() << '\n'
                << prefix << "ns=" << row.total_ns.load() << '\n'
                << prefix << "local_lock_ns=" << row.local_lock_ns.load() << '\n'
                << prefix << "transfer_log_wait_ns=" << row.transfer_log_wait_ns.load() << '\n'
                << prefix << "transfer_log_wait_count=" << row.transfer_log_wait_count.load() << '\n';
        }
}

enum CommitPath { ReadOnly, SingleLocal, SingleRemote, MultiWriter, CommitPathCount };
inline constexpr const char* commit_paths[] = {"read_only", "single_local", "single_remote", "multi_writer"};
inline constexpr const char* commit_parts[] = {"prepare", "decision_log", "final_commit"};
struct CommitRow {
    uint64_t count = 0, total_ns = 0, execute_ns = 0;
    std::array<uint64_t, 3> part_ns{};
    PrepareSample prepare;
    void Merge(const CommitRow& other) {
        count += other.count; total_ns += other.total_ns; execute_ns += other.execute_ns;
        prepare.Merge(other.prepare);
        for (size_t i = 0; i < part_ns.size(); ++i) part_ns[i] += other.part_ns[i];
    }
    void Dump(std::ostream& out, const std::string& prefix) const {
        out << prefix << "count=" << count << '\n'
            << prefix << "total_ns=" << total_ns << '\n'
            << prefix << "execute_ns=" << execute_ns << '\n';
        prepare.Dump(out, prefix);
        for (size_t i = 0; i < part_ns.size(); ++i)
            out << prefix << commit_parts[i] << "_ns=" << part_ns[i] << '\n';
    }
};

// Actual synchronous wait calls, including participant RPC handlers. These
// overlap coordinator phase times and are not a transaction critical path.
enum LogWait { CommitLog, PrepareLog, DecisionLog, AbortLog, PageLog, LogWaitCount };
inline constexpr const char* log_wait_names[] = {"commit_log", "prepare_log", "decision_log", "abort_log", "page_log"};
struct LogWaitRow {
    std::atomic<uint64_t> count{0}, total_ns{0};
    void Add(uint64_t ns) { count.fetch_add(1, std::memory_order_relaxed); total_ns.fetch_add(ns, std::memory_order_relaxed); }
    void Reset() { count = 0; total_ns = 0; }
    void Dump(std::ostream& out, const char* name) const {
        out << name << "_count=" << count.load(std::memory_order_relaxed) << '\n'
            << name << "_ns=" << total_ns.load(std::memory_order_relaxed) << '\n';
    }
};

// Elapsed wall time at the caller, including blocking and scheduling.
class ScopedTimer {
public:
    explicit ScopedTimer(LogWaitRow& row) : row_(row), start_(NowNs()) {}
    ScopedTimer(const ScopedTimer&) = delete;
    ScopedTimer& operator=(const ScopedTimer&) = delete;
    ~ScopedTimer() { row_.Add(NowNs() - start_); }
private:
    LogWaitRow& row_;
    uint64_t start_;
};

inline std::array<LogWaitRow, LogWaitCount> log_waits;
inline LogWaitRow commit_wal_wait;
inline LogWaitRow tso_snapshot_rpc;
inline LogWaitRow tso_commit_rpc;
inline LogWaitRow tit_local_commit;
inline LogWaitRow tit_status_rpc;
inline LogWaitRow tit_pending_wait;
inline LogWaitRow remote_release_completion;
inline LogWaitRow undo_begin;
inline LogWaitRow undo_admit;
inline LogWaitRow undo_redo_enqueue;
inline LogWaitRow undo_page_apply;
inline LogWaitRow undo_history_step;
inline LogWaitRow undo_rollback_read, undo_rollback_step;

struct NamedTiming {
    const char* name;
    LogWaitRow& row;
};

inline const NamedTiming mvcc_timings[] = {
    {"commit_wal_wait", commit_wal_wait},
    {"tso_snapshot_rpc", tso_snapshot_rpc},
    {"tso_commit_rpc", tso_commit_rpc},
    {"tit_local_commit", tit_local_commit},
    {"tit_status_rpc", tit_status_rpc},
    {"tit_pending_wait", tit_pending_wait},
    {"remote_release_completion", remote_release_completion},
    {"undo_begin", undo_begin},
    {"undo_admit", undo_admit},
    {"undo_redo_enqueue", undo_redo_enqueue},
    {"undo_page_apply", undo_page_apply},
    {"undo_history_step", undo_history_step},
    {"undo_rollback_read", undo_rollback_read},
    {"undo_rollback_step", undo_rollback_step},
};

inline void DumpMvccTimings(std::ostream& out) {
    for (const auto& timing : mvcc_timings) timing.row.Dump(out, timing.name);
}

inline LogWaitRow storage_undo_redo, storage_undo_validate;

// Each synchronous transaction WAL wait is attributed to the batch carrying its target record.
// All components use the same population; a shared batch is weighted per waiter.
struct LogDetailRow {
    std::atomic<uint64_t> count{0}, queue_ns{0}, rpc_ns{0}, resume_ns{0};
    std::atomic<uint64_t> storage_ns{0}, write_ns{0}, sync_ns{0};
    void Add(uint64_t queue, uint64_t rpc, uint64_t resume,
             uint64_t storage, uint64_t write, uint64_t sync) {
        count.fetch_add(1, std::memory_order_relaxed);
        queue_ns.fetch_add(queue, std::memory_order_relaxed);
        rpc_ns.fetch_add(rpc, std::memory_order_relaxed);
        resume_ns.fetch_add(resume, std::memory_order_relaxed);
        storage_ns.fetch_add(storage, std::memory_order_relaxed);
        write_ns.fetch_add(write, std::memory_order_relaxed);
        sync_ns.fetch_add(sync, std::memory_order_relaxed);
    }
    void Dump(std::ostream& out, const std::string& prefix) const {
        out << prefix << "count=" << count.load() << '\n'
            << prefix << "queue_to_rpc_ns=" << queue_ns.load() << '\n'
            << prefix << "rpc_ns=" << rpc_ns.load() << '\n'
            << prefix << "resume_ns=" << resume_ns.load() << '\n'
            << prefix << "storage_ns=" << storage_ns.load() << '\n'
            << prefix << "write_ns=" << write_ns.load() << '\n'
            << prefix << "fdatasync_ns=" << sync_ns.load() << '\n';
    }
};
inline std::array<LogDetailRow, LogWaitCount> log_details;
inline std::array<LogWaitRow, 2> remote_fetch; // successful reply, conflict reply
// No active transactions or statistics-producing RPCs at this boundary.
// Worker readiness/configuration are synchronization state, not measurements.
inline void ResetBenchmarkStats() {
    runtime.start_ns = 0; runtime.last_finish_ns = 0; runtime.end_ns = 0;
    runtime.attempts = 0; runtime.commits = 0; runtime.aborts = 0;
    runtime.window_attempts = 0; runtime.window_commits = 0; runtime.window_aborts = 0;
    for (auto& phase : page_access)
        for (auto& row : phase) {
            row.count = 0; row.total_ns = 0; row.local_lock_ns = 0;
            row.transfer_log_wait_ns = 0; row.transfer_log_wait_count = 0;
        }
    for (const auto& timing : mvcc_timings) timing.row.Reset();
    for (auto& row : log_waits) row.Reset();
    for (auto& row : remote_fetch) row.Reset();
    for (auto& row : log_details) {
        row.count = 0; row.queue_ns = 0; row.rpc_ns = 0; row.resume_ns = 0;
        row.storage_ns = 0; row.write_ns = 0; row.sync_ns = 0;
    }
}
inline void DumpRemoteFetch(std::ostream& out) {
    for (size_t i = 0; i < remote_fetch.size(); ++i) {
        const std::string prefix = i ? "stats_remote_fetch_conflict_" : "stats_remote_fetch_success_";
        out << prefix << "count=" << remote_fetch[i].count.load() << '\n'
            << prefix << "ns=" << remote_fetch[i].total_ns.load() << '\n';
    }
}

inline void DumpLogWaits(std::ostream& out) {
    for (size_t i = 0; i < log_waits.size(); ++i) {
        out << "stats_log_wait_" << log_wait_names[i] << "_count=" << log_waits[i].count.load() << '\n'
            << "stats_log_wait_" << log_wait_names[i] << "_ns=" << log_waits[i].total_ns.load() << '\n';
        if (i != PageLog)
            log_details[i].Dump(out, std::string("stats_log_detail_") + log_wait_names[i] + "_");
    }
}

struct Totals {
    std::array<Row, 6> rows{};
    std::array<CommitRow, CommitPathCount> commits{};
    std::map<unsigned, CommitRow> writers;
    void Merge(const Totals& other) {
        for (size_t i = 0; i < rows.size(); ++i) rows[i].Merge(other.rows[i]);
        for (size_t i = 0; i < commits.size(); ++i) commits[i].Merge(other.commits[i]);
        for (const auto& entry : other.writers) writers[entry.first].Merge(entry.second);
    }
    void Dump(std::ostream& out) const {
        out << "stats_schema_version=1\n";
        Row all;
        for (size_t i = 0; i < rows.size(); ++i) {
            all.Merge(rows[i]);
            rows[i].Dump(out, std::string("stats_path_") + paths[i] + "_");
        }
        all.Dump(out, "stats_txn_");
        for (size_t i = 0; i < commits.size(); ++i)
            commits[i].Dump(out, std::string("stats_commit_") + commit_paths[i] + "_");
        for (const auto& entry : writers) entry.second.Dump(out, "stats_writer_" + std::to_string(entry.first) + "_");
    }
};
struct Attempt {
    bool active = false;
    uint64_t start_ns = 0;
    std::array<uint64_t, StageCount> stage_ns{};
    std::array<uint64_t, 3> commit_part_ns{};
    PrepareSample prepare;
    void Start() { active = true; start_ns = NowNs(); stage_ns.fill(0); commit_part_ns.fill(0); prepare = {}; }
    void CommitFinished(Totals& totals, CommitPath path, uint64_t start, unsigned writers) {
        if (!active) return;
        const auto elapsed = NowNs() - start;
        stage_ns[Commit] += elapsed;
        CommitRow sample;
        sample.count = 1; sample.total_ns = elapsed; sample.execute_ns = stage_ns[Execute];
        sample.part_ns = commit_part_ns; sample.prepare = prepare;
        totals.commits[path].Merge(sample);
        totals.writers[writers].Merge(sample);
    }
    void Add(Stage stage, uint64_t start) {
        if (active) stage_ns[stage] += NowNs() - start;
    }
    void Finish(Totals& totals, int mode, bool distributed, bool success) {
        if (!active) return;
        const int path = mode == 4 ? (distributed ? 5 : 4) : mode;
        if (path < 0 || path >= 6) return;
        auto& row = totals.rows[path];
        const auto elapsed = NowNs() - start_ns;
        ++row.completed; row.total_ns += elapsed;
        if (success) { ++row.committed; row.committed_ns += elapsed; }
        else { ++row.aborted; row.aborted_ns += elapsed; }
        for (int i = 0; i < StageCount; ++i) row.stage_ns[i] += stage_ns[i];
        active = false;
    }
};
}  // namespace benchmark_stats
