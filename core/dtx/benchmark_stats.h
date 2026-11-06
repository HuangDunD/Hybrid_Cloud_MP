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
    double run_seconds = 60;
    std::atomic<unsigned> ready{0};
    std::atomic<uint64_t> start_ns{0}, last_finish_ns{0};
    std::atomic<uint64_t> attempts{0}, commits{0}, aborts{0};
    std::atomic<uint64_t> window_attempts{0}, window_commits{0}, window_aborts{0};
    uint64_t end_ns = 0; // Written only after all workers have finished.
    void Configure(const std::string& mode, double seconds) {
        if ((mode != "count" && mode != "time") || !(seconds > 0 && seconds <= 86400))
            throw std::invalid_argument("Invalid benchmark run mode/run time");
        timed = mode == "time"; run_seconds = seconds;
    }
    uint64_t Deadline() const { return start_ns.load() + static_cast<uint64_t>(run_seconds * 1e9); }
    void Start(uint64_t now = NowNs()) { start_ns.store(now); }
    bool WithinWindow(uint64_t now) const { return !timed || now < Deadline(); }
    bool TryStart(uint64_t local_attempts, uint64_t limit, uint64_t now = NowNs()) {
        if (timed ? !WithinWindow(now) : local_attempts >= limit) return false;
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
                LocalUpgrade, StoragePage, WaitRelease, PagePathCount };
inline constexpr const char* page_paths[] = {"local_ready", "local_manager", "owner_manager",
    "forwarded_page", "local_upgrade", "storage_page", "wait_release"};
inline constexpr const char* page_phases[] = {"begin", "execute", "commit", "abort", "unscoped"};
struct PageRow {
    std::atomic<uint64_t> count{0}, total_ns{0}, local_lock_ns{0};
    void Add(uint64_t ns, uint64_t wait_ns) {
        count.fetch_add(1, std::memory_order_relaxed);
        total_ns.fetch_add(ns, std::memory_order_relaxed);
        local_lock_ns.fetch_add(wait_ns, std::memory_order_relaxed);
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
                << prefix << "local_lock_ns=" << row.local_lock_ns.load() << '\n';
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
};
inline std::array<LogWaitRow, LogWaitCount> log_waits;
inline std::array<LogWaitRow, 2> remote_fetch; // successful reply, conflict reply
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
