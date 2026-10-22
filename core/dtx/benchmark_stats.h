#pragma once

#include <array>
#include <atomic>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <ostream>
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
struct Totals {
    std::array<Row, 6> rows{};
    void Merge(const Totals& other) {
        for (size_t i = 0; i < rows.size(); ++i) rows[i].Merge(other.rows[i]);
    }
    void Dump(std::ostream& out) const {
        out << "stats_schema_version=1\n";
        Row all;
        for (size_t i = 0; i < rows.size(); ++i) {
            all.Merge(rows[i]);
            rows[i].Dump(out, std::string("stats_path_") + paths[i] + "_");
        }
        all.Dump(out, "stats_txn_");
    }
};
struct Attempt {
    bool active = false;
    uint64_t start_ns = 0;
    std::array<uint64_t, StageCount> stage_ns{};
    void Start() { active = true; start_ns = NowNs(); stage_ns.fill(0); }
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
