#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <ostream>
#include <string>

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
