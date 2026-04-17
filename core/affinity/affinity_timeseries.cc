#include "affinity_timeseries.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <thread>

#include "affinity_metrics.h"
#include "compute_server/server.h"
#include "config.h"

namespace affinity {

namespace {

std::atomic<bool> g_ts_stop{false};

uint64_t NowMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(
               steady_clock::now().time_since_epoch())
        .count();
}

uint64_t WallMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(
               system_clock::now().time_since_epoch())
        .count();
}

void WriteHeader(std::ofstream& f) {
    f << "wall_ms,elapsed_ms,"
      << "edgecut,n_vertices,n_edges,"
      << "from_remote_ratio,from_storage_ratio,from_local_ratio,"
      << "from_remote_delta,from_storage_delta,from_local_delta,"
      << "samples_pushed_delta,samples_dropped_delta,"
      << "partition_runs,partition_skipped,"
      << "migrations_planned_delta,migrations_done_delta,migrations_failed_delta\n";
    f.flush();
}

}  // namespace

void RequestTimeseriesStop() {
    g_ts_stop.store(true, std::memory_order_relaxed);
}

void TimeseriesLoop(ComputeServer* cs) {
    if (!enable_affinity) return;
    if (affinity_timeseries_tick_ms <= 0) return;

    // One CSV per compute node so concurrent runs don't clobber each other.
    // node_id is appended; result.txt convention from compute_server places
    // outputs in the run cwd.
    const int node_id = cs ? cs->GetNodeID() : -1;
    std::string path = affinity_timeseries_csv_path;
    if (node_id >= 0) {
        const auto dot = path.rfind('.');
        const std::string suffix = "." + std::to_string(node_id);
        if (dot == std::string::npos) {
            path += suffix;
        } else {
            path.insert(dot, suffix);
        }
    }

    std::ofstream f(path, std::ios::out | std::ios::trunc);
    if (!f.is_open()) {
        std::fprintf(stderr,
                     "[affinity] timeseries: failed to open %s — disabling\n",
                     path.c_str());
        return;
    }
    WriteHeader(f);

    auto* node = cs ? cs->get_node() : nullptr;

    uint64_t prev_remote = node ? (uint64_t)node->get_fetch_from_remote_cnt()  : 0;
    uint64_t prev_storage= node ? (uint64_t)node->get_fetch_from_storage_cnt() : 0;
    uint64_t prev_local  = node ? (uint64_t)node->get_fetch_from_local_cnt()   : 0;
    uint64_t prev_pushed   = stats.samples_pushed.load();
    uint64_t prev_dropped  = stats.samples_dropped.load();
    uint64_t prev_planned  = stats.migrations_planned.load();
    uint64_t prev_done     = stats.migrations_done.load();
    uint64_t prev_failed   = stats.migrations_failed.load();
    uint64_t prev_part_runs = stats.partition_runs.load();
    uint64_t prev_part_skipped = stats.partition_skipped.load();

    const uint64_t start_ms = NowMs();

    while (!g_ts_stop.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(
            std::chrono::milliseconds(affinity_timeseries_tick_ms));

        // One read per counter per tick.
        const uint64_t cur_remote  = node ? (uint64_t)node->get_fetch_from_remote_cnt()  : 0;
        const uint64_t cur_storage = node ? (uint64_t)node->get_fetch_from_storage_cnt() : 0;
        const uint64_t cur_local   = node ? (uint64_t)node->get_fetch_from_local_cnt()   : 0;
        const uint64_t cur_pushed  = stats.samples_pushed.load();
        const uint64_t cur_dropped = stats.samples_dropped.load();
        const uint64_t cur_planned = stats.migrations_planned.load();
        const uint64_t cur_done    = stats.migrations_done.load();
        const uint64_t cur_failed  = stats.migrations_failed.load();
        const uint64_t cur_part_runs = stats.partition_runs.load();
        const uint64_t cur_part_skipped = stats.partition_skipped.load();

        const uint64_t d_remote   = cur_remote  - prev_remote;
        const uint64_t d_storage  = cur_storage - prev_storage;
        const uint64_t d_local    = cur_local   - prev_local;
        const uint64_t d_pushed   = cur_pushed  - prev_pushed;
        const uint64_t d_dropped  = cur_dropped - prev_dropped;
        const uint64_t d_planned  = cur_planned - prev_planned;
        const uint64_t d_done     = cur_done    - prev_done;
        const uint64_t d_failed   = cur_failed  - prev_failed;
        const uint64_t d_part_skipped = cur_part_skipped - prev_part_skipped;

        const uint64_t total_cur = cur_remote + cur_storage + cur_local;
        const double rem_r = total_cur > 0 ? (double)cur_remote  / (double)total_cur : 0.0;
        const double sto_r = total_cur > 0 ? (double)cur_storage / (double)total_cur : 0.0;
        const double loc_r = total_cur > 0 ? (double)cur_local   / (double)total_cur : 0.0;

        f << WallMs() << ','
          << (NowMs() - start_ms) << ','
          << stats.last_edgecut.load() << ','
          << stats.graph_vertices.load() << ','
          << stats.graph_edges.load() << ','
          << rem_r << ',' << sto_r << ',' << loc_r << ','
          << d_remote << ',' << d_storage << ',' << d_local << ','
          << d_pushed << ',' << d_dropped << ','
          << cur_part_runs << ','
          << stats.partition_skipped.load() << ','
          << d_planned << ',' << d_done << ',' << d_failed << '\n';
        f.flush();

        prev_remote  = cur_remote;
        prev_storage = cur_storage;
        prev_local   = cur_local;
        prev_pushed  = cur_pushed;
        prev_dropped = cur_dropped;
        prev_planned = cur_planned;
        prev_done    = cur_done;
        prev_failed  = cur_failed;
        prev_part_runs = cur_part_runs;
        prev_part_skipped = cur_part_skipped;
    }

    f.flush();
    f.close();
}

}  // namespace affinity
