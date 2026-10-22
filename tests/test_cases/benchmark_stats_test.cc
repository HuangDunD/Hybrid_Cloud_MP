#include "core/dtx/benchmark_stats.h"
#include <cassert>
#include <sstream>

int main() {
    using namespace benchmark_stats;
    Totals first, second;
    Attempt attempt;
    attempt.Start();
    auto start = NowNs();
    attempt.Add(Execute, start);
    attempt.Finish(first, 4, false, true);
    attempt.Finish(first, 4, false, true);  // A second finalization must not count twice.
    attempt.Start();
    attempt.Finish(second, 4, true, false);
    first.Merge(second);
    assert(first.rows[4].completed == 1 && first.rows[4].committed == 1);
    assert(first.rows[5].completed == 1 && first.rows[5].aborted == 1);
    assert(first.rows[4].total_ns >= first.rows[4].stage_ns[Execute]);
    std::ostringstream out;
    first.Dump(out);
    assert(out.str().find("stats_txn_completed=2\n") != std::string::npos);
    assert(out.str().find("stats_txn_committed=1\n") != std::string::npos);
    assert(out.str().find("stats_txn_aborted=1\n") != std::string::npos);
}
