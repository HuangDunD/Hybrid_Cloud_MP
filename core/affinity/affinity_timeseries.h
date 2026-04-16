// Phase 6 — per-second time-series writer.
// Spawns a background thread that samples affinity counters + page-fetch
// breakdowns every `affinity_timeseries_tick_ms` and appends one CSV row to
// `affinity_timeseries_csv_path`. Used to plot edgecut/from_remote_ratio over
// time for the paper.
#pragma once

class ComputeServer;

namespace affinity {

void TimeseriesLoop(ComputeServer* cs);

void RequestTimeseriesStop();

}  // namespace affinity
