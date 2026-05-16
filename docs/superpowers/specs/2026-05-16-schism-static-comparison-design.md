# Schism Static Comparison Design

## Goal

Build a defensible comparison between WookongDB MP's online affinity
repartitioning and a Schism-style workload-driven static partitioning baseline.
Show whether the online scheme improves throughput, tail latency, and locality
under the same cluster, workload, and runtime parameters.

This is an experiment for the paper's E4 baseline, not a production feature.
Total implementation budget is on the order of a few hundred lines of C++ plus
a few hundred lines of Python.

## Scope

The Schism baseline here is `schism_static`: a static partition map generated
from a training workload trace, applied to the cluster, then frozen.

Replication is out of scope — the current system is page-owned shared storage
with online ownership migration, and adding tuple replication would change
storage semantics. To stay honest, the baseline is named **placement-only
Schism** in the report. The headline claim is "`affinity_online` beats a
placement-only Schism static baseline", not "beats full Schism".

## Baselines

Three arms:

1. `baseline` — `affinity.enable=false`, no `SCHISM_STATIC` env. Current page
   ownership and MP-Router behavior with no repartitioning.
2. `schism_static` — load a static Schism-style CSV into `AssignmentTable`,
   physically migrate pages to match (apply phase), then run with all online
   repartitioning threads stopped (measure phase). A small static-mode runtime
   remains enabled for assignment lookup, apply-phase migration, and
   measurement-only time-series. See *Runtime Integration*.
3. `affinity_online` — current online pipeline (aggregator + partitioner +
   migration) fully enabled.

MP-Router is held constant across all three arms (see *Experiment Protocol*).

## Schism Static Algorithm

Training:

- Reuse the existing `sample_buffer + aggregator` pipeline. Run with
  `affinity.enable=true` for the training workload and dump the in-memory
  edge graph (vertex list + edge list) at the end. No new sampler.
- Add an explicit graph dump hook, controlled by
  `AFFINITY_GRAPH_DUMP_PATH`. When set, the aggregator writes a deterministic
  CSV edge dump on publish and on shutdown so the offline Schism tool does not
  depend on ParMETIS sidecar internals.

Graph + partition (offline, on the driver host):

- Undirected weighted graph: vertex per tuple, edge between every pair of
  tuples co-accessed in a sampled transaction, edge weight = co-access count,
  vertex weight = access count.
- Partition with **serial METIS** (`gpmetis` CLI or `pip install metis`) into
  `ComputeNodeCount` parts with a fixed seed. Do **not** reuse the ParMETIS
  sidecar: it calls `ParMETIS_V3_AdaptiveRepart`, which assumes a prior
  partition and is wrong for from-scratch k-way cuts. Keeping METIS offline
  also keeps the static and online code paths independent.

Output CSV, one line per tuple, sorted by `tuple_id` for deterministic re-runs:

```text
tuple_id,table_id,item_key,node_id
```

`tuple_id = (table_id << 48) | item_key` — the existing affinity packing
(`core/affinity/affinity_config.h`). The `table_id` and `item_key` columns are
kept for human inspection.

## Runtime Integration

Four small changes land the static CSV into the live system.

### 1. Lookup gate (server.h:500)

The hot-path lookup currently short-circuits to the page-owner fallback
whenever `enable_affinity` is false:

```cpp
if (!enable_affinity || table_id >= 10000 || item_key == -1) return fallback;
```

Add an env-gated bypass so the gate also opens under `SCHISM_STATIC`:

```cpp
static const bool schism_static_mode = std::getenv("SCHISM_STATIC") != nullptr;
if ((!enable_affinity && !schism_static_mode) || table_id >= 10000 || ...) return fallback;
```

The `assigned == fallback ? assigned : fallback` filter at `server.h:515` stays
as-is: until pages are physically migrated, routing must still follow the
current page owner. The apply phase below is what makes the static placement
take effect.

### 2. Selective thread start (worker/handler.cc)

`StartAffinityRuntimeIfEnabled` currently starts five threads as one bundle
(partitioner, aggregator, edge_shuffler, migration_worker, affinity_service).
Under `SCHISM_STATIC`, use a separate `schism_static_mode` gate rather than
`enable_affinity`. Static mode:

- loads the CSV assignment;
- starts `migration_worker` during the apply phase;
- starts `TimeseriesLoop` for measurement-only locality counters;
- does not start aggregator, edge_shuffler, partitioner, sidecar, or online
  assignment RPCs.

Tear-down is phase-aware: the apply phase stops `migration_worker`; the
measure phase leaves `TimeseriesLoop` running until result collection finishes.

### 3. AssignmentTable::LoadFromCsv(path)

One additive function. Parses the CSV, builds a `Snapshot`, calls
`Replace(snap)`. Called once at startup if `SCHISM_STATIC` is set, before any
worker threads come up.

### 4. Hot-group offset for drift

Add a workload config field named `hot_account_offset`. For SmallBank
training, the offset is `0`; for measured arms, the offset is `N/2`. The
workload generator treats the configured hot range as
`[hot_account_offset, hot_account_offset + num_hot_accounts)`, wrapping or
clamping if needed. This makes the drift explicit and reproducible instead of
depending on RNG seed differences.

## Experiment Protocol

### Cluster (constant across arms)

- compute: `10.10.2.31`, `10.10.2.32`, `10.10.2.33`, `10.10.2.34`;
- service/storage: `10.10.2.38`;
- MP-Router: `/root/mingtai/MP-Router`, a single mode for all arms (default
  `--system-mode 24` — ownership + load balance, no online metis training, so
  the router cannot smuggle a partitioner into any arm). The driver script
  refuses to start if a per-arm override changes this.

### Workload drift

Default drift = **hot-group shift**, not RNG seed:

- training uses hot account range `[0, K)`;
- measurement uses `[N/2, N/2 + K)`.

Same Zipfian theta, same account count, same worker count, same batch size,
same transaction mix. Seed-only drift would leave the access-frequency
distribution unchanged and produce no static-vs-online signal.

### Run order

1. **Training** for `schism_static` — `affinity.enable=true`, hot range
   `[0, K)`. Dump the aggregator's edge graph at end.
2. **Offline partition** — driver host runs the graph-to-CSV tool: METIS with
   fixed seed, sorted CSV out.
3. **Measurement arms** — each from a clean cluster cold-start, hot range
   `[N/2, N/2 + K)`:
   - `baseline`: `affinity.enable=false`, no SCHISM_STATIC.
   - `schism_static`:
     - **apply phase**: `SCHISM_STATIC=1`, `affinity.enable=false`, CSV
       preloaded. Wait until
       `migrations_done / max(1, migrations_planned) >= 0.99` and
       `migrations_failed / max(1, migrations_planned) <= 0.01`,
       or 60 s, whichever first. Then `migration_worker` exits before the
       measured workload starts.
     - **measure phase**: start the workload clock. `migration_worker` is
       stopped; only `TimeseriesLoop` remains active for counters. No online
       graph collection, repartitioning, sidecar, or migration runs during
       measurement.
   - `affinity_online`: `affinity.enable=true`. Full online pipeline.

Each arm is one run; no averaging across seeds (experiment grade — `paper E4`
can be expanded later if reviewers ask).

## Metrics

`schism_compare_summary.txt` per multi-arm run, with per-arm columns and delta
lines:

- throughput (post-warmup) and total transactions;
- exec P50/P95/P99 and fetch P50/P95/P99;
- cluster local / remote / storage fetch ratios; per-node remote ratio;
- edge cut and weighted cut ratio:
  - `schism_static`: METIS's reported cut on the training graph;
  - `affinity_online`: the partitioner's final epoch values;
  - `baseline`: omit;
- migration counters (only meaningful for `schism_static` apply phase and
  `affinity_online`);
- run exit code, incomplete-collection flag.

Deltas reported: `affinity_online − schism_static`, `schism_static − baseline`,
and `affinity_online − baseline`.

## Success Criteria

1. One driver command runs all three arms and emits
   `schism_compare_summary.txt`.
2. `schism_static` arm provably uses the static map: pre-measure log lines
   show `AssignmentTable.Size() > 0` after CSV load, and apply-phase
   convergence ratio above threshold (otherwise the arm is marked failed and
   the headline claim is suppressed).
3. If a cluster host fails or collection is incomplete, the summary records
   that status rather than a clean number.

## Risks And Controls

- **Apply phase doesn't converge.** If `migration_worker` can't move pages
  fast enough (priority churn, lock contention, peer unreachable), the
  measure phase starts with mostly-still-hash placement and the comparison is
  invalid. Mitigation: log convergence ratio every second; if final ratio is
  below threshold, mark the arm failed.
- **MP-Router policy drift.** Any per-arm change to MP-Router mode silently
  changes routing behavior. Mitigation: spec pins one mode; driver script
  asserts the same `--system-mode` for all three arms.
- **Naming clarity.** `schism_static` is not canonical Schism (no
  replication, no migration management). Paper writeup uses "placement-only
  Schism" and cites Schism only for the graph-construction idea.
- **Determinism.** METIS uses a fixed seed; the offline tool sorts vertices
  before writing the CSV so a re-run produces an identical file.

## Implementation Driver

Extend `tests/scripts/multinode_mprouter_smoke.py`. Its existing `--compare`
flag already runs `baseline` + `affinity_on` as cold-restarted arms via
`run_case()`. Additions:

- `--three-arm` switch — adds the `schism_static` arm between the two
  existing ones;
- `--schism-train-cycles` — training transaction count or duration;
- `--schism-csv` — path the offline partition tool writes to and the
  compute hosts read from;
- per-arm assertion that MP-Router `--system-mode` is identical.

The third arm is a new `run_case("schism_static", ...)` call that sets
`SCHISM_STATIC=1` and `affinity.enable=false` on the compute hosts, uploads
the CSV, waits for apply-phase convergence, then starts the measurement
workload. Reuse `parse_summary`, `collect_results`, and extend
`write_compare_summary` to three columns.

Offline graph-to-CSV tool lives in `tests/scripts/schism_partition.py` (~150
LOC): read aggregator edge dump, build adjacency, shell out to `gpmetis`,
emit sorted CSV.
