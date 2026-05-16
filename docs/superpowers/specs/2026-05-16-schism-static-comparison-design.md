# Schism Static Comparison Design

## Goal

Build and run a defensible comparison between WookongDB MP's online affinity
repartitioning and a Schism-style workload-driven static partitioning baseline.
The comparison should show whether the online scheme improves throughput,
tail latency, and data locality under the same cluster, workload, and runtime
parameters.

## Scope

The Schism baseline in this project is `schism_static`: a static partition map
generated from a training workload trace. It follows Schism's central idea:
construct a tuple co-access graph from observed transactions, partition that
graph to reduce distributed transactions, then run using the resulting
assignment.

This work does not implement Schism's full replication management or a new
storage protocol. Replication is out of scope because the current system is
page-owned shared storage with online ownership migration. Adding tuple
replication would change storage semantics and make the experiment too large
for a clean comparison.

## Baselines

The experiment will produce three arms:

1. `baseline`: affinity disabled. This preserves the current page ownership and
   MP-Router behavior without online repartitioning.
2. `schism_static`: train a static Schism-style tuple assignment, preload or
   apply that assignment, then run with online repartitioning disabled.
3. `affinity_online`: the current online affinity pipeline with graph
   collection, ParMETIS repartitioning, and page ownership migration enabled.

The headline claim is based on `affinity_online` versus `schism_static`.
`baseline` remains in the report as a sanity check.

## Schism Static Algorithm

The training step records transaction samples as sets of tuple IDs. Each tuple
ID is packed as `(table_id, item_key)` using the existing affinity tuple packing
format.

The graph builder creates an undirected weighted graph:

- one vertex per tuple seen in training;
- one edge between every pair of tuples in a transaction;
- edge weight equals co-access frequency;
- optional vertex weight equals tuple access frequency.

The partitioner runs METIS for `ComputeNodeCount` partitions with a reproducible
seed. The output is a CSV:

```text
tuple_id,table_id,item_key,node_id
```

This file is the Schism static assignment. It is loaded by the compute tier or
an experiment helper before the measured run. During the measured
`schism_static` run, online repartitioning and migration decisions are disabled
except for any one-time setup needed to establish the static placement.

## Runtime Integration

The implementation should reuse existing affinity primitives where possible:

- `core/affinity` already has tuple IDs, graph samples, assignment tables,
  migration planning, and time-series metrics.
- `tests/scripts/multinode_mprouter_smoke.py` already runs MP-Router against
  the 4-compute Hybrid cluster and collects summary metrics.
- `tests/scripts/multinode_parmetis_smoke.py` already provides config upload,
  process cleanup, build, and result collection helpers.

The new Schism path should be explicit in configuration and scripts. A run
should never silently mix static Schism assignment with online affinity
repartitioning.

## Experiment Protocol

Each comparison run uses the same cluster topology:

- compute: `10.10.2.31`, `10.10.2.32`, `10.10.2.33`, `10.10.2.34`;
- service/storage: `10.10.2.38`;
- MP-Router local tree: `/root/mingtai/MP-Router`.

The script will execute:

1. training workload for `schism_static`;
2. graph build and static partition generation;
3. measured `baseline`;
4. measured `schism_static`;
5. measured `affinity_online`.

For a strong comparison, the measured workload should drift from the training
phase. The default drift is a changed SmallBank affinity workload seed or hot
group layout while preserving account count, Zipfian theta, worker count, batch
size, and transaction mix. This makes Schism's static nature visible while
keeping the comparison fair.

## Metrics

The result directory will include `schism_compare_summary.txt` with:

- throughput and throughput after warmup;
- total transactions and elapsed time;
- exec latency P50/P95/P99;
- fetch-to-complete latency P50/P95/P99;
- cluster local, remote, and storage fetch ratios;
- per-node remote ratios;
- page operations and page ID changes;
- Schism graph size, edge cut, and weighted cut ratio;
- affinity partition runs, migrations planned/done/failed, and migration
  success ratio;
- run exit code and incomplete-collection warnings.

The report should compute deltas for:

- `affinity_online` versus `schism_static`;
- `schism_static` versus `baseline`.

## Success Criteria

The work is successful when:

1. A developer can run one command to execute the three-arm comparison.
2. The result directory contains raw logs, per-arm summaries, and
   `schism_compare_summary.txt`.
3. The summary clearly states whether `affinity_online` beats
   `schism_static` on throughput, P99 fetch latency, and remote fetch ratio.
4. If a cluster host fails or result collection is incomplete, the summary
   records that status instead of presenting a clean result.
5. Unit tests cover graph construction, METIS input/output parsing, static
   assignment loading, and comparison summary generation.

## Risks And Controls

Schism is a paper algorithm, not a drop-in implementation. The comparison must
state that this is a Schism-style static workload-driven partitioning baseline,
not a full replication implementation.

Long runs can leave cluster processes behind. The experiment script should keep
the existing preflight and `--force-clean` behavior, and should write enough
state for manual recovery.

Training and measured phases must be separated. The measured
`schism_static` arm must not continue learning online; otherwise the baseline
would no longer represent static Schism.
