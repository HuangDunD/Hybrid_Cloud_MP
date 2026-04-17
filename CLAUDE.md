# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

WookongDB MP ("Chimera" upstream) is a shared-storage, multi-primary cloud-native database. It runs in two modes:
- **SQL mode** — a SQL frontend (2-table join max) that is only supported under the `lazy` page-release strategy.
- **Workload mode** — built-in drivers for SmallBank / YCSB / TPCC with `eager` or `lazy` page fetch/release. 2PL concurrency control.

Under `lazy`, correctness of a page is restored by **log replay** before use. The storage / metadata / compute tiers must all be launched with a *matching* mode (all `sql`, all `ycsb`, etc.).

## Topology (three-tier)

Every run needs one of each, started in this order:

1. `storage_server/storage_pool <mode>` — disk + WAL + log replay; owns `DiskManager`, `LogManager`, `RmManager`, `storage_bufferpool`.
2. `remote_server/remote_node <mode>` — global metadata + lock services: `PageTableService`, `PartitionTableService`, `TimeStampService` (in `core/remote_page_table/` and `core/GPLM/`).
3. `compute_server/compute_server …` — SQL/workload front end. One per compute node; node 0 exposes port 9095, node 1 → 9096, etc., when launched in SQL mode.

Ports, IP lists, and machine counts are configured statically in `config/*.json`; there is **no dynamic membership**. When changing node count, edit all three of `compute_node_config.json`, `storage_node_config.json`, `remote_server_config.json` and keep them in sync across every machine.

## Build

Out-of-tree CMake build rooted at the repo top level:

```bash
mkdir -p build && cd build
cmake ..
make -j
```

Things to know:
- Requires **brpc** (path via `-DBRPC_ROOT=...`, default `/home/wlj/include`), **gflags**, **protobuf**, **leveldb**, **openssl**, **boost_coroutine/context/system**, thrift (optional). `.proto` files under `core/**` are compiled in-place by `CMakeLists.txt` via a `protoc` `execute_process` step — generated `*.pb.cc/*.pb.h` are gitignored but must be present for the build.
- C++17, heavy use of brpc + coroutines. `CMAKE_BUILD_TYPE` is hardcoded to `Debug` / `-O0` in the top-level `CMakeLists.txt`; change it there, not via `-DCMAKE_BUILD_TYPE=` (it gets overwritten).
- `thirdparty/brpc` is a git submodule — clone with `--recursive` or run `git submodule update --init`.
- Build artifacts live at `build/compute_server/compute_server`, `build/storage_server/storage_pool`, `build/remote_server/remote_node`, `build/wookongdb-mp-client/WookongDB_client`, and `build/tests/{concurrency_test,cache_consistency_test}`.
- **ParMETIS sidecar is opt-in**: pass `-DBUILD_PARMETIS_SIDECAR=ON` to build `build/parmetis_sidecar/parmetis_sidecar`. Requires `libparmetis-dev`, `libmetis-dev`, and an MPI implementation. Off by default so plain `cmake ..` works without those deps — but affinity-driven repartitioning (see below) won't function without the sidecar binary.

## Running

### SQL mode (3 args: node_id, db_name)
```bash
./build/storage_server/storage_pool sql
./build/remote_server/remote_node sql
./build/compute_server/compute_server 0 test_db   # per compute node; node id increments, port = 9095 + node_id
./build/wookongdb-mp-client/WookongDB_client -h 127.0.0.1 -p 9095
```

### Workload mode (6 args)
```
./compute_server <workload> <mode> <thread_num> <write_txn_ratio> <local_txn_ratio> <machine_id>
# workload ∈ {smallbank, ycsb, tpcc}; mode ∈ {eager, lazy}
```
`run.cc` dispatches on `argc`: 3 → `StartDatabaseSQL`, 7 → `GenThreads` + end-of-run stats dump to stdout and `result.txt`.

### Multi-machine automation
`test_multi_node.py` is the driver for remote deploy + parameter sweep (uses paramiko; SSH creds and IPs are derived from `config/compute_node_config.json` plus env vars `COMPUTE_SSH_*`, `REMOTE_HOST`, `REMOTE_PORT`, `REMOTE_USER`, `REMOTE_PASS`, `REMOTE_KEY`). `deploy_and_run.sh` backgrounds this with timestamped logging to `run.log`.

## Tests

Python harnesses under `tests/scripts/` run end-to-end with a live cluster. Orchestrator:
```bash
python3 tests/scripts/run_all_tests.py       # must be run from repo root; chdirs there itself
python3 tests/scripts/basic_query_test.py    # single suite
```
Scripts available: `basic_query_test`, `cache_consistency_test`, `concurrency_test`, `join_test`, `load_table_test`, `fdatasync_parallel_bench`. The C++ test binaries `concurrency_test` and `cache_consistency_test` (built from `tests/test_cases/`) are invoked by their Python wrappers. All suites share `tests/scripts/test_env.py` helpers (`wait_for_listen`, `wait_for_processes_gone`, `kill_processes`, atexit-based process reaping) — use these rather than ad-hoc `time.sleep` when touching the harness.

Two affinity-specific regression drivers also live there:
- `stage2_local_affinity.py` — boots storage + remote + 2 compute nodes on localhost under YCSB, runs with `affinity.enable=true`, and asserts the partitioner + migration loops produce a non-empty `affinity_timeseries.csv.<node_id>`. Respects `STAGE2_ATTEMPTED_NUM`, `STAGE2_THREADS` env overrides.
- `stage4_local_compare.py` — runs the same workload with affinity on and off, diffs the two result.txt / timeseries outputs, and invokes `plot_affinity.py` to render edgecut / from_remote_ratio / migrations charts into `build/stage4_compare/`.

## Reports & profiling

- `./run_report.sh` → `generate_report.py` (pulls `.pydeps/` onto PYTHONPATH; expects repo at `/usr/local/workspace/Hybrid_Cloud_MP`).
- `generate_lazy_diff_report.py`, `generate_time_breakdown.py` for targeted analysis.
- `./gen_flamegraph.sh <process_name> [seconds] [freq] [fp|dwarf]` → `flame.svg` via FlameGraph + perf; needs `sudo perf` and `pidof`.
- `plot_affinity.py affinity_timeseries.csv.0 affinity_timeseries.csv.1 --out <prefix>` renders per-node edgecut / remote-access ratio / migration counters (needs matplotlib).

## Architecture map

Top-level globals live in `config.h` / `config.cc` / `common.h`:
- `SYSTEM_MODE` (algorithm: 0 baseline, 1 lazy-release, 2-5 phase switch / delay variants), `WORKLOAD_MODE`, `LOCK_MODE` (`NO_WAIT` vs `WAIT_DIE`), `ComputeNodeCount`, `thread_num_per_node`, `use_rdma`. RDMA is toggled via `SET_BRPC_RDMA_OPTION` which adapts to both old (`use_rdma`) and new (`socket_mode`) brpc APIs.
- Type aliases (`page_id_t`, `itemkey_t`, `lsn_t`, `tx_id_t`, …) and lock-word bit layout (`EXCLUSIVE_LOCKED`, `INSERT_LOCKED`, `DELETE_LOCKED`, `MASKED_SHARED_LOCKS`) are in `common.h`.

### core/ layout
- **`core/dtx/`** — distributed transaction runtime. `dtx.{h,cc}` + `dtx_exe.cc` + `dtx_log.cc` is the hot path (begin/read/write/commit/abort, log write, 2PL acquisition).
- **`core/GPLM/`** (Global Page Lock Manager) + **`core/LPLM/`** (Local Page Lock Manager) — two-tier locking. GPLM lives with `remote_server`; LPLM is per compute node. Split between `_ER_` (eager-release) and `_LR_` (lazy-release) variants.
- **`core/storage/`** — WAL (`log_manager`, `log_record`, `txn_log`, `logreplay`), record files (`rm_*`), `sm_manager` catalog, `storage_bufferpool` + `lru_replacer`, BLink tree + B+ tree on-disk pages, `fsm_tree` free-space map. `storage_service.proto` is the compute↔storage RPC.
- **`core/index/`** — in-compute-node indexes. `bp_tree/latch_crabbing/` is the latched B+tree used by SQL; `bp_tree/blink/` is the BLink (concurrent) version. `index_manager` arbitrates.
- **`core/remote_page_table/`** — compute↔meta RPCs: page table, partition table, timestamp allocator. All three are separate protobuf services.
- **`core/compute_node/`** — protos for compute↔compute (page push/pull) RPCs, plus the `calvin` and `twoPC` protos for the alternate commit protocols.
- **`core/sql_executor/`** — parser (flex `lex.l` + bison `yacc.y`, pre-generated into `lex.yy.cpp`, `yacc.tab.cpp`), AST, planner/optimizer, volcano-style executors (`ExecutorSeqScan`, `ExecutorBPTree`, `ExecutorInsert/Update/Delete/Projection`, `ExecutorJoin`). Linked as a separate `sql` library via `core/sql_executor/CMakeLists.txt`.
- **`core/fiber/`** + **`core/scheduler/`** — bthread/boost coroutine scheduling (each worker has `ThreadPoolSizePerWorker` = 2).
- **`core/connection/meta_manager.{h,cc}`** — owns the brpc channels and RDMA option wiring to the other tiers.
- **`core/affinity/`** — online tuple-level repartitioning (paper experiment; off by default). Pipeline per compute node: `sample_buffer` (per-worker lock-free ring of `TxnSample`) → `aggregator` (50 ms tick, merges into a local edge graph + node-access histograms) → `edge_shuffler` (brpc all-to-all of `ShuffleEdges` + barrier, sharded by `owner_rank(tuple_id)`) → `partitioner` (5 s cycle: `PushVertexInventory` allgather → build global CSR → UDS send to `parmetis_sidecar` → receive part[] → `PushAssignmentSlice` broadcast → swap `AssignmentTable`) → `migration_worker` (200 ms tick: copy misassigned tuples to their target node via BLink re-point + WAL records; Strategy A — *not* recoverable from kill -9 mid-migration). `sidecar_supervisor` lets machine_id==0 auto-spawn `mpirun parmetis_sidecar` on startup. `affinity_timeseries.{h,cc}` emits per-second CSV; `affinity_service.proto` defines the 4 RPCs.
- **`parmetis_sidecar/`** — separate MPI+ParMETIS process, one rank per compute node, connected over UDS at `/tmp/wookong_parmetis.sock`. Isolated from the main build (opt-in via `BUILD_PARMETIS_SIDECAR`) so the main binary doesn't link MPI. `uds_protocol.h` is the shared wire format; `idx_t`/`real_t` widths mirror the installed ParMETIS headers.

### compute_server/ layout
`run.cc` is the entrypoint; it delegates to `Handler` (`compute_server/worker/handler.{h,cc}`). The per-algorithm server loops live side-by-side and are selected via `SYSTEM_MODE`:
- `baseline_server.cc`, `lazyrelease_server.cc`, `twopc_server.cc`, `twopc_fetch_server.cc`, `single_server.cc`, `ts_fetch_server.cc`. All share `server.{h,cc}` (`ComputeServer` class + brpc service impls for page push/pull/notify/get).
- `worker/worker.{h,cc}` runs the coordinator threads that spawn coroutines; `worker/handler.{h,cc}` does argument parsing + thread spawning + end-of-run stat printing. Note `run.cc` `#include`s `worker/worker.cc` directly (so the worker compilation unit is pulled into the executable alongside the `worker` library).

### workload/
Three self-contained drivers under `smallbank/`, `ycsb/`, `tpcc/`. Each defines its DB (loader + schemas) and transaction mix (`*_txn.{h,cc}`). SmallBank/YCSB/TPCC transaction-type counts (`SmallBank_TX_TYPES`, `TPCC_TX_TYPES`, `YCSB_TX_TYPES`) drive the stats tables in `run.cc`.

## Gotchas

- `Readme.md` calls out that **the buffer-pool config keys are per-table**: `table_buffer_pool_size_per_table`, `index_buffer_pool_size_per_table`, `partition_size_per_table` — adjust in `config/compute_node_config.json`.
- `BRPC_ROOT` defaults to `/home/wlj/include`; override on the CMake command line if brpc sits elsewhere. `find_library` for `brpc` picks `.a` vs `.so` based on `LINK_SO` (default `ON`).
- `remote_server/server.cc` loads config with a **relative** path (`../../config/...`) — run binaries from their `build/<tier>/` directory or cwd must equivalently be two levels below `config/`.
- End-of-run output (`result.txt`) is produced only in the 7-arg workload path. The SQL path runs until killed.
- `RAFT` (`config.h`) and `AsyncCommit2pc` are compile-time toggles, not runtime flags.
- Affinity knobs come from the `"affinity": { ... }` block in `config/compute_node_config.json` and land in `extern`s declared at the bottom of `config.h` (`enable_affinity`, `affinity_partition_cycle_ms`, `affinity_migration_batch`, `affinity_sidecar_uds_path`, `affinity_auto_spawn_sidecar`, …). With `auto_spawn_sidecar=1` (default), the leader compute_server fork+execs `mpirun` itself; set `AFFINITY=1` in the environment for `deploy_and_run.sh` to use the legacy out-of-process `affinity_sidecar_launch.sh {start|stop|status}` driver instead. Sidecar UDS path must match between the JSON and the launcher.
