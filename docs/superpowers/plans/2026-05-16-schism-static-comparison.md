# Schism Static Comparison Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Schism-style static placement baseline and a three-arm experiment path comparing `baseline`, `schism_static`, and `affinity_online`.

**Architecture:** Keep the online affinity pipeline unchanged. Add an env-gated static runtime path that loads a CSV assignment, runs only the migration worker for a bounded apply window, and keeps the time-series loop alive for locality counters. Generate static assignments offline from deterministic graph dumps and orchestrate the comparison from the existing MP-Router smoke driver.

**Tech Stack:** C++17, existing affinity runtime, CMake tests, Python 3 `unittest`, `argparse`, `subprocess`, and serial METIS `gpmetis` when available.

---

## File Structure

- Create `core/affinity/schism_static.h`: env helpers and apply convergence predicate.
- Create `core/affinity/graph_dump.h`.
- Create `core/affinity/graph_dump.cc`: deterministic CSV dump for `LocalGraph`.
- Modify `core/affinity/assignment_table.h`.
- Modify `core/affinity/assignment_table.cc`: CSV loader for static assignment.
- Modify `core/affinity/aggregator.cc`: optional `AFFINITY_GRAPH_DUMP_PATH` dump on publish and shutdown.
- Modify `core/affinity/migration_worker.cc`: allow `SCHISM_STATIC`, stop after static apply convergence or timeout.
- Modify `core/affinity/affinity_timeseries.cc`: allow measurement-only time-series under `SCHISM_STATIC`.
- Modify `compute_server/server.h`: open the assignment lookup gate under `SCHISM_STATIC`.
- Modify `compute_server/worker/handler.cc`: selective static runtime startup and CSV load.
- Modify `core/CMakeLists.txt`: add `graph_dump.cc`.
- Create `tests/test_cases/affinity_schism_static_test.cc`.
- Modify `tests/CMakeLists.txt`: register the C++ test.
- Create `tests/scripts/schism_partition.py`: graph dump to static CSV, using `gpmetis` or a deterministic test fallback.
- Create `tests/scripts/test_schism_partition.py`.
- Modify `tests/scripts/multinode_parmetis_smoke.py`: write `hot_account_offset` config values.
- Modify `tests/scripts/multinode_mprouter_smoke.py`: add training/static apply/three-arm summary plumbing.
- Modify `tests/scripts/test_multinode_mprouter_smoke.py`: cover config, env, and summary behavior.

## Task 1: Static Assignment CSV Loader

**Files:**
- Modify: `core/affinity/assignment_table.h`
- Modify: `core/affinity/assignment_table.cc`
- Test: `tests/test_cases/affinity_schism_static_test.cc`
- Modify: `tests/CMakeLists.txt`

- [ ] **Step 1: Write the failing C++ test**

Add `tests/test_cases/affinity_schism_static_test.cc` with this behavior:

```cpp
#include <cassert>
#include <cstdint>
#include <fstream>
#include <string>

#include "affinity/affinity_config.h"
#include "affinity/assignment_table.h"

int main() {
    const std::string path = "/tmp/wookong_schism_assignment.csv";
    {
        std::ofstream f(path, std::ios::out | std::ios::trunc);
        f << "tuple_id,table_id,item_key,node_id\n";
        f << affinity::pack_tuple_id(0, 10) << ",0,10,2\n";
        f << affinity::pack_tuple_id(1, 20) << ",1,20,3\n";
        f << affinity::pack_tuple_id(0, 30) << ",0,30,1\n";
    }

    affinity::AssignmentTable table;
    const auto loaded = table.LoadFromCsv(path, 4);
    assert(loaded.ok);
    assert(loaded.rows_loaded == 3);
    assert(table.Size() == 3);
    assert(table.Lookup(affinity::pack_tuple_id(0, 10), 0) == 2);
    assert(table.Lookup(affinity::pack_tuple_id(1, 20), 0) == 3);
    assert(table.Lookup(affinity::pack_tuple_id(0, 999), 0) == 0);

    const std::string bad_path = "/tmp/wookong_schism_assignment_bad.csv";
    {
        std::ofstream f(bad_path, std::ios::out | std::ios::trunc);
        f << "tuple_id,table_id,item_key,node_id\n";
        f << affinity::pack_tuple_id(0, 10) << ",0,10,99\n";
    }
    const auto bad = table.LoadFromCsv(bad_path, 4);
    assert(!bad.ok);
    assert(bad.rows_loaded == 0);
    assert(table.Size() == 3);
    return 0;
}
```

Register it in `tests/CMakeLists.txt`:

```cmake
add_executable(affinity_schism_static_test
    test_cases/affinity_schism_static_test.cc
    ../core/affinity/assignment_table.cc
)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cmake --build build --target affinity_schism_static_test -j`

Expected: compile failure containing `no member named 'LoadFromCsv'`.

- [ ] **Step 3: Implement loader**

In `core/affinity/assignment_table.h`, add:

```cpp
struct LoadResult {
    bool ok = false;
    size_t rows_loaded = 0;
    std::string error;
};

LoadResult LoadFromCsv(const std::string& path, int compute_node_count);
```

In `core/affinity/assignment_table.cc`, parse headered CSV rows, validate four comma-separated fields, validate `node_id` in `[0, compute_node_count)`, verify `tuple_id == pack_tuple_id(table_id, item_key)`, then publish a fresh snapshot with `version = 1`. On error, return `ok=false` and leave the current snapshot unchanged.

- [ ] **Step 4: Run test to verify it passes**

Run: `cmake --build build --target affinity_schism_static_test -j && ./build/tests/affinity_schism_static_test`

Expected: command exits 0.

- [ ] **Step 5: Commit**

Run:

```bash
git add core/affinity/assignment_table.h core/affinity/assignment_table.cc tests/test_cases/affinity_schism_static_test.cc tests/CMakeLists.txt
git commit -m "Add Schism static assignment loader"
```

## Task 2: Deterministic Affinity Graph Dump

**Files:**
- Create: `core/affinity/graph_dump.h`
- Create: `core/affinity/graph_dump.cc`
- Modify: `core/affinity/aggregator.cc`
- Modify: `core/CMakeLists.txt`
- Test: `tests/test_cases/affinity_schism_static_test.cc`

- [ ] **Step 1: Add failing graph dump assertions**

Append to the C++ test:

```cpp
#include <sstream>
#include "affinity/graph.h"
#include "affinity/graph_dump.h"

static std::string read_all(const std::string& path) {
    std::ifstream f(path);
    std::ostringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

static void test_graph_dump() {
    affinity::LocalGraph g;
    g.epoch = 7;
    g.total_samples = 2;
    g.AddEdge(30, 10, 4);
    g.AddEdge(10, 20, 3);
    g.AddNodeAccess(10, 0);
    g.AddNodeAccess(10, 0);
    g.AddNodeAccess(20, 1);

    std::string error;
    assert(affinity::DumpLocalGraphCsv(g, "/tmp/wookong_schism_graph.csv", &error));
    const std::string text = read_all("/tmp/wookong_schism_graph.csv");
    assert(text.find("record_type,tuple_id_a,tuple_id_b,weight,node_id,access_count,epoch,total_samples\n") != std::string::npos);
    assert(text.find("edge,10,20,3,,0,7,2\n") != std::string::npos);
    assert(text.find("edge,10,30,4,,0,7,2\n") != std::string::npos);
    assert(text.find("access,10,,0,0,2,7,2\n") != std::string::npos);
    assert(text.find("access,20,,0,1,1,7,2\n") != std::string::npos);
}
```

Call `test_graph_dump();` from `main()`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cmake --build build --target affinity_schism_static_test -j`

Expected: compile failure containing `graph_dump.h: No such file`.

- [ ] **Step 3: Implement graph dump helper**

`core/affinity/graph_dump.h`:

```cpp
#pragma once

#include <string>

#include "graph.h"

namespace affinity {

bool DumpLocalGraphCsv(const LocalGraph& graph,
                       const std::string& path,
                       std::string* error = nullptr);

}  // namespace affinity
```

`core/affinity/graph_dump.cc` sorts undirected edges by `(min(u,v), max(u,v))`, sorts access rows by `(tuple_id, node_id)`, writes to `path + ".tmp"` then renames to `path`.

Add `affinity/graph_dump.cc` to `AFFINITY_SRC` in `core/CMakeLists.txt`.

- [ ] **Step 4: Hook aggregator**

In `core/affinity/aggregator.cc`, include `graph_dump.h` and read `AFFINITY_GRAPH_DUMP_PATH` once. After each `EnqueueLocalGraph(accumulator)` call, dump `*frozen` when the env var is non-empty. After the loop exits, dump the remaining `accumulator` if it has vertices. Log failures with `std::fprintf(stderr, ...)`.

- [ ] **Step 5: Run test**

Run: `cmake --build build --target affinity_schism_static_test -j && ./build/tests/affinity_schism_static_test`

Expected: command exits 0 and `/tmp/wookong_schism_graph.csv` contains sorted `edge` rows.

- [ ] **Step 6: Commit**

Run:

```bash
git add core/affinity/graph_dump.h core/affinity/graph_dump.cc core/affinity/aggregator.cc core/CMakeLists.txt tests/test_cases/affinity_schism_static_test.cc
git commit -m "Dump affinity graph for Schism training"
```

## Task 3: Schism Static Runtime Gate

**Files:**
- Create: `core/affinity/schism_static.h`
- Modify: `compute_server/server.h`
- Modify: `compute_server/worker/handler.cc`
- Modify: `core/affinity/migration_worker.cc`
- Modify: `core/affinity/affinity_timeseries.cc`
- Test: `tests/test_cases/affinity_schism_static_test.cc`

- [ ] **Step 1: Add failing helper tests**

Append to the C++ test:

```cpp
#include "affinity/schism_static.h"

static void test_schism_apply_convergence() {
    assert(!affinity::SchismApplyConverged(0, 0, 0));
    assert(affinity::SchismApplyConverged(100, 99, 0));
    assert(!affinity::SchismApplyConverged(100, 98, 0));
    assert(!affinity::SchismApplyConverged(100, 99, 2));
}
```

Call `test_schism_apply_convergence();` from `main()`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cmake --build build --target affinity_schism_static_test -j`

Expected: compile failure containing `schism_static.h: No such file`.

- [ ] **Step 3: Add static-mode helper**

Create `core/affinity/schism_static.h`:

```cpp
#pragma once

#include <cstdlib>
#include <string>

namespace affinity {

inline bool EnvFlagEnabled(const char* name) {
    const char* raw = std::getenv(name);
    return raw != nullptr && std::string(raw) != "0" && std::string(raw) != "false";
}

inline bool IsSchismStaticEnabled() {
    return EnvFlagEnabled("SCHISM_STATIC");
}

inline std::string SchismStaticCsvPath() {
    const char* raw = std::getenv("SCHISM_STATIC_CSV");
    return raw == nullptr ? std::string() : std::string(raw);
}

inline int SchismStaticApplyMs() {
    const char* raw = std::getenv("SCHISM_STATIC_APPLY_MS");
    if (raw == nullptr) return 60000;
    const int parsed = std::atoi(raw);
    return parsed > 0 ? parsed : 60000;
}

inline bool SchismApplyConverged(uint64_t planned,
                                 uint64_t done,
                                 uint64_t failed) {
    if (planned == 0) return false;
    return (done * 100 >= planned * 99) && (failed * 100 <= planned);
}

}  // namespace affinity
```

- [ ] **Step 4: Wire runtime gates**

In `server.h`, change the lookup gate to:

```cpp
const bool static_assignment_enabled = affinity::IsSchismStaticEnabled();
if ((!enable_affinity && !static_assignment_enabled) || table_id >= 10000 ||
    item_key == static_cast<itemkey_t>(-1)) {
    return fallback;
}
```

In `handler.cc`, allow `StartAffinityRuntimeIfEnabled` when `enable_affinity || IsSchismStaticEnabled()`. In static mode, call `affinity::Init()`, load `SCHISM_STATIC_CSV`, log the loaded row count, start one `MigrationLoop`, and start `TimeseriesLoop`; do not start sidecars, aggregator, shuffler, partitioner, or affinity service.

In `migration_worker.cc` and `affinity_timeseries.cc`, replace `if (!enable_affinity) return;` with `if (!enable_affinity && !affinity::IsSchismStaticEnabled()) return;`.

In `MigrationLoop`, record `start = steady_clock::now()` and break in static mode once `SchismApplyConverged(planned, done, failed)` is true or elapsed exceeds `SchismStaticApplyMs()`.

- [ ] **Step 5: Run test and build compute_server**

Run:

```bash
cmake --build build --target affinity_schism_static_test -j
./build/tests/affinity_schism_static_test
cmake --build build --target compute_server -j
```

Expected: all commands exit 0.

- [ ] **Step 6: Commit**

Run:

```bash
git add core/affinity/schism_static.h compute_server/server.h compute_server/worker/handler.cc core/affinity/migration_worker.cc core/affinity/affinity_timeseries.cc tests/test_cases/affinity_schism_static_test.cc
git commit -m "Add Schism static runtime mode"
```

## Task 4: Offline Graph To Static CSV Tool

**Files:**
- Create: `tests/scripts/schism_partition.py`
- Create: `tests/scripts/test_schism_partition.py`

- [ ] **Step 1: Write failing Python tests**

Create `tests/scripts/test_schism_partition.py`:

```python
import tempfile
import unittest
from pathlib import Path

from schism_partition import load_graph_dump, write_assignment_csv, fallback_partition


class SchismPartitionTest(unittest.TestCase):
    def test_load_graph_dump_reads_edges_and_accesses(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "graph.csv"
            path.write_text(
                "record_type,tuple_id_a,tuple_id_b,weight,node_id,access_count,epoch,total_samples\n"
                "edge,10,20,3,,0,7,2\n"
                "access,10,,0,0,2,7,2\n"
                "access,20,,0,1,1,7,2\n",
                encoding="utf-8",
            )
            graph = load_graph_dump(path)
            self.assertEqual(graph.vertices, [10, 20])
            self.assertEqual(graph.edges, [(10, 20, 3)])
            self.assertEqual(graph.access[10], 2)

    def test_write_assignment_csv_is_sorted(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "assignment.csv"
            write_assignment_csv(out, {20: 1, 10: 0})
            self.assertEqual(
                out.read_text(encoding="utf-8"),
                "tuple_id,table_id,item_key,node_id\n"
                "10,0,10,0\n"
                "20,0,20,1\n",
            )

    def test_fallback_partition_is_deterministic(self):
        self.assertEqual(fallback_partition([10, 20, 30], 2), {10: 0, 20: 1, 30: 0})


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 tests/scripts/test_schism_partition.py`

Expected: import failure for `schism_partition`.

- [ ] **Step 3: Implement script**

Create `tests/scripts/schism_partition.py` with:

- `GraphDump(vertices: list[int], edges: list[tuple[int,int,int]], access: dict[int,int])`
- `load_graph_dump(path: Path) -> GraphDump`
- `fallback_partition(vertices: list[int], parts: int) -> dict[int,int]`
- `write_assignment_csv(path: Path, assignment: dict[int,int]) -> None`
- CLI args: `--graph`, `--out`, `--parts`, `--gpmetis`, `--allow-fallback`

`gpmetis` path writes METIS format to a temp dir, invokes `gpmetis -seed=1 graph.metis <parts>`, reads `graph.metis.part.<parts>`, and emits sorted CSV. If `--allow-fallback` is set, use round-robin sorted vertices instead of failing when `gpmetis` is missing.

- [ ] **Step 4: Run tests**

Run: `python3 tests/scripts/test_schism_partition.py`

Expected: 3 tests pass.

- [ ] **Step 5: Commit**

Run:

```bash
git add tests/scripts/schism_partition.py tests/scripts/test_schism_partition.py
git commit -m "Add Schism offline partition tool"
```

## Task 5: Driver Script Three-Arm Plumbing

**Files:**
- Modify: `tests/scripts/multinode_parmetis_smoke.py`
- Modify: `tests/scripts/multinode_mprouter_smoke.py`
- Modify: `tests/scripts/test_multinode_mprouter_smoke.py`

- [ ] **Step 1: Write failing Python tests**

Add tests that assert:

```python
def test_make_configs_sets_hot_account_offset(self):
    configs = make_configs([...], "10.10.2.38", self.make_config_args(hot_account_offset=250000), enable_affinity=True)
    cfg = json.loads(configs["smallbank_aff_config.json"])
    self.assertEqual(cfg["smallbank_aff"]["hot_account_offset"], 250000)

def test_run_mprouter_passes_constant_system_mode(self):
    # existing FakeProcess test keeps mprouter_system_mode=24 and asserts the Popen command includes ["--system-mode", "24"].
```

Add tests for a new `write_schism_compare_summary(stamp_dir, base, schism, aff)` that writes all three throughput lines and the two deltas:

```python
baseline_throughput_tps=100.000000
schism_static_throughput_tps=110.000000
affinity_throughput_tps=130.000000
affinity_vs_schism_throughput_delta_pct=18.18
affinity_vs_baseline_throughput_delta_pct=30.00
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 tests/scripts/test_multinode_mprouter_smoke.py`

Expected: failures for missing `hot_account_offset` and missing three-arm summary helper.

- [ ] **Step 3: Implement config and summary helpers**

In `multinode_parmetis_smoke.py`, add to both SmallBank config maps:

```python
"hot_account_offset": int(getattr(args, "hot_account_offset", 0)),
```

In `multinode_mprouter_smoke.py`, add CLI args:

```python
p.add_argument("--three-arm", action="store_true")
p.add_argument("--schism-csv", type=Path, default=None)
p.add_argument("--schism-apply-ms", type=int, default=60000)
p.add_argument("--schism-train-try-count", type=int, default=5000)
p.add_argument("--hot-account-offset", type=int, default=0)
```

Add `write_schism_compare_summary(stamp_dir, base, schism, aff)` beside `write_compare_summary`.

- [ ] **Step 4: Implement static run env**

Change `start_compute_interactive` to accept `extra_env: dict[str, str] | None` and emit `export KEY=value` lines before `nohup`. Change `boot_cluster` to accept and pass `compute_env`.

Change `run_case(case_name, enable_affinity, compute_env=None)` to pass static env to `boot_cluster`.

For `schism_static`, require `--schism-csv`, upload it to each compute host as `${remote_dir}/config/schism_static_assignment.csv`, and set:

```python
compute_env={
    "SCHISM_STATIC": "1",
    "SCHISM_STATIC_CSV": f"{args.remote_dir}/config/schism_static_assignment.csv",
    "SCHISM_STATIC_APPLY_MS": str(args.schism_apply_ms),
}
```

- [ ] **Step 5: Run tests**

Run: `python3 tests/scripts/test_multinode_mprouter_smoke.py`

Expected: all tests pass.

- [ ] **Step 6: Commit**

Run:

```bash
git add tests/scripts/multinode_parmetis_smoke.py tests/scripts/multinode_mprouter_smoke.py tests/scripts/test_multinode_mprouter_smoke.py
git commit -m "Add three-arm Schism comparison driver"
```

## Task 6: Build, Smoke, And Experiment Gate

**Files:**
- No new source files unless verification reveals a bug.

- [ ] **Step 1: Run focused tests**

Run:

```bash
cmake --build build --target affinity_schism_static_test compute_server -j
./build/tests/affinity_schism_static_test
python3 tests/scripts/test_schism_partition.py
python3 tests/scripts/test_multinode_mprouter_smoke.py
```

Expected: all commands exit 0.

- [ ] **Step 2: Preflight cluster before long work**

Run:

```bash
python3 tests/scripts/multinode_mprouter_smoke.py --three-arm --disable-affinity --try-count 1 --timeout 30
```

Expected: if any host is unreachable or dirty, the script exits non-zero and writes `preflight.txt`. Do not claim experiment success until all four compute hosts and the service host pass preflight.

- [ ] **Step 3: If preflight passes, run a short three-arm smoke**

Run:

```bash
python3 tests/scripts/multinode_mprouter_smoke.py --three-arm --force-clean --num-accounts 500000 --worker-threads 4 --try-count 500 --batch-size 200 --num-bucket 4 --mprouter-system-mode 24 --schism-csv /tmp/schism_static_assignment.csv --timeout 600
```

Expected: result directory contains `baseline/summary.txt`, `schism_static/summary.txt`, `affinity_on/summary.txt`, and `schism_compare_summary.txt`.

- [ ] **Step 4: If smoke passes, run the paper-scale comparison**

Use the agreed duration/transaction knobs from the latest MP-Router experiment, keeping `--mprouter-system-mode 24` identical for all arms. Record the exact command and result directory in the final response.

## Self-Review

- Spec coverage: CSV loading, graph dump, static runtime, measurement-only time-series, offline partitioning, three-arm summary, and incomplete cluster handling are covered.
- Known gap: MP-Router does not currently expose a hot-account offset. Hybrid config support is included, but strict MP-Router hot-range drift requires editing `/root/mingtai/MP-Router` or adding that tree to the writable workspace.
- Placeholder scan: no `TBD`/`TODO`/`implement later` placeholders remain.
- Type consistency: runtime env names are `SCHISM_STATIC`, `SCHISM_STATIC_CSV`, `SCHISM_STATIC_APPLY_MS`, and `AFFINITY_GRAPH_DUMP_PATH`.
