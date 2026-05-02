# Repository Guidelines

## Project Structure & Module Organization

WookongDB MP is a C++17 shared-storage, multi-primary database with storage, metadata, and compute tiers. Core runtime code lives in `core/`: transactions in `core/dtx/`, lock managers in `core/GPLM/` and `core/LPLM/`, storage/WAL in `core/storage/`, SQL execution in `core/sql_executor/`, and affinity repartitioning in `core/affinity/`. Tier entrypoints are under `storage_server/`, `remote_server/`, `compute_server/`, and `wookongdb-mp-client/`. Workload drivers are in `workload/{smallbank,ycsb,tpcc,smallbank_aff}`. Tests are split between Python harnesses in `tests/scripts/` and C++ cases plus SQL fixtures in `tests/test_cases/`. Configuration JSON files live in `config/`; keep compute, storage, and remote node configs in sync.

## Build, Test, and Development Commands

- `git submodule update --init --recursive`: fetches third-party dependencies.
- `mkdir -p build && cd build && cmake .. && make -j`: configures and builds all default targets.
- `cmake .. -DBRPC_ROOT=/path/to/brpc`: use when brpc is not under the default `/home/wlj/include`.
- `cmake .. -DBUILD_PARMETIS_SIDECAR=ON && make -j`: opt-in build for the ParMETIS affinity sidecar.
- `python3 tests/scripts/run_all_tests.py`: runs the main end-to-end SQL suites from the repo root.
- `python3 tests/scripts/stage2_local_affinity.py`: local affinity regression; tune with `STAGE2_ATTEMPTED_NUM` and `STAGE2_THREADS`.

## Coding Style & Naming Conventions

Follow surrounding style: C++ uses 4-space indentation, `.cc`/`.h` pairs, C++17, and CMake targets per module. Python scripts use 4-space indentation and `snake_case`. Use existing names for workload modes (`smallbank`, `ycsb`, `tpcc`) and page strategies (`eager`, `lazy`). No repo-wide formatter is configured; keep patches small and consistent. Do not hand-edit generated `*.pb.cc` or `*.pb.h` files.

## Testing Guidelines

Add C++ tests under `tests/test_cases/` and register binaries in `tests/CMakeLists.txt`. Add end-to-end orchestration under `tests/scripts/`, reusing `tests/scripts/test_env.py` helpers for process cleanup and listen checks. Name SQL fixtures and expected outputs after the suite, for example `join_test.sql` and `join_test_output.txt`.

## Commit & Pull Request Guidelines

Recent commits use short imperative subjects, for example `Stabilize affinity migration fetch paths` or `Batch affinity migrations by source page`. Keep PRs focused, describe the affected tier or workload, list commands run, and call out config or topology changes. Include logs or plots only when they support a benchmark or regression claim; do not commit `build/`, `result/`, logs, perf output, or generated protobuf files.

## Runtime & Configuration Notes

Start tiers in order: storage, remote metadata, then compute. SQL mode expects all tiers launched as `sql`; workload mode expects matching workload names across tiers. Binaries often assume they run from their `build/<tier>/` directories because some config paths are relative.
