#!/usr/bin/env python3
"""Versioned, read-only metric interpretation; no SSH or runner imports.

python3 -m tools.benchmark.report RESULT_RUN [--output-dir OUTPUT]
Rebuild stats.log and comparison.csv from either directory layout. Legacy files
are never rewritten. Missing measurements remain N/A, not zero.
"""
import argparse
import csv
import hashlib
import io
import json
import math
import os
from pathlib import Path

PATHS = ("eager", "lazy", "2pc", "single", "mix_lazy", "mix_2pc")
STAGES = ("begin", "execute", "commit", "abort")


def atomic_text(path, text):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    pending = path.with_name(path.name + '.tmp')
    with pending.open('w') as out:
        out.write(text)
        out.flush()
        os.fsync(out.fileno())
    pending.replace(path)
    fd = os.open(str(path.parent), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)

def atomic_json(path, data):
    atomic_text(path, json.dumps(data, indent=2) + '\n')


def read_kv(path):
    values = {}
    if not Path(path).is_file():
        return values
    for line in Path(path).read_text(errors="replace").splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip()
        try:
            parts = value.split()
            nums = [int(x) if x.lstrip("-").isdigit() else float(x) for x in parts]
            if not nums or not all(math.isfinite(x) for x in nums):
                raise ValueError()
            value = nums[0] if len(nums) == 1 else nums
        except ValueError:
            pass
        values[key.strip()] = value
    return values


def number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def total(nodes, key):
    vals = [n.get(key) for n in nodes]
    return sum(vals) if vals and all(number(v) for v in vals) else None


def divide(value, count, scale=1):
    return value * scale / count if number(value) and number(count) and count > 0 else None


def txn_counts(node):
    pairs = [v for k, v in node.items() if k.endswith("_try_commit")
             and isinstance(v, list) and len(v) == 2 and all(number(x) for x in v)]
    if not pairs:
        return None, None
    return sum(p[0] for p in pairs), sum(p[1] for p in pairs)


def redact(value):
    if isinstance(value, dict):
        return {k: redact(v) for k, v in value.items()
                if not any(s in k.lower() for s in ("password", "passwd", "secret", "token", "private_key"))}
    if isinstance(value, list):
        return [redact(v) for v in value]
    return value


def fingerprint(settings):
    return hashlib.sha256(json.dumps(redact(settings), sort_keys=True).encode()).hexdigest()


def discover(root):
    # Do not traverse compatibility symlinks and report the same trial twice.
    root = Path(root).resolve()
    if (root / "summary_human.txt").is_file():
        return [root]
    return sorted({p.parent.resolve() for p in root.rglob("summary_human.txt")
                   if "legacy" not in p.relative_to(root).parts})


def display(value):
    if value is None or value == "":
        return "N/A"
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value)


def table(title, columns, rows):
    rows = [[display(x) for x in row] for row in rows]
    columns = ["title"] + list(columns)
    rows = [[title] + row for row in rows]
    widths = [max([len(col)] + [len(row[i]) for row in rows]) for i, col in enumerate(columns)]
    def line(row):
        return " | ".join(cell.ljust(w) for cell, w in zip(row, widths))
    return "\n".join([line(columns), "-+-".join("-" * w for w in widths)] +
                     [line(row) for row in rows] + [f"({len(rows)} rows)", ""])


def build_report(directory, metadata=None):
    directory = Path(directory)
    settings = read_kv(directory / "summary_human.txt")
    if (directory / "metadata.json").exists():
        settings.update(json.loads((directory / "metadata.json").read_text()))
    settings.update(metadata or {})
    files = sorted(directory.glob("node[0-9]*/result.txt"))
    nodes = [read_kv(f) for f in files]
    expected = settings.get("node_count", len(nodes))
    complete = len(nodes) == expected and bool(nodes)
    # A missing node invalidates cluster totals; still expose all available rows.
    cluster = nodes if complete else nodes + [{}]
    counts = [txn_counts(n) for n in cluster]
    attempts = sum(x[0] for x in counts) if counts and all(number(x[0]) for x in counts) else None
    commits = sum(x[1] for x in counts) if counts and all(number(x[1]) for x in counts) else None
    aborts = attempts - commits if attempts is not None and commits is not None else None
    sections = []
    warnings = []
    if settings.get('status', 'complete') != 'complete':
        warnings.append('Trial is incomplete; do not use it for comparisons.')
    if not complete:
        warnings.append(f"Incomplete nodes: expected {expected}, found {len(nodes)}; cluster totals are N/A.")
    sections.append(table("run_settings", ("setting", "value"), [
        (k, settings.get(k)) for k in ("case_id", "bench_name", "system_name", "node_count", "thread_num",
        "run_mode", "run_seconds",
        "attempts_per_thread", "local_ratio", "zipf_theta", "tx_hot", "write_txn_ratio", "lock_mode",
        "generate_log", "parallel_page_fetch", "hybrid_skew_threshold", "hot_key_top_n", "code_revision",
        "source_fingerprint", "run_fingerprint")] ))
    # Legacy throughput is a sum of worker rates, not a common-window cluster TPS.
    sections.append(table("txn_summary", ("attempts", "commits", "aborts", "abort_pct",
        "legacy_sum_worker_tps", "measured_completed", "coverage_pct"), [[
        attempts, commits, aborts, divide(aborts, attempts, 100), total(cluster, "throughput"),
        total(cluster, "stats_txn_completed"), divide(total(cluster, "stats_txn_completed"), attempts, 100)]]))

    sections.append(table("throughput_summary", ("sum_node_window_tps", "window_commits", "window_aborts",
        "final_commits_incl_drain", "final_aborts_incl_drain"), [[
        total(cluster, "benchmark_committed_tps"), total(cluster, "benchmark_window_commits"),
        total(cluster, "benchmark_window_aborts"), total(cluster, "benchmark_final_commits"),
        total(cluster, "benchmark_final_aborts")]]))
    sections.append(table("node_throughput", ("node", "run_mode", "measured_s", "drain_s", "committed_tps",
        "completed_tps", "window_commits", "final_commits"), [
        (f.parent.name, n.get("benchmark_run_mode"), n.get("benchmark_measurement_seconds"),
         n.get("benchmark_drain_seconds"), n.get("benchmark_committed_tps"), n.get("benchmark_completed_tps"),
         n.get("benchmark_window_commits"), n.get("benchmark_final_commits")) for f, n in zip(files, nodes)]))

    measured = total(cluster, "stats_txn_completed")
    if measured is not None and attempts is not None and measured != attempts:
        warnings.append(f'Timing coverage mismatch: {measured} completed measurements / {attempts} issued attempts.')
    rows = []
    if measured is not None:
        elapsed = total(cluster, "stats_txn_total_ns")
        stage_values = [total(cluster, f"stats_txn_{s}_ns") for s in STAGES]
        residual = elapsed - sum(stage_values) if elapsed is not None and all(number(v) for v in stage_values) else None
        if residual is not None and residual < 0:
            warnings.append("Negative unaccounted time: timing boundaries need investigation; value not clamped.")
        for stage, value in list(zip(STAGES, stage_values)) + [("other", residual), ("total", elapsed)]:
            rows.append((stage, divide(value, 1e9), divide(value, measured, 1e-6), measured,
                         "completed attempts incl. aborts"))
    else:
        warnings.append("Legacy binary: end-to-end and per-path timings are unavailable; stage sums are NOT total latency.")
        for stage, key in zip(STAGES, ("tx_begin_time", "tx_exe_time", "tx_commit_time", "tx_abort_time")):
            value = total(cluster, key)
            rows.append((stage, value, divide(value, attempts, 1000), attempts, "issued attempts (legacy stage timer)"))
        rows += [("other", None, None, attempts, "not measured"), ("total", None, None, attempts, "not measured")]
    sections.append(table("txn_time_breakdown", ("stage", "total_s", "avg_ms_per_attempt", "denominator", "population"), rows))

    rows = []
    for path in PATHS:
        prefix = f"stats_path_{path}_"
        count = total(cluster, prefix + "completed")
        if count == 0:
            continue
        mode = settings.get('system_name')
        relevant = ('mix_lazy', 'mix_2pc') if mode == 'mix' else (mode,)
        if count is None and mode in PATHS + ('mix',) and path not in relevant:
            continue
        path_total = total(cluster, prefix + 'total_ns')
        path_stages = [total(cluster, prefix + s + '_ns') for s in STAGES]
        path_other = path_total - sum(path_stages) if number(path_total) and all(number(v) for v in path_stages) else None
        rows.append([path, count, total(cluster, prefix + "committed"), total(cluster, prefix + "aborted"),
                     divide(path_total, count, 1e-6)] +
                    [divide(v, count, 1e-6) for v in path_stages + [path_other]])
    sections.append(table("execution_path_timing", ("path", "completed", "committed", "aborted",
        "total_ms", "begin_ms", "execute_ms", "commit_ms", "abort_ms", "other_ms"), rows or [["none", 0, 0, 0] + [None]*6]))
    sections.append(table("outcome_latency", ("outcome", "count", "avg_ms"), [
        (s, total(cluster, "stats_txn_" + s), divide(total(cluster, "stats_txn_" + s + "_ns"),
                                                 total(cluster, "stats_txn_" + s), 1e-6))
        for s in ("committed", "aborted")]))

    sections.append(table("lock_wait_stats", ("scope", "waits", "total_s", "ms_per_issued_attempt",
        "suspended_p50_ms", "suspended_p99_ms", "max_queue"), [
        ["cluster", total(cluster, "lock_wait_total"), total(cluster, "lock_wait_time"),
         divide(total(cluster, "lock_wait_time"), attempts, 1000), None, None,
         max((n.get("lock_wait_wq_depth_max", 0) for n in nodes), default=0) if all("lock_wait_wq_depth_max" in n for n in cluster) else None]] + [
        [f.parent.name, n.get("lock_wait_total"), n.get("lock_wait_time"),
         divide(n.get("lock_wait_time"), txn_counts(n)[0], 1000), divide(n.get("lock_wait_us_p50"), 1000),
         divide(n.get("lock_wait_us_p99"), 1000), n.get("lock_wait_wq_depth_max")]
        for f, n in zip(files, nodes)]))

    # These are diagnostic slices, NOT additional mutually exclusive stages.
    slices = [
        ("fetch_execute", "tx_fetch_exe_time", "execute; includes locking"),
        ("row_lock_wait", "lock_wait_time", "execute; may overlap remote RPCs"),
        ("ownership_transfer", "ownership_transfer_time_total", "execute/commit/abort"),
        ("remote_2pc_fetch", "twopc_remote_fetch_time", "execute"),
        ("storage_page_fetch_all", "fetch_storage_page_time", "all page types; includes metadata pages"),
        ("wait_commit_log", "wait_commit_log_time", "commit; includes participant-side work"),
        ("wait_prepare_log", "wait_prepare_log_time", "prepare; participant-side"),
        ("wait_backup_log", "wait_backup_log_time", "commit"),
        ("wait_log_flush_all", "wait_log_flush_time", "multiple phases; overlaps rows above"),
        ("prepare_phase", "tx_write_prepare_log_time", "commit; coordinator-side"),
        ("backup_phase", "tx_write_backup_log_time", "commit; coordinator-side"),
        ("distributed_commit_phase", "tx_write_commit_log_time2", "commit; coordinator-side"),
    ]
    sections.append(table("time_details", ("slice", "total_s", "ms_per_issued_attempt", "included_in"), [
        (label, total(cluster, key), divide(total(cluster, key), attempts, 1000), parent)
        for label, key, parent in slices]))
    sections.append(table("page_access_stats", ("operation", "count", "per_issued_attempt", "avg_ms_per_call"), [
        (label, total(cluster, count), divide(total(cluster, count), attempts),
         divide(total(cluster, timing), total(cluster, count), 1000) if timing else None)
        for label, count, timing in (
            ("local_fetch", "fetch_from_local_count", None), ("remote_page_fetch", "fetch_from_remote_count", None),
            ("storage_data_page_fetch", "fetch_from_storage_count", None),
            ("ownership_transfer", "ownership_transfer_count", "ownership_transfer_time_total"),
            ("remote_2pc_fetch", "twopc_remote_fetch_count", "twopc_remote_fetch_time"),
            ("eviction", "evicted_pages_count", None))]))
    sections.append(table("commit_path_stats", ("counter", "count", "meaning"), [
        ("single_writer_fast_path", total(cluster, "single_txn_count"), "2pc path, <=1 write participant"),
        ("multi_writer_2pc", total(cluster, "distribute_txn_count"), "2pc path, >1 write participants"),
        ("mix_2pc_commit_calls", total(cluster, "hybrid_2pc_commit_count"), "includes single-writer fast path"),
        ("mix_lazy_commit_calls", total(cluster, "hybrid_lazy_commit_count"), "commit path entered; excludes execution aborts")]))
    sections.append(table("compute_log_stats", ("flush_batches", "records_in_batches", "records_per_batch",
        "rpc_total_s", "rpc_avg_ms", "flush_wait_total_s"), [[total(cluster, "log_flush_count"),
        total(cluster, "log_flush_total_batch"), divide(total(cluster, "log_flush_total_batch"), total(cluster, "log_flush_count")),
        total(cluster, "log_flush_storage_rpc_time"), divide(total(cluster, "log_flush_storage_rpc_time"), total(cluster, "log_flush_count"), 1000),
        total(cluster, "wait_log_flush_time")]]))
    storage = read_kv(directory / "storage" / "storage_timing_stats.txt")
    sections.append(table("storage_log_stats", ("logwrite_calls", "fdatasync_calls", "write_total_s",
        "sync_total_s", "avg_sync_ms", "logwrite_calls_per_sync"), [[
        storage.get("storage_logwrite_count"), storage.get("storage_log_fdatasync_count"),
        storage.get("storage_log_write_time_sec"), storage.get("storage_log_fdatasync_time_sec"),
        divide(storage.get("storage_log_fdatasync_time_sec"), storage.get("storage_log_fdatasync_count"), 1000),
        storage.get("storage_log_avg_logwrite_per_fdatasync") if storage.get("storage_log_fdatasync_count", 0) > 0 else None]]))
    if not storage:
        warnings.append("Storage statistics unavailable (not collected or shutdown did not produce a fresh snapshot).")
    sections.append(table("node_summary", ("node", "attempts", "commits", "abort_pct", "legacy_sum_worker_tps",
        "avg_worker_elapsed_s", "generate_log"), [
        [f.parent.name, *txn_counts(n), divide(txn_counts(n)[0] - txn_counts(n)[1], txn_counts(n)[0], 100)
         if all(number(x) for x in txn_counts(n)) else None,
         n.get("throughput"), n.get("total_time_seconds"), n.get("stats_generate_log")]
        for f, n in zip(files, nodes)]))
    notes = [
        f"Source: {directory.resolve()}",
        "stats.log schema=1; raw result.txt and legacy summary_human.txt are preserved.",
        "New timings: monotonic clock, TxBegin entry to commit/abort completion, coordinator-side only.",
        "Main stage rows are exclusive. 'other' includes workload/key generation between DTX calls and measurement overhead.",
        "Execution can invoke TxExe multiple times; all calls are accumulated for that attempt.",
        "Commit includes rollback done inside a failed commit. Abort row measures explicit TxAbortWorkLoad only.",
        "Per-path averages divide by that path's completed attempts, including aborted attempts.",
        "time_details are overlapping diagnostic slices, NOT additive to txn_time_breakdown.",
        "Participant and background work are not coordinator latency; cluster percentiles require merged histograms and remain N/A.",
        "Window TPS counts successful commits inside the node measurement window; drain completions are excluded.",
        "Cluster sum_node_window_tps sums node rates; nodes use local start barriers, not a synchronized cluster window.",
        "Transaction breakdown counts all completed attempts, including drain; it is not limited to the TPS window.",
        "Time-mode legacy latency percentiles use the first up to 100000 commits per worker, not an unbiased whole-run sample.",
        "legacy_sum_worker_tps sums per-worker rates. avg_worker_elapsed_s is NOT experiment wall time.",
        "YCSB write_txn_ratio controls writes among ten operations, not the fraction of write-only transactions.",
        "Log record construction counters are not persisted-record counters, especially with generate_log=0.",
        "Storage data-page count excludes metadata pages; all-page fetch time cannot be divided by that count.",
    ] + ["WARNING: " + x for x in warnings]
    overview = {k: settings.get(k) for k in ("case_id", "bench_name", "system_name", "case_name", "lock_mode",
                "run_mode", "run_seconds", "generate_log", "thread_num", "node_count", "local_ratio", "write_txn_ratio", "zipf_theta")}
    overview.update(sum_node_window_tps=total(cluster, "benchmark_committed_tps"), attempts=attempts, commits=commits, abort_pct=divide(aborts, attempts, 100),
                    legacy_sum_worker_tps=total(cluster, "throughput"),
                    avg_completed_attempt_ms=divide(total(cluster, "stats_txn_total_ns"), measured, 1e-6),
                    lock_wait_ms_per_attempt=divide(total(cluster, "lock_wait_time"), attempts, 1000),
                    source=str(directory))
    return "\n\n".join(sections) + "\n" + "\n".join(notes) + "\n", overview


def write_stats(directory, metadata=None, output=None):
    report, overview = build_report(directory, metadata)
    output = Path(output) if output else Path(directory) / "stats.log"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(report)
    return overview


def result_complete(directory, case, mode, run_fingerprint):
    directory = Path(directory)
    try:
        data = json.loads((directory / 'metadata.json').read_text())
    except (OSError, ValueError):
        return False
    if (data.get('status') != 'complete' or data.get('run_fingerprint') != run_fingerprint
            or data.get('case_fingerprint') != fingerprint(case) or data.get('system_name') != mode):
        return False
    for name in ('summary_human.txt', 'summary_matrix.txt', 'stats.log'):
        p = directory / name
        if not p.is_file() or not p.stat().st_size:
            return False
    for idx in range(case['nodes']):
        node = read_kv(directory / f'node{idx}/result.txt')
        attempts, commits = txn_counts(node)
        tps_log = directory / f'node{idx}/tps.log'
        if not tps_log.is_file() or not tps_log.stat().st_size:
            return False
        if attempts is None or commits is None or not 0 <= commits <= attempts:
            return False
        if case['run_mode'] == 'count' and attempts != case['threads'] * case['attempts']:
            return False
        if (node.get('benchmark_run_mode') != case['run_mode']
                or node.get('benchmark_final_attempts') != attempts
                or node.get('benchmark_final_commits') != commits
                or node.get('benchmark_final_aborts') != attempts - commits):
            return False
        seconds = node.get('benchmark_measurement_seconds')
        if not number(seconds) or seconds <= 0:
            return False
        if case['run_mode'] == 'time' and not math.isclose(seconds, case['run_seconds'], rel_tol=1e-5):
            return False
    return True


def write_comparison(root):
    root = Path(root)
    dirs = discover(root)
    if (root / 'case_plan.tsv').is_file():
        saved = json.loads((root / 'run.json').read_text())
        valid = []
        for directory in dirs:
            data = json.loads((directory / 'metadata.json').read_text())
            case = data.get('planned_case')
            if case and result_complete(directory, case, data['system_name'], saved['run_fingerprint']):
                valid.append(directory)
        dirs = valid
    rows = []
    for p in dirs:
        manifest = p / 'metadata.json'
        if manifest.exists() and json.loads(manifest.read_text()).get('status') != 'complete':
            continue
        rows.append(build_report(p)[1])
    if rows:
        out = io.StringIO()
        writer = csv.DictWriter(out, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
        atomic_text(root / 'comparison.csv', out.getvalue())
    else:
        (Path(root) / 'comparison.csv').unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    dirs = discover(args.root)
    for directory in dirs:
        relative = directory.relative_to(args.root.resolve())
        output = args.output_dir / relative / "stats.log" if args.output_dir else None
        write_stats(directory, output=output)
    if not args.output_dir:
        write_comparison(args.root)
    print(f"Generated {len(dirs)} stats.log files; legacy summaries unchanged.")


if __name__ == "__main__":
    main()
