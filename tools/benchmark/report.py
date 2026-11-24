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
import re
from datetime import datetime
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


def table(title, columns, rows, row_titles=None):
    rows = [[display(x) for x in row] for row in rows]
    columns = ["title"] + list(columns)
    rows = [[label] + row for label, row in zip(row_titles or [title] * len(rows), rows)]
    widths = [max([len(col)] + [len(row[i]) for row in rows]) for i, col in enumerate(columns)]
    def line(row):
        return " | ".join(cell.ljust(w) for cell, w in zip(row, widths))
    return "\n".join([line(columns), "-+-".join("-" * w for w in widths)] +
                     [line(row) for row in rows] + [f"({len(rows)} rows)", ""])


def storage_device_io(directory, files):
    """Whole-device I/O between the first and last samples in the shared TPS window."""
    path = directory / "storage" / "disk_io.jsonl"
    if not path.is_file() or not files:
        return None
    windows = []
    for f in files:
        output = f.parent / "compute.out"
        if not output.is_file():
            return None
        times = [datetime.fromisoformat(t) for t in re.findall(
            r"^\[([0-9-]+ [0-9:.]+)\] \[Node \d+\] \[Commit TPS\]", output.read_text(errors="replace"), re.M)]
        if len(times) < 2:
            return None
        windows.append((times[0], times[-1]))
    begin, end = max(w[0] for w in windows), min(w[1] for w in windows)
    samples = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    samples = [s for s in samples if begin <= datetime.fromisoformat(s["time"]) <= end]
    if len(samples) < 2:
        return None
    first, last = samples[0], samples[-1]
    seconds = last["monotonic_s"] - first["monotonic_s"]
    reads, writes = last["reads"] - first["reads"], last["writes"] - first["writes"]
    if seconds <= 0 or reads < 0 or writes < 0 or any(s["device"] != first["device"] for s in samples):
        return None
    peak = max((b["reads"] + b["writes"] - a["reads"] - a["writes"]) /
               (b["monotonic_s"] - a["monotonic_s"]) for a, b in zip(samples, samples[1:]))
    io = {"device": first["device"], "seconds": seconds, "count": reads + writes,
          "read_iops": reads / seconds, "write_iops": writes / seconds,
          "avg_iops": (reads + writes) / seconds, "peak_iops": peak}
    for key in ("read_bytes", "write_bytes"):
        before, after = first.get(key), last.get(key)
        io[key] = after - before if number(before) and number(after) and after >= before else None
    return io


# Keys match the cumulative nanosecond counters written by compute and storage.
# A few existing counters predate the <operation>_count naming convention.
MVCC_COMPUTE_TIMINGS = {
    "tso_snapshot_rpc": "Begin: caller elapsed time to fetch snapshot S from remote_server",
    "tso_commit_rpc": "Commit: caller elapsed time to fetch writer timestamp C from remote_server",
    "commit_wal_wait": "home writer, stamped commit record enqueue through durable WAL acknowledgement",
    "tit_local_commit": "publish the durable commit decision in the writer home TIT",
    "tit_status_rpc": "remote creator status lookup at its home, including any pending wait",
    "tit_pending_wait": "home TIT wait for a creator whose decision affects the reader snapshot",
    "undo_begin": "open or reuse the transaction undo segment, including participant calls",
    "undo_admit": "check or reserve undo capacity for a write",
    "undo_append": "append a previous version, including page access and redo",
    "undo_redo_enqueue": "create and enqueue one undo-page redo record on the writer",
    "undo_page_apply": "apply one undo redo and dirty the writer's page",
    "undo_seal": "finish a segment, including finishes with no page to seal",
    "mvcc_visible": "resolve the visible row, including any history walks",
    "undo_history_step": "read and reconstruct one previous version during a visibility walk",
    "undo_read": "read an undo record for visibility or rollback",
    "undo_rollback_step": "undo one installed Lazy write, including row fetch, restore and update log enqueue",
    "undo_rollback_read": "rollback subset of undo_read",
    "undo_fetch": "obtain an undo page, including cache hits and permission waits",
    "undo_wal_wait": "wait for undo-page WAL on page handoff or eviction",
}
MVCC_STORAGE_TIMINGS = {
    "storage_undo_redo": "submit one undo redo to replay, including records applied inline",
    "storage_undo_validate": "validate an installing row update against its undo during replay",
    "storage_page_replay_wait": "page requests waiting for replay, across all page types",
}
UNDO_ACTIVITY = (
    ("undo_pages", "end gauge", "allocated undo pages at the end of the run"),
    ("undo_evictions", "counter", "undo pages evicted"),
    ("undo_permission_transfers", "counter", "undo page permission acquisitions or transfers"),
    ("undo_remote_fetches", "counter", "undo fetches requiring a permission request to another node"),
    ("undo_capacity_rejects", "counter", "writes rejected by undo admission"),
    ("undo_admitted_reservations", "end gauge", "admitted segments still awaiting their first undo page"),
    ("mvcc_history_steps", "counter", "previous versions traversed by snapshot reads"),
)
MVCC_COUNT_KEYS = {"mvcc_visible": "mvcc_visible_reads", "undo_wal_wait": "undo_wal_waits"}
MVCC_NS_KEYS = {"storage_page_replay_wait": "storage_page_replay_wait_time_ns"}


def mvcc_timing_report(cluster, storage, attempts):
    rows, overview = [], {}
    def add(name, calls, ns, meaning):
        per_attempt = divide(ns, attempts, 1e-3)
        overview[name + "_us_per_attempt"] = per_attempt
        per_call = divide(ns, calls, 1e-3)
        if name in ("tso_snapshot_rpc", "tso_commit_rpc"):
            overview[name + "_us_per_call"] = per_call
        rows.append((name, calls, divide(ns, 1e6), per_call, per_attempt, meaning))

    add("txn_begin", total(cluster, "stats_txn_completed"), total(cluster, "stats_txn_begin_ns"),
        "whole Begin phase for completed attempts")
    commit_counts = [total(cluster, f"stats_commit_{path}_count")
                     for path in ("read_only", "single_local", "single_remote", "multi_writer")]
    entered = sum(commit_counts) if all(number(n) for n in commit_counts) else None
    add("txn_commit", entered, total(cluster, "stats_txn_commit_ns"),
        "whole Commit phase for attempts that entered commit, including failed commits")
    for sources, metrics in ((cluster, MVCC_COMPUTE_TIMINGS), ([storage], MVCC_STORAGE_TIMINGS)):
        for name, meaning in metrics.items():
            calls = total(sources, MVCC_COUNT_KEYS.get(name, name + "_count"))
            ns = total(sources, MVCC_NS_KEYS.get(name, name + "_ns"))
            add(name, calls, ns, meaning)
    timing = table("mvcc_timing", ("operation", "calls", "total_ms", "us_per_call",
                                    "us_per_attempt", "scope"), rows)
    timing += ("Times include blocking. Nested operations and background work overlap. "
               "Per-attempt values divide cumulative work by all issued attempts, including aborts.")
    return timing, overview


def build_report(directory, metadata=None, *, on_mvcc_timing=None):
    directory = Path(directory)
    settings = read_kv(directory / "summary_human.txt")
    if (directory / "metadata.json").exists():
        settings.update(json.loads((directory / "metadata.json").read_text()))
    settings.update(metadata or {})
    files = sorted(directory.glob("node[0-9]*/result.txt"))
    nodes = [read_kv(f) for f in files]
    protocols = {node.get("mvcc_commit_protocol") for node in nodes}
    if len(protocols) == 1:
        settings["mvcc_commit_protocol"] = protocols.pop()
    # Earlier page-hold results used "closed" for completed holding periods.
    for node in nodes:
        for key in tuple(node):
            if key.startswith("stats_page_hold_") and "_closed_" in key:
                node.setdefault(key.replace("_closed_", "_completed_"), node[key])
    storage = read_kv(directory / "storage" / "storage_timing_stats.txt")
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
        "run_mode", "run_seconds", "prewarm_seconds", "mvcc_commit_protocol",
        "attempts_per_thread", "local_ratio", "zipf_theta", "tx_hot", "write_txn_ratio", "lock_mode",
        "generate_log", "parallel_page_fetch", "rpc_transport", "hybrid_skew_threshold", "hot_key_top_n", "code_revision",
        "source_fingerprint", "run_fingerprint")] ))
    sections.append(table("node_throughput", ("node", "run_mode", "measured_s", "drain_s", "committed_tps",
        "completed_tps", "window_commits", "final_commits"), [
        (f.parent.name, n.get("benchmark_run_mode"), n.get("benchmark_measurement_seconds"),
         n.get("benchmark_drain_seconds"), n.get("benchmark_committed_tps"), n.get("benchmark_completed_tps"),
         n.get("benchmark_window_commits"), n.get("benchmark_final_commits")) for f, n in zip(files, nodes)]))

    sections.append(table("outcome_latency", ("outcome", "count", "avg_ms"), [
        (s, total(cluster, "stats_txn_" + s), divide(total(cluster, "stats_txn_" + s + "_ns"),
                                                 total(cluster, "stats_txn_" + s), 1e-6))
        for s in ("committed", "aborted")]))

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

    timing, mvcc_overview = mvcc_timing_report(cluster, storage, attempts)
    sections.append(timing)
    sections.append(table("undo_activity", ("metric", "kind", "cluster", *(f.parent.name for f in files), "scope"), [
        (key, kind, total(cluster, key), *(node.get(key) for node in nodes), scope)
        for key, kind, scope in UNDO_ACTIVITY]))
    if on_mvcc_timing is not None:
        on_mvcc_timing(timing, mvcc_overview)

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
    if all("stats_commit_read_only_count" in n for n in cluster):
        # Parent/child totals partition the same measurement. Participant waits
        # and other diagnostics are separate roots because they overlap stages.
        details = []
        def detail(label, count, ns, meaning):
            details.append((label, count, divide(ns, 1e9), divide(ns, count, 1e-6), meaning))
        paths = ("read_only", "single_local", "single_remote", "multi_writer")
        counts = {p: total(cluster, "stats_commit_" + p + "_count") for p in paths}
        entered = sum(counts.values())
        single_count = counts["single_local"] + counts["single_remote"]
        detail("transaction_stages", None, None, "coordinator time; count = transactions")
        for stage, stem, last in (("execute", "execute_ns", False), ("commit", "total_ns", True)):
            values = {p: total(cluster, "stats_commit_" + p + "_" + stem) for p in paths}
            stage_total = total(cluster, "stats_txn_" + stage + "_ns")
            detail(("└─ " if last else "├─ ") + stage,
                   entered if last else measured, stage_total,
                   "entered commit" if last else "all completed attempts, including execution aborts")
            prefix = "   " if last else "│  "
            detail(prefix + "├─ read_only", counts["read_only"], values["read_only"], "0 write participants")
            detail(prefix + "├─ single_writer", single_count, values["single_local"] + values["single_remote"], "1 write participant")
            detail(prefix + "│  ├─ local", counts["single_local"], values["single_local"], "writer is home; includes Lazy writes")
            detail(prefix + "│  └─ remote", counts["single_remote"], values["single_remote"], "writer is remote")
            detail(prefix + ("└─ " if last else "├─ ") + "multi_writer", counts["multi_writer"], values["multi_writer"],
                   ">1 write participants")
            if last:
                parts = {p: total(cluster, "stats_commit_multi_writer_" + p + "_ns")
                         for p in ("prepare", "decision_log", "final_commit")}
                parts["other"] = values["multi_writer"] - sum(parts.values())
                for i, (part, ns) in enumerate(parts.items()):
                    detail(prefix + "   " + ("└─ " if i == 3 else "├─ ") + part,
                           counts["multi_writer"], ns, "per multi-writer transaction; sequential phase")
                    if part == "prepare":
                        samples = total(cluster, "stats_commit_multi_writer_prepare_sample_count")
                        if samples:
                            first = total(cluster, "stats_commit_multi_writer_prepare_first_ns")
                            tail = total(cluster, "stats_commit_multi_writer_prepare_tail_ns")
                            all_yes = total(cluster, "stats_commit_multi_writer_prepare_all_ns")
                            detail(prefix + "   │  ├─ first_yes", samples, first, "start of fanout to first durable YES callback")
                            detail(prefix + "   │  ├─ first_to_last_yes", samples, tail, "last callback minus first; not full 2PC premium")
                            detail(prefix + "   │  └─ setup_and_join", samples, ns - all_yes,
                                   "request construction and remaining coordinator work")
                            if first + tail != all_yes or samples != counts["multi_writer"] or ns < all_yes:
                                warnings.append("Prepare first/tail timing does not reconcile with the measured phase.")
                if sum(values.values()) != stage_total or parts["other"] < 0:
                    warnings.append("Commit path/phase timing does not reconcile with coordinator commit time.")
            else:
                detail(prefix + "└─ before_commit_abort", measured - entered,
                       stage_total - sum(values.values()), "attempts that never entered commit")
        if not all("stats_log_detail_commit_log_count" in n for n in cluster):
            wait_names = ("commit_log", "prepare_log", "decision_log", "page_log", "abort_log")
            wait_counts = [total(cluster, "stats_log_wait_" + p + "_count") for p in wait_names]
            wait_ns = [total(cluster, "stats_log_wait_" + p + "_ns") for p in wait_names]
            detail("log_waits", total(cluster, "stats_log_wait_total_count"), total(cluster, "stats_log_wait_total_ns"),
                   "count = synchronous wait calls across nodes; overlaps transaction stages")
            if sum(wait_counts) != total(cluster, "stats_log_wait_total_count") or sum(wait_ns) != total(cluster, "stats_log_wait_total_ns"):
                warnings.append("Log wait categories do not reconcile with the total wait counter.")
            for i, (name, count, ns) in enumerate(zip(wait_names, wait_counts, wait_ns)):
                detail(("└─ " if i == len(wait_names) - 1 else "├─ ") + name, count, ns,
                       "per wait call; includes participant-side calls")
        # These replace the old multi-site 2RTT/3RTT event counters. Read old files unchanged.
        page_paths = ("local_ready", "local_manager", "owner_manager", "forwarded_page",
                      "local_upgrade", "storage_page")
        page_phases = ("begin", "execute", "commit", "abort", "unscoped")
        if all("stats_lazy_page_execute_local_ready_count" in n for n in cluster):
            transfer_waits = all("stats_lazy_page_execute_local_ready_transfer_log_wait_ns" in n for n in cluster)
            legacy_release = any("stats_lazy_page_execute_wait_release_count" in n for n in cluster)
            if legacy_release:
                page_paths += ("wait_release",)
            page = {(ph, path, field): total(cluster, f"stats_lazy_page_{ph}_{path}_{field}")
                    for ph in page_phases for path in page_paths for field in ("count", "ns", "local_lock_ns")}
            if legacy_release:
                for ph in page_phases:
                    for field in ("count", "ns", "local_lock_ns"):
                        page[ph, "wait_release", field] = sum(n.get(f"stats_lazy_page_{ph}_wait_release_{field}", 0) for n in cluster)
            accesses = sum(page[ph, path, "count"] for ph in page_phases for path in page_paths)
            page_ns = sum(page[ph, path, "ns"] for ph in page_phases for path in page_paths)
            remote = sum(page[ph, path, "count"] for ph in page_phases for path in page_paths[1:])
            if remote != total(cluster, "ownership_transfer_count"):
                warnings.append("Lazy acquisition paths do not reconcile with ownership transfer count.")
            if accesses:
                detail("lazy_page_access", accesses, page_ns, "request-side calls; includes local lock; overlaps stages")
                used = [ph for ph in page_phases if sum(page[ph, path, "count"] for path in page_paths)]
                for j, ph in enumerate(used):
                    detail(("└─ " if j == len(used)-1 else "├─ ") + ph,
                           sum(page[ph, path, "count"] for path in page_paths),
                           sum(page[ph, path, "ns"] for path in page_paths), "same page calls grouped by transaction phase")
                detail("lazy_page_paths", accesses, page_ns, "same calls grouped by route; NOT extra time or measured RTTs")
                used = list(page_paths) if transfer_waits else [path for path in page_paths if sum(page[ph, path, "count"] for ph in page_phases)]
                for j, path in enumerate(used):
                    count = sum(page[ph, path, "count"] for ph in page_phases)
                    ns = sum(page[ph, path, "ns"] for ph in page_phases)
                    detail(("└─ " if j == len(used)-1 else "├─ ") + path,
                           count, ns, "one mutually exclusive path per acquisition")
                    if transfer_waits and path != "wait_release":
                        log_ns = sum(total(cluster, f"stats_lazy_page_{ph}_{path}_transfer_log_wait_ns") for ph in page_phases)
                        log_counts = [total(cluster, f"stats_lazy_page_{ph}_{path}_transfer_log_wait_count") for ph in page_phases]
                        log_count = sum(log_counts) if all(number(v) for v in log_counts) else None
                        details.append((("   " if j == len(used)-1 else "│  ") + "└─ page_transfer_log_wait",
                                        log_count, divide(log_ns, 1e9), divide(log_ns, count, 1e-6),
                                        "count = actual waits; avg_ms per parent Fetch; included in parent"))
                        for n in cluster:
                            for ph in page_phases:
                                prefix = f"stats_lazy_page_{ph}_{path}_"
                                if not 0 <= n[prefix + "transfer_log_wait_ns"] <= n[prefix + "ns"]:
                                    warnings.append("Page transfer log wait exceeds its acquisition time.")
                if legacy_release:
                    detail("legacy_page_paths", None, None,
                           "wait_release is whole acquisition time; archived data cannot recover routes/log waits")
                detail("lazy_local_lock_call", accesses,
                       sum(page[ph, path, "local_lock_ns"] for ph in page_phases for path in page_paths),
                       "included in page access; includes uncontended lock overhead")
        if all("stats_remote_fetch_success_count" in n for n in cluster):
            for outcome in ("success", "conflict"):
                count = total(cluster, f"stats_remote_fetch_{outcome}_count")
                if count:
                    detail("remote_fetch_" + outcome, count, total(cluster, f"stats_remote_fetch_{outcome}_ns"),
                           "per RPC, including reply handling; execute diagnostic")
        writer_counts = sorted({int(k.split("_")[2]) for n in cluster for k in n
                                if k.startswith("stats_writer_") and k.endswith("_count") and "prepare" not in k})
        if writer_counts:
            detail("commit_by_writer_count", entered, total(cluster, "stats_txn_commit_ns"), "same commit time grouped by writer count")
            for j, writers in enumerate(writer_counts):
                count = sum(n.get(f"stats_writer_{writers}_count", 0) for n in cluster)
                ns = sum(n.get(f"stats_writer_{writers}_total_ns", 0) for n in cluster)
                detail(("└─ " if j == len(writer_counts)-1 else "├─ ") + str(writers) + "_writers", count, ns,
                       "actual commit writers; Lazy concentrates writes at home")
        if storage.get("storage_prepare_complete_count", 0):
            detail("storage_first_to_last_yes", storage["storage_prepare_complete_count"],
                   storage.get("storage_prepare_vote_tail_ns"), "Cornus-opt durable-vote observation gap; not coordinator elapsed time")
        detail("overlapping_diagnostics", None, None, "not additive; count = issued attempts")
        for i, (label, key, parent) in enumerate(slices[:5]):
            seconds = total(cluster, key)
            detail(("└─ " if i == 4 else "├─ ") + label, attempts,
                   seconds * 1e9 if number(seconds) else None, parent)
        sections.append(table("time_details", ("slice", "count", "total_s", "avg_ms", "meaning"), details))
    else:
        # Historical results do not contain path times or wait-call counts.
        sections.append(table("time_details", ("slice", "total_s", "ms_per_issued_attempt", "included_in"), [
            (label, total(cluster, key), divide(total(cluster, key), attempts, 1000), parent)
            for label, key, parent in slices]))
    disk = (storage_device_io(directory, files) if complete else None) or {}
    sample_s = disk.get("seconds")
    sections.append(table("storage_device_bytes", ("device", "sample_s", "read_bytes", "write_bytes",
        "read_MiB/s", "write_MiB/s", "window"), [[disk.get("device"), sample_s,
        disk.get("read_bytes"), disk.get("write_bytes"),
        divide(disk.get("read_bytes"), sample_s, 1 / 2**20), divide(disk.get("write_bytes"), sample_s, 1 / 2**20),
        "whole device; intersection of compute nodes' first/last TPS samples; may include transaction drain"]]))
    if all("stats_log_detail_commit_log_count" in n for n in cluster):
        log_rows = []
        if disk:
            log_rows.append(("storage_device_io", disk["count"], disk["seconds"], None,
                f"{disk['device']}; avg_iops={disk['avg_iops']:.1f} (read={disk['read_iops']:.1f}, write={disk['write_iops']:.1f}); "
                f"peak_1s_iops={disk['peak_iops']:.1f}; total_s=sample window; whole device, not commit-only/cloud quota units"))
        else:
            log_rows.append(("storage_device_io", None, None, None,
                "IOPS unavailable: no device samples in a common interior TPS window"))
        def log_detail(label, count, ns, meaning):
            log_rows.append((label, count, divide(ns, 1e9), divide(ns, count, 1e-6), meaning))
        for reason in ("commit_log", "prepare_log", "decision_log", "abort_log"):
            prefix = "stats_log_detail_" + reason + "_"
            count = total(cluster, prefix + "count")
            values = {k: total(cluster, prefix + k + "_ns")
                      for k in ("queue_to_rpc", "rpc", "resume", "storage", "write", "fdatasync")}
            elapsed = values["queue_to_rpc"] + values["rpc"] + values["resume"]
            log_detail(reason, count, elapsed, "enqueue to waiter return; includes participant waits")
            if count != total(cluster, "stats_log_wait_" + reason + "_count"):
                warnings.append("Log timing samples do not cover synchronous " + reason + " waits.")
            if not count:
                continue
            log_detail("├─ queue_to_rpc", count, values["queue_to_rpc"], "enqueue to RPC start; previous batch, batching and serialization")
            log_detail("├─ rpc", count, values["rpc"], "same waiters, their own batch RPC; includes storage below")
            log_detail("│  ├─ storage", count, values["storage"], "handler processing of the same batch; weighted per waiter")
            log_detail("│  │  ├─ write", count, values["write"], "append syscall time")
            log_detail("│  │  ├─ fdatasync", count, values["fdatasync"], "sync syscall time, including device wait")
            storage_other = values["storage"] - values["write"] - values["fdatasync"]
            rpc_other = values["rpc"] - values["storage"]
            log_detail("│  │  └─ other", count, storage_other, "remaining storage processing, including locks and durable notifications")
            log_detail("│  └─ transport_and_rpc", count, rpc_other, "RPC minus handler time; transport/framework/scheduling, not pure RTT")
            log_detail("└─ resume", count, values["resume"], "RPC return to waiter return; watermark publication and wakeup")
            if storage_other < 0 or rpc_other < 0:
                warnings.append("Log storage/RPC timing does not reconcile for " + reason + ".")
        sections.append(table("log_time_details", ("slice", "count", "total_s", "avg_ms", "meaning"), log_rows,
            row_titles=["IOPS"] + ["log_time_details"] * (len(log_rows) - 1)))
    sections.append(table("page_access_stats", ("operation", "count", "per_issued_attempt"), [
        (label, total(cluster, count), divide(total(cluster, count), attempts))
        for label, count in (
            ("local_fetch", "fetch_from_local_count"), ("remote_page_fetch", "fetch_from_remote_count"),
            ("storage_data_page_fetch", "fetch_from_storage_count"),
            ("ownership_transfer", "ownership_transfer_count"),
            ("remote_2pc_fetch", "twopc_remote_fetch_count"),
            ("eviction", "evicted_pages_count"))]))
    sections.append(table("lock_wait_stats", ("scope", "waits", "total_s", "ms_per_issued_attempt",
        "suspended_p50_ms", "suspended_p99_ms", "max_queue"), [
        ["cluster", total(cluster, "lock_wait_total"), total(cluster, "lock_wait_time"),
         divide(total(cluster, "lock_wait_time"), attempts, 1000), None, None,
         max((n.get("lock_wait_wq_depth_max", 0) for n in nodes), default=0) if all("lock_wait_wq_depth_max" in n for n in cluster) else None]] + [
        [f.parent.name, n.get("lock_wait_total"), n.get("lock_wait_time"),
         divide(n.get("lock_wait_time"), txn_counts(n)[0], 1000), divide(n.get("lock_wait_us_p50"), 1000),
         divide(n.get("lock_wait_us_p99"), 1000), n.get("lock_wait_wq_depth_max")]
        for f, n in zip(files, nodes)]))

    if any("stats_page_hold_completed_count" in n for n in nodes):
        if any(n.get("page_cost_scope") == "model_history_including_prewarm" for n in nodes):
            sections.append("page_hold_stats / page_cost.tsv / page_hold.tsv: model history includes prewarm; performance statistics exclude prewarm.")
        completed = total(cluster, "stats_page_hold_completed_count")
        completed_accesses = total(cluster, "stats_page_hold_completed_accesses")
        active = total(cluster, "stats_page_hold_active_count")
        active_accesses = total(cluster, "stats_page_hold_active_accesses")
        observed = completed + active if number(completed) and number(active) else None
        accesses = completed_accesses + active_accesses if number(completed_accesses) and number(active_accesses) else None
        sections.append(table("page_hold_stats", ("period", "holds", "accesses", "accesses_per_hold", "meaning"), [
            ("completed", completed, completed_accesses, divide(completed_accesses, completed), "ended by permission loss/eviction"),
            ("active", active, active_accesses, divide(active_accesses, active), "still held; accesses observed so far"),
            ("all_observed", observed, accesses, divide(accesses, observed), "includes unfinished holds; not completed lifetime mean"),
        ]))
        # Preserve the interpretation of archived runs that included local cleanup.
        legacy_holds = any("stats_page_hold_cached_visits" in n for n in nodes)
        phases = ("begin", "execute", "commit", "abort", "unscoped") if legacy_holds else ("begin", "execute", "abort", "unscoped")
        page_calls = sum(total(cluster, f"stats_lazy_page_{phase}_{path}_count") or 0
                         for phase in phases
                         for path in ("local_ready", "local_manager", "owner_manager", "forwarded_page",
                                      "local_upgrade", "storage_page", "wait_release")) if complete else None
        cached = total(cluster, "stats_page_hold_cached_visits") if legacy_holds else 0
        if legacy_holds:
            sections.append("page_hold_stats: legacy run; includes commit fetches and local try-visits.")
        # Holds retain model history; measured fetch counters were reset after prewarm.
        if number(page_calls):
            for f, n in zip(files, nodes):
                if (n.get("page_cost_scope") == "model_history_including_prewarm"
                        and n.get("benchmark_prewarm_seconds", 0) > 0):
                    baseline = f.parent / "page_cost_prewarm.tsv"
                    if not baseline.is_file():
                        warnings.append("Page hold reconciliation needs page_cost_prewarm.tsv.")
                        page_calls = None
                        break
                    with baseline.open() as source:
                        page_calls += sum(int(r["count"]) for r in csv.DictReader(source, delimiter="\t")
                                          if r["operation"] in ("lazy_local", "lazy_acquire"))
        if number(accesses) and number(page_calls) and number(cached) and accesses != page_calls + cached:
            warnings.append("Page hold accesses do not match the recorded fetch scope.")
    sections.append(table("commit_path_stats", ("counter", "count", "meaning"), [
        ("single_writer_fast_path", total(cluster, "single_txn_count"), "2pc path, <=1 write participant"),
        ("multi_writer_2pc", total(cluster, "distribute_txn_count"), "2pc path, >1 write participants"),
        ("mix_2pc_commit_calls", total(cluster, "hybrid_2pc_commit_count"), "includes single-writer fast path"),
        ("mix_lazy_commit_calls", total(cluster, "hybrid_lazy_commit_count"), "commit path entered; excludes execution aborts")]))
    rows = []
    for path in PATHS:
        prefix = f"stats_path_{path}_"
        count = total(cluster, prefix + "completed")
        if count == 0:
            continue
        mode = settings.get('system_name')
        execution_mode = '2pc' if mode in ('2pc-cornus', '2pc-cornus-opt') else mode
        relevant = ('mix_lazy', 'mix_2pc') if mode == 'mix' else (execution_mode,)
        if count is None and mode in PATHS + ('mix',) and path not in relevant:
            continue
        path_total = total(cluster, prefix + 'total_ns')
        path_stages = [total(cluster, prefix + s + '_ns') for s in STAGES]
        path_other = path_total - sum(path_stages) if number(path_total) and all(number(v) for v in path_stages) else None
        label = mode if path == '2pc' and execution_mode == '2pc' else path
        rows.append([label, count, total(cluster, prefix + "committed"), total(cluster, prefix + "aborted"),
                     divide(path_total, count, 1e-6)] +
                    [divide(v, count, 1e-6) for v in path_stages + [path_other]])
    sections.append(table("execution_path_timing", ("path", "completed", "committed", "aborted",
        "total_ms", "begin_ms", "execute_ms", "commit_ms", "abort_ms", "other_ms"), rows or [["none", 0, 0, 0] + [None]*6]))

    # Optional additive section; never reinterpret old result counters.
    if any("hybrid_cost_decisions" in n for n in nodes):
        decisions = total(cluster, "hybrid_cost_decisions")
        sections.append(table("hybrid_cost_model", ("decisions", "selected_2pc", "selected_lazy",
            "fallback_txns", "page_samples", "page_history_hits", "page_history_misses",
            "predicted_lazy_ms", "predicted_2pc_access_ms", "decision_overhead_ms"), [[
            decisions, total(cluster, "hybrid_cost_2pc"), total(cluster, "hybrid_cost_lazy"),
            total(cluster, "hybrid_cost_fallback"), total(cluster, "hybrid_cost_page_samples"),
            total(cluster, "hybrid_cost_page_history_hits"), total(cluster, "hybrid_cost_page_history_misses"),
            divide(total(cluster, "hybrid_cost_predicted_lazy_total_ms"), decisions),
            divide(total(cluster, "hybrid_cost_predicted_2pc_access_total_ms"), decisions),
            divide(total(cluster, "hybrid_cost_decision_total_ms"), decisions)]]))
        sections.append(table("hybrid_cost_settings", ("node", "active", "configured_r_ms", "configured_f_ms",
            "fixed_commit_delta_ms", "include_remote_fetch", "include_2pc_execution_cost"), [
            [f.parent.name] + [n.get(k) for k in ("hybrid_cost_active", "hybrid_cost_rpc_rtt_ms",
             "hybrid_cost_log_wait_ms", "hybrid_cost_commit_delta_ms", "hybrid_cost_include_remote_fetch",
             "hybrid_cost_include_2pc_execution_cost")]
            for f, n in zip(files, nodes)]))
    if any("hybrid_online_enabled" in n for n in nodes):
        sections.append(table("hybrid_online_costs", ("node", "active", "f_ready", "f_ms",
            "r_ready", "r_ms", "log_samples", "rpc_samples", "log_updates", "rpc_updates",
            "cold_or_stale_fallback", "used_commit_delta_ms", "freeze_after_ms"), [
            [f.parent.name] + [n.get(k) for k in ("hybrid_online_active", "hybrid_online_f_ready",
                "hybrid_online_f_ms", "hybrid_online_r_ready", "hybrid_online_r_ms",
                "hybrid_online_log_samples", "hybrid_online_rpc_samples", "hybrid_online_log_updates",
                "hybrid_online_rpc_updates", "hybrid_online_unavailable")]
            + [divide(n.get("hybrid_cost_predicted_commit_delta_total_ms"), n.get("hybrid_cost_decisions")),
               n.get("hybrid_online_freeze_after_ms")]
            for f, n in zip(files, nodes)]))
        sections.append(table("hybrid_online_log_phases", ("node", "commit_samples", "commit_f_ms",
            "prepare_samples", "prepare_f_ms", "backup_samples", "backup_f_ms"), [
            [f.parent.name] + [n.get("hybrid_online_" + phase + suffix)
                for phase in ("commit", "prepare", "backup") for suffix in ("_samples", "_f_ms")]
            for f, n in zip(files, nodes)]))
    sections.append(table("compute_log_stats", ("flush_batches", "records_in_batches", "records_per_batch",
        "rpc_total_s", "rpc_avg_ms", "flush_wait_total_s"), [[total(cluster, "log_flush_count"),
        total(cluster, "log_flush_total_batch"), divide(total(cluster, "log_flush_total_batch"), total(cluster, "log_flush_count")),
        total(cluster, "log_flush_storage_rpc_time"), divide(total(cluster, "log_flush_storage_rpc_time"), total(cluster, "log_flush_count"), 1000),
        total(cluster, "wait_log_flush_time")]]))
    if "storage_page_read_io" in storage:
        columns = ["io", "read_calls", "read_MiB", "read_total_s", "read_avg_ms"]
        values = [storage.get("storage_page_read_io"), storage.get("storage_page_read_count"),
            divide(storage.get("storage_page_read_bytes"), 1048576),
            divide(storage.get("storage_page_read_time_ns"), 1e9),
            divide(storage.get("storage_page_read_time_ns"), storage.get("storage_page_read_count"), 1e-6)]
        if "storage_page_replay_wait_count" in storage:
            columns += ["replay_waits", "replay_wait_s"]
            values += [storage["storage_page_replay_wait_count"],
                divide(storage.get("storage_page_replay_wait_time_ns"), 1e9)]
        else:
            columns += ["lsn_retries"]
            values += [storage.get("storage_page_read_lsn_retries")]
        sections.append(table("storage_page_reads", columns, [values]))
    storage_log_columns = ["logwrite_calls", "fdatasync_calls", "write_total_s",
        "sync_total_s", "avg_sync_ms", "logwrite_calls_per_sync"]
    storage_log_values = [
        storage.get("storage_logwrite_count"), storage.get("storage_log_fdatasync_count"),
        storage.get("storage_log_write_time_sec"), storage.get("storage_log_fdatasync_time_sec"),
        divide(storage.get("storage_log_fdatasync_time_sec"), storage.get("storage_log_fdatasync_count"), 1000),
        storage.get("storage_log_avg_logwrite_per_fdatasync") if storage.get("storage_log_fdatasync_count", 0) > 0 else None]
    if "storage_wal_initialized_segments" in storage:
        storage_log_columns += ["initialized_segments", "init_total_s"]
        storage_log_values += [storage["storage_wal_initialized_segments"],
            divide(storage.get("storage_wal_initialization_ns"), 1e9)]
    sections.append(table("storage_log_stats", storage_log_columns, [storage_log_values]))
    if not storage:
        warnings.append("Storage statistics unavailable (not collected or shutdown did not produce a fresh snapshot).")
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
                "run_mode", "run_seconds", "prewarm_seconds", "generate_log", "thread_num", "node_count", "local_ratio", "write_txn_ratio", "zipf_theta")}
    overview.update(sum_node_window_tps=total(cluster, "benchmark_committed_tps"), attempts=attempts, commits=commits, abort_pct=divide(aborts, attempts, 100),
                    legacy_sum_worker_tps=total(cluster, "throughput"),
                    avg_completed_attempt_ms=divide(total(cluster, "stats_txn_total_ns"), measured, 1e-6),
                    execute_ms_per_attempt=divide(total(cluster, "stats_txn_execute_ns"), measured, 1e-6),
                    commit_ms_per_attempt=divide(total(cluster, "stats_txn_commit_ns"), measured, 1e-6),
                    prepare_ms_per_attempt=divide(total(cluster, "tx_write_prepare_log_time"), attempts, 1000),
                    backup_ms_per_attempt=divide(total(cluster, "tx_write_backup_log_time"), attempts, 1000),
                    distributed_commit_ms_per_attempt=divide(total(cluster, "tx_write_commit_log_time2"), attempts, 1000),
                    lock_wait_ms_per_attempt=divide(total(cluster, "lock_wait_time"), attempts, 1000),
                    source=str(directory))
    overview.update(mvcc_overview)
    return "\n\n".join(sections) + "\n" + "\n".join(notes) + "\n", overview


def write_stats(directory, metadata=None, output=None, *, on_mvcc_timing=None):
    report, overview = build_report(directory, metadata, on_mvcc_timing=on_mvcc_timing)
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
        runtime_log = directory / f'node{idx}/compute.out'
        if not runtime_log.is_file() or not runtime_log.stat().st_size:
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
        if node.get('benchmark_prewarm_seconds', 0) != case.get('prewarm_seconds', 0):
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
        saved_path = root / 'config.json'
        if not saved_path.is_file():
            saved_path = root / 'run.json'
        saved = json.loads(saved_path.read_text())
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
