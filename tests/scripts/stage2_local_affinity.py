#!/usr/bin/env python3
import os
import signal
import shutil
import subprocess
import sys
import time
import json
from pathlib import Path


ROOT = Path("/home/mingtai/ClionProject/Hybrid_Cloud_MP")
BUILD = ROOT / "build"
STORAGE_DIR = BUILD / "storage_server"
REMOTE_DIR = BUILD / "remote_server"
COMPUTE_DIR = BUILD / "compute_server"
STORAGE_BIN = STORAGE_DIR / "storage_pool"
REMOTE_BIN = REMOTE_DIR / "remote_node"
COMPUTE_BIN = COMPUTE_DIR / "compute_server"

LOG_STORAGE = ROOT / "build" / "storage_server" / "stage2_storage.log"
LOG_REMOTE = ROOT / "build" / "remote_server" / "stage2_remote.log"
STORAGE_TIMING_STATS = STORAGE_DIR / "storage_timing_stats.txt"
LOG_C0 = Path("/tmp/stage2_compute0.log")
LOG_C1 = Path("/tmp/stage2_compute1.log")
PID_DIR = Path("/tmp/stage2_local_affinity")
PID_STORAGE = PID_DIR / "storage.pid"
PID_REMOTE = PID_DIR / "remote.pid"
PID_C0 = PID_DIR / "compute0.pid"
PID_C1 = PID_DIR / "compute1.pid"
WORKLOAD = os.environ.get("STAGE2_WORKLOAD", "ycsb")
WORKLOAD_CONFIG = ROOT / "config" / f"{WORKLOAD}_config.json"

STARTED = []

ATTEMPTED_NUM = int(os.environ.get("STAGE2_ATTEMPTED_NUM", "2000"))
THREADS = os.environ.get("STAGE2_THREADS", "2")

# Hard ceiling on compute_server wallclock. Workers exit only when they hit
# ATTEMPTED_NUM (see worker.cc), and under affinity-on the migration loop
# steals enough CPU / locks that the target is reachable but slowly. If the
# ceiling is hit we SIGTERM and synthesize a sentinel result.txt from the
# affinity CSVs so stage4_local_compare can still produce a summary instead
# of hanging inside subprocess.run.
WAIT_TIMEOUT_S = float(os.environ.get("STAGE2_WAIT_TIMEOUT_S", "900"))

RESULT_NODE_PATHS = [
    COMPUTE_DIR / "result.node0.txt",
    COMPUTE_DIR / "result.node1.txt",
]

NODE_SUM_KEYS = {
    "throughput",
    "fetch_all_count",
    "lock_remote_count",
    "fetch_from_remote_count",
    "fetch_from_storage_count",
    "fetch_from_local_count",
    "evicted_pages_count",
    "wait_log_flush_count",
    "ownership_transfer_count",
    "ownership_transfer_time_total",
    "notify_push_page_count",
    "notify_push_page_time",
    "wait_log_flush_time",
    "wait_log_flush_push_page_time",
    "wait_log_flush_evict_page_time",
    "wait_log_flush_tx_over_time",
    "log_flush_count",
    "log_flush_time",
    "log_flush_to_lock_done_time",
    "log_flush_to_max_lsn_time",
    "log_flush_to_serialize_done_time",
    "log_flush_storage_rpc_time",
    "log_flush_update_persist_lsn_time",
    "log_flush_total_batch",
    "commit_log_count",
    "prepare_log_count",
    "backup_log_count",
    "update_log_count",
    "lazy_getpage_dire",
    "lazy_getpage_wait",
    "lazy_2RTT_count",
    "lazy_3RTT_count",
    "tx_begin_time",
    "tx_exe_time",
    "tx_fetch_exe_time",
    "tx_commit_time",
    "tx_abort_time",
    "TxWaitAbortLogTime",
    "wait_commit_log_time",
    "wait_prepare_log_time",
    "wait_backup_log_time",
    "tx_write_commit_log_time",
    "tx_write_commit_log_time2",
    "tx_write_prepare_log_time",
    "tx_write_backup_log_time",
    "tx_get_timestamp_time1",
    "tx_get_timestamp_time2",
    "twopc_remote_fetch_time",
    "twopc_remote_fetch_count",
    "fetch_storage_page_time",
    "single_txn_count",
    "distribute_txn_count",
    "affinity_samples_pushed",
    "affinity_samples_dropped",
    "affinity_samples_consumed",
    "affinity_graph_vertices",
    "affinity_graph_edges",
    "affinity_graph_node_access_vertices",
    "affinity_edges_pruned_min_weight",
    "affinity_last_partition_owned_vertices",
    "affinity_last_partition_changed_vertices",
    "affinity_last_assignment_size",
    "affinity_partition_total_ms",
    "affinity_migrations_planned",
    "affinity_migrations_done",
    "affinity_migrations_failed",
}

NODE_MAX_KEYS = {
    "total_time_seconds",
    "log_flush_max_batch",
    "affinity_enabled",
    "affinity_last_edgecut",
    "affinity_partition_runs",
    "affinity_partition_skipped",
}


def assert_executable(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"missing binary: {path}")
    if path.stat().st_size == 0:
        raise RuntimeError(f"binary is empty: {path}")
    if not os.access(path, os.X_OK):
        raise PermissionError(f"binary is not executable: {path}")


def load_json(path: Path):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def run(cmd, cwd=None, check=True):
    return subprocess.run(cmd, cwd=cwd, check=check, text=True, capture_output=True)


def read_pid(pidfile: Path):
    try:
        return int(pidfile.read_text().strip())
    except (FileNotFoundError, ValueError):
        return None


def kill_pid(pid: int | None) -> None:
    if not pid:
        return
    try:
        os.killpg(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    except PermissionError:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            return


def cleanup() -> None:
    for pidfile in [PID_C0, PID_C1, PID_REMOTE, PID_STORAGE]:
        kill_pid(read_pid(pidfile))
    time.sleep(1)
    for pidfile in [PID_C0, PID_C1, PID_REMOTE, PID_STORAGE]:
        try:
            pidfile.unlink()
        except FileNotFoundError:
            pass


def wait_for_listen(port: int, timeout_s: float = 30.0) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        proc = subprocess.run(
            ["ss", "-ltn"],
            check=False,
            text=True,
            capture_output=True,
        )
        if f":{port} " in proc.stdout:
            return True
        time.sleep(0.2)
    return False


def wait_for_path(path: Path, timeout_s: float = 30.0) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if path.exists():
            return True
        time.sleep(0.2)
    return False


def start_logged(cmd, cwd: Path, log_path: Path, pidfile: Path, name: str, env=None) -> subprocess.Popen:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    pidfile.parent.mkdir(parents=True, exist_ok=True)
    fh = open(log_path, "wb")
    shell_cmd = "exec " + " ".join(subprocess.list2cmdline([part]) for part in cmd)
    proc = subprocess.Popen(
        ["/bin/bash", "-lc", shell_cmd],
        cwd=str(cwd),
        stdout=fh,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        env=env,
    )
    pidfile.write_text(str(proc.pid))
    STARTED.append((name, proc, fh, log_path))
    return proc


def tail(path: Path, lines: int = 80) -> str:
    if not path.exists():
        return f"(missing) {path}"
    proc = subprocess.run(
        ["tail", "-n", str(lines), str(path)],
        check=False,
        text=True,
        capture_output=True,
    )
    return proc.stdout


def read_result_kv(path: Path):
    data = {}
    order = []
    if not path.exists():
        return data, order
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or "=" not in line:
                continue
            key, raw_value = line.split("=", 1)
            key = key.strip()
            raw_value = raw_value.strip()
            try:
                parts = raw_value.split()
                value = float(parts[0]) if len(parts) == 1 else [float(x) for x in parts]
            except (ValueError, IndexError):
                continue
            data[key] = value
            order.append(key)
    return data, order


def ordered_union(key_lists):
    seen = set()
    ordered = []
    for keys in key_lists:
        for key in keys:
            if key not in seen:
                seen.add(key)
                ordered.append(key)
    return ordered


def is_sum_key(key: str) -> bool:
    return (
        key in NODE_SUM_KEYS
        or key.endswith("_try_commit")
        or (key.endswith("_count") and not key.endswith("_avg_count"))
    )


def set_derived_metric(result, order, key, value):
    result[key] = value
    if key not in order:
        order.append(key)


def recompute_derived_metrics(result, order):
    remote = float(result.get("fetch_from_remote_count", 0.0) or 0.0)
    storage = float(result.get("fetch_from_storage_count", 0.0) or 0.0)
    local = float(result.get("fetch_from_local_count", 0.0) or 0.0)
    total_fetch = remote + storage + local
    if total_fetch > 0:
        set_derived_metric(result, order, "from_remote_ratio", remote / total_fetch)
        set_derived_metric(result, order, "from_storage_ratio", storage / total_fetch)
        set_derived_metric(result, order, "from_local_ratio", local / total_fetch)

    fetch_all = float(result.get("fetch_all_count", 0.0) or 0.0)
    lock_remote = float(result.get("lock_remote_count", 0.0) or 0.0)
    if fetch_all > 0:
        set_derived_metric(result, order, "lock_ratio", lock_remote / fetch_all)

    owner_count = float(result.get("ownership_transfer_count", 0.0) or 0.0)
    owner_time = float(result.get("ownership_transfer_time_total", 0.0) or 0.0)
    if owner_count > 0:
        set_derived_metric(result, order, "ownership_transfer_time_avg_ms", owner_time * 1000.0 / owner_count)

    log_flush_count = float(result.get("log_flush_count", 0.0) or 0.0)
    log_flush_total_batch = float(result.get("log_flush_total_batch", 0.0) or 0.0)
    if log_flush_count > 0:
        set_derived_metric(result, order, "log_flush_avg_batch", log_flush_total_batch / log_flush_count)

    for key, value in list(result.items()):
        if not key.endswith("_try_commit") or not isinstance(value, list) or len(value) < 2:
            continue
        attempted, committed = float(value[0]), float(value[1])
        rollback_rate = ((attempted - committed) / attempted) if attempted > 0 else 0.0
        prefix = key[: -len("_try_commit")]
        set_derived_metric(result, order, f"{prefix}_rollback_rate", rollback_rate)
        if prefix == "ycsb_tx":
            set_derived_metric(result, order, "ycsb_tx0_rollback_rate", rollback_rate)


def aggregate_node_results(paths, out_path: Path) -> bool:
    dicts = []
    orders = []
    missing = []
    for path in paths:
        data, order = read_result_kv(path)
        if not data:
            missing.append(str(path))
            continue
        dicts.append(data)
        orders.append(order)
    if missing:
        print("[stage2] missing per-node result(s): " + ", ".join(missing))
        return False

    order = ordered_union(orders)
    result = {}
    for key in order:
        vals = [d[key] for d in dicts if key in d]
        if not vals:
            continue
        if isinstance(vals[0], list):
            val_len = len(vals[0])
            combined = [0.0] * val_len
            for v_list in vals:
                if isinstance(v_list, list) and len(v_list) == val_len:
                    for idx, value in enumerate(v_list):
                        combined[idx] += value
            result[key] = combined
            continue
        nums = [float(v) for v in vals]
        if key in NODE_MAX_KEYS:
            result[key] = max(nums)
        elif is_sum_key(key):
            result[key] = sum(nums)
        else:
            result[key] = sum(nums) / len(nums)

    recompute_derived_metrics(result, order)
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write("aggregation_scope=cluster\n")
        fh.write(f"aggregation_node_count={len(paths)}\n")
        for key in order:
            if key not in result:
                continue
            value = result[key]
            if isinstance(value, list):
                fh.write(f"{key}=" + " ".join(str(x) for x in value) + "\n")
            else:
                fh.write(f"{key}={value}\n")
    return True


def proc_state(proc: subprocess.Popen) -> str:
    rc = proc.poll()
    return "running" if rc is None else f"exit={rc}"


def wait_for_exit(proc: subprocess.Popen, timeout_s: float) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if proc.poll() is not None:
            return True
        time.sleep(0.5)
    return proc.poll() is not None


def wait_for_exit_until(proc: subprocess.Popen, deadline: float) -> bool:
    while time.time() < deadline:
        if proc.poll() is not None:
            return True
        time.sleep(0.5)
    return proc.poll() is not None


def force_terminate(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=10)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass


def scrape_affinity_csv(paths):
    """Aggregate the per-node affinity CSVs into the counters stage4 reads.

    Returns: dict with partition_runs / migrations_{planned,done,failed}
    (summed over per-second delta columns and across nodes), and the final
    from_remote_ratio + edgecut (averaged across nodes' last rows).
    Missing / malformed inputs fall back to zeros — always returns a full dict.
    """
    partition_runs = 0
    planned = 0
    done = 0
    failed = 0
    from_remote_ratios = []
    total_remote_delta = 0
    total_storage_delta = 0
    total_local_delta = 0
    edgecuts = []
    for p in paths:
        if not p.exists():
            continue
        try:
            rows = p.read_text(encoding="utf-8").strip().splitlines()
        except OSError:
            continue
        if len(rows) < 2:
            continue
        header = rows[0].split(",")
        idx = {name: i for i, name in enumerate(header)}
        last_cols = None
        for row in rows[1:]:
            cols = row.split(",")
            if len(cols) != len(header):
                continue
            last_cols = cols
            for key, bucket in (
                ("migrations_planned_delta", "planned"),
                ("migrations_done_delta", "done"),
                ("migrations_failed_delta", "failed"),
            ):
                if key not in idx:
                    continue
                try:
                    v = int(cols[idx[key]])
                except ValueError:
                    continue
                if bucket == "planned":
                    planned += v
                elif bucket == "done":
                    done += v
                elif bucket == "failed":
                    failed += v
            for key, bucket in (
                ("from_remote_delta", "remote"),
                ("from_storage_delta", "storage"),
                ("from_local_delta", "local"),
            ):
                if key not in idx:
                    continue
                try:
                    v = int(cols[idx[key]])
                except ValueError:
                    continue
                if bucket == "remote":
                    total_remote_delta += v
                elif bucket == "storage":
                    total_storage_delta += v
                elif bucket == "local":
                    total_local_delta += v
        if last_cols is not None:
            if "partition_runs" in idx:
                try:
                    partition_runs = max(partition_runs, int(last_cols[idx["partition_runs"]]))
                except ValueError:
                    pass
            if "from_remote_ratio" in idx:
                try:
                    from_remote_ratios.append(float(last_cols[idx["from_remote_ratio"]]))
                except ValueError:
                    pass
            if "edgecut" in idx:
                try:
                    edgecuts.append(int(last_cols[idx["edgecut"]]))
                except ValueError:
                    pass
    avg = lambda xs: (sum(xs) / len(xs)) if xs else 0.0
    total_fetch_delta = total_remote_delta + total_storage_delta + total_local_delta
    weighted_remote_ratio = (
        total_remote_delta / total_fetch_delta
        if total_fetch_delta > 0 else avg(from_remote_ratios)
    )

    return {
        "partition_runs": partition_runs,
        "migrations_planned": planned,
        "migrations_done": done,
        "migrations_failed": failed,
        "from_remote_ratio": weighted_remote_ratio,
        "last_edgecut": max(edgecuts) if edgecuts else 0,
    }


def synthesize_sentinel_result(path: Path, csv_paths, *, reason: str, wait_timeout_s: float) -> None:
    """Write a minimal result.txt so stage4_local_compare.parse_result() still
    finds the keys it needs (throughput / from_remote_ratio / affinity_*).
    status=<reason> marks this as an incomplete run; the summary will read 0
    throughput but real migration counters scraped from the affinity CSVs."""
    m = scrape_affinity_csv(csv_paths)
    lines = [
        f"status={reason}",
        f"wait_timeout_s={wait_timeout_s}",
        "throughput=0",
        f"from_remote_ratio={m['from_remote_ratio']:.6f}",
        f"affinity_partition_runs={m['partition_runs']}",
        f"affinity_migrations_planned={m['migrations_planned']}",
        f"affinity_migrations_done={m['migrations_done']}",
        f"affinity_migrations_failed={m['migrations_failed']}",
        f"last_edgecut={m['last_edgecut']}",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[stage2] synthesized sentinel {path}:")
    for line in lines:
        print("  " + line)


def main() -> int:
    for binary in [STORAGE_BIN, REMOTE_BIN, COMPUTE_BIN]:
        assert_executable(binary)

    cfg = load_json(ROOT / "config" / "compute_node_config.json")
    affinity_enabled = bool(cfg.get("affinity", {}).get("enable", False))

    cleanup()

    if not WORKLOAD_CONFIG.exists():
        raise FileNotFoundError(f"missing workload config: {WORKLOAD_CONFIG}")

    with open(WORKLOAD_CONFIG, "r", encoding="utf-8") as fh:
        original_workload_cfg = fh.read()
    workload_cfg = json.loads(original_workload_cfg)
    if WORKLOAD not in workload_cfg:
        raise KeyError(f"section '{WORKLOAD}' missing in {WORKLOAD_CONFIG}")
    workload_cfg[WORKLOAD]["attempted_num"] = ATTEMPTED_NUM
    with open(WORKLOAD_CONFIG, "w", encoding="utf-8") as fh:
        json.dump(workload_cfg, fh, indent=2)

    for path in [
        LOG_STORAGE,
        LOG_REMOTE,
        STORAGE_TIMING_STATS,
        LOG_C0,
        LOG_C1,
        COMPUTE_DIR / "affinity_sidecar.log",
        COMPUTE_DIR / "result.txt",
        *RESULT_NODE_PATHS,
        COMPUTE_DIR / "affinity_timeseries.0.csv",
        COMPUTE_DIR / "affinity_timeseries.1.csv",
    ]:
        try:
            path.unlink()
        except FileNotFoundError:
            pass

    db_dir = STORAGE_DIR / WORKLOAD
    if db_dir.exists():
        shutil.rmtree(db_dir, ignore_errors=True)

    storage = start_logged([str(STORAGE_BIN), WORKLOAD], STORAGE_DIR, LOG_STORAGE, PID_STORAGE, "storage")
    if not wait_for_listen(15980, timeout_s=60):
        print("storage_pool failed to listen on 15980")
        print(tail(LOG_STORAGE))
        cleanup()
        with open(WORKLOAD_CONFIG, "w", encoding="utf-8") as fh:
            fh.write(original_workload_cfg)
        return 1

    remote = start_logged([str(REMOTE_BIN), WORKLOAD], REMOTE_DIR, LOG_REMOTE, PID_REMOTE, "remote")
    if not wait_for_listen(31508) or not wait_for_listen(31509):
        print("remote_node failed to listen on 31508/31509")
        print(tail(LOG_REMOTE))
        cleanup()
        with open(WORKLOAD_CONFIG, "w", encoding="utf-8") as fh:
            fh.write(original_workload_cfg)
        return 1

    compute_env = os.environ.copy()
    compute_env.setdefault("WOOKONG_CPU_OFFSET_BY_MACHINE", "1")

    c1 = start_logged(
        [str(COMPUTE_BIN), WORKLOAD, "lazy", THREADS, "0.2", "0.5", "1"],
        COMPUTE_DIR,
        LOG_C1,
        PID_C1,
        "compute1",
        env=compute_env,
    )
    time.sleep(2)
    c0 = start_logged(
        [str(COMPUTE_BIN), WORKLOAD, "lazy", THREADS, "0.2", "0.5", "0"],
        COMPUTE_DIR,
        LOG_C0,
        PID_C0,
        "compute0",
        env=compute_env,
    )

    if affinity_enabled:
        if not wait_for_path(COMPUTE_DIR / "affinity_sidecar.log", timeout_s=20):
            print("affinity_sidecar.log did not appear")
            print("compute0:")
            print(tail(LOG_C0))
            print("compute1:")
            print(tail(LOG_C1))
            cleanup()
            with open(WORKLOAD_CONFIG, "w", encoding="utf-8") as fh:
                fh.write(original_workload_cfg)
            return 1
        wait_for_path(COMPUTE_DIR / "affinity_timeseries.0.csv", timeout_s=20)
        wait_for_path(COMPUTE_DIR / "affinity_timeseries.1.csv", timeout_s=20)
    deadline = time.time() + WAIT_TIMEOUT_S
    c0_done = wait_for_exit_until(c0, deadline)
    c1_done = wait_for_exit_until(c1, deadline)
    timed_out = not (c0_done and c1_done)
    if timed_out:
        print(f"[stage2] TIMEOUT after {WAIT_TIMEOUT_S}s "
              f"(compute0_done={c0_done} compute1_done={c1_done}); "
              "forcing termination and synthesizing sentinel result.txt")
        # compute_server has no SIGTERM handler, so this will NOT flush result.txt
        # through the normal run.cc path — sentinel takes over below.
        force_terminate(c0)
        force_terminate(c1)
    time.sleep(2)

    result_path = COMPUTE_DIR / "result.txt"
    if not timed_out:
        if not aggregate_node_results(RESULT_NODE_PATHS, result_path):
            print("[stage2] per-node result files are required for correct cluster aggregation.")
            cleanup()
            with open(WORKLOAD_CONFIG, "w", encoding="utf-8") as fh:
                fh.write(original_workload_cfg)
            return 1

    if not result_path.exists():
        csv_paths = [
            COMPUTE_DIR / "affinity_timeseries.0.csv",
            COMPUTE_DIR / "affinity_timeseries.1.csv",
        ]
        reason = "timeout_killed" if timed_out else "missing_result_txt"
        synthesize_sentinel_result(
            result_path,
            csv_paths,
            reason=reason,
            wait_timeout_s=WAIT_TIMEOUT_S,
        )

    print("=== compute0 ===")
    print(tail(LOG_C0, 60))
    print("=== compute1 ===")
    print(tail(LOG_C1, 60))
    for idx, path in enumerate(RESULT_NODE_PATHS):
        print(f"=== result.node{idx}.txt ===")
        print(tail(path, 120))
    if affinity_enabled:
        print("=== affinity_sidecar.log ===")
        print(tail(COMPUTE_DIR / "affinity_sidecar.log", 120))

        for csv in [COMPUTE_DIR / "affinity_timeseries.0.csv", COMPUTE_DIR / "affinity_timeseries.1.csv"]:
            print(f"=== {csv.name} ===")
            print(tail(csv, 20))

    print("=== result.txt ===")
    print(tail(COMPUTE_DIR / "result.txt", 120))
    if STORAGE_TIMING_STATS.exists():
        print("=== storage_timing_stats.txt ===")
        print(tail(STORAGE_TIMING_STATS, 80))

    for proc, name in [(storage, "storage"), (remote, "remote"), (c0, "compute0"), (c1, "compute1")]:
        rc = proc.poll()
        print(f"{name}_rc={rc}")
    for name, proc, _, log_path in STARTED:
        print(f"{name}_state={proc_state(proc)} log={log_path}")

    cleanup()
    with open(WORKLOAD_CONFIG, "w", encoding="utf-8") as fh:
        fh.write(original_workload_cfg)
    return 0


if __name__ == "__main__":
    sys.exit(main())
