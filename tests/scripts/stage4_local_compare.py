#!/usr/bin/env python3
import atexit
import csv
import json
import os
import signal
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path


ROOT = Path("/home/mingtai/ClionProject/Hybrid_Cloud_MP")
CONFIG = ROOT / "config" / "compute_node_config.json"
STAGE2 = ROOT / "tests" / "scripts" / "stage2_local_affinity.py"
PLOT = ROOT / "plot_affinity.py"
COMPUTE_DIR = ROOT / "build" / "compute_server"
STORAGE_DIR = ROOT / "build" / "storage_server"
OUT_DIR = Path(os.environ.get("STAGE4_OUT_DIR", str(ROOT / "build" / "stage4_compare")))
PID_DIR = Path("/tmp/stage2_local_affinity")
WORKLOAD = os.environ.get("STAGE2_WORKLOAD", "smallbank_aff")
ATTEMPTED_NUM = int(os.environ.get("STAGE4_ATTEMPTED_NUM", "10000"))
THREADS = int(os.environ.get("STAGE4_THREADS", "2"))
PARTITION_CYCLE_MS = int(os.environ.get("STAGE4_PARTITION_CYCLE_MS", "1000"))
AFFINITY_MIGRATION_BATCH = int(os.environ.get("STAGE4_AFFINITY_MIGRATION_BATCH", "50"))
BASELINE_MIGRATION_BATCH = int(os.environ.get("STAGE4_BASELINE_MIGRATION_BATCH", "0"))
STAGE2_WAIT_TIMEOUT_S = float(os.environ.get("STAGE4_STAGE2_WAIT_TIMEOUT_S", "500"))
GENERATE_LOG_OVERRIDE = os.environ.get("STAGE4_GENERATE_LOG")
PID_FILES = [
    PID_DIR / "compute0.pid",
    PID_DIR / "compute1.pid",
    PID_DIR / "remote.pid",
    PID_DIR / "storage.pid",
]

ACTIVE_STAGE2_PROC = None
PROGRESS_INTERVAL_S = float(os.environ.get("STAGE4_PROGRESS_INTERVAL_S", "10"))


def run(cmd, env=None):
    return subprocess.run(cmd, cwd=str(ROOT), check=True, text=True, capture_output=True, env=env)


def read_pid(pidfile: Path):
    try:
        return int(pidfile.read_text().strip())
    except (FileNotFoundError, ValueError):
        return None


def kill_pid(pid: int | None):
    if not pid:
        return
    try:
        os.killpg(pid, signal.SIGTERM)
        return
    except (ProcessLookupError, PermissionError):
        pass
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass


def cleanup_stage2_processes(remove_pidfiles: bool = True):
    for pidfile in PID_FILES:
        kill_pid(read_pid(pidfile))
    if remove_pidfiles:
        for pidfile in PID_FILES:
            try:
                pidfile.unlink()
            except FileNotFoundError:
                pass


def _handle_exit_signal(signum, _frame):
    cleanup_stage2_processes()
    raise SystemExit(128 + signum)


def _drain_stream(stream, chunks):
    try:
        while True:
            data = stream.read()
            if not data:
                break
            chunks.append(data)
    finally:
        stream.close()


def load_json(path: Path):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def dump_json(path: Path, data):
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2)
        fh.write("\n")


def parse_result(path: Path):
    metrics = {}
    if not path.exists():
        return metrics
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            metrics[k] = v
    return metrics


def copy_if_exists(src: Path, dst: Path):
    if src.exists():
        shutil.copy2(src, dst)


def parse_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def parse_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def format_float(value, digits=6):
    if value is None:
        return "missing"
    return f"{value:.{digits}f}"


def read_affinity_csv(path: Path):
    rows = []
    if not path.exists():
        return rows
    with open(path, "r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows.append(row)
    return rows


def summarize_affinity_csvs(paths):
    partition_runs = 0
    migrations_planned = 0
    migrations_done = 0
    migrations_failed = 0
    max_migrations_per_tick = 0  # 新增：单次最大迁移量

    first_active_edgecuts = []
    final_edgecuts = []
    best_edgecuts = []
    steady_edgecuts = []
    first_cut_ratios = []
    final_cut_ratios = []
    best_cut_ratios = []
    steady_cut_ratios = []
    final_remote_ratios = []
    total_remote_delta = 0
    total_storage_delta = 0
    total_local_delta = 0
    active_partition_ticks = 0
    migration_active_ticks = 0
    first_migration_s = None
    last_migration_s = None

    for path in paths:
        rows = read_affinity_csv(path)
        if not rows:
            continue

        active_rows = []
        for row in rows:
            partition_runs = max(partition_runs, parse_int(row.get("partition_runs")))
            mig_planned = parse_int(row.get("migrations_planned_delta"))
            mig_done = parse_int(row.get("migrations_done_delta"))
            mig_failed = parse_int(row.get("migrations_failed_delta"))
            total_remote_delta += parse_int(row.get("from_remote_delta"))
            total_storage_delta += parse_int(row.get("from_storage_delta"))
            total_local_delta += parse_int(row.get("from_local_delta"))
            elapsed_s = parse_float(row.get("elapsed_ms")) / 1000.0

            migrations_planned += mig_planned
            migrations_done += mig_done
            migrations_failed += mig_failed
            max_migrations_per_tick = max(max_migrations_per_tick, mig_done)

            if mig_planned > 0 or mig_done > 0 or mig_failed > 0:
                migration_active_ticks += 1
            if mig_done > 0:
                if first_migration_s is None or elapsed_s < first_migration_s:
                    first_migration_s = elapsed_s
                if last_migration_s is None or elapsed_s > last_migration_s:
                    last_migration_s = elapsed_s

            if parse_int(row.get("partition_runs")) > 0:
                active_rows.append(row)

        if not active_rows:
            continue

        active_partition_ticks += len(active_rows)
        edgecuts = [parse_int(row.get("edgecut")) for row in active_rows]
        cut_ratios = []
        for row in active_rows:
            n_edges = parse_int(row.get("n_edges"))
            edgecut = parse_int(row.get("edgecut"))
            cut_ratios.append((edgecut / n_edges) if n_edges > 0 else 0.0)

        first_active_edgecuts.append(edgecuts[0])
        final_edgecuts.append(edgecuts[-1])
        best_edgecuts.append(min(edgecuts))
        steady_edgecuts.extend(edgecuts[-min(5, len(edgecuts)):])
        first_cut_ratios.append(cut_ratios[0])
        final_cut_ratios.append(cut_ratios[-1])
        best_cut_ratios.append(min(cut_ratios))
        steady_cut_ratios.extend(cut_ratios[-min(5, len(cut_ratios)):])
        final_remote_ratios.append(parse_float(active_rows[-1].get("from_remote_ratio")))

    avg = lambda xs: (sum(xs) / len(xs)) if xs else None

    edgecut_first_avg = avg(first_active_edgecuts)
    edgecut_final_avg = avg(final_edgecuts)
    edgecut_best_avg = avg(best_edgecuts)
    edgecut_steady_avg = avg(steady_edgecuts)
    migration_success_ratio = None
    if migrations_planned > 0:
        migration_success_ratio = migrations_done / migrations_planned

    migration_duration = None
    if first_migration_s is not None and last_migration_s is not None:
        migration_duration = last_migration_s - first_migration_s

    total_fetch_delta = total_remote_delta + total_storage_delta + total_local_delta
    weighted_final_remote_ratio = (
        total_remote_delta / total_fetch_delta
        if total_fetch_delta > 0 else avg(final_remote_ratios)
    )

    return {
        "partition_runs": partition_runs,
        "migrations_planned": migrations_planned,
        "migrations_done": migrations_done,
        "migrations_failed": migrations_failed,
        "max_migrations_per_tick": max_migrations_per_tick,
        "migration_success_ratio": migration_success_ratio,
        "active_partition_ticks": active_partition_ticks,
        "migration_active_ticks": migration_active_ticks,
        "first_migration_s": first_migration_s,
        "last_migration_s": last_migration_s,
        "migration_phase_duration_s": migration_duration,
        "edgecut_first_avg": edgecut_first_avg,
        "edgecut_final_avg": edgecut_final_avg,
        "edgecut_best_avg": edgecut_best_avg,
        "edgecut_steady_last5_avg": edgecut_steady_avg,
        "cut_ratio_first_avg": avg(first_cut_ratios),
        "cut_ratio_final_avg": avg(final_cut_ratios),
        "cut_ratio_best_avg": avg(best_cut_ratios),
        "cut_ratio_steady_last5_avg": avg(steady_cut_ratios),
        "final_remote_ratio_avg": weighted_final_remote_ratio,
    }


def run_case(name: str, enable_affinity: bool, migration_batch: int, attempted_num: int, threads: int, partition_cycle_ms: int):
    global ACTIVE_STAGE2_PROC

    cfg = load_json(CONFIG)
    cfg["affinity"]["enable"] = enable_affinity
    cfg["affinity"]["migration_batch"] = migration_batch
    cfg["affinity"]["partition_cycle_ms"] = partition_cycle_ms
    if GENERATE_LOG_OVERRIDE is not None:
        cfg["local_compute_node"]["generate_log"] = int(GENERATE_LOG_OVERRIDE)
    dump_json(CONFIG, cfg)

    case_dir = OUT_DIR / name
    case_dir.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["STAGE2_ATTEMPTED_NUM"] = str(attempted_num)
    env["STAGE2_THREADS"] = str(threads)
    env["STAGE2_WORKLOAD"] = WORKLOAD
    env["STAGE2_WAIT_TIMEOUT_S"] = str(STAGE2_WAIT_TIMEOUT_S)

    print(
        f"[*] Running case={name} "
        f"workload={WORKLOAD} attempted_num={attempted_num} "
        f"threads={threads} partition_cycle_ms={partition_cycle_ms} "
        f"affinity_enable={int(enable_affinity)} migration_batch={migration_batch} "
        f"stage2_timeout_s={STAGE2_WAIT_TIMEOUT_S}",
        flush=True,
    )
    proc = subprocess.Popen(
        [sys.executable, str(STAGE2)],
        cwd=str(ROOT),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )
    ACTIVE_STAGE2_PROC = proc
    stdout_chunks = []
    stderr_chunks = []
    stdout_thread = threading.Thread(
        target=_drain_stream, args=(proc.stdout, stdout_chunks), daemon=True
    )
    stderr_thread = threading.Thread(
        target=_drain_stream, args=(proc.stderr, stderr_chunks), daemon=True
    )
    stdout_thread.start()
    stderr_thread.start()
    start_time = time.monotonic()
    next_progress = start_time + PROGRESS_INTERVAL_S
    try:
        while proc.poll() is None:
            now = time.monotonic()
            if now >= next_progress:
                elapsed = now - start_time
                print(
                    f"[progress] case={name} pid={proc.pid} "
                    f"elapsed_s={elapsed:.1f} timeout_s={STAGE2_WAIT_TIMEOUT_S}",
                    flush=True,
                )
                next_progress += PROGRESS_INTERVAL_S
            time.sleep(0.5)
        stdout_thread.join(timeout=5)
        stderr_thread.join(timeout=5)
        stdout = "".join(stdout_chunks)
        stderr = "".join(stderr_chunks)
    except BaseException:
        cleanup_stage2_processes()
        if proc.poll() is None:
            try:
                proc.terminate()
            except ProcessLookupError:
                pass
        stdout_thread.join(timeout=1)
        stderr_thread.join(timeout=1)
        raise
    finally:
        ACTIVE_STAGE2_PROC = None

    (case_dir / "stage2_stdout.txt").write_text(stdout or "", encoding="utf-8")
    (case_dir / "stage2_stderr.txt").write_text(stderr or "", encoding="utf-8")

    copy_if_exists(COMPUTE_DIR / "result.txt", case_dir / "result.txt")
    copy_if_exists(COMPUTE_DIR / "result.node0.txt", case_dir / "result.node0.txt")
    copy_if_exists(COMPUTE_DIR / "result.node1.txt", case_dir / "result.node1.txt")
    copy_if_exists(STORAGE_DIR / "storage_timing_stats.txt", case_dir / "storage_timing_stats.txt")
    copy_if_exists(COMPUTE_DIR / "affinity_sidecar.log", case_dir / "affinity_sidecar.log")
    copy_if_exists(COMPUTE_DIR / "affinity_timeseries.0.csv", case_dir / "affinity_timeseries.0.csv")
    copy_if_exists(COMPUTE_DIR / "affinity_timeseries.1.csv", case_dir / "affinity_timeseries.1.csv")

    # 优化：错误时输出关键的 stderr 内容以便快速排查
    if proc.returncode != 0:
        err_tail = stderr.strip()[-500:] if stderr else "No stderr output"
        raise RuntimeError(f"❌ {name} failed with rc={proc.returncode}.\nTail of stderr:\n{err_tail}")

    return case_dir


def main():
    if OUT_DIR.exists():
        shutil.rmtree(OUT_DIR)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    original = CONFIG.read_text(encoding="utf-8")
    try:
        attempted_num = ATTEMPTED_NUM
        threads = THREADS
        partition_cycle_ms = PARTITION_CYCLE_MS

        print("=" * 72, flush=True)
        print("STAGE 4 Local Compare", flush=True)
        print(
            f"workload={WORKLOAD} attempted_num={attempted_num} threads={threads} "
            f"partition_cycle_ms={partition_cycle_ms} "
            f"baseline_migration_batch={BASELINE_MIGRATION_BATCH} "
            f"affinity_migration_batch={AFFINITY_MIGRATION_BATCH} "
            f"generate_log_override={GENERATE_LOG_OVERRIDE if GENERATE_LOG_OVERRIDE is not None else 'config'} "
            f"stage2_wait_timeout_s={STAGE2_WAIT_TIMEOUT_S} "
            f"progress_interval_s={PROGRESS_INTERVAL_S}",
            flush=True,
        )
        print(f"config={CONFIG}", flush=True)
        print(f"output_dir={OUT_DIR}", flush=True)
        print("=" * 72, flush=True)

        baseline_dir = run_case(
            "baseline",
            enable_affinity=False,
            migration_batch=BASELINE_MIGRATION_BATCH,
            attempted_num=attempted_num,
            threads=threads,
            partition_cycle_ms=partition_cycle_ms,
        )
        affinity_dir = run_case(
            "affinity_on",
            enable_affinity=True,
            migration_batch=AFFINITY_MIGRATION_BATCH,
            attempted_num=attempted_num,
            threads=threads,
            partition_cycle_ms=partition_cycle_ms,
        )

        affinity_csvs = [
            affinity_dir / "affinity_timeseries.0.csv",
            affinity_dir / "affinity_timeseries.1.csv",
            ]
        existing_csvs = [str(p) for p in affinity_csvs if p.exists()]
        affinity_csv_summary = summarize_affinity_csvs(affinity_csvs)
        baseline = parse_result(baseline_dir / "result.txt")
        affinity = parse_result(affinity_dir / "result.txt")

        summary = []
        # ====== 核心配置 ======
        summary.append("workload=" + WORKLOAD)
        summary.append("attempted_num=" + str(attempted_num))
        summary.append("attempted_num_scope=per_worker")
        summary.append("threads=" + str(threads))
        summary.append("partition_cycle_ms=" + str(partition_cycle_ms))
        summary.append("generate_log=" + (
            str(GENERATE_LOG_OVERRIDE) if GENERATE_LOG_OVERRIDE is not None
            else str(load_json(CONFIG).get("local_compute_node", {}).get("generate_log", "config"))
        ))

        # ====== 状态与性能对比 ======
        summary.append("baseline_status=" + baseline.get("status", "ok"))
        summary.append("affinity_status=" + affinity.get("status", "ok"))
        summary.append("baseline_aggregation_scope=" + baseline.get("aggregation_scope", "unknown"))
        summary.append("affinity_aggregation_scope=" + affinity.get("aggregation_scope", "unknown"))
        summary.append("baseline_aggregation_node_count=" + baseline.get("aggregation_node_count", "missing"))
        summary.append("affinity_aggregation_node_count=" + affinity.get("aggregation_node_count", "missing"))
        baseline_node_count = parse_int(baseline.get("aggregation_node_count"), default=0)
        if baseline_node_count > 0:
            summary.append(
                "expected_cluster_attempted_num="
                + str(attempted_num * threads * baseline_node_count)
            )

        base_tput = parse_float(baseline.get("throughput"))
        aff_tput = parse_float(affinity.get("throughput"))
        summary.append("baseline_throughput=" + baseline.get("throughput", "missing"))
        summary.append("affinity_throughput=" + affinity.get("throughput", "missing"))

        # 优化：计算吞吐量提升幅度
        if base_tput > 0 and aff_tput > 0:
            improvement = ((aff_tput - base_tput) / base_tput) * 100
            summary.append(f"throughput_improvement_pct={improvement:.2f}")

        # 优化：自动提取可能存在的重要延迟/中止率指标
        for metric in ["p50_latency", "p99_latency", "avg_latency", "abort_rate"]:
            if metric in baseline:
                summary.append(f"baseline_{metric}=" + baseline[metric])
            if metric in affinity:
                summary.append(f"affinity_{metric}=" + affinity[metric])

        # ====== 亲和性(Affinity)专项指标 ======
        summary.append("affinity_partition_runs=" + str(affinity_csv_summary["partition_runs"]))
        summary.append("affinity_active_partition_ticks=" + str(affinity_csv_summary["active_partition_ticks"]))

        summary.append("affinity_edgecut_first_avg=" + format_float(affinity_csv_summary["edgecut_first_avg"], 3))
        summary.append("affinity_edgecut_final_avg=" + format_float(affinity_csv_summary["edgecut_final_avg"], 3))
        summary.append("affinity_cut_ratio_first_avg=" + format_float(affinity_csv_summary["cut_ratio_first_avg"], 4))
        summary.append("affinity_cut_ratio_final_avg=" + format_float(affinity_csv_summary["cut_ratio_final_avg"], 4))

        summary.append("affinity_migrations_planned=" + str(affinity_csv_summary["migrations_planned"]))
        summary.append("affinity_migrations_done=" + str(affinity_csv_summary["migrations_done"]))
        summary.append("affinity_migrations_failed=" + str(affinity_csv_summary["migrations_failed"]))
        summary.append("affinity_max_migrations_per_tick=" + str(affinity_csv_summary["max_migrations_per_tick"]))
        summary.append("affinity_migration_success_ratio=" + format_float(affinity_csv_summary["migration_success_ratio"], 4))

        summary.append("affinity_first_migration_s=" + format_float(affinity_csv_summary["first_migration_s"], 3))
        summary.append("affinity_last_migration_s=" + format_float(affinity_csv_summary["last_migration_s"], 3))
        summary.append("affinity_migration_phase_duration_s=" + format_float(affinity_csv_summary["migration_phase_duration_s"], 3))

        summary.append("baseline_remote_ratio_diag=" + baseline.get("from_remote_ratio", "missing"))
        summary.append("affinity_remote_ratio_diag=" + format_float(affinity_csv_summary["final_remote_ratio_avg"], 6))

        # 绘图逻辑保持不变
        if existing_csvs:
            try:
                run([sys.executable, str(PLOT), *existing_csvs, "--out", str(OUT_DIR / "run1")])
                summary.append("plots_generated=1")
            except subprocess.CalledProcessError as e:
                summary.append("plots_generated=0")
                summary.append("plot_error=" + (e.stderr.strip() or e.stdout.strip() or f"rc={e.returncode}").replace('\n', ' '))
        else:
            summary.append("plots_generated=0")
            summary.append("plot_error=missing_csv")

        # 写入文件并格式化控制台输出
        summary_text = "\n".join(summary) + "\n"
        (OUT_DIR / "summary.txt").write_text(summary_text, encoding="utf-8")

        print("\n" + "="*50)
        print("🎉 STAGE 4 COMPARE FINISHED 🎉")
        print("="*50)
        for line in summary:
            # 格式化输出，让键值对在控制台看起来更清晰
            parts = line.split("=", 1)
            if len(parts) == 2:
                print(f"{parts[0]:<40}: {parts[1]}")
            else:
                print(line)
        print("="*50)
        print(f"Output directory: {OUT_DIR}")

    finally:
        cleanup_stage2_processes()
        CONFIG.write_text(original, encoding="utf-8")


atexit.register(cleanup_stage2_processes)
signal.signal(signal.SIGINT, _handle_exit_signal)
signal.signal(signal.SIGTERM, _handle_exit_signal)


if __name__ == "__main__":
    main()
