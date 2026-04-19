#!/usr/bin/env python3
import json
import shutil
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path("/home/mingtai/ClionProject/Hybrid_Cloud_MP")
CONFIG = ROOT / "config" / "compute_node_config.json"
STAGE2 = ROOT / "tests" / "scripts" / "stage2_local_affinity.py"
PLOT = ROOT / "plot_affinity.py"
COMPUTE_DIR = ROOT / "build" / "compute_server"
OUT_DIR = ROOT / "build" / "stage4_compare"


def run(cmd, env=None):
    return subprocess.run(cmd, cwd=str(ROOT), check=True, text=True, capture_output=True, env=env)


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


def run_case(name: str, enable_affinity: bool, migration_batch: int, attempted_num: int, threads: int, partition_cycle_ms: int):
    cfg = load_json(CONFIG)
    cfg["affinity"]["enable"] = enable_affinity
    cfg["affinity"]["migration_batch"] = migration_batch
    cfg["affinity"]["partition_cycle_ms"] = partition_cycle_ms
    dump_json(CONFIG, cfg)

    case_dir = OUT_DIR / name
    case_dir.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["STAGE2_ATTEMPTED_NUM"] = str(attempted_num)
    env["STAGE2_THREADS"] = str(threads)
    proc = subprocess.run(
        [sys.executable, str(STAGE2)],
        cwd=str(ROOT),
        check=False,
        text=True,
        capture_output=True,
        env=env,
    )
    (case_dir / "stage2_stdout.txt").write_text(proc.stdout, encoding="utf-8")
    (case_dir / "stage2_stderr.txt").write_text(proc.stderr, encoding="utf-8")
    if proc.returncode != 0:
        raise RuntimeError(f"{name} failed with rc={proc.returncode}")

    copy_if_exists(COMPUTE_DIR / "result.txt", case_dir / "result.txt")
    copy_if_exists(COMPUTE_DIR / "affinity_sidecar.log", case_dir / "affinity_sidecar.log")
    copy_if_exists(COMPUTE_DIR / "affinity_timeseries.0.csv", case_dir / "affinity_timeseries.0.csv")
    copy_if_exists(COMPUTE_DIR / "affinity_timeseries.1.csv", case_dir / "affinity_timeseries.1.csv")
    return case_dir


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    original = CONFIG.read_text(encoding="utf-8")
    try:
        # Keep the experiment fair: both lanes must use identical workload
        # parameters and differ only in affinity enablement.
        attempted_num = 50000
        threads = 2
        partition_cycle_ms = 1000

        baseline_dir = run_case(
            "baseline",
            enable_affinity=False,
            migration_batch=0,
            attempted_num=attempted_num,
            threads=threads,
            partition_cycle_ms=partition_cycle_ms,
        )
        affinity_dir = run_case(
            "affinity_on",
            enable_affinity=True,
            migration_batch=50,
            attempted_num=attempted_num,
            threads=threads,
            partition_cycle_ms=partition_cycle_ms,
        )

        affinity_csvs = [
            affinity_dir / "affinity_timeseries.0.csv",
            affinity_dir / "affinity_timeseries.1.csv",
        ]
        existing_csvs = [str(p) for p in affinity_csvs if p.exists()]
        baseline = parse_result(baseline_dir / "result.txt")
        affinity = parse_result(affinity_dir / "result.txt")

        summary = []
        summary.append("baseline_status=" + baseline.get("status", "ok"))
        summary.append("affinity_status=" + affinity.get("status", "ok"))
        summary.append("baseline_throughput=" + baseline.get("throughput", "missing"))
        summary.append("affinity_throughput=" + affinity.get("throughput", "missing"))
        summary.append("baseline_remote_ratio=" + baseline.get("from_remote_ratio", "missing"))
        summary.append("affinity_remote_ratio=" + affinity.get("from_remote_ratio", "missing"))
        summary.append("attempted_num=" + str(attempted_num))
        summary.append("threads=" + str(threads))
        summary.append("partition_cycle_ms=" + str(partition_cycle_ms))
        if "wait_timeout_s" in affinity:
            summary.append("affinity_wait_timeout_s=" + affinity["wait_timeout_s"])
        summary.append("affinity_partition_runs=" + affinity.get("affinity_partition_runs", "missing"))
        summary.append("affinity_migrations_planned=" + affinity.get("affinity_migrations_planned", "missing"))
        summary.append("affinity_migrations_done=" + affinity.get("affinity_migrations_done", "missing"))
        summary.append("affinity_migrations_failed=" + affinity.get("affinity_migrations_failed", "missing"))
        if existing_csvs:
            try:
                run([sys.executable, str(PLOT), *existing_csvs, "--out", str(OUT_DIR / "run1")])
                summary.append("plots_generated=1")
            except subprocess.CalledProcessError as e:
                summary.append("plots_generated=0")
                summary.append("plot_error=" + (e.stderr.strip() or e.stdout.strip() or f"rc={e.returncode}"))
        else:
            summary.append("plots_generated=0")
            summary.append("plot_error=missing_csv")
        (OUT_DIR / "summary.txt").write_text("\n".join(summary) + "\n", encoding="utf-8")

        print(f"wrote {OUT_DIR}")
        print((OUT_DIR / "summary.txt").read_text(encoding="utf-8"), end="")
    finally:
        CONFIG.write_text(original, encoding="utf-8")


if __name__ == "__main__":
    main()
