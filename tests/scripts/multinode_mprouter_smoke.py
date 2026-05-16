#!/usr/bin/env python3
"""
Multi-node smoke test for the MP-Router → Hybrid_Cloud_MP pipeline.

Modeled on tests/scripts/multinode_parmetis_smoke.py — same 5-host topology
(4 compute hosts + 1 service host running storage_pool+remote_node) and the
same SSH/rsync/build helpers — but instead of running the embedded
``compute_server smallbank_aff lazy …`` workload, it:

  * launches each compute_server in **interactive mode**:
        ./compute_server interactive smallbank_aff <node_id>
    listening on ``9115 + node_id`` for the LOOKUP/SB router protocol;
  * runs the MP-Router ``run`` driver locally with ``--system-mode``
    (default: 23)
    pointed at the 4 remote compute interactive ports;
  * lets MP-Router's SmartRouter pull the initial key→page map via batched
    LOOKUP, then route SmallBank txns to the cluster.

Storage_pool generates the SmallBank tables on first start using the
``smallbank_aff`` config we upload, so there is no separate "load data" step.

Defaults assume:
  * cluster:  10.10.2.31..34 (compute) + 10.10.2.38 (service)
  * MP-Router source tree: /root/mingtai/MP-Router (override via
    WOOKONG_MPROUTER_DIR or --mprouter-dir)

Examples
--------
Quick smoke (2 minutes), affinity off::

    python3 tests/scripts/multinode_mprouter_smoke.py \\
        --num-accounts 500000 --worker-threads 4 --try-count 500 \\
        --warmup-rounds 0 --disable-affinity

A/B compare baseline vs affinity::

    python3 tests/scripts/multinode_mprouter_smoke.py --compare --rebuild \\
        --num-accounts 5000000 --worker-threads 16 --try-count 35000 \\
        --batch-size 10000 --num-bucket 4 --warmup-rounds 0
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import posixpath
import re
import shlex
import shutil
import subprocess
import sys
import textwrap
import time
from pathlib import Path

# Reuse the SSH / rsync / build / config helpers from the parmetis sibling.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from multinode_parmetis_smoke import (  # noqa: E402
    DEFAULT_PASSWORD,
    DEFAULT_REMOTE_DIR,
    DEFAULT_USER,
    Host,
    build_project,
    install_mpi_ssh_wrapper,
    kill_cluster,
    log,
    make_configs,
    parse_timeseries_metrics,
    require_local_tools,
    rsync_project,
    run_parallel,
    run_ssh,
    ssh,
    sftp_get_if_exists,
    start_services,
    upload_configs,
    wait_remote,
)


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_COMPUTE_HOSTS = ["10.10.2.31", "10.10.2.32", "10.10.2.33", "10.10.2.34"]
DEFAULT_SERVICE_HOST = "10.10.2.38"
DEFAULT_RESULT_DIR = ROOT / "result" / "multinode_mprouter_smoke"
DEFAULT_MPROUTER_DIR = Path(
    os.environ.get("WOOKONG_MPROUTER_DIR", "/root/mingtai/MP-Router")
)
COMPUTE_INTERACTIVE_BASE = 9115


def stop_process_group(proc: subprocess.Popen, reason: str) -> None:
    log(f"mprouter: {reason} — sending SIGTERM")
    os.killpg(proc.pid, 15)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        log("mprouter: still alive — sending SIGKILL")
        os.killpg(proc.pid, 9)


# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------

_LEFTOVER_PATTERN = (
    "compute_server|storage_pool|remote_node|parmetis_sidecar"
)
_PORTS_RE = "(15980|15981|31508|31509|9115|9116|9117|9118|2868[8-9]|2869[01])"


def preflight_remote(hosts: list[Host]) -> dict[str, str]:
    """Per-host scan for leftover db processes and busy ports.

    Returns a dict ip -> textual report. A non-empty report means we found
    something that would conflict with a fresh boot.
    """
    def check(host: Host) -> str:
        cmd = textwrap.dedent(f"""\
            set +e
            echo "host=$(hostname) ip={host.ip}"
            echo "--- leftover processes ---"
            pgrep -af '{_LEFTOVER_PATTERN}' \\
                | grep -v -E 'sshd|/bin/bash|/usr/bin/python' || true
            echo "--- busy ports ---"
            ss -ltn 2>/dev/null | awk 'NR>1{{print $4}}' \\
                | grep -E ':{_PORTS_RE}$' || true
        """)
        try:
            _, out, err = run_ssh(host, "bash -lc " + shlex.quote(cmd),
                                  timeout=20, check=False)
        except Exception as exc:
            return f"preflight_error={exc}\n"
        # Strip the section headers + the host line so callers can ask
        # "is this string non-empty?" cleanly.
        lines = (out + err).splitlines()
        body = [
            l for l in lines
            if l.strip()
            and not l.startswith(("host=", "---"))
            and "已恢复当前 shell 代理环境" not in l
        ]
        return "\n".join(body) + ("\n" if body else "")

    return run_parallel(hosts, "preflight scan", check)


def preflight_local(args: argparse.Namespace) -> Path:
    """Verify the MP-Router run binary exists locally."""
    bin_path = args.mprouter_dir / "build" / "serve" / "test" / "run"
    if not bin_path.exists():
        raise RuntimeError(
            f"mprouter binary missing: {bin_path}\n"
            f"Build it first (cd {args.mprouter_dir}/build && cmake .. && make -j run) "
            "or pass --rebuild."
        )
    return bin_path


# ---------------------------------------------------------------------------
# Cluster boot
# ---------------------------------------------------------------------------

def start_compute_interactive(host: Host, remote_dir: str, workload: str,
                              node_id: int,
                              extra_env: dict[str, str] | None = None) -> str:
    env_lines = "\n".join(
        f"export {key}={shlex.quote(str(value))}"
        for key, value in sorted((extra_env or {}).items())
    )
    cmd = f"""
set -euo pipefail
cd {shlex.quote(remote_dir)}/build/compute_server
rm -f result.txt result.node*.txt affinity_timeseries*.csv \\
      affinity_sidecar.log compute_smoke_{node_id}.log \\
      computeserver.log*
export OMPI_ALLOW_RUN_AS_ROOT=1
export OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1
{env_lines}
nohup ./compute_server interactive {shlex.quote(workload)} {int(node_id)} \\
    > compute_smoke_{node_id}.log 2>&1 < /dev/null &
echo $! >/tmp/wookong_compute_{node_id}.pid
"""
    run_ssh(host, "bash -lc " + shlex.quote(cmd), timeout=20)
    return "started"


def boot_cluster(args: argparse.Namespace, all_hosts: list[Host],
                 compute_hosts: list[Host], service: Host,
                 compute_env: dict[str, str] | None = None) -> None:
    kill_cluster(all_hosts)

    log("boot: storage_pool + remote_node on " + service.ip)
    start_services(service, args.remote_dir, args.workload, args)

    log("boot: 4 compute_server (interactive) on " +
        ",".join(h.ip for h in compute_hosts))
    # Storage's meta socket accepts compute_node_count handshakes once and
    # then closes; if any compute boots before storage is ready, the early
    # one will crash with "Connection refused" on :15981. We staggered with
    # 0.4s in the local smoke test and that proved sufficient. Keep the
    # same gap here.
    for idx, host in enumerate(compute_hosts):
        start_compute_interactive(
            host, args.remote_dir, args.workload, idx, compute_env
        )
        time.sleep(0.4)

    for idx, host in enumerate(compute_hosts):
        port = COMPUTE_INTERACTIVE_BASE + idx
        wait_remote(
            host,
            f"ss -ltn | grep -q ':{port} '",
            timeout_s=180,
            label=f"compute_{idx} interactive port {port}",
        )
    log("boot: cluster up.")


# ---------------------------------------------------------------------------
# MP-Router driver (runs locally; connects to remote computes via TCP)
# ---------------------------------------------------------------------------

def run_mprouter(args: argparse.Namespace, run_dir: Path, log_path: Path,
                 hosts_spec: str) -> int:
    bin_path = args.mprouter_dir / "build" / "serve" / "test" / "run"
    cmd = [
        str(bin_path),
        "--workload", "smallbank",
        "--system-mode", str(args.mprouter_system_mode),
        "--access-pattern", "1",
        "--zipfian-theta", str(args.mprouter_zipfian_theta),
        "--account-count", str(args.num_accounts),
        "--worker-threads", str(args.worker_threads),
        "--try-count", str(args.try_count),
        "--affinity-txn-ratio", str(args.mprouter_affinity_txn_ratio),
        "--batch-size", str(args.batch_size),
        "--num-bucket", str(args.num_bucket),
    ]
    env = os.environ.copy()
    env["HYBRID_WARMUP_ROUNDS"] = str(args.warmup_rounds)
    env["HYBRID_COMPUTE_HOSTS"] = hosts_spec

    log("mprouter cwd=" + str(run_dir))
    log("mprouter cmd=" + " ".join(shlex.quote(c) for c in cmd))
    log(f"mprouter HYBRID_COMPUTE_HOSTS={hosts_spec}")
    log(f"mprouter HYBRID_WARMUP_ROUNDS={args.warmup_rounds}")
    log(f"mprouter timeout={args.timeout}s")

    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "wb") as fh:
        proc = subprocess.Popen(
            cmd, cwd=str(run_dir), env=env,
            stdout=fh, stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            rc = proc.wait(timeout=args.timeout)
        except subprocess.TimeoutExpired:
            stop_process_group(proc, "timeout")
            rc = -1
        except KeyboardInterrupt:
            stop_process_group(proc, "interrupted")
            raise
    log(f"mprouter exit_code={rc}")
    return rc


# ---------------------------------------------------------------------------
# Result collection + summary
# ---------------------------------------------------------------------------

def collect_results(args: argparse.Namespace, compute_hosts: list[Host],
                    service: Host, mprouter_run_dir: Path,
                    local_log_dir: Path, case_dir: Path) -> None:
    case_dir.mkdir(parents=True, exist_ok=True)

    # MP-Router result.txt + driver log (both local).
    src = mprouter_run_dir / "result.txt"
    if src.exists():
        shutil.copy(src, case_dir / "mprouter_result.txt")
    src = local_log_dir / "mprouter.log"
    if src.exists():
        shutil.copy(src, case_dir / "mprouter.log")

    # Per-compute logs + affinity timeseries.
    # core/affinity/affinity_timeseries.cc inserts ".<node>" before the LAST
    # ".", giving "affinity_timeseries.<node>.csv". An older variant used
    # "affinity_timeseries.csv.<node>" — try both for safety.
    for idx, host in enumerate(compute_hosts):
        node_dir = case_dir / f"node{idx}_{host.ip}"
        for remote_name in [
            f"compute_smoke_{idx}.log",
            "affinity_sidecar.log",
            f"affinity_timeseries.{idx}.csv",
            f"affinity_timeseries.csv.{idx}",
        ]:
            sftp_get_if_exists(
                host,
                posixpath.join(args.remote_dir, "build", "compute_server",
                               remote_name),
                node_dir / remote_name,
            )

    # Service host logs.
    svc_dir = case_dir / "service"
    svc_dir.mkdir(parents=True, exist_ok=True)
    for remote_name, sub in [
        ("storage_smoke.log", "storage_server"),
        ("remote_smoke.log", "remote_server"),
    ]:
        sftp_get_if_exists(
            service,
            posixpath.join(args.remote_dir, "build", sub, remote_name),
            svc_dir / remote_name,
        )


def sftp_put_file(host: Host, local_path: Path, remote_path: str) -> None:
    run_ssh(
        host,
        "mkdir -p " + shlex.quote(posixpath.dirname(remote_path)),
        timeout=20,
    )
    client = ssh(host)
    try:
        sftp = client.open_sftp()
        try:
            sftp.put(str(local_path), remote_path)
        finally:
            sftp.close()
    finally:
        client.close()


def upload_schism_csv(compute_hosts: list[Host], args: argparse.Namespace,
                       local_csv: Path) -> str:
    if not local_csv.exists():
        raise FileNotFoundError(f"schism csv missing: {local_csv}")
    remote_path = posixpath.join(
        args.remote_dir, "config", "schism_static_assignment.csv"
    )
    run_parallel(
        compute_hosts,
        "upload schism csv",
        lambda h: (sftp_put_file(h, local_csv, remote_path) or "csv_uploaded"),
    )
    return remote_path


def _read_timeseries_rows(node_dir: Path) -> list[dict]:
    """Return rows for whichever timeseries CSV variant exists in node_dir."""
    candidates = sorted(node_dir.glob("affinity_timeseries.*.csv"))
    plain = node_dir / "affinity_timeseries.csv"
    if plain.exists() and plain not in candidates:
        candidates.append(plain)
    for path in candidates:
        try:
            with path.open("r", encoding="utf-8", errors="replace",
                           newline="") as fh:
                rows = list(csv.DictReader(fh))
            if rows:
                return rows
        except OSError:
            continue
    return []


def _fetch_metrics_for_node(node_dir: Path) -> dict:
    """Extract per-node fetch + migration counters from the timeseries CSV.

    The timeseries loop emits running ratios in `from_*_ratio` and per-tick
    deltas in `from_*_delta`. We use the last-row ratios as the steady-state
    snapshot, and sum the deltas for cluster-wide aggregation.
    """
    rows = _read_timeseries_rows(node_dir)
    if not rows:
        return {}

    last = rows[-1]

    def fl(k: str) -> float:
        try:
            return float(last.get(k) or "0")
        except (TypeError, ValueError):
            return 0.0

    def isum(k: str) -> int:
        s = 0
        for r in rows:
            try:
                s += int(float(r.get(k) or "0"))
            except (TypeError, ValueError):
                pass
        return s

    return {
        "from_remote_ratio_final": fl("from_remote_ratio"),
        "from_storage_ratio_final": fl("from_storage_ratio"),
        "from_local_ratio_final": fl("from_local_ratio"),
        "from_remote_count": isum("from_remote_delta"),
        "from_storage_count": isum("from_storage_delta"),
        "from_local_count": isum("from_local_delta"),
        "migrations_planned": isum("migrations_planned_delta"),
        "migrations_done": isum("migrations_done_delta"),
        "migrations_failed": isum("migrations_failed_delta"),
        "partition_rejected_final": int(fl("partition_rejected")),
    }


def _section_after(text: str, anchor: str, count: int = 4) -> dict[str, str]:
    """Pull "Average:/P50:/P95:/P99:" lines from the section starting at anchor.

    MP-Router emits two such sections — "Latency Statistics (After Warmup):"
    (per-tx exec latency) and "Fetch-to-Complete Latency Statistics" (queue
    + exec). The previous parser used a global last-occurrence sweep which
    silently grabbed only the fetch-to-complete numbers.
    """
    out: dict[str, str] = {}
    idx = text.find(anchor)
    if idx == -1:
        return out
    sub = text[idx + len(anchor):]
    for m in re.finditer(
        r"^\s+(Average|P50|P95|P99):\s*([\d.]+)\s*ms",
        sub, re.MULTILINE,
    ):
        out[m.group(1)] = m.group(2)
        if len(out) >= count:
            break
    return out


def parse_summary(case_dir: Path, compute_hosts: list[Host]) -> dict:
    out: dict[str, str] = {}
    res = case_dir / "mprouter_result.txt"
    if res.exists():
        text = res.read_text(encoding="utf-8", errors="replace")
        m = re.search(r"^Throughput:\s*([\d.]+)\s*transactions per second",
                      text, re.MULTILINE)
        if m:
            out["mprouter_throughput_tps"] = m.group(1)
        m = re.search(r"^Throughput \(after warmup\):\s*([\d.]+)",
                      text, re.MULTILINE)
        if m:
            out["mprouter_throughput_after_warmup_tps"] = m.group(1)
        m = re.search(r"^Total transactions executed:\s*(\d+)",
                      text, re.MULTILINE)
        if m:
            out["mprouter_total_txns"] = m.group(1)
        m = re.search(r"^Elapsed time:\s*([\d.]+)\s*milliseconds",
                      text, re.MULTILINE)
        if m:
            out["mprouter_elapsed_ms"] = m.group(1)
        for k, v in re.findall(r"^node (\d+) routed txn count:\s*(\d+)",
                               text, re.MULTILINE):
            out[f"mprouter_node{k}_routed"] = v
        matches = re.findall(r"^Page Operations count:\s*(\d+)",
                             text, re.MULTILINE)
        if matches:
            out["mprouter_page_ops_total"] = matches[-1]
        matches = re.findall(r"^Page ID changes:\s*(\d+)", text, re.MULTILINE)
        if matches:
            out["mprouter_page_id_changes"] = matches[-1]

        # Latencies: parse the two sections separately.
        exec_lat = _section_after(text, "Latency Statistics (After Warmup):")
        fetch_lat = _section_after(
            text, "Fetch-to-Complete Latency Statistics")
        for label, key in [
            ("Average", "mprouter_exec_avg_ms"),
            ("P50", "mprouter_exec_p50_ms"),
            ("P95", "mprouter_exec_p95_ms"),
            ("P99", "mprouter_exec_p99_ms"),
        ]:
            if label in exec_lat:
                out[key] = exec_lat[label]
        for label, key in [
            ("Average", "mprouter_fetch_avg_ms"),
            ("P50", "mprouter_fetch_p50_ms"),
            ("P95", "mprouter_fetch_p95_ms"),
            ("P99", "mprouter_fetch_p99_ms"),
        ]:
            if label in fetch_lat:
                out[key] = fetch_lat[label]

    # Per-node timeseries rollup (reuse parmetis helper).
    edgecut_finals = []
    total_edge_weight_finals = []
    weighted_cut_ratio_finals = []
    for idx in range(len(compute_hosts)):
        node_dir = case_dir / f"node{idx}_{compute_hosts[idx].ip}"
        if not node_dir.exists():
            continue
        kv = parse_timeseries_metrics(node_dir)
        if "affinity_edgecut_final" in kv:
            try:
                edgecut_finals.append(int(float(kv["affinity_edgecut_final"])))
            except ValueError:
                pass
        if "affinity_total_edge_weight_final" in kv:
            try:
                total_edge_weight_finals.append(
                    int(float(kv["affinity_total_edge_weight_final"]))
                )
            except ValueError:
                pass
        if "affinity_weighted_cut_ratio_final" in kv:
            try:
                weighted_cut_ratio_finals.append(
                    float(kv["affinity_weighted_cut_ratio_final"])
                )
            except ValueError:
                pass
    if edgecut_finals:
        out["affinity_edgecut_final_avg"] = (
            f"{sum(edgecut_finals) / len(edgecut_finals):.1f}"
        )
        out["affinity_edgecut_final_per_node"] = ",".join(
            str(x) for x in edgecut_finals
        )
    if total_edge_weight_finals:
        out["affinity_total_edge_weight_final_avg"] = (
            f"{sum(total_edge_weight_finals) / len(total_edge_weight_finals):.1f}"
        )
        out["affinity_total_edge_weight_final_per_node"] = ",".join(
            str(x) for x in total_edge_weight_finals
        )
    if weighted_cut_ratio_finals:
        out["affinity_weighted_cut_ratio_final_avg"] = (
            f"{sum(weighted_cut_ratio_finals) / len(weighted_cut_ratio_finals):.6f}"
        )
        out["affinity_weighted_cut_ratio_final_per_node"] = ",".join(
            f"{x:.6f}" for x in weighted_cut_ratio_finals
        )

    # Per-node fetch + migration counters → cluster aggregate.
    cluster = {
        "remote_count": 0, "storage_count": 0, "local_count": 0,
        "mig_planned": 0, "mig_done": 0, "mig_failed": 0,
        "partition_rejected": 0,
    }
    per_node_remote_ratios = []
    for idx in range(len(compute_hosts)):
        host = compute_hosts[idx]
        node_dir = case_dir / f"node{idx}_{host.ip}"
        if not node_dir.exists():
            continue
        m = _fetch_metrics_for_node(node_dir)
        if not m:
            continue
        out[f"node{idx}_from_remote_ratio_final"] = (
            f"{m['from_remote_ratio_final']:.6f}")
        out[f"node{idx}_from_storage_ratio_final"] = (
            f"{m['from_storage_ratio_final']:.6f}")
        out[f"node{idx}_from_local_ratio_final"] = (
            f"{m['from_local_ratio_final']:.6f}")
        out[f"node{idx}_migrations_done"] = str(m["migrations_done"])
        out[f"node{idx}_migrations_planned"] = str(m["migrations_planned"])
        out[f"node{idx}_migrations_failed"] = str(m["migrations_failed"])
        per_node_remote_ratios.append(m["from_remote_ratio_final"])
        cluster["remote_count"] += m["from_remote_count"]
        cluster["storage_count"] += m["from_storage_count"]
        cluster["local_count"] += m["from_local_count"]
        cluster["mig_planned"] += m["migrations_planned"]
        cluster["mig_done"] += m["migrations_done"]
        cluster["mig_failed"] += m["migrations_failed"]
        cluster["partition_rejected"] += m["partition_rejected_final"]

    total_fetches = (cluster["remote_count"] + cluster["storage_count"]
                     + cluster["local_count"])
    if total_fetches > 0:
        out["cluster_from_remote_ratio"] = (
            f"{cluster['remote_count'] / total_fetches:.6f}")
        out["cluster_from_storage_ratio"] = (
            f"{cluster['storage_count'] / total_fetches:.6f}")
        out["cluster_from_local_ratio"] = (
            f"{cluster['local_count'] / total_fetches:.6f}")
    out["cluster_from_remote_count"] = str(cluster["remote_count"])
    out["cluster_from_storage_count"] = str(cluster["storage_count"])
    out["cluster_from_local_count"] = str(cluster["local_count"])
    if per_node_remote_ratios:
        out["per_node_from_remote_ratio_final"] = ",".join(
            f"{r:.6f}" for r in per_node_remote_ratios
        )

    out["affinity_migrations_planned_total"] = str(cluster["mig_planned"])
    out["affinity_migrations_done_total"] = str(cluster["mig_done"])
    out["affinity_migrations_failed_total"] = str(cluster["mig_failed"])
    if cluster["mig_planned"] > 0:
        out["affinity_migration_success_ratio"] = (
            f"{cluster['mig_done'] / cluster['mig_planned']:.6f}")
        out["affinity_migration_failure_ratio"] = (
            f"{cluster['mig_failed'] / cluster['mig_planned']:.6f}")
    out["affinity_migration_backlog"] = str(
        max(cluster["mig_planned"] - cluster["mig_done"], 0))
    out["affinity_partition_rejected_total"] = str(
        cluster["partition_rejected"])

    return out


def print_summary(case: str, summary: dict) -> None:
    headline = [
        "mprouter_exit_code",
        "mprouter_throughput_tps",
        "mprouter_throughput_after_warmup_tps",
        "mprouter_total_txns",
        "mprouter_elapsed_ms",
        # Per-tx exec latency (work done on a compute, not queue time)
        "mprouter_exec_avg_ms",
        "mprouter_exec_p50_ms",
        "mprouter_exec_p95_ms",
        "mprouter_exec_p99_ms",
        # End-to-end fetch-to-complete latency (queue + exec)
        "mprouter_fetch_avg_ms",
        "mprouter_fetch_p50_ms",
        "mprouter_fetch_p95_ms",
        "mprouter_fetch_p99_ms",
        # Routing balance
        "mprouter_node0_routed",
        "mprouter_node1_routed",
        "mprouter_node2_routed",
        "mprouter_node3_routed",
        "mprouter_page_ops_total",
        "mprouter_page_id_changes",
        # Cluster fetch-locality (the headline affinity claim)
        "cluster_from_remote_ratio",
        "cluster_from_storage_ratio",
        "cluster_from_local_ratio",
        "per_node_from_remote_ratio_final",
        # Affinity health
        "affinity_edgecut_final_avg",
        "affinity_edgecut_final_per_node",
        "affinity_total_edge_weight_final_avg",
        "affinity_total_edge_weight_final_per_node",
        "affinity_weighted_cut_ratio_final_avg",
        "affinity_weighted_cut_ratio_final_per_node",
        "affinity_migrations_planned_total",
        "affinity_migrations_done_total",
        "affinity_migrations_failed_total",
        "affinity_migration_success_ratio",
        "affinity_migration_failure_ratio",
        "affinity_migration_backlog",
        "affinity_partition_rejected_total",
    ]
    log(f"=== {case} summary ===")
    for k in headline:
        v = summary.get(k)
        if v is not None:
            log(f"  {k:42s} = {v}")


def write_compare_summary(stamp_dir: Path, base: dict, aff: dict) -> None:
    def f(d: dict, k: str) -> float:
        try:
            return float(d.get(k, "0") or "0")
        except (TypeError, ValueError):
            return 0.0

    base_t = f(base, "mprouter_throughput_tps")
    aff_t = f(aff, "mprouter_throughput_tps")
    pct = ((aff_t - base_t) / base_t * 100.0) if base_t > 0 else 0.0
    base_remote = f(base, "cluster_from_remote_ratio")
    aff_remote = f(aff, "cluster_from_remote_ratio")
    base_local = f(base, "cluster_from_local_ratio")
    aff_local = f(aff, "cluster_from_local_ratio")
    out = [
        # Throughput
        f"baseline_throughput_tps={base_t:.6f}",
        f"affinity_throughput_tps={aff_t:.6f}",
        f"throughput_improvement_pct={pct:.2f}",
        # Latency (exec)
        f"baseline_exec_p50_ms={base.get('mprouter_exec_p50_ms', 'missing')}",
        f"affinity_exec_p50_ms={aff.get('mprouter_exec_p50_ms', 'missing')}",
        f"baseline_exec_p99_ms={base.get('mprouter_exec_p99_ms', 'missing')}",
        f"affinity_exec_p99_ms={aff.get('mprouter_exec_p99_ms', 'missing')}",
        # Latency (fetch-to-complete)
        f"baseline_fetch_p50_ms={base.get('mprouter_fetch_p50_ms', 'missing')}",
        f"affinity_fetch_p50_ms={aff.get('mprouter_fetch_p50_ms', 'missing')}",
        f"baseline_fetch_p99_ms={base.get('mprouter_fetch_p99_ms', 'missing')}",
        f"affinity_fetch_p99_ms={aff.get('mprouter_fetch_p99_ms', 'missing')}",
        # Cluster fetch locality (the headline affinity claim).
        f"baseline_cluster_from_remote_ratio={base_remote:.6f}",
        f"affinity_cluster_from_remote_ratio={aff_remote:.6f}",
        f"from_remote_ratio_delta_pp={(aff_remote - base_remote) * 100.0:+.2f}",
        f"baseline_cluster_from_local_ratio={base_local:.6f}",
        f"affinity_cluster_from_local_ratio={aff_local:.6f}",
        f"from_local_ratio_delta_pp={(aff_local - base_local) * 100.0:+.2f}",
        f"baseline_cluster_from_storage_ratio="
        f"{base.get('cluster_from_storage_ratio', 'missing')}",
        f"affinity_cluster_from_storage_ratio="
        f"{aff.get('cluster_from_storage_ratio', 'missing')}",
        # Affinity health (only meaningful on the affinity_on arm).
        f"affinity_edgecut_final_avg="
        f"{aff.get('affinity_edgecut_final_avg', 'missing')}",
        f"affinity_total_edge_weight_final_avg="
        f"{aff.get('affinity_total_edge_weight_final_avg', 'missing')}",
        f"affinity_weighted_cut_ratio_final_avg="
        f"{aff.get('affinity_weighted_cut_ratio_final_avg', 'missing')}",
        f"affinity_migrations_planned_total="
        f"{aff.get('affinity_migrations_planned_total', '0')}",
        f"affinity_migrations_done_total="
        f"{aff.get('affinity_migrations_done_total', '0')}",
        f"affinity_migrations_failed_total="
        f"{aff.get('affinity_migrations_failed_total', '0')}",
        f"affinity_migration_success_ratio="
        f"{aff.get('affinity_migration_success_ratio', 'missing')}",
        f"affinity_migration_failure_ratio="
        f"{aff.get('affinity_migration_failure_ratio', 'missing')}",
        # Router-side reaction to migrations
        f"baseline_page_id_changes="
        f"{base.get('mprouter_page_id_changes', 'missing')}",
        f"affinity_page_id_changes="
        f"{aff.get('mprouter_page_id_changes', 'missing')}",
        f"baseline_page_ops_total="
        f"{base.get('mprouter_page_ops_total', 'missing')}",
        f"affinity_page_ops_total="
        f"{aff.get('mprouter_page_ops_total', 'missing')}",
    ]
    (stamp_dir / "compare_summary.txt").write_text(
        "\n".join(out) + "\n", encoding="utf-8")
    log("compare summary:\n" + "\n".join(out))


def write_schism_compare_summary(stamp_dir: Path, base: dict,
                                  schism: dict, aff: dict) -> None:
    def f(d: dict, k: str) -> float:
        try:
            return float(d.get(k, "0") or "0")
        except (TypeError, ValueError):
            return 0.0

    base_t = f(base, "mprouter_throughput_tps")
    schism_t = f(schism, "mprouter_throughput_tps")
    aff_t = f(aff, "mprouter_throughput_tps")

    def pct_delta(new: float, old: float) -> float:
        return ((new - old) / old * 100.0) if old > 0 else 0.0

    out = [
        f"baseline_throughput_tps={base_t:.6f}",
        f"schism_static_throughput_tps={schism_t:.6f}",
        f"affinity_throughput_tps={aff_t:.6f}",
        f"affinity_vs_schism_throughput_delta_pct="
        f"{pct_delta(aff_t, schism_t):.2f}",
        f"affinity_vs_baseline_throughput_delta_pct="
        f"{pct_delta(aff_t, base_t):.2f}",
        f"schism_vs_baseline_throughput_delta_pct="
        f"{pct_delta(schism_t, base_t):.2f}",
        f"baseline_fetch_p99_ms={base.get('mprouter_fetch_p99_ms', 'missing')}",
        f"schism_static_fetch_p99_ms="
        f"{schism.get('mprouter_fetch_p99_ms', 'missing')}",
        f"affinity_fetch_p99_ms={aff.get('mprouter_fetch_p99_ms', 'missing')}",
        f"baseline_cluster_from_remote_ratio="
        f"{base.get('cluster_from_remote_ratio', 'missing')}",
        f"schism_static_cluster_from_remote_ratio="
        f"{schism.get('cluster_from_remote_ratio', 'missing')}",
        f"affinity_cluster_from_remote_ratio="
        f"{aff.get('cluster_from_remote_ratio', 'missing')}",
        f"schism_static_migrations_planned_total="
        f"{schism.get('affinity_migrations_planned_total', '0')}",
        f"schism_static_migrations_done_total="
        f"{schism.get('affinity_migrations_done_total', '0')}",
        f"schism_static_migrations_failed_total="
        f"{schism.get('affinity_migrations_failed_total', '0')}",
        f"schism_static_migration_success_ratio="
        f"{schism.get('affinity_migration_success_ratio', 'missing')}",
        f"affinity_migrations_planned_total="
        f"{aff.get('affinity_migrations_planned_total', '0')}",
        f"affinity_migrations_done_total="
        f"{aff.get('affinity_migrations_done_total', '0')}",
        f"affinity_migrations_failed_total="
        f"{aff.get('affinity_migrations_failed_total', '0')}",
        f"affinity_migration_success_ratio="
        f"{aff.get('affinity_migration_success_ratio', 'missing')}",
    ]
    (stamp_dir / "schism_compare_summary.txt").write_text(
        "\n".join(out) + "\n", encoding="utf-8"
    )
    log("schism compare summary:\n" + "\n".join(out))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Multi-node MP-Router + Hybrid_Cloud_MP smoke test.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    # Cluster topology.
    p.add_argument("--compute-hosts", default=",".join(DEFAULT_COMPUTE_HOSTS))
    p.add_argument("--service-host", default=DEFAULT_SERVICE_HOST)
    p.add_argument("--user", default=DEFAULT_USER)
    p.add_argument("--password", default=DEFAULT_PASSWORD)
    p.add_argument("--remote-dir", default=DEFAULT_REMOTE_DIR)
    p.add_argument("--mprouter-dir", type=Path, default=DEFAULT_MPROUTER_DIR)

    p.add_argument("--workload", default="smallbank_aff",
                   choices=["smallbank", "smallbank_aff"])

    # MP-Router knobs.
    p.add_argument("--worker-threads", type=int, default=4,
                   help="MP-Router workers per compute (its own --worker-threads).")
    p.add_argument("--try-count", type=int, default=500,
                   help="MP-Router --try-count (per-thread tx target).")
    p.add_argument("--batch-size", type=int, default=200,
                   help="MP-Router router batch size.")
    p.add_argument("--num-bucket", type=int, default=4)
    p.add_argument("--mprouter-system-mode", type=int, default=24,
                   help="MP-Router CLI --system-mode.")
    p.add_argument("--mprouter-zipfian-theta", type=float, default=0.8,
                   help="MP-Router CLI --zipfian-theta.")
    p.add_argument("--mprouter-affinity-txn-ratio", type=float, default=0.5,
                   help="MP-Router CLI --affinity-txn-ratio.")
    p.add_argument("--warmup-rounds", type=int, default=0,
                   help="HYBRID_WARMUP_ROUNDS env var; 0 skips MetisWarmupRound.")

    # Hybrid (storage / compute) knobs — these flow into make_configs(...).
    p.add_argument("--num-accounts", type=int,
                   default=int(os.environ.get(
                       "WOOKONG_SMOKE_NUM_ACCOUNTS", "500000")))
    p.add_argument("--num-hot-accounts", type=int,
                   default=int(os.environ.get(
                       "WOOKONG_SMOKE_NUM_HOT_ACCOUNTS", "100000")))
    p.add_argument("--threads", type=int,
                   default=int(os.environ.get("WOOKONG_SMOKE_THREADS", "1")),
                   help="Hybrid local_compute_node.thread_num_per_machine.")
    p.add_argument("--use-zipfian", type=int, choices=[0, 1], default=1,
                   help="SmallBank config use_zipfian.")
    p.add_argument("--zipf-theta", type=float, default=None,
                   help="SmallBank config zipf_theta (defaults: 0.80 / 0.92).")
    p.add_argument("--partition-cycle-ms", type=int, default=10000)
    p.add_argument("--migration-tick-ms", type=int, default=200)
    p.add_argument("--migration-batch", type=int, default=200)
    p.add_argument("--migration-workers", type=int, default=1,
                   help="Number of MigrationLoop drainer threads per compute node")
    p.add_argument("--edge-min-weight", type=float, default=1.0)
    p.add_argument("--edge-decay-factor", type=float, default=0.5,
                   help="Affinity graph EWMA decay factor.")
    p.add_argument("--repart-itr", type=float, default=5000.0,
                   help="ParMETIS AdaptiveRepart itr tradeoff.")
    p.add_argument("--ubvec", type=float, default=1.20,
                   help="ParMETIS balance tolerance.")
    p.add_argument("--max-changed-vertices-ratio", type=float, default=1.0,
                   help="Reject partition epochs changing more than this ratio; >=1 disables.")
    p.add_argument("--log-flush-interval-ms", type=int, default=3)
    p.add_argument("--log-flush-batch-trigger", type=int, default=16)
    p.add_argument("--log-flush-notify-threshold", type=int, default=4)
    p.add_argument("--push-page-scheduler-threads", type=int, default=4)
    p.add_argument("--log-group-commit-wait-us", type=int, default=-1)
    p.add_argument("--parallel-page-fetch", type=int, default=0)
    p.add_argument("--disable-wal", action="store_true")
    p.add_argument("--random-generate", action="store_true",
                   help="Set storage_node_config.local_storage_node.random_generate=true.")
    p.add_argument("--hot-account-offset", type=int, default=0,
                   help="Hybrid SmallBank hot_account_offset config.")
    # The following are unused by the MP-Router-driven path but make_configs
    # peeks at them; provide harmless defaults.
    p.add_argument("--attempted-num", type=int,
                   default=int(os.environ.get(
                       "WOOKONG_SMOKE_ATTEMPTED_NUM", "5000")))
    p.add_argument("--wr-ratio", type=float, default=0.2)
    p.add_argument("--local-ratio", type=float, default=0.5)

    # Run knobs.
    aff = p.add_mutually_exclusive_group()
    aff.add_argument("--enable-affinity", dest="enable_affinity",
                     action="store_const", const=True,
                     help="Force affinity.enable=true in the generated config.")
    aff.add_argument("--disable-affinity", dest="enable_affinity",
                     action="store_const", const=False,
                     help="Force affinity.enable=false in the generated config.")
    p.set_defaults(enable_affinity=True)

    p.add_argument("--compare", action="store_true",
                   help="Run baseline (affinity off) and affinity_on, "
                        "diff results into compare_summary.txt.")
    p.add_argument("--three-arm", action="store_true",
                   help="Run baseline, schism_static, and affinity_on.")
    p.add_argument("--schism-csv", type=Path, default=None,
                   help="Static Schism assignment CSV to upload for --three-arm.")
    p.add_argument("--schism-apply-ms", type=int, default=60000,
                   help="SCHISM_STATIC_APPLY_MS for the static apply phase.")
    p.add_argument("--schism-train-try-count", type=int, default=5000,
                   help="Reserved training transaction count for graph dumps.")
    p.add_argument("--rebuild", action="store_true",
                   help="rsync + cmake/make Hybrid on every cluster host AND "
                        "rebuild MP-Router locally.")
    p.add_argument("--force-clean", action="store_true",
                   help="Kill leftover db processes on every host if "
                        "preflight finds any.")
    p.add_argument("--keep-cluster", action="store_true",
                   help="Don't tear down the cluster on exit.")
    p.add_argument("--timeout", type=int, default=600,
                   help="MP-Router run timeout in seconds.")
    p.add_argument("--build-type", default="Debug",
                   choices=["Debug", "Release", "RelWithDebInfo"])
    p.add_argument("--result-dir", default=str(DEFAULT_RESULT_DIR))
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    args = parse_args()
    require_local_tools()
    preflight_local(args)

    compute_ips = [h.strip() for h in args.compute_hosts.split(",") if h.strip()]
    if len(compute_ips) != 4:
        log(f"WARN: expected 4 compute hosts, got {len(compute_ips)}; "
            "the script and protocol assume 4-node clusters but will continue.")
    compute_hosts = [
        Host(ip, "compute0" if i == 0 else "compute", args.user, args.password)
        for i, ip in enumerate(compute_ips)
    ]
    service = Host(args.service_host, "service", args.user, args.password)
    all_hosts = compute_hosts + [service]

    stamp_dir = Path(args.result_dir) / time.strftime("%Y%m%d_%H%M%S")
    stamp_dir.mkdir(parents=True, exist_ok=True)
    (stamp_dir / "experiment_args.json").write_text(
        json.dumps(
            {k: (str(v) if isinstance(v, Path) else v)
             for k, v in vars(args).items()},
            indent=2, sort_keys=True,
        ) + "\n",
        encoding="utf-8",
    )
    log(f"results -> {stamp_dir}")

    # Preflight: every cluster host must be clean.
    log("preflight: scanning cluster hosts")
    pre = preflight_remote(all_hosts)
    (stamp_dir / "preflight.txt").write_text(
        "\n\n".join(f"===== {ip} =====\n{text}" for ip, text in pre.items()),
        encoding="utf-8",
    )
    dirty = [ip for ip, text in pre.items() if text.strip()]
    if dirty:
        log("preflight: dirty hosts: " + ", ".join(dirty))
        for ip in dirty:
            for line in pre[ip].splitlines()[:3]:
                log(f"  {ip}: {line}")
        if not args.force_clean:
            log("preflight: rerun with --force-clean to wipe leftovers.")
            return 1
        log("preflight: --force-clean → killing leftovers via kill_cluster")
        kill_cluster(all_hosts)

    if args.rebuild:
        run_parallel(all_hosts, "sync project",
                     lambda h: rsync_project(h, args.remote_dir))
        run_parallel(all_hosts, "build project",
                     lambda h: build_project(h, args.remote_dir, args.build_type))
        log("rebuild: cmake + make in MP-Router local tree")
        mprouter_build = args.mprouter_dir / "build"
        mprouter_build.mkdir(parents=True, exist_ok=True)
        subprocess.run(["cmake", ".."], cwd=mprouter_build, check=True)
        subprocess.run(["make", "-j", "run"], cwd=mprouter_build, check=True)

    hosts_spec = ",".join(
        f"{ip}:{COMPUTE_INTERACTIVE_BASE + i}"
        for i, ip in enumerate(compute_ips)
    )

    def run_case(case_name: str, enable_affinity: bool,
                 compute_env: dict[str, str] | None = None) -> dict:
        log(f"case {case_name}: enable_affinity={enable_affinity}")
        case_dir = stamp_dir / case_name
        case_dir.mkdir(parents=True, exist_ok=True)
        local_log_dir = case_dir / "_local_logs"
        local_log_dir.mkdir(exist_ok=True)

        # Generate + upload per-run configs (per parmetis pattern: never
        # mutate the in-tree config files).
        configs = make_configs(
            compute_ips, args.service_host, args,
            enable_affinity=enable_affinity,
        )
        run_parallel(all_hosts, "upload configs",
                     lambda h: upload_configs(h, args.remote_dir, configs))
        if enable_affinity:
            run_parallel(
                compute_hosts, "install mpi ssh wrapper",
                lambda h: (install_mpi_ssh_wrapper(h, args.password)
                           or "wrapper_installed"),
            )

        boot_cluster(args, all_hosts, compute_hosts, service, compute_env)

        run_dir = args.mprouter_dir / "build" / "serve" / "test"
        rc = run_mprouter(args, run_dir,
                          local_log_dir / "mprouter.log", hosts_spec)

        collect_results(args, compute_hosts, service, run_dir,
                        local_log_dir, case_dir)
        summary = parse_summary(case_dir, compute_hosts)
        summary["mprouter_exit_code"] = str(rc)
        (case_dir / "summary.txt").write_text(
            "\n".join(f"{k}={v}" for k, v in summary.items()) + "\n",
            encoding="utf-8",
        )
        print_summary(case_name, summary)
        log(f"case {case_name}: results saved in {case_dir}")
        return summary

    try:
        if args.three_arm:
            if args.schism_csv is None:
                log("three-arm: --schism-csv is required.")
                return 1
            remote_schism_csv = upload_schism_csv(
                compute_hosts, args, args.schism_csv
            )
            base = run_case("baseline", enable_affinity=False)
            kill_cluster(all_hosts)
            time.sleep(2)
            schism = run_case(
                "schism_static",
                enable_affinity=False,
                compute_env={
                    "SCHISM_STATIC": "1",
                    "SCHISM_STATIC_CSV": remote_schism_csv,
                    "SCHISM_STATIC_APPLY_MS": str(args.schism_apply_ms),
                },
            )
            kill_cluster(all_hosts)
            time.sleep(2)
            aff = run_case("affinity_on", enable_affinity=True)
            write_schism_compare_summary(stamp_dir, base, schism, aff)
        elif args.compare:
            base = run_case("baseline", enable_affinity=False)
            kill_cluster(all_hosts)
            time.sleep(2)
            aff = run_case("affinity_on", enable_affinity=True)
            write_compare_summary(stamp_dir, base, aff)
        else:
            run_case("default", enable_affinity=bool(args.enable_affinity))
    finally:
        if not args.keep_cluster:
            log("teardown: killing cluster processes")
            kill_cluster(all_hosts)
        else:
            log("teardown: --keep-cluster set, leaving cluster alive.")
    log(f"DONE: results saved in {stamp_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
