#!/usr/bin/env python3
"""
Five-node WookongDB MP smoke test for the 10.10.2.31-34 / 10.10.2.38 cluster.

The first hard gate is a ParMETIS/METIS/MPI environment check on every host.
If that passes, the script syncs this workspace to all hosts, builds with
BUILD_PARMETIS_SIDECAR=ON, writes a temporary multi-node config remotely, and
runs a small smallbank_aff lazy workload. Summaries are locality-first:
local_ratio and compute_local_ratio are the primary placement indicators.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import json
import os
import posixpath
import re
import shlex
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import paramiko


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_COMPUTE_HOSTS = ["10.10.2.31", "10.10.2.32", "10.10.2.33", "10.10.2.34"]
DEFAULT_SERVICE_HOST = "10.10.2.38"
DEFAULT_USER = "root"
DEFAULT_PASSWORD = os.environ.get("WOOKONG_SSH_PASS", "wljwlj123")
DEFAULT_REMOTE_DIR = os.environ.get("WOOKONG_REMOTE_DIR", "/usr/local/exper/Hybrid_Cloud_MP")
DEFAULT_RESULT_DIR = ROOT / "result" / "multinode_parmetis_smoke"


@dataclass(frozen=True)
class Host:
    ip: str
    role: str
    user: str
    password: str
    port: int = 22


def log(msg: str) -> None:
    print(time.strftime("[%Y-%m-%d %H:%M:%S] ") + msg, flush=True)


EXECUTED_TXN_RE = re.compile(r"Executed Txn Cnt\s*=\s*(\d+)")


def parse_executed_txn_count(text: str) -> int:
    matches = EXECUTED_TXN_RE.findall(text)
    return int(matches[-1]) if matches else 0


def parse_key_value_lines(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in text.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key:
            values[key] = value.strip()
    return values


def format_progress_line(progress: dict[str, dict[str, str]], elapsed_s: int, timeout_s: int) -> str:
    parts = [f"compute progress: elapsed={elapsed_s}/{timeout_s}s"]
    for ip in sorted(progress):
        kv = progress[ip]
        state = "run" if kv.get("running") == "1" else "done"
        pid = kv.get("pid") or "-"
        txn = kv.get("txn", "0")
        result = kv.get("result_ready", "0")
        ts_rows = kv.get("timeseries_rows", "0")
        log_age = kv.get("log_age_s", "-1")
        parts.append(
            f"{ip}:{state} pid={pid} txn={txn} result={result} "
            f"ts_rows={ts_rows} log_age={log_age}s"
        )
    return " | ".join(parts)


def ssh(host: Host) -> paramiko.SSHClient:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        hostname=host.ip,
        port=host.port,
        username=host.user,
        password=host.password,
        timeout=20,
        banner_timeout=60,
        auth_timeout=20,
    )
    return client


def run_ssh(host: Host, cmd: str, *, timeout: int | None = None, check: bool = True) -> tuple[int, str, str]:
    client = ssh(host)
    try:
        stdin, stdout, stderr = client.exec_command(cmd, timeout=timeout)
        out = stdout.read().decode("utf-8", errors="replace")
        err = stderr.read().decode("utf-8", errors="replace")
        rc = stdout.channel.recv_exit_status()
        if check and rc != 0:
            raise RuntimeError(f"{host.ip} rc={rc}\nCMD: {cmd}\nSTDOUT:\n{out}\nSTDERR:\n{err}")
        return rc, out, err
    finally:
        client.close()


def sftp_write_text(host: Host, remote_path: str, text: str) -> None:
    client = ssh(host)
    try:
        run_ssh(host, f"mkdir -p {shlex.quote(posixpath.dirname(remote_path))}", check=True)
        sftp = client.open_sftp()
        try:
            with sftp.open(remote_path, "w") as fh:
                fh.write(text)
        finally:
            sftp.close()
    finally:
        client.close()


def sftp_get_if_exists(host: Host, remote_path: str, local_path: Path) -> bool:
    client = ssh(host)
    try:
        sftp = client.open_sftp()
        try:
            try:
                sftp.stat(remote_path)
            except FileNotFoundError:
                return False
            local_path.parent.mkdir(parents=True, exist_ok=True)
            sftp.get(remote_path, str(local_path))
            return True
        finally:
            sftp.close()
    finally:
        client.close()


def require_local_tools() -> None:
    missing = [tool for tool in ["rsync", "sshpass"] if shutil.which(tool) is None]
    if missing:
        raise RuntimeError("local missing tools: " + ", ".join(missing))


def parmetis_check(host: Host) -> str:
    extra_tools = " sshpass" if host.role == "compute0" else ""
    check_src = r'''
set -euo pipefail
echo "host=$(hostname) ip=''' + shlex.quote(host.ip) + r'''"
for c in gcc g++ cmake make mpicc mpicxx mpirun ssh''' + extra_tools + r'''; do
  printf "%-8s" "$c"; command -v "$c"
done
test -r /usr/include/parmetis.h
test -r /usr/include/metis.h
ldconfig -p 2>/dev/null | grep -E 'libparmetis\.so|libmetis\.so' >/dev/null
cat >/tmp/wookong_check_parmetis.c <<'EOF'
#include <parmetis.h>
#include <metis.h>
int main(void) { return 0; }
EOF
mpicc /tmp/wookong_check_parmetis.c -o /tmp/wookong_check_parmetis -lparmetis -lmetis
/tmp/wookong_check_parmetis
echo "parmetis_env=ok"
'''
    _, out, err = run_ssh(host, "bash -lc " + shlex.quote(check_src), timeout=60)
    return out + err


def run_parallel(hosts: list[Host], title: str, worker) -> dict[str, str]:
    log(f"{title}: start")
    results: dict[str, str] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(hosts)) as pool:
        futs = {pool.submit(worker, host): host for host in hosts}
        for fut in concurrent.futures.as_completed(futs):
            host = futs[fut]
            try:
                results[host.ip] = fut.result()
                log(f"{title}: {host.ip} ok")
            except Exception as exc:
                log(f"{title}: {host.ip} failed: {exc}")
                raise
    log(f"{title}: done")
    return results


def rsync_project(host: Host, remote_dir: str) -> str:
    parent = posixpath.dirname(remote_dir.rstrip("/"))
    run_ssh(host, f"mkdir -p {shlex.quote(parent)}", timeout=30)
    cmd = [
        "sshpass", "-p", host.password,
        "rsync", "-az", "--delete",
        "--exclude", ".git",
        "--exclude", "build",
        "--exclude", "result",
        "--exclude", "third_party",
        "-e", f"ssh -p {host.port} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null",
        str(ROOT) + "/",
        f"{host.user}@{host.ip}:{remote_dir}/",
    ]
    proc = subprocess.run(cmd, text=True, capture_output=True)
    if proc.returncode != 0:
        raise RuntimeError(f"rsync failed rc={proc.returncode}\n{proc.stdout}\n{proc.stderr}")
    return "synced"


def make_configs(
    compute_hosts: list[str],
    service_host: str,
    args: argparse.Namespace,
    *,
    enable_affinity: bool,
) -> dict[str, str]:
    compute_cfg = {
        "local_compute_node": {
            "machine_id": 0,
            "parallel_page_fetch": args.parallel_page_fetch,
            "thread_num_per_machine": args.threads,
            "coroutine_num": 1,
            "txn_system": 1,
            "batch_size": 400,
            "generate_log": 0 if args.disable_wal else 1,
        },
        "table_buffer_pool_size_per_table": 40000,
        "index_buffer_pool_size_per_table": 10000,
        "fsm_buffer_pool_size_per_table": 2000,
        "partition_size_per_table": 400,
        "log_flush_interval_ms": args.log_flush_interval_ms,
        "log_flush_batch_trigger": args.log_flush_batch_trigger,
        "log_flush_notify_threshold": args.log_flush_notify_threshold,
        "push_page_with_scheduler": 1,
        "push_page_scheduler_threads": getattr(args, "push_page_scheduler_threads", 4),
        "ts_time": 100000,
        "remote_compute_nodes": {
            "remote_compute_node_ips": compute_hosts,
            "remote_compute_node_port": [28688 + i for i in range(len(compute_hosts))],
        },
        "remote_server_nodes": {
            "remote_server_node_ips": [service_host],
            "remote_server_node_port": [31508],
            "remote_server_node_meta_port": [31509],
        },
        "remote_storage_nodes": {
            "remote_storage_node_ips": [service_host],
            "remote_storage_node_rpc_port": [15980],
            "remote_storage_node_meta_port": [15981],
        },
        "affinity": {
            "enable": enable_affinity,
            "aggregator_tick_ms": 50,
            "partition_cycle_ms": args.partition_cycle_ms,
            "migration_tick_ms": args.migration_tick_ms,
            "migration_batch": args.migration_batch,
            "migration_workers": args.migration_workers,
            "edge_min_weight": args.edge_min_weight,
            "edge_decay_factor": 0.5,
            "assignment_ttl_epochs": 30,
            "max_vertices": 5000000,
            "shuffle_barrier_ms": 30000,
            "uds_recv_timeout_ms": 30000,
            "repart_itr": 5000.0,
            "ubvec": 1.20,
            "sidecar_uds_path": "/tmp/wookong_parmetis.sock",
            "timeseries_tick_ms": 1000,
            "timeseries_csv_path": "affinity_timeseries.csv",
            "auto_spawn_sidecar": 1,
            "sidecar_binary_path": "../parmetis_sidecar/parmetis_sidecar",
            "sidecar_hostfile_path": "/tmp/wookong_affinity_hostfile",
            "sidecar_mpirun_bin": "mpirun",
            "sidecar_mpirun_extra_args": "--allow-run-as-root --mca plm_rsh_agent /tmp/wookong_mpi_ssh",
            "sidecar_log_path": "affinity_sidecar.log",
        },
    }
    storage_cfg = {
        "local_storage_node": {
            "machine_num": 1,
            "machine_id": 0,
            "local_rpc_port": 15980,
            "local_meta_port": 15981,
            "use_rdma": False,
            "random_generate": False,
        }
    }
    remote_cfg = {
        "local_server_node": {
            "local_rpc_port": 31508,
            "local_meta_port": 31509,
        }
    }
    smallbank_zipf_theta = args.zipf_theta if args.zipf_theta is not None else 0.80
    smallbank_cfg = {
        "smallbank": {
            "num_accounts": args.num_accounts,
            "num_hot_accounts": min(args.num_hot_accounts, args.num_accounts),
            "attempted_num": args.attempted_num,
            "num_hot_rate": 50,
            "use_zipfian": args.use_zipfian,
            "zipf_theta": smallbank_zipf_theta,
        }
    }
    smallbank_aff_zipf_theta = args.zipf_theta if args.zipf_theta is not None else 0.92
    smallbank_aff_cfg = {
        "smallbank_aff": {
            "num_accounts": args.num_accounts,
            "num_hot_accounts": min(args.num_hot_accounts, args.num_accounts),
            "attempted_num": args.attempted_num,
            "num_hot_rate": 50,
            "use_zipfian": args.use_zipfian,
            "zipf_theta": smallbank_aff_zipf_theta,
            "affinity_txn_ratio": 0.98,
            "friend_degree_min": 4,
            "friend_degree_max": 6,
            "affinity_graph_mode": "interleaved_hub",
            "affinity_group_count": 64,
            "affinity_group_hubs": 2,
            "affinity_hub_weight": 0.9,
            "override_workgen": True,
            "freq_amalgamate": 40,
            "freq_balance": 5,
            "freq_deposit_checking": 5,
            "freq_send_payment": 40,
            "freq_transact_saving": 5,
            "freq_write_check": 5,
        }
    }
    return {
        "compute_node_config.json": json.dumps(compute_cfg, indent=2) + "\n",
        "storage_node_config.json": json.dumps(storage_cfg, indent=2) + "\n",
        "remote_server_config.json": json.dumps(remote_cfg, indent=2) + "\n",
        "smallbank_config.json": json.dumps(smallbank_cfg, indent=2) + "\n",
        "smallbank_aff_config.json": json.dumps(smallbank_aff_cfg, indent=2) + "\n",
    }


def upload_configs(host: Host, remote_dir: str, configs: dict[str, str]) -> str:
    for name, text in configs.items():
        sftp_write_text(host, posixpath.join(remote_dir, "config", name), text)
    return "configs_uploaded"


def install_mpi_ssh_wrapper(host: Host, password: str) -> None:
    script = (
        "#!/bin/sh\n"
        "exec sshpass -p " + shlex.quote(password) +
        " ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \"$@\"\n"
    )
    sftp_write_text(host, "/tmp/wookong_mpi_ssh", script)
    run_ssh(host, "chmod 700 /tmp/wookong_mpi_ssh", timeout=10)


def build_project(host: Host, remote_dir: str, build_type: str = "Debug") -> str:
    cmd = f"""
set -euo pipefail
cd {shlex.quote(remote_dir)}
rm -rf build
mkdir -p build
cmake -S . -B build -DCMAKE_BUILD_TYPE={shlex.quote(build_type)} -DBUILD_PARMETIS_SIDECAR=ON
cmake --build build -j$(nproc)
test -x build/parmetis_sidecar/parmetis_sidecar
test -x build/compute_server/compute_server
test -x build/storage_server/storage_pool
test -x build/remote_server/remote_node
"""
    _, out, err = run_ssh(host, "bash -lc " + shlex.quote(cmd), timeout=900)
    return (out + err)[-4000:]


def kill_cluster(hosts: list[Host]) -> None:
    def kill_one(host: Host) -> str:
        run_ssh(
            host,
            "pkill -TERM -f 'compute_server|storage_pool|remote_node|parmetis_sidecar|mpirun' || true; "
            "sleep 1; "
            "pkill -KILL -f 'compute_server|storage_pool|remote_node|parmetis_sidecar|mpirun' || true",
            timeout=20,
            check=False,
        )
        return "killed"

    run_parallel(hosts, "cleanup processes", kill_one)


def wait_remote(host: Host, predicate_cmd: str, timeout_s: int, label: str) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        rc, _, _ = run_ssh(host, predicate_cmd, timeout=10, check=False)
        if rc == 0:
            return
        time.sleep(1)
    raise TimeoutError(f"timeout waiting for {label} on {host.ip}")


def start_services(service: Host, remote_dir: str, workload: str, args: argparse.Namespace) -> None:
    group_commit_env = ""
    if args.log_group_commit_wait_us >= 0:
        group_commit_env = f"export LOG_GROUP_COMMIT_WAIT_US={int(args.log_group_commit_wait_us)}\n"
    cmd = f"""
set -euo pipefail
cd {shlex.quote(remote_dir)}/build/storage_server
rm -rf {shlex.quote(workload)} LOG_FILE storage_timing_stats.txt
{group_commit_env}\
nohup ./storage_pool {shlex.quote(workload)} > storage_smoke.log 2>&1 < /dev/null &
echo $! >/tmp/wookong_storage.pid
cd {shlex.quote(remote_dir)}/build/remote_server
rm -f remote_smoke.log
nohup ./remote_node {shlex.quote(workload)} > remote_smoke.log 2>&1 < /dev/null &
echo $! >/tmp/wookong_remote.pid
"""
    run_ssh(service, "bash -lc " + shlex.quote(cmd), timeout=20)
    wait_remote(service, "ss -ltn | grep -q ':15980 '", 120, "storage rpc port")
    wait_remote(service, "ss -ltn | grep -q ':31508 ' && ss -ltn | grep -q ':31509 '", 60, "remote ports")


def start_compute(host: Host, remote_dir: str, workload: str, node_id: int, args: argparse.Namespace) -> None:
    cmd = f"""
set -euo pipefail
cd {shlex.quote(remote_dir)}/build/compute_server
rm -f result.txt result.node*.txt affinity_timeseries*.csv affinity_sidecar.log compute_smoke_{node_id}.log
export OMPI_ALLOW_RUN_AS_ROOT=1
export OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1
export WOOKONG_CPU_OFFSET_BY_MACHINE=1
nohup ./compute_server {shlex.quote(workload)} lazy {int(args.threads)} {float(args.wr_ratio)} {float(args.local_ratio)} {int(node_id)} > compute_smoke_{node_id}.log 2>&1 < /dev/null &
echo $! >/tmp/wookong_compute_{node_id}.pid
"""
    run_ssh(host, "bash -lc " + shlex.quote(cmd), timeout=20)


def remote_compute_progress(host: Host, remote_dir: str, node_id: int) -> dict[str, str]:
    cmd = f"""
set +e
cd {shlex.quote(remote_dir)}/build/compute_server || exit 0
log=compute_smoke_{int(node_id)}.log
ts=affinity_timeseries.{int(node_id)}.csv
now=$(date +%s)
pid=$(pgrep -x compute_server | paste -sd, -)
running=0
if [ -n "$pid" ]; then running=1; fi
txn=$(grep -aoE 'Executed Txn Cnt = [0-9]+' "$log" 2>/dev/null | tail -1 | awk '{{print $5}}')
if [ -z "$txn" ]; then txn=0; fi
result_ready=0
if [ -s result.txt ] || [ -s result.node{int(node_id)}.txt ]; then result_ready=1; fi
timeseries_rows=0
if [ -f "$ts" ]; then timeseries_rows=$(wc -l < "$ts" 2>/dev/null || echo 0); fi
log_age_s=-1
log_size=0
if [ -f "$log" ]; then
  log_mtime=$(stat -c %Y "$log" 2>/dev/null || echo "$now")
  log_age_s=$((now - log_mtime))
  log_size=$(stat -c %s "$log" 2>/dev/null || echo 0)
fi
echo "running=$running"
echo "pid=$pid"
echo "txn=$txn"
echo "result_ready=$result_ready"
echo "timeseries_rows=$timeseries_rows"
echo "log_age_s=$log_age_s"
echo "log_size=$log_size"
"""
    _, out, err = run_ssh(host, "bash -lc " + shlex.quote(cmd), timeout=10, check=False)
    kv = parse_key_value_lines(out + err)
    kv.setdefault("running", "0")
    kv.setdefault("pid", "")
    kv.setdefault("txn", "0")
    kv.setdefault("result_ready", "0")
    kv.setdefault("timeseries_rows", "0")
    kv.setdefault("log_age_s", "-1")
    return kv


def collect_compute_progress(
    compute_hosts: list[Host],
    remote_dir: str,
) -> dict[str, dict[str, str]]:
    progress: dict[str, dict[str, str]] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(compute_hosts)) as pool:
        futs = {
            pool.submit(remote_compute_progress, host, remote_dir, idx): host
            for idx, host in enumerate(compute_hosts)
        }
        for fut in concurrent.futures.as_completed(futs):
            host = futs[fut]
            try:
                progress[host.ip] = fut.result()
            except Exception as exc:
                progress[host.ip] = {
                    "running": "0",
                    "pid": "",
                    "txn": "0",
                    "result_ready": "0",
                    "timeseries_rows": "0",
                    "log_age_s": "-1",
                    "error": str(exc),
                }
    return progress


def wait_computes(
    compute_hosts: list[Host],
    remote_dir: str,
    timeout_s: int,
    progress_interval_s: int,
) -> None:
    started = time.time()
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        progress = collect_compute_progress(compute_hosts, remote_dir)
        running = [ip for ip, kv in progress.items() if kv.get("running") == "1"]
        elapsed_s = int(time.time() - started)
        log(format_progress_line(progress, elapsed_s, timeout_s))
        if not running:
            return
        time.sleep(max(progress_interval_s, 1))
    progress = collect_compute_progress(compute_hosts, remote_dir)
    raise TimeoutError(
        "compute_server did not finish before timeout; "
        + format_progress_line(progress, int(time.time() - started), timeout_s)
    )


def collect_results(hosts: list[Host], remote_dir: str, out_dir: Path) -> dict[str, dict[str, str]]:
    summary: dict[str, dict[str, str]] = {}
    for idx, host in enumerate(hosts):
        node_dir = out_dir / f"node{idx}_{host.ip}"
        for remote_name in [
            "result.txt",
            f"compute_smoke_{idx}.log",
            "affinity_sidecar.log",
            "affinity_timeseries.csv",
            "affinity_timeseries.0.csv",
            "affinity_timeseries.1.csv",
            "affinity_timeseries.2.csv",
            "affinity_timeseries.3.csv",
        ]:
            sftp_get_if_exists(
                host,
                posixpath.join(remote_dir, "build", "compute_server", remote_name),
                node_dir / remote_name,
            )
        result = node_dir / "result.txt"
        kv: dict[str, str] = {}
        if result.exists():
            for line in result.read_text(encoding="utf-8", errors="replace").splitlines():
                if "=" in line:
                    k, v = line.split("=", 1)
                    kv[k.strip()] = v.strip()
        kv.update(parse_timeseries_metrics(node_dir))
        summary[host.ip] = kv
    service_dir = out_dir / "service"
    service_dir.mkdir(parents=True, exist_ok=True)
    return summary


def collect_service_logs(service: Host, remote_dir: str, out_dir: Path) -> None:
    service_dir = out_dir / "service"
    for remote_path, local_name in [
        (posixpath.join(remote_dir, "build", "storage_server", "storage_smoke.log"), "storage_smoke.log"),
        (posixpath.join(remote_dir, "build", "storage_server", "storage_timing_stats.txt"), "storage_timing_stats.txt"),
        (posixpath.join(remote_dir, "build", "remote_server", "remote_smoke.log"), "remote_smoke.log"),
        (posixpath.join(remote_dir, "build", "remote_server", "LOG.log"), "remote_LOG.log"),
    ]:
        sftp_get_if_exists(service, remote_path, service_dir / local_name)


def get_float(kv: dict[str, str], key: str) -> float:
    try:
        return float(kv.get(key, "0").split()[0])
    except (ValueError, IndexError):
        return 0.0


def get_int(kv: dict[str, str], key: str) -> int:
    try:
        return int(float(kv.get(key, "0").split()[0]))
    except (ValueError, IndexError):
        return 0


def avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def parse_csv_int(row: dict[str, str], key: str) -> int:
    try:
        return int(float(row.get(key, "0") or "0"))
    except (ValueError, TypeError):
        return 0


def parse_timeseries_metrics(node_dir: Path) -> dict[str, str]:
    paths = sorted(node_dir.glob("affinity_timeseries.*.csv"))
    plain = node_dir / "affinity_timeseries.csv"
    if plain.exists():
        paths.append(plain)

    rows: list[dict[str, str]] = []
    for path in paths:
        try:
            with path.open("r", encoding="utf-8", errors="replace", newline="") as fh:
                rows = list(csv.DictReader(fh))
        except OSError:
            rows = []
        if rows:
            break
    if not rows:
        return {}

    edge_rows = [
        row for row in rows
        if parse_csv_int(row, "partition_runs") > 0 and parse_csv_int(row, "n_edges") > 0
    ]
    if not edge_rows:
        edge_rows = [row for row in rows if parse_csv_int(row, "n_edges") > 0]
    if not edge_rows:
        edge_rows = rows

    edgecuts = [parse_csv_int(row, "edgecut") for row in edge_rows]
    cut_ratios = [
        (parse_csv_int(row, "edgecut") / parse_csv_int(row, "n_edges"))
        for row in edge_rows
        if parse_csv_int(row, "n_edges") > 0
    ]
    steady_count = min(5, len(edgecuts))
    steady_ratios = cut_ratios[-min(5, len(cut_ratios)):] if cut_ratios else []
    last = rows[-1]

    return {
        "affinity_timeseries_rows": str(len(rows)),
        "affinity_timeseries_active_rows": str(len(edge_rows)),
        "affinity_edgecut_first": str(edgecuts[0] if edgecuts else 0),
        "affinity_edgecut_final": str(edgecuts[-1] if edgecuts else 0),
        "affinity_edgecut_best": str(min(edgecuts) if edgecuts else 0),
        "affinity_edgecut_steady_last5_avg": f"{avg([float(x) for x in edgecuts[-steady_count:]]):.6f}",
        "affinity_cut_ratio_first": f"{cut_ratios[0] if cut_ratios else 0.0:.6f}",
        "affinity_cut_ratio_final": f"{cut_ratios[-1] if cut_ratios else 0.0:.6f}",
        "affinity_cut_ratio_best": f"{min(cut_ratios) if cut_ratios else 0.0:.6f}",
        "affinity_cut_ratio_steady_last5_avg": f"{avg(steady_ratios):.6f}",
        "affinity_timeseries_final_n_vertices": str(parse_csv_int(last, "n_vertices")),
        "affinity_timeseries_final_n_edges": str(parse_csv_int(last, "n_edges")),
        "affinity_timeseries_planned_delta_sum": str(
            sum(parse_csv_int(row, "migrations_planned_delta") for row in rows)
        ),
        "affinity_timeseries_done_delta_sum": str(
            sum(parse_csv_int(row, "migrations_done_delta") for row in rows)
        ),
        "affinity_timeseries_failed_delta_sum": str(
            sum(parse_csv_int(row, "migrations_failed_delta") for row in rows)
        ),
        "affinity_timeseries_edges_pruned_sum": str(
            sum(parse_csv_int(row, "edges_pruned_min_weight_delta") for row in rows)
        ),
    }


def ratios_from_counts(remote: float, storage: float, local: float) -> dict[str, float]:
    total = remote + storage + local
    compute_total = remote + local
    return {
        "remote_ratio": remote / total if total > 0 else 0.0,
        "storage_ratio": storage / total if total > 0 else 0.0,
        "local_ratio": local / total if total > 0 else 0.0,
        "compute_local_ratio": local / compute_total if compute_total > 0 else 0.0,
        "compute_remote_ratio": remote / compute_total if compute_total > 0 else 0.0,
    }


def summary_counts(summary: dict[str, dict[str, str]]) -> dict[str, float]:
    planned = sum(get_float(kv, "affinity_migrations_planned") for kv in summary.values())
    done = sum(get_float(kv, "affinity_migrations_done") for kv in summary.values())
    failed = sum(get_float(kv, "affinity_migrations_failed") for kv in summary.values())
    partition_runs_sum = sum(get_float(kv, "affinity_partition_runs") for kv in summary.values())
    partition_total_ms = sum(get_float(kv, "affinity_partition_total_ms") for kv in summary.values())
    edgecut_final = [get_float(kv, "affinity_edgecut_final") for kv in summary.values() if "affinity_edgecut_final" in kv]
    edgecut_best = [get_float(kv, "affinity_edgecut_best") for kv in summary.values() if "affinity_edgecut_best" in kv]
    edgecut_steady = [
        get_float(kv, "affinity_edgecut_steady_last5_avg")
        for kv in summary.values()
        if "affinity_edgecut_steady_last5_avg" in kv
    ]
    cut_ratio_final = [
        get_float(kv, "affinity_cut_ratio_final")
        for kv in summary.values()
        if "affinity_cut_ratio_final" in kv
    ]
    cut_ratio_best = [
        get_float(kv, "affinity_cut_ratio_best")
        for kv in summary.values()
        if "affinity_cut_ratio_best" in kv
    ]
    return {
        "cluster_throughput_sum": sum(get_float(kv, "throughput") for kv in summary.values()),
        "max_affinity_partition_runs": max(
            [get_float(kv, "affinity_partition_runs") for kv in summary.values()] or [0.0]
        ),
        "sum_affinity_partition_runs": partition_runs_sum,
        "sum_affinity_partition_total_ms": partition_total_ms,
        "avg_affinity_partition_ms_per_run": (
            partition_total_ms / partition_runs_sum if partition_runs_sum > 0 else 0.0
        ),
        "sum_affinity_migrations_planned": planned,
        "sum_affinity_migrations_done": done,
        "sum_affinity_migrations_failed": failed,
        "sum_affinity_migration_backlog": max(planned - done, 0.0),
        "affinity_migration_success_ratio": done / planned if planned > 0 else 0.0,
        "affinity_edgecut_final_avg": avg(edgecut_final),
        "affinity_edgecut_best_avg": avg(edgecut_best),
        "affinity_edgecut_steady_last5_avg": avg(edgecut_steady),
        "affinity_cut_ratio_final_avg": avg(cut_ratio_final),
        "affinity_cut_ratio_best_avg": avg(cut_ratio_best),
        "sum_fetch_remote": sum(get_float(kv, "fetch_from_remote_count") for kv in summary.values()),
        "sum_fetch_storage": sum(get_float(kv, "fetch_from_storage_count") for kv in summary.values()),
        "sum_fetch_local": sum(get_float(kv, "fetch_from_local_count") for kv in summary.values()),
        "sum_ownership_transfer_count": sum(get_float(kv, "ownership_transfer_count") for kv in summary.values()),
        "sum_ownership_transfer_time_total": sum(get_float(kv, "ownership_transfer_time_total") for kv in summary.values()),
        "sum_ownership_transfer_direct_count": sum(get_float(kv, "ownership_transfer_direct_count") for kv in summary.values()),
        "sum_ownership_transfer_wait_count": sum(get_float(kv, "ownership_transfer_wait_count") for kv in summary.values()),
        "sum_ownership_transfer_storage_fetch_count": sum(get_float(kv, "ownership_transfer_storage_fetch_count") for kv in summary.values()),
        "sum_ownership_transfer_push_wait_count": sum(get_float(kv, "ownership_transfer_push_wait_count") for kv in summary.values()),
        "sum_ownership_transfer_push_forward_count": sum(get_float(kv, "ownership_transfer_push_forward_count") for kv in summary.values()),
        "sum_ownership_transfer_lock_request_time": sum(get_float(kv, "ownership_transfer_lock_request_time") for kv in summary.values()),
        "sum_ownership_transfer_wait_lock_success_time": sum(get_float(kv, "ownership_transfer_wait_lock_success_time") for kv in summary.values()),
        "sum_ownership_transfer_wait_push_page_time": sum(get_float(kv, "ownership_transfer_wait_push_page_time") for kv in summary.values()),
        "sum_ownership_transfer_storage_fetch_time": sum(get_float(kv, "ownership_transfer_storage_fetch_time") for kv in summary.values()),
        "sum_ownership_transfer_push_forward_time": sum(get_float(kv, "ownership_transfer_push_forward_time") for kv in summary.values()),
        "sum_ownership_transfer_other_time": sum(get_float(kv, "ownership_transfer_other_time") for kv in summary.values()),
    }


def write_cluster_summary(summary: dict[str, dict[str, str]], out_dir: Path) -> None:
    metrics = summary_counts(summary)
    ratios = ratios_from_counts(
        metrics["sum_fetch_remote"],
        metrics["sum_fetch_storage"],
        metrics["sum_fetch_local"],
    )

    lines = [
        f"node_count={len(summary)}",
        f"cluster_throughput_sum={metrics['cluster_throughput_sum']:.6f}",
        f"cluster_local_ratio={ratios['local_ratio']:.6f}",
        f"cluster_compute_local_ratio={ratios['compute_local_ratio']:.6f}",
        f"cluster_storage_ratio={ratios['storage_ratio']:.6f}",
        f"max_affinity_partition_runs={metrics['max_affinity_partition_runs']:.0f}",
        f"avg_affinity_partition_ms_per_run={metrics['avg_affinity_partition_ms_per_run']:.3f}",
        f"affinity_edgecut_final_avg={metrics['affinity_edgecut_final_avg']:.3f}",
        f"affinity_edgecut_best_avg={metrics['affinity_edgecut_best_avg']:.3f}",
        f"affinity_edgecut_steady_last5_avg={metrics['affinity_edgecut_steady_last5_avg']:.3f}",
        f"affinity_cut_ratio_final_avg={metrics['affinity_cut_ratio_final_avg']:.6f}",
        f"affinity_cut_ratio_best_avg={metrics['affinity_cut_ratio_best_avg']:.6f}",
        f"sum_affinity_migrations_planned={metrics['sum_affinity_migrations_planned']:.0f}",
        f"sum_affinity_migrations_done={metrics['sum_affinity_migrations_done']:.0f}",
        f"sum_affinity_migrations_failed={metrics['sum_affinity_migrations_failed']:.0f}",
        f"sum_affinity_migration_backlog={metrics['sum_affinity_migration_backlog']:.0f}",
        f"affinity_migration_success_ratio={metrics['affinity_migration_success_ratio']:.6f}",
        f"diagnostic_cluster_remote_ratio={ratios['remote_ratio']:.6f}",
        f"ownership_transfer_count={metrics['sum_ownership_transfer_count']:.0f}",
        f"ownership_transfer_time_total={metrics['sum_ownership_transfer_time_total']:.6f}",
        f"ownership_transfer_avg_ms={(metrics['sum_ownership_transfer_time_total'] * 1000.0 / metrics['sum_ownership_transfer_count'] if metrics['sum_ownership_transfer_count'] > 0 else 0.0):.6f}",
        f"ownership_transfer_direct_count={metrics['sum_ownership_transfer_direct_count']:.0f}",
        f"ownership_transfer_wait_count={metrics['sum_ownership_transfer_wait_count']:.0f}",
        f"ownership_transfer_storage_fetch_count={metrics['sum_ownership_transfer_storage_fetch_count']:.0f}",
        f"ownership_transfer_push_wait_count={metrics['sum_ownership_transfer_push_wait_count']:.0f}",
        f"ownership_transfer_push_forward_count={metrics['sum_ownership_transfer_push_forward_count']:.0f}",
        f"ownership_transfer_lock_request_time={metrics['sum_ownership_transfer_lock_request_time']:.6f}",
        f"ownership_transfer_wait_lock_success_time={metrics['sum_ownership_transfer_wait_lock_success_time']:.6f}",
        f"ownership_transfer_wait_push_page_time={metrics['sum_ownership_transfer_wait_push_page_time']:.6f}",
        f"ownership_transfer_storage_fetch_time={metrics['sum_ownership_transfer_storage_fetch_time']:.6f}",
        f"ownership_transfer_push_forward_time={metrics['sum_ownership_transfer_push_forward_time']:.6f}",
        f"ownership_transfer_other_time={metrics['sum_ownership_transfer_other_time']:.6f}",
    ]
    for ip, kv in summary.items():
        node_ratios = ratios_from_counts(
            get_float(kv, "fetch_from_remote_count"),
            get_float(kv, "fetch_from_storage_count"),
            get_float(kv, "fetch_from_local_count"),
        )
        planned = get_float(kv, "affinity_migrations_planned")
        done = get_float(kv, "affinity_migrations_done")
        runs = get_float(kv, "affinity_partition_runs")
        part_total_ms = get_float(kv, "affinity_partition_total_ms")
        lines.append(f"{ip}.throughput={kv.get('throughput', 'missing')}")
        lines.append(f"{ip}.local_ratio={node_ratios['local_ratio']:.6f}")
        lines.append(f"{ip}.compute_local_ratio={node_ratios['compute_local_ratio']:.6f}")
        lines.append(f"{ip}.storage_ratio={node_ratios['storage_ratio']:.6f}")
        lines.append(f"{ip}.affinity_partition_runs={kv.get('affinity_partition_runs', 'missing')}")
        lines.append(f"{ip}.affinity_partition_avg_ms={(part_total_ms / runs if runs > 0 else 0.0):.3f}")
        lines.append(f"{ip}.affinity_edgecut_final={kv.get('affinity_edgecut_final', kv.get('affinity_last_edgecut', 'missing'))}")
        lines.append(f"{ip}.affinity_edgecut_best={kv.get('affinity_edgecut_best', 'missing')}")
        lines.append(f"{ip}.affinity_cut_ratio_final={kv.get('affinity_cut_ratio_final', 'missing')}")
        lines.append(f"{ip}.affinity_migrations_done={done:.0f}")
        lines.append(f"{ip}.affinity_migrations_failed={get_float(kv, 'affinity_migrations_failed'):.0f}")
        lines.append(f"{ip}.affinity_migration_backlog={max(planned - done, 0.0):.0f}")
        lines.append(f"{ip}.ownership_transfer_count={get_float(kv, 'ownership_transfer_count'):.0f}")
        lines.append(f"{ip}.ownership_transfer_time_total={get_float(kv, 'ownership_transfer_time_total'):.6f}")
        lines.append(f"{ip}.ownership_transfer_lock_request_time={get_float(kv, 'ownership_transfer_lock_request_time'):.6f}")
        lines.append(f"{ip}.ownership_transfer_wait_lock_success_time={get_float(kv, 'ownership_transfer_wait_lock_success_time'):.6f}")
        lines.append(f"{ip}.ownership_transfer_wait_push_page_time={get_float(kv, 'ownership_transfer_wait_push_page_time'):.6f}")
        lines.append(f"{ip}.ownership_transfer_storage_fetch_time={get_float(kv, 'ownership_transfer_storage_fetch_time'):.6f}")
        lines.append(f"{ip}.ownership_transfer_push_forward_time={get_float(kv, 'ownership_transfer_push_forward_time'):.6f}")
        lines.append(f"{ip}.ownership_transfer_other_time={get_float(kv, 'ownership_transfer_other_time'):.6f}")
    (out_dir / "summary.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    log("summary:\n" + "\n".join(lines))


def summarize_metrics(summary: dict[str, dict[str, str]]) -> dict[str, float]:
    return summary_counts(summary)


def write_compare_summary(case_metrics: dict[str, dict[str, float]], out_dir: Path) -> None:
    base = case_metrics.get("baseline", {})
    aff = case_metrics.get("affinity_on", {})
    base_tput = base.get("cluster_throughput_sum", 0.0)
    aff_tput = aff.get("cluster_throughput_sum", 0.0)
    tput_improve = ((aff_tput - base_tput) / base_tput * 100.0) if base_tput > 0 else 0.0

    base_ratios = ratios_from_counts(
        base.get("sum_fetch_remote", 0.0),
        base.get("sum_fetch_storage", 0.0),
        base.get("sum_fetch_local", 0.0),
    )
    aff_ratios = ratios_from_counts(
        aff.get("sum_fetch_remote", 0.0),
        aff.get("sum_fetch_storage", 0.0),
        aff.get("sum_fetch_local", 0.0),
    )

    lines = [
        f"baseline_cluster_throughput={base_tput:.6f}",
        f"affinity_cluster_throughput={aff_tput:.6f}",
        f"throughput_improvement_pct={tput_improve:.2f}",
        f"baseline_local_ratio={base_ratios['local_ratio']:.6f}",
        f"affinity_local_ratio={aff_ratios['local_ratio']:.6f}",
        f"local_ratio_delta_pp={(aff_ratios['local_ratio'] - base_ratios['local_ratio']) * 100.0:.2f}",
        f"baseline_compute_local_ratio={base_ratios['compute_local_ratio']:.6f}",
        f"affinity_compute_local_ratio={aff_ratios['compute_local_ratio']:.6f}",
        f"compute_local_ratio_delta_pp={(aff_ratios['compute_local_ratio'] - base_ratios['compute_local_ratio']) * 100.0:.2f}",
        f"baseline_storage_ratio={base_ratios['storage_ratio']:.6f}",
        f"affinity_storage_ratio={aff_ratios['storage_ratio']:.6f}",
        f"affinity_partition_runs={aff.get('max_affinity_partition_runs', 0.0):.0f}",
        f"affinity_partition_avg_ms={aff.get('avg_affinity_partition_ms_per_run', 0.0):.3f}",
        f"affinity_edgecut_final_avg={aff.get('affinity_edgecut_final_avg', 0.0):.3f}",
        f"affinity_edgecut_best_avg={aff.get('affinity_edgecut_best_avg', 0.0):.3f}",
        f"affinity_edgecut_steady_last5_avg={aff.get('affinity_edgecut_steady_last5_avg', 0.0):.3f}",
        f"affinity_cut_ratio_final_avg={aff.get('affinity_cut_ratio_final_avg', 0.0):.6f}",
        f"affinity_cut_ratio_best_avg={aff.get('affinity_cut_ratio_best_avg', 0.0):.6f}",
        f"affinity_migrations_planned={aff.get('sum_affinity_migrations_planned', 0.0):.0f}",
        f"affinity_migrations_done={aff.get('sum_affinity_migrations_done', 0.0):.0f}",
        f"affinity_migrations_failed={aff.get('sum_affinity_migrations_failed', 0.0):.0f}",
        f"affinity_migration_backlog={aff.get('sum_affinity_migration_backlog', 0.0):.0f}",
        f"affinity_migration_success_ratio={aff.get('affinity_migration_success_ratio', 0.0):.6f}",
        f"diagnostic_baseline_remote_ratio={base_ratios['remote_ratio']:.6f}",
        f"diagnostic_affinity_remote_ratio={aff_ratios['remote_ratio']:.6f}",
        f"baseline_ownership_transfer_count={base.get('sum_ownership_transfer_count', 0.0):.0f}",
        f"affinity_ownership_transfer_count={aff.get('sum_ownership_transfer_count', 0.0):.0f}",
        f"baseline_ownership_transfer_time_total={base.get('sum_ownership_transfer_time_total', 0.0):.6f}",
        f"affinity_ownership_transfer_time_total={aff.get('sum_ownership_transfer_time_total', 0.0):.6f}",
        f"baseline_ownership_transfer_avg_ms={(base.get('sum_ownership_transfer_time_total', 0.0) * 1000.0 / base.get('sum_ownership_transfer_count', 0.0) if base.get('sum_ownership_transfer_count', 0.0) > 0 else 0.0):.6f}",
        f"affinity_ownership_transfer_avg_ms={(aff.get('sum_ownership_transfer_time_total', 0.0) * 1000.0 / aff.get('sum_ownership_transfer_count', 0.0) if aff.get('sum_ownership_transfer_count', 0.0) > 0 else 0.0):.6f}",
        f"baseline_ownership_transfer_lock_request_time={base.get('sum_ownership_transfer_lock_request_time', 0.0):.6f}",
        f"affinity_ownership_transfer_lock_request_time={aff.get('sum_ownership_transfer_lock_request_time', 0.0):.6f}",
        f"baseline_ownership_transfer_wait_lock_success_time={base.get('sum_ownership_transfer_wait_lock_success_time', 0.0):.6f}",
        f"affinity_ownership_transfer_wait_lock_success_time={aff.get('sum_ownership_transfer_wait_lock_success_time', 0.0):.6f}",
        f"baseline_ownership_transfer_wait_push_page_time={base.get('sum_ownership_transfer_wait_push_page_time', 0.0):.6f}",
        f"affinity_ownership_transfer_wait_push_page_time={aff.get('sum_ownership_transfer_wait_push_page_time', 0.0):.6f}",
        f"baseline_ownership_transfer_storage_fetch_time={base.get('sum_ownership_transfer_storage_fetch_time', 0.0):.6f}",
        f"affinity_ownership_transfer_storage_fetch_time={aff.get('sum_ownership_transfer_storage_fetch_time', 0.0):.6f}",
        f"baseline_ownership_transfer_push_forward_time={base.get('sum_ownership_transfer_push_forward_time', 0.0):.6f}",
        f"affinity_ownership_transfer_push_forward_time={aff.get('sum_ownership_transfer_push_forward_time', 0.0):.6f}",
        f"baseline_ownership_transfer_other_time={base.get('sum_ownership_transfer_other_time', 0.0):.6f}",
        f"affinity_ownership_transfer_other_time={aff.get('sum_ownership_transfer_other_time', 0.0):.6f}",
    ]
    (out_dir / "compare_summary.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    log("compare summary:\n" + "\n".join(lines))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run a 5-node ParMETIS smoke test.")
    p.add_argument("--compute-hosts", default=",".join(DEFAULT_COMPUTE_HOSTS))
    p.add_argument("--service-host", default=DEFAULT_SERVICE_HOST)
    p.add_argument("--user", default=DEFAULT_USER)
    p.add_argument("--password", default=DEFAULT_PASSWORD)
    p.add_argument("--remote-dir", default=DEFAULT_REMOTE_DIR)
    p.add_argument("--attempted-num", type=int, default=int(os.environ.get("WOOKONG_SMOKE_ATTEMPTED_NUM", "5000")))
    p.add_argument("--threads", type=int, default=int(os.environ.get("WOOKONG_SMOKE_THREADS", "1")))
    p.add_argument("--num-accounts", type=int, default=int(os.environ.get("WOOKONG_SMOKE_NUM_ACCOUNTS", "500000")))
    p.add_argument("--num-hot-accounts", type=int, default=int(os.environ.get("WOOKONG_SMOKE_NUM_HOT_ACCOUNTS", "100000")))
    p.add_argument("--workload", default="smallbank_aff", choices=["smallbank", "smallbank_aff"])
    p.add_argument("--wr-ratio", type=float, default=0.2)
    p.add_argument("--local-ratio", type=float, default=0.5)
    p.add_argument("--use-zipfian", type=int, choices=[0, 1], default=1)
    p.add_argument("--zipf-theta", type=float, default=None)
    p.add_argument("--partition-cycle-ms", type=int, default=10000)
    p.add_argument("--migration-tick-ms", type=int, default=200)
    p.add_argument("--migration-batch", type=int, default=200)
    p.add_argument("--migration-workers", type=int, default=1,
                   help="Number of MigrationLoop drainer threads per compute node")
    p.add_argument("--edge-min-weight", type=float, default=1.0)
    p.add_argument("--log-flush-interval-ms", type=int, default=3)
    p.add_argument("--log-flush-batch-trigger", type=int, default=16)
    p.add_argument("--log-flush-notify-threshold", type=int, default=4)
    p.add_argument("--push-page-scheduler-threads", type=int, default=4)
    p.add_argument(
        "--log-group-commit-wait-us",
        type=int,
        default=-1,
        help="Set LOG_GROUP_COMMIT_WAIT_US for storage_pool; -1 keeps the binary default.",
    )
    p.add_argument("--timeout", type=int, default=int(os.environ.get("WOOKONG_SMOKE_TIMEOUT", "420")))
    p.add_argument(
        "--progress-interval",
        type=int,
        default=int(os.environ.get("WOOKONG_SMOKE_PROGRESS_INTERVAL", "10")),
        help="Seconds between remote compute progress snapshots while the workload is running.",
    )
    p.add_argument("--result-dir", default=str(DEFAULT_RESULT_DIR))
    p.add_argument("--skip-sync-build", action="store_true", help="Only upload configs and run; still checks ParMETIS first.")
    p.add_argument("--compare", action="store_true", help="Run baseline first, then affinity_on, and write compare_summary.txt.")
    p.add_argument("--run-baseline", action="store_true", help="Alias for --compare; keep it off to run affinity_on only.")
    p.add_argument("--baseline-only", action="store_true", help="Run one affinity-disabled case only.")
    p.add_argument("--disable-wal", action="store_true", help="Set local_compute_node.generate_log=0 in generated configs.")
    p.add_argument("--parallel-page-fetch", type=int, default=0, help="Set local_compute_node.parallel_page_fetch (0=serial, 1=parallel).")
    p.add_argument("--build-type", default="Debug", choices=["Debug", "Release", "RelWithDebInfo"], help="CMAKE_BUILD_TYPE for remote builds.")
    args = p.parse_args()
    args.compare = args.compare or args.run_baseline
    if args.baseline_only and args.compare:
        p.error("--baseline-only cannot be combined with --compare/--run-baseline")
    return args


def prepare_remote(
    args: argparse.Namespace,
    all_hosts: list[Host],
    compute_hosts: list[Host],
    compute_ips: list[str],
    service_host: str,
    *,
    enable_affinity: bool,
    sync_and_build: bool,
) -> None:
    if sync_and_build:
        run_parallel(all_hosts, "sync project", lambda h: rsync_project(h, args.remote_dir))
    configs = make_configs(compute_ips, service_host, args, enable_affinity=enable_affinity)
    run_parallel(all_hosts, "upload configs", lambda h: upload_configs(h, args.remote_dir, configs))
    run_parallel(
        compute_hosts,
        "install mpi ssh wrapper",
        lambda h: (install_mpi_ssh_wrapper(h, args.password) or "wrapper_installed"),
    )
    if sync_and_build:
        run_parallel(all_hosts, "build project", lambda h: build_project(h, args.remote_dir, args.build_type))


def run_case(
    case_name: str,
    args: argparse.Namespace,
    all_hosts: list[Host],
    compute_hosts: list[Host],
    service: Host,
    result_dir: Path,
) -> dict[str, dict[str, str]]:
    log(f"case {case_name}: start")
    case_dir = result_dir / case_name
    case_dir.mkdir(parents=True, exist_ok=True)
    kill_cluster(all_hosts)
    start_services(service, args.remote_dir, args.workload, args)
    for idx in range(1, len(compute_hosts)):
        start_compute(compute_hosts[idx], args.remote_dir, args.workload, idx, args)
        time.sleep(1)
    start_compute(compute_hosts[0], args.remote_dir, args.workload, 0, args)
    time.sleep(8)
    rc, sidecar_log, _ = run_ssh(
        compute_hosts[0],
        f"tail -n 80 {shlex.quote(args.remote_dir)}/build/compute_server/affinity_sidecar.log 2>/dev/null || true",
        timeout=10,
        check=False,
    )
    if sidecar_log.strip():
        log(f"case {case_name}: compute0 affinity_sidecar.log tail:\n{sidecar_log}")
    wait_error: Exception | None = None
    try:
        wait_computes(compute_hosts, args.remote_dir, args.timeout, args.progress_interval)
    except Exception as exc:
        wait_error = exc
    summary = collect_results(compute_hosts, args.remote_dir, case_dir)
    collect_service_logs(service, args.remote_dir, case_dir)
    write_cluster_summary(summary, case_dir)
    missing_results = [ip for ip, kv in summary.items() if "throughput" not in kv]
    if wait_error is not None:
        raise RuntimeError(
            f"case {case_name}: workload did not finish cleanly; "
            f"diagnostics saved in {case_dir}; cause: {wait_error}"
        ) from wait_error
    if missing_results:
        raise RuntimeError(
            f"case {case_name}: missing result.txt metrics for "
            + ",".join(missing_results)
            + f"; diagnostics saved in {case_dir}"
        )
    log(f"case {case_name}: done, results saved in {case_dir}")
    return summary


def main() -> int:
    args = parse_args()
    require_local_tools()

    compute_ips = [h.strip() for h in args.compute_hosts.split(",") if h.strip()]
    compute_hosts = [
        Host(ip, "compute0" if idx == 0 else "compute", args.user, args.password)
        for idx, ip in enumerate(compute_ips)
    ]
    service = Host(args.service_host, "service", args.user, args.password)
    all_hosts = compute_hosts + [service]
    result_dir = Path(args.result_dir) / time.strftime("%Y%m%d_%H%M%S")
    result_dir.mkdir(parents=True, exist_ok=True)

    experiment = {
        "workload": args.workload,
        "compute_hosts": compute_ips,
        "service_host": args.service_host,
        "attempted_num": args.attempted_num,
        "threads": args.threads,
        "num_accounts": args.num_accounts,
        "num_hot_accounts": min(args.num_hot_accounts, args.num_accounts),
        "wr_ratio": args.wr_ratio,
        "local_ratio_arg": args.local_ratio,
        "use_zipfian": args.use_zipfian,
        "zipf_theta": args.zipf_theta,
        "partition_cycle_ms": args.partition_cycle_ms,
        "migration_tick_ms": args.migration_tick_ms,
        "edge_min_weight": args.edge_min_weight,
        "migration_batch": args.migration_batch,
        "migration_workers": args.migration_workers,
        "log_flush_interval_ms": args.log_flush_interval_ms,
        "log_flush_batch_trigger": args.log_flush_batch_trigger,
        "log_flush_notify_threshold": args.log_flush_notify_threshold,
        "push_page_scheduler_threads": args.push_page_scheduler_threads,
        "log_group_commit_wait_us": args.log_group_commit_wait_us,
        "wal_enabled": not args.disable_wal,
        "generate_log": 0 if args.disable_wal else 1,
        "compare": args.compare,
        "baseline_only": args.baseline_only,
        "run_baseline": args.compare,
        "skip_sync_build": args.skip_sync_build,
        "build_type": args.build_type,
        "progress_interval": args.progress_interval,
    }
    (result_dir / "experiment_config.json").write_text(
        json.dumps(experiment, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    log(
        "experiment config: "
        f"workload={args.workload} "
        f"attempted_num={args.attempted_num} threads={args.threads} "
        f"wr_ratio={args.wr_ratio} local_ratio_arg={args.local_ratio} "
        f"use_zipfian={args.use_zipfian} "
        f"zipf_theta={args.zipf_theta if args.zipf_theta is not None else 'default'} "
        f"partition_cycle_ms={args.partition_cycle_ms} "
        f"migration_tick_ms={args.migration_tick_ms} "
        f"edge_min_weight={args.edge_min_weight} "
        f"migration_batch={args.migration_batch} "
        f"migration_workers={args.migration_workers} "
        f"log_flush={args.log_flush_interval_ms}ms/"
        f"{args.log_flush_batch_trigger}/"
        f"{args.log_flush_notify_threshold} "
        f"group_commit_wait_us={args.log_group_commit_wait_us} "
        f"wal_enabled={int(not args.disable_wal)} "
        f"compare={int(args.compare)} "
        f"baseline_only={int(args.baseline_only)} "
        f"build_type={args.build_type} "
        f"progress_interval={args.progress_interval}s"
    )

    log("ParMETIS environment check is the first gate")
    env_report = run_parallel(all_hosts, "parmetis env check", parmetis_check)
    (result_dir / "parmetis_env.txt").write_text(
        "\n\n".join(f"===== {ip} =====\n{text}" for ip, text in env_report.items()),
        encoding="utf-8",
    )

    try:
        case_metrics: dict[str, dict[str, float]] = {}
        sync_and_build = not args.skip_sync_build
        if args.compare:
            prepare_remote(
                args,
                all_hosts,
                compute_hosts,
                compute_ips,
                args.service_host,
                enable_affinity=False,
                sync_and_build=sync_and_build,
            )
            baseline = run_case("baseline", args, all_hosts, compute_hosts, service, result_dir)
            case_metrics["baseline"] = summarize_metrics(baseline)
            sync_and_build = False
            prepare_remote(
                args,
                all_hosts,
                compute_hosts,
                compute_ips,
                args.service_host,
                enable_affinity=True,
                sync_and_build=False,
            )
            affinity = run_case("affinity_on", args, all_hosts, compute_hosts, service, result_dir)
            case_metrics["affinity_on"] = summarize_metrics(affinity)
            write_compare_summary(case_metrics, result_dir)
        elif args.baseline_only:
            prepare_remote(
                args,
                all_hosts,
                compute_hosts,
                compute_ips,
                args.service_host,
                enable_affinity=False,
                sync_and_build=sync_and_build,
            )
            summary = run_case("baseline", args, all_hosts, compute_hosts, service, result_dir)
            write_cluster_summary(summary, result_dir)
        else:
            prepare_remote(
                args,
                all_hosts,
                compute_hosts,
                compute_ips,
                args.service_host,
                enable_affinity=True,
                sync_and_build=sync_and_build,
            )
            summary = run_case("affinity_on", args, all_hosts, compute_hosts, service, result_dir)
            write_cluster_summary(summary, result_dir)
        log(f"PASS: results saved in {result_dir}")
        return 0
    finally:
        kill_cluster(all_hosts)


if __name__ == "__main__":
    sys.exit(main())
