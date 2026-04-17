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
LOG_C0 = Path("/tmp/stage2_compute0.log")
LOG_C1 = Path("/tmp/stage2_compute1.log")
PID_DIR = Path("/tmp/stage2_local_affinity")
PID_STORAGE = PID_DIR / "storage.pid"
PID_REMOTE = PID_DIR / "remote.pid"
PID_C0 = PID_DIR / "compute0.pid"
PID_C1 = PID_DIR / "compute1.pid"
YCSB_CONFIG = ROOT / "config" / "ycsb_config.json"

STARTED = []

ATTEMPTED_NUM = int(os.environ.get("STAGE2_ATTEMPTED_NUM", "2000"))
THREADS = os.environ.get("STAGE2_THREADS", "2")


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


def start_logged(cmd, cwd: Path, log_path: Path, pidfile: Path, name: str) -> subprocess.Popen:
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


def main() -> int:
    for binary in [STORAGE_BIN, REMOTE_BIN, COMPUTE_BIN]:
        assert_executable(binary)

    cfg = load_json(ROOT / "config" / "compute_node_config.json")
    affinity_enabled = bool(cfg.get("affinity", {}).get("enable", False))

    cleanup()

    with open(YCSB_CONFIG, "r", encoding="utf-8") as fh:
        original_ycsb = fh.read()
    ycsb_cfg = json.loads(original_ycsb)
    ycsb_cfg["ycsb"]["attempted_num"] = ATTEMPTED_NUM
    with open(YCSB_CONFIG, "w", encoding="utf-8") as fh:
        json.dump(ycsb_cfg, fh, indent=2)

    for path in [
        LOG_STORAGE,
        LOG_REMOTE,
        LOG_C0,
        LOG_C1,
        COMPUTE_DIR / "affinity_sidecar.log",
        COMPUTE_DIR / "result.txt",
        COMPUTE_DIR / "affinity_timeseries.0.csv",
        COMPUTE_DIR / "affinity_timeseries.1.csv",
    ]:
        try:
            path.unlink()
        except FileNotFoundError:
            pass

    db_dir = STORAGE_DIR / "ycsb"
    # The storage server uses DB name "ycsb" in benchmark mode.
    if db_dir.exists():
        shutil.rmtree(db_dir, ignore_errors=True)

    storage = start_logged([str(STORAGE_BIN), "ycsb"], STORAGE_DIR, LOG_STORAGE, PID_STORAGE, "storage")
    if not wait_for_listen(15980, timeout_s=60):
        print("storage_pool failed to listen on 15980")
        print(tail(LOG_STORAGE))
        cleanup()
        with open(YCSB_CONFIG, "w", encoding="utf-8") as fh:
            fh.write(original_ycsb)
        return 1

    remote = start_logged([str(REMOTE_BIN), "sql"], REMOTE_DIR, LOG_REMOTE, PID_REMOTE, "remote")
    if not wait_for_listen(31508) or not wait_for_listen(31509):
        print("remote_node failed to listen on 31508/31509")
        print(tail(LOG_REMOTE))
        cleanup()
        with open(YCSB_CONFIG, "w", encoding="utf-8") as fh:
            fh.write(original_ycsb)
        return 1

    c1 = start_logged(
        [str(COMPUTE_BIN), "ycsb", "lazy", THREADS, "0.2", "0.5", "1"],
        COMPUTE_DIR,
        LOG_C1,
        PID_C1,
        "compute1",
    )
    time.sleep(2)
    c0 = start_logged(
        [str(COMPUTE_BIN), "ycsb", "lazy", THREADS, "0.2", "0.5", "0"],
        COMPUTE_DIR,
        LOG_C0,
        PID_C0,
        "compute0",
    )

    if affinity_enabled:
        if not wait_for_path(COMPUTE_DIR / "affinity_sidecar.log", timeout_s=20):
            print("affinity_sidecar.log did not appear")
            print("compute0:")
            print(tail(LOG_C0))
            print("compute1:")
            print(tail(LOG_C1))
            cleanup()
            with open(YCSB_CONFIG, "w", encoding="utf-8") as fh:
                fh.write(original_ycsb)
            return 1
        wait_for_path(COMPUTE_DIR / "affinity_timeseries.0.csv", timeout_s=20)
        wait_for_path(COMPUTE_DIR / "affinity_timeseries.1.csv", timeout_s=20)
    wait_for_exit(c0, timeout_s=120)
    wait_for_exit(c1, timeout_s=120)
    time.sleep(2)

    print("=== compute0 ===")
    print(tail(LOG_C0, 60))
    print("=== compute1 ===")
    print(tail(LOG_C1, 60))
    if affinity_enabled:
        print("=== affinity_sidecar.log ===")
        print(tail(COMPUTE_DIR / "affinity_sidecar.log", 120))

        for csv in [COMPUTE_DIR / "affinity_timeseries.0.csv", COMPUTE_DIR / "affinity_timeseries.1.csv"]:
            print(f"=== {csv.name} ===")
            print(tail(csv, 20))

    print("=== result.txt ===")
    print(tail(COMPUTE_DIR / "result.txt", 120))

    for proc, name in [(storage, "storage"), (remote, "remote"), (c0, "compute0"), (c1, "compute1")]:
        rc = proc.poll()
        print(f"{name}_rc={rc}")
    for name, proc, _, log_path in STARTED:
        print(f"{name}_state={proc_state(proc)} log={log_path}")

    cleanup()
    with open(YCSB_CONFIG, "w", encoding="utf-8") as fh:
        fh.write(original_ycsb)
    return 0


if __name__ == "__main__":
    sys.exit(main())
