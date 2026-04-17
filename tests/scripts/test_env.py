import atexit
import os
import signal
import shutil
import subprocess
import time


_STARTED_PROCS = []
_PROCESS_PATTERNS = ["remote_node", "storage_pool", "compute_server"]


def kill_processes():
    for pattern in _PROCESS_PATTERNS:
        subprocess.run(["pkill", pattern], check=False)
    wait_for_processes_gone()


def _terminate_started_procs():
    while _STARTED_PROCS:
        proc = _STARTED_PROCS.pop()
        if proc.poll() is None:
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except ProcessLookupError:
                continue
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
    wait_for_processes_gone()


atexit.register(_terminate_started_procs)


def wait_for_listen(port, timeout_sec=30.0):
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        proc = subprocess.run(
            ["ss", "-ltn"],
            check=False,
            capture_output=True,
            text=True,
        )
        if f":{port} " in proc.stdout:
            return True
        time.sleep(0.2)
    return False


def wait_for_processes_gone(timeout_sec=15.0):
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        proc = subprocess.run(
            ["ps", "-eo", "args="],
            check=False,
            capture_output=True,
            text=True,
        )
        lines = proc.stdout.splitlines()
        if not any(any(pattern in line for pattern in _PROCESS_PATTERNS) for line in lines):
            return True
        time.sleep(0.2)
    return False


def start_background(cmd, cwd, log_path):
    log_file = open(log_path, "wb")
    proc = subprocess.Popen(
        ["script", "-q", "-c", " ".join(cmd), "/dev/null"],
        cwd=cwd,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    proc._omx_log_file = log_file
    _STARTED_PROCS.append(proc)
    return proc


def start_sql_cluster(project_root, database_name, compute_node_count=2):
    kill_processes()
    shutil.rmtree(
        os.path.join(project_root, "build", "storage_server", database_name),
        ignore_errors=True,
    )

    storage_proc = start_background(
        ["./storage_pool", "sql"],
        os.path.join(project_root, "build", "storage_server"),
        os.path.join(project_root, "build", "storage_server", "storage_boot.log"),
    )
    remote_proc = start_background(
        ["./remote_node", "sql"],
        os.path.join(project_root, "build", "remote_server"),
        os.path.join(project_root, "build", "remote_server", "remote_boot.log"),
    )

    if not wait_for_listen(15980):
        raise RuntimeError("storage_pool failed to listen on 15980")
    if not wait_for_listen(31508):
        raise RuntimeError("remote_node failed to listen on 31508")
    if not wait_for_listen(31509):
        raise RuntimeError("remote_node failed to listen on 31509")

    compute_procs = []
    launch_order = list(range(compute_node_count))
    if compute_node_count > 1:
        launch_order = list(range(1, compute_node_count)) + [0]

    for node_id in launch_order:
        proc = start_background(
            ["./compute_server", str(node_id), database_name],
            os.path.join(project_root, "build", "compute_server"),
            os.path.join(project_root, "build", "compute_server", f"compute_server_{node_id}.out"),
        )
        compute_procs.append(proc)

    for node_id in range(compute_node_count):
        port = 9095 + node_id
        if not wait_for_listen(port, timeout_sec=45.0):
            raise RuntimeError(f"compute_server {node_id} failed to listen on {port}")

    return {
        "storage": storage_proc,
        "remote": remote_proc,
        "compute": compute_procs,
    }
