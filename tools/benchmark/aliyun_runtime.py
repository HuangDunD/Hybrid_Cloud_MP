"""Run the benchmark from AliYun1 with a fresh database per case on the workspace disk."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
import errno
import fcntl
import hashlib
import logging
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile


# This fragment is also executed on the compute hosts. /proc/<pid>/exe identifies
# the specific executable opened by this case even when the file is a hard link.
_STOP_OWNED = """import os, signal, sys, time
from pathlib import Path
paths = set(sys.argv[1:])
def matching():
    for process in Path('/proc').glob('[0-9]*/exe'):
        try:
            if os.readlink(process) in paths:
                yield int(process.parent.name)
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            pass
for sig in (signal.SIGTERM, signal.SIGKILL):
    for pid in matching():
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            pass
    for _ in range(50):
        if not any(matching()):
            sys.exit(0)
        time.sleep(0.1)
raise SystemExit('Could not stop case processes under ' + str(paths))
"""

# Zombie processes have exited and hold no files or service ports.
_ACTIVE = """ps -eo stat=,comm= | awk '$1 !~ /^Z/ && ($2=="storage_pool" || $2=="remote_node" || $2=="compute_server")'"""


def remote(client, command):
    _, stdout, stderr = client.exec_command(command)
    output = stdout.read().decode().strip()
    detail = stderr.read().decode().strip()
    if stdout.channel.recv_exit_status():
        raise RuntimeError(f'{command} failed: {detail or output}')
    return output


def parallel(function, nodes):
    with ThreadPoolExecutor(max_workers=len(nodes)) as pool:
        list(pool.map(function, nodes))


def link_image(source, target):
    target.parent.mkdir(parents=True)
    try:
        os.link(source, target)
    except OSError as error:
        if error.errno != errno.EXDEV:
            raise
        shutil.copy2(source, target)


@contextmanager
def execution(b, result_dir):
    workspace = Path(b.workspace)
    build = workspace / 'build'
    compute = build / 'compute_server/compute_server'
    servers = ('storage_server/storage_pool', 'remote_server/remote_node')
    for binary in (compute, *(build / name for name in servers)):
        if not os.access(binary, os.X_OK) or not binary.is_file():
            raise FileNotFoundError(f'Build the benchmark binary first: {binary}')
    scratch = workspace.parent / ".hybrid-benchmark-runtime"
    scratch.mkdir(mode=0o700, exist_ok=True)
    nodes = list(zip(b.compute_server_hostnames, b.compute_server_ports,
                     b.compute_server_usernames, b.compute_server_passwords))

    with (scratch / '.aliyun-multi-node.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('Another benchmark is using the AliYun1 cluster') from None
        slot = Path(tempfile.mkdtemp(prefix='multi-node.', dir=scratch))
        logging.info('Benchmark runtime %s, results %s', slot, result_dir)
        # Explicit result locations outside the checkout's default result root
        # keep their entire output at the requested location.
        compute_hash = hashlib.sha256(compute.read_bytes()).hexdigest()
        cached = slot / 'compute_server'
        q = shlex.quote

        def stage(node):
            with closing(b.ssh_client(*node)) as client:
                remote(client, f'mkdir -p {q(str(slot.parent))} && mkdir -m 700 {q(str(slot))}')
                with client.open_sftp() as sftp:
                    sftp.put(str(compute), str(cached))
                    sftp.chmod(str(cached), 0o755)
                digest = remote(client, f'sha256sum {q(str(cached))}').split()[0]
                if digest != compute_hash:
                    raise RuntimeError(f'Compute binary transfer failed on {node[0]}')

        def run_case(root, case, mode, settings, fingerprint):
            runtime = Path(tempfile.mkdtemp(prefix=f"case-{case['case_id']:03d}.", dir=slot))
            runtime_build = runtime / 'build'
            binary = runtime_build / 'compute_server/compute_server'
            report = Path(b.plan.result_path(root, case, mode))
            logging.info('Case runtime %s, report %s', runtime, report)
            b.remote_workspace = str(runtime)
            b.remote_build_dir = str(runtime_build)
            b.compute_server_build_dir = str(binary.parent)

            def stop_compute(client):
                remote(client, shlex.join(['python3', '-c', _STOP_OWNED, str(binary)]))

            def prepare(node):
                with closing(b.ssh_client(*node)) as client:
                    active = remote(client, _ACTIVE)
                    if active:
                        raise RuntimeError(f'{node[0]} already running database services: {active}')
                    remote(client, f'mkdir -p {q(str(binary.parent))} && ln {q(str(cached))} {q(str(binary))}')
                    if case['workload'] == 'tpcc':
                        inputs = [workspace/'config/tpcc_config.json',
                                  *(workspace/'workload/tpcc/tpcc_tables').glob('*.json')]
                        with client.open_sftp() as sftp:
                            for source in inputs:
                                target = runtime / source.relative_to(workspace)
                                remote(client, f'mkdir -p {q(str(target.parent))}')
                                sftp.put(str(source), str(target))

            def stop_node(node):
                with closing(b.ssh_client(*node)) as client:
                    stop_compute(client)

            active = subprocess.check_output(_ACTIVE, shell=True, text=True).strip()
            if active:
                raise RuntimeError(f'AliYun1 already running database services: {active}')
            for name in servers:
                link_image(build/name, runtime_build/name)
            if case['workload'] == 'tpcc':
                (runtime/'config').mkdir()
                shutil.copy2(workspace/'config/tpcc_config.json', runtime/'config')
                (runtime/'workload').symlink_to(workspace/'workload', target_is_directory=True)
            parallel(prepare, nodes)
            b.ensure_compute_killed = stop_compute
            try:
                b.run_case_mode(root, case, mode, settings, fingerprint)
                if not b.plan.complete(root, case, mode, fingerprint):
                    raise RuntimeError(f'Incomplete results in {report}')
            finally:
                try:
                    parallel(stop_node, nodes)
                finally:
                    subprocess.run(['python3', '-c', _STOP_OWNED,
                                    *(str(runtime_build/name) for name in servers)], check=True)
                    for name, log in [('storage_server','storage_boot.log'), ('remote_server','remote_boot.log')]:
                        source = runtime_build / name / log
                        if source.is_file():
                            (report / 'storage').mkdir(parents=True, exist_ok=True)
                            shutil.copy2(source, report / 'storage' / log)
            # A successful workload has a verified report. Its WAL/undo workspace
            # can be released before preallocation for the next workload starts.
            def remove_case(node):
                with closing(b.ssh_client(*node)) as client:
                    remote(client, f'rm -rf -- {q(str(runtime))}')
            parallel(remove_case, nodes)
            shutil.rmtree(runtime)

        original = (b.remote_workspace, b.remote_build_dir, b.compute_server_build_dir,
                    b.ensure_compute_killed, b.isolated_runtime)
        b.isolated_runtime = True
        try:
            parallel(stage, nodes)
            yield run_case
            def remove_slot(node):
                with closing(b.ssh_client(*node)) as client:
                    remote(client, f'rm -f -- {q(str(cached))} && rmdir -- {q(str(slot))}')
            parallel(remove_slot, nodes)
            slot.rmdir()
        finally:
            (b.remote_workspace, b.remote_build_dir, b.compute_server_build_dir,
             b.ensure_compute_killed, b.isolated_runtime) = original
