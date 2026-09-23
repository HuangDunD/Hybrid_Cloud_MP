#!/usr/bin/env python3
import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import platform
import select
import shutil
import signal
import socket
import stat
import subprocess
import sys
import time
import traceback

PROJECT = Path(os.environ.get('HCM_PROJECT_ROOT', Path(__file__).resolve().parents[2])).resolve()
LIBC = ctypes.CDLL(None, use_errno=True)


def pidfd_open(pid):
    if platform.machine() != 'x86_64':
        raise RuntimeError('pidfd syscall mapping requires Linux x86_64')
    fd = LIBC.syscall(434, pid, 0)
    if fd < 0:
        raise OSError(ctypes.get_errno(), 'pidfd_open')
    return fd


def identity(pid):
    p = Path('/proc') / str(pid)
    fields = (p / 'stat').read_text().rsplit(')', 1)[1].split()
    if fields[0] == 'Z':
        raise ProcessLookupError(pid)
    return dict(pid=pid, starttime=fields[19], exe=os.readlink(p / 'exe'),
                cwd=os.readlink(p / 'cwd'), boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip())


def send_verified(meta, sig, fd):
    if identity(meta['pid']) != meta:
        raise RuntimeError('process identity mismatch; refusing signal')
    if LIBC.syscall(424, fd, sig, 0, 0) < 0:
        raise OSError(ctypes.get_errno(), 'pidfd_send_signal')


def process_alive(entry):
    # A process that exits inside the observation window must be reported as
    # gone, never as a supervisor error: poll() first (reaping a direct child
    # is what makes its pidfd readable), and treat /proc disappearance or a
    # zombie as an exit even while the pidfd is still unreadable (this kernel
    # keeps a pidfd unreadable until the process is reaped, and a group leader
    # whose thread group is still winding down cannot be reaped yet).
    if entry.get('proc') is not None and entry['proc'].poll() is not None:
        return False
    if select.select([entry['fd']], [], [], 0)[0]:
        if entry.get('proc') is not None:
            entry['proc'].poll()
        return False
    if entry['meta'] is None:
        raise RuntimeError('process has no verified identity: ' + entry['role'])
    try:
        current = identity(entry['meta']['pid'])
    except (FileNotFoundError, ProcessLookupError):
        if entry.get('proc') is not None:
            entry['proc'].poll()
        return False
    if current != entry['meta']:
        raise RuntimeError('owned process identity changed: ' + entry['role'])
    return True


def process_fully_exited(entry):
    if entry.get('proc') is not None and entry['proc'].poll() is not None:
        return True
    return bool(select.select([entry['fd']], [], [], 0)[0])


def write_json(path, data):
    with path.open('w') as f:
        json.dump(data, f, indent=2)
        f.write('\n')
        f.flush()
        os.fsync(f.fileno())


def event(run, kind, **fields):
    with (run / 'commands.jsonl').open('a') as f:
        f.write(json.dumps(dict(time=time.time(), event=kind, **fields)) + '\n')
        f.flush()
        os.fsync(f.fileno())


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def freeze_file(source, destination):
    before = digest(source)
    shutil.copy2(source, destination)
    after = digest(destination)
    if before != after or digest(source) != after:
        raise RuntimeError('source changed while freezing: ' + str(source))
    destination.chmod(destination.stat().st_mode & ~0o222)
    return dict(source=str(source), copy=str(destination), sha256=after)


def source_fingerprint(run):
    def git(*args):
        return subprocess.check_output(['git', *args], cwd=PROJECT)
    head = git('rev-parse', 'HEAD').decode().strip()
    patch = git('diff', 'HEAD', '--binary', '--no-ext-diff')
    staged = git('diff', '--cached', '--binary', '--no-ext-diff')
    (run / 'source.patch').write_bytes(patch)
    (run / 'source-staged.patch').write_bytes(staged)
    changed = set(filter(None, git('diff', 'HEAD', '--name-only', '-z', '--no-renames').decode().split('\0')))
    changed.update(filter(None, git('diff', '--cached', '--name-only', '-z', '--no-renames').decode().split('\0')))
    excluded_roots = {'runs', '.codebuddy', 'thirdparty', 'third_party', 'third-party'}
    untracked = set()
    for name in git('ls-files', '--others', '--exclude-standard', '-z').decode().split('\0'):
        if not name:
            continue
        parts = Path(name).parts
        if (parts[0] in excluded_roots or parts[0].startswith(('build', 'cmake-build')) or
                '__pycache__' in parts or run == PROJECT / parts[0]):
            continue
        untracked.add(name)
    records = {}
    for name in sorted(changed | untracked):
        path = PROJECT / name
        if path.is_symlink():
            records[name] = dict(kind='symlink', target=os.readlink(path))
        elif path.is_file():
            records[name] = dict(kind='file', sha256=digest(path), size=path.stat().st_size)
        elif not path.exists():
            records[name] = dict(kind='deleted')
        else:
            records[name] = dict(kind='directory_or_submodule')
        records[name]['untracked'] = name in untracked
    data = dict(git_head=head, tracked_diff_sha256=hashlib.sha256(patch).hexdigest(),
                staged_diff_sha256=hashlib.sha256(staged).hexdigest(), files=records,
                untracked_exclusions=['runs/', 'build*/', 'cmake-build*/', '__pycache__/'],
                scope='all tracked changes against HEAD plus nonignored non-generated additions')
    data['sha256'] = hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()
    write_json(run / 'source-fingerprint.json', data)
    return data


def verify_compute_drain(data, workers):
    ticket = data.get('enqueued_ticket')
    if (any(type(data.get(key)) is not int for key in ('enqueued_ticket', 'acked_ticket', 'queued_records', 'workers_joined')) or
            ticket < 0 or data.get('acked_ticket') != ticket or data.get('queued_records') != 0 or
            data.get('flush_in_flight') is not False or data.get('workers_joined') != workers or
            data.get('log_thread_joined') is not True):
        raise RuntimeError('compute drain counters are incomplete or inconsistent')


def verify_storage_drain(data):
    tail = data.get('wal_tail_inclusive')
    if (any(type(data.get(key)) is not int for key in ('wal_tail_inclusive', 'replay_inclusive', 'active_undo_transactions')) or
            tail < 0 or data.get('replay_inclusive') != tail or data.get('active_undo_transactions') != 0 or
            data.get('rpc_stopped_and_joined') is not True or data.get('wal_final_fdatasync') is not True or
            data.get('database_final_fdatasync') is not True):
        raise RuntimeError('storage drain did not prove replay catch-up, zero active undo and database/WAL durability')


def tree_bytes(root):
    total = 0
    pending = [root]
    while pending:
        path = pending.pop()
        try:
            info = path.lstat()
            if stat.S_ISREG(info.st_mode):
                total += info.st_size
            elif stat.S_ISDIR(info.st_mode):
                with os.scandir(path) as entries:
                    for entry in entries:
                        if entry.is_dir(follow_symlinks=False):
                            pending.append(Path(entry.path))
                        elif entry.is_file(follow_symlinks=False):
                            try:
                                total += entry.stat(follow_symlinks=False).st_size
                            except FileNotFoundError:
                                pass
        except FileNotFoundError:
            pass
    return total


def listening(pid, port):
    inodes = set()
    try:
        fds = list((Path('/proc') / str(pid) / 'fd').iterdir())
    except FileNotFoundError:
        return False
    for fd in fds:
        try:
            link = os.readlink(fd)
            if link.startswith('socket:['):
                inodes.add(link[8:-1])
        except FileNotFoundError:
            pass
    for name in ('tcp', 'tcp6'):
        for line in Path('/proc/net', name).read_text().splitlines()[1:]:
            cols = line.split()
            if cols[3] == '0A' and int(cols[1].split(':')[1], 16) == port and cols[9] in inodes:
                return True
    return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('run_dir', type=Path)
    parser.add_argument('action', choices=['start', 'stop', 'status', 'kill-A'])
    parser.add_argument('--build-dir', type=Path)
    parser.add_argument('--num-record', type=int, default=3000)
    parser.add_argument('--attempted', type=int, default=100)
    parser.add_argument('--threads', type=int, default=1)
    parser.add_argument('--nodes', type=int, choices=[1, 3], default=1)
    parser.add_argument('--mode', choices=['lazy'], default='lazy')
    parser.add_argument('--timeout', type=int, default=120)
    parser.add_argument('--request-driver', type=Path)
    parser.add_argument('--driver-mode', choices=['serial', 'mixed', 'load', 'fault'], default='serial')
    parser.add_argument('--fsm-heap-pages', type=int, default=4096)
    parser.add_argument('--allow-fault', action='store_true',
                        help='permit the request driver to trigger a single supervised compute-node SIGKILL '
                             '(R2 small-scale fault recovery test); the victim exit is expected, not a failure')
    parser.add_argument('--hold-after-validation', type=int, default=0, metavar='SECONDS',
                        help='keep compute alive after driver validation for 1..3600 seconds within --timeout; default off, never R2')
    args = parser.parse_args()
    if args.hold_after_validation < 0 or args.hold_after_validation > 3600:
        parser.error('hold budget must be between 0 and 3600 seconds')
    if args.hold_after_validation and (not args.request_driver or args.hold_after_validation >= args.timeout):
        parser.error('hold requires a request driver and must fit strictly within --timeout')
    run = args.run_dir.resolve()
    if PROJECT not in run.parents:
        parser.error('run directory must be inside project')
    if args.action == 'kill-A':
        parser.error('R2 disabled: R1 has not passed')
    if args.action != 'start':
        records = sorted((run / 'pids').glob('*.json'))
        if not records:
            parser.error('no v3 process identities; legacy runs require separate verification')
        for path in records:
            meta = json.loads(path.read_text())
            try:
                fd = pidfd_open(meta['pid'])
            except OSError as exc:
                if exc.errno == 3:
                    print(path.stem, 'EXITED')
                    continue
                raise
            try:
                current = identity(meta['pid'])
                if current != meta:
                    raise RuntimeError('identity mismatch: ' + path.stem)
                if meta['cwd'] != str(run) and not meta['cwd'].startswith(str(run) + '/'):
                    raise RuntimeError('identity is not owned by this run: ' + path.stem)
                print(path.stem, 'RUNNING', meta['pid'])
                if args.action == 'stop':
                    send_verified(meta, signal.SIGTERM, fd)
                    event(run, 'external_failure_cleanup_NOT_R2', name=path.stem, identity=meta)
            except (FileNotFoundError, ProcessLookupError):
                print(path.stem, 'EXITED')
            finally:
                os.close(fd)
        return 0
    if not args.build_dir:
        parser.error('--build-dir is required')
    build = args.build_dir.resolve()
    if PROJECT not in build.parents:
        parser.error('build must be inside project')
    if args.request_driver:
        if args.num_record < 0 or min(args.attempted, args.threads, args.timeout, args.fsm_heap_pages) <= 0:
            parser.error('request workload requires non-negative records and positive budgets')
        if not args.request_driver.is_file():
            parser.error('request driver does not exist')
    elif min(args.num_record, args.attempted, args.threads, args.timeout) <= 0:
        parser.error('counts/timeouts must be positive; empty loader not yet validated')
    if args.request_driver:
        if args.threads > 5 or args.num_record > 30000 or args.attempted > 2000 or args.timeout > 7200:
            parser.error('request workload resource budget exceeded')
    elif args.threads > 5 or args.num_record > 30000 or args.attempted > 2000 or args.timeout > 300:
        parser.error('diagnostic resource budget exceeded')
    if args.allow_fault and (args.driver_mode != 'fault' or not args.request_driver or args.nodes != 3):
        parser.error('--allow-fault requires --driver-mode fault with a request driver on a 3-node cluster')
    tooling_source = PROJECT / 'scripts' / 'r0r1'
    if args.request_driver and args.request_driver.resolve().parent != tooling_source.resolve():
        parser.error('request driver must belong to scripts/r0r1 so its complete tooling can be frozen')
    started = time.monotonic()
    run.mkdir(exist_ok=False, mode=0o700)
    for name in ('pids', 'logs', 'bin', 'tooling', 'env'):
        (run / name).mkdir(mode=0o700)
    budget = dict(max_rss_bytes=32*1024**3,
                  max_artifact_bytes=(160 if args.driver_mode == 'load' else 20)*1024**3,
                  max_wal_bytes=128*1024**3, min_free_bytes=100*1024**3,
                  wall_seconds=args.timeout, hold_seconds=args.hold_after_validation,
                  cleanup_reserve_seconds=min(10, args.timeout / 4),
                  filesystem_audit_seconds=2, process_sample_seconds=0.1,
                  scope='exclusive run services, driver, checker and all observed driver descendants')
    active_deadline = started + args.timeout - budget['cleanup_reserve_seconds']
    manifest = dict(run_dir=str(run), scope='healthy lifecycle only; R1 acceptance requires independent main verifier',
                    git_head=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=PROJECT).decode().strip(),
                    args=vars(args).copy(), build_dir=str(build), binaries={}, tooling={}, processes={}, budget=budget)
    manifest['args'] = {k: str(v) if isinstance(v, Path) else v for k, v in manifest['args'].items()}
    manifest['source_fingerprint'] = source_fingerprint(run)
    manifest['launcher'] = freeze_file(Path(__file__).resolve(), run / 'launcher.py')
    for source in sorted(tooling_source.rglob('*.py')):
        dst = run / 'tooling' / source.relative_to(tooling_source)
        dst.parent.mkdir(parents=True, exist_ok=True)
        manifest['tooling'][str(source.relative_to(tooling_source))] = freeze_file(source, dst)
    for name, rel in dict(storage='storage_server/storage_pool', remote='remote_server/remote_node', compute='compute_server/compute_server').items():
        manifest['binaries'][name] = freeze_file(build / rel, run / 'bin' / name)
    if args.request_driver:
        manifest['binaries']['tree_stats'] = freeze_file(build / 'tests' / 'tree_stats', run / 'bin' / 'tree_stats')
        manifest['abi'] = freeze_file(build.parent / 'abi-current.json', run / 'bin' / 'abi.json')
    manifest['environment_policy'] = 'exact child environments saved privately under env/; no inherited HCM_* for services'
    write_json(run / 'manifest.json', manifest)
    if shutil.disk_usage(run).free < budget['min_free_bytes']:
        write_json(run / 'summary.json', dict(status='FAIL', R1='NOT_ACCEPTED', R2='NOT_RUN',
                                              detail='minimum free disk budget unavailable before startup'))
        return 1
    reservations = []
    for _ in range(args.nodes + 6):
        sock = socket.socket()
        sock.bind(('127.0.0.1', 0))
        reservations.append(sock)
    ports = [s.getsockname()[1] for s in reservations]
    compute_ports = ports[:args.nodes]
    storage_rpc, storage_meta, raft0, raft1, remote_rpc, remote_meta = ports[args.nodes:]
    # Storage Raft listeners use rpc+2 and rpc+3, not independent config fields.
    for sock in reservations:
        sock.close()
    reservations = []
    for _ in range(100):
        probe = socket.socket(); probe.bind(('127.0.0.1', 0)); base = probe.getsockname()[1]; probe.close()
        if base + 3 > 65535 or any(p in compute_ports + [remote_rpc, remote_meta] for p in range(base, base+4)):
            continue
        held = []
        try:
            for p in compute_ports + [remote_rpc, remote_meta] + list(range(base, base+4)):
                s = socket.socket(); held.append(s); s.bind(('127.0.0.1', p))
            reservations = held
            storage_rpc, storage_meta, raft0, raft1 = range(base, base+4)
            break
        except OSError:
            for s in held: s.close()
    if not reservations:
        raise RuntimeError('cannot reserve isolated ports')
    manifest['ports'] = dict(compute=compute_ports, storage=[storage_rpc, storage_meta, raft0, raft1], remote=[remote_rpc, remote_meta])
    configs = {name: json.loads((PROJECT/'config'/name).read_text()) for name in
               ('compute_node_config.json', 'storage_node_config.json', 'remote_server_config.json', 'ycsb_config.json')}
    c = configs['compute_node_config.json']
    c['local_compute_node'].update(machine_num=args.nodes, thread_num_per_machine=args.threads, coroutine_num=1, txn_system=1)
    c['remote_compute_nodes'] = dict(remote_compute_node_ips=['127.0.0.1']*args.nodes, remote_compute_node_port=compute_ports)
    c['remote_server_nodes'] = dict(remote_server_node_ips=['127.0.0.1'], remote_server_node_port=[remote_rpc], remote_server_node_meta_port=[remote_meta])
    c['remote_storage_nodes'] = dict(remote_storage_node_ips=['127.0.0.1'], remote_storage_node_rpc_port=[storage_rpc], remote_storage_node_meta_port=[storage_meta])
    for name in ('storage_node_config.json', 'remote_server_config.json'):
        configs[name]['remote_compute_nodes'] = dict(compute_node_ips=['127.0.0.1']*args.nodes, compute_node_ports=compute_ports)
    configs['storage_node_config.json']['local_storage_node'].update(local_rpc_port=storage_rpc, local_meta_port=storage_meta, use_rdma=False)
    configs['remote_server_config.json']['local_server_node'].update(local_rpc_port=remote_rpc, local_meta_port=remote_meta)
    configs['remote_server_config.json']['remote_storage_nodes'] = dict(storage_node_ips=['127.0.0.1'], storage_node_ports=[storage_rpc])
    configs['ycsb_config.json']['ycsb'].update(num_record=args.num_record, attempted_num=args.attempted,
        num_hot_record=min(100, args.num_record-1), read_percent=40, write_percent=60,
        crud_read_percent=40, crud_update_percent=20, crud_insert_percent=20, crud_delete_percent=20)
    roles = ['storage', 'remote'] + ['compute_' + chr(65+i) for i in range(args.nodes)]
    cwds = {}
    for role in roles:
        home = run / role
        cfg = home / 'config'; cfg.mkdir(parents=True)
        cwd = home / 'build' / ('storage_server' if role == 'storage' else role)
        cwd.mkdir(parents=True)
        cwds[role] = cwd
        c['local_compute_node']['machine_id'] = roles.index(role)-2 if role.startswith('compute_') else 0
        for name, data in configs.items():
            write_json(cfg / name, data)
    manifest['paths'] = dict(storage_cwd=str(cwds['storage']), wal=str(cwds['storage']/'log_v2'), undo=str(cwds['storage']/'undo'),
                             raft=str(cwds['storage']/'Raft_Log*'), compute_cwds={r:str(p) for r,p in cwds.items() if r.startswith('compute_')},
                             logical_replay_index=str(cwds['storage']/'ycsb_user_table_bl'),
                             actual_compute_spill_index=str(cwds['storage']/'ycsb_user_table_bl_compute'),
                             compute_snapshot_indexes={r:str(run/'control'/r/'compute-snapshot-*'/'ycsb_user_table_bl')
                                                       for r in roles if r.startswith('compute_')},
                             storage_checkpoint_indexes=str(run/'ledger'/'checkpoint-*'/'storage'/'ycsb_user_table_bl'))
    manifest['index_page_spaces'] = dict(logical_replay_suffix='_bl', physical_compute_suffix='_bl_compute',
                                        routing='physical RPCs use registered compute routes; unregistered SQL paths unchanged',
                                        checkpoint_freeze='checkpoint-N.json through checkpoint-N.release.request; checkpoint-N-released.json acknowledges release')
    write_json(run / 'manifest.json', manifest)
    children = []
    owned = []
    peak_rss = 0
    last_filesystem_audit = -float('inf')
    filesystem_sample = {}
    phase = 'STARTUP'
    result = dict(status='FAIL', R0='NOT_ACCEPTED', R1='NOT_ACCEPTED', R2='NOT_RUN', outcomes='UNVERIFIED')

    def alive(entry):
        # module-level process_alive keeps the exit-window semantics testable;
        # the nested binding preserves all historical call sites unchanged
        return process_alive(entry)

    def save_environment(role, env):
        path = run / 'env' / (role + '.json')
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'w') as f:
            json.dump(env, f, indent=2, sort_keys=True)
            f.write('\n')
            f.flush()
            os.fsync(f.fileno())
        return str(path)

    def register(entry, meta):
        entry['meta'] = meta
        write_json(run / 'pids' / (entry['role'] + '.json'), meta)
        manifest['processes'][entry['role']] = meta
        write_json(run / 'manifest.json', manifest)
        event(run, 'pidfd_identity', name=entry['role'], identity=meta, pidfd=entry['fd'],
              parent=entry.get('parent'), argv=entry.get('argv'))

    def discover_driver_children():
        known = {e['meta']['pid'] for e in owned if e['meta'] is not None and
                 not select.select([e['fd']], [], [], 0)[0]}
        for parent in list(owned):
            if not parent.get('driver_tree') or not alive(parent):
                continue
            parent_pid = parent['meta']['pid']
            descendants = set()
            try:
                for task in (Path('/proc') / str(parent_pid) / 'task').iterdir():
                    try:
                        descendants.update(map(int, (task / 'children').read_text().split()))
                    except FileNotFoundError:
                        continue
            except FileNotFoundError:
                if alive(parent):
                    raise
                continue
            for pid in descendants - known:
                fd = None
                try:
                    fd = pidfd_open(pid)
                    meta = identity(pid)
                    proc_path = Path('/proc') / str(pid)
                    ppid = int((proc_path / 'stat').read_text().rsplit(')', 1)[1].split()[1])
                    argv = [a.decode(errors='replace') for a in (proc_path / 'cmdline').read_bytes().split(b'\0') if a]
                    if ppid != parent_pid or not alive(parent):
                        raise RuntimeError('driver descendant ancestry changed before registration')
                    if argv == parent.get('argv'):
                        continue
                    entry = dict(role='driver_child_' + str(pid) + '_' + meta['starttime'], proc=None, fd=fd, meta=None,
                                 driver_tree=True, parent=parent['role'], argv=argv, out=None)
                    owned.append(entry)
                    fd = None
                    register(entry, meta)
                    known.add(pid)
                    environment = {}
                    for raw in (proc_path / 'environ').read_bytes().split(b'\0'):
                        key, separator, value = raw.partition(b'=')
                        if separator:
                            environment[key.decode(errors='replace')] = value.decode(errors='replace')
                    env_path = save_environment(entry['role'], environment)
                    event(run, 'driver_child_observed', name=entry['role'], identity=meta, argv=argv,
                          parent=parent['meta'], env_path=env_path)
                    if (meta['cwd'] != str(run) and not meta['cwd'].startswith(str(run) + '/')) or not (
                            meta['exe'].startswith(str(run / 'bin') + '/') or
                            (meta['exe'] == str(Path(sys.executable).resolve()) and
                             any(arg.startswith(str(run / 'tooling') + '/') for arg in argv))):
                        raise RuntimeError('driver descendant is not using frozen tooling: ' + str(argv))
                except (FileNotFoundError, ProcessLookupError):
                    continue
                finally:
                    if fd is not None:
                        os.close(fd)

    def supervise(force=False):
        nonlocal peak_rss, last_filesystem_audit, filesystem_sample
        elapsed = time.monotonic() - started
        if time.monotonic() >= active_deadline:
            raise RuntimeError('active wall budget exhausted, including startup and hold; reserved time is failure cleanup only, not R2')
        discover_driver_children()
        rss_by_role = {}
        for entry in owned:
            if entry['meta'] is None:
                continue
            if alive(entry):
                try:
                    pages = int((Path('/proc') / str(entry['meta']['pid']) / 'statm').read_text().split()[1])
                    if alive(entry):
                        rss_by_role[entry['role']] = pages * os.sysconf('SC_PAGE_SIZE')
                except (FileNotFoundError, ProcessLookupError):
                    if alive(entry):
                        raise
        rss = sum(rss_by_role.values())
        peak_rss = max(peak_rss, rss)
        due = force or time.monotonic() - last_filesystem_audit >= budget['filesystem_audit_seconds']
        if due:
            filesystem_sample = dict(artifact_bytes=tree_bytes(run),
                                     wal_bytes=tree_bytes(cwds['storage'] / 'log_v2'),
                                     free_bytes=shutil.disk_usage(run).free)
            last_filesystem_audit = time.monotonic()
            audit_files()
        exceeded = []
        if rss > budget['max_rss_bytes']:
            exceeded.append('exclusive RSS')
        for metric, bound in (('artifact_bytes', 'max_artifact_bytes'), ('wal_bytes', 'max_wal_bytes')):
            if filesystem_sample.get(metric, 0) > budget[bound]:
                exceeded.append(metric)
        if filesystem_sample.get('free_bytes', budget['min_free_bytes']) < budget['min_free_bytes']:
            exceeded.append('minimum free disk')
        if due or exceeded:
            with (run / 'resources.jsonl').open('a') as f:
                f.write(json.dumps(dict(time=time.time(), elapsed_seconds=elapsed, phase=phase,
                                       rss_bytes=rss, peak_rss_bytes=peak_rss, rss_by_role=rss_by_role,
                                       identities={e['role']: e['meta'] for e in owned if e['role'] in rss_by_role},
                                       filesystem_sample_age_seconds=time.monotonic()-last_filesystem_audit,
                                       **filesystem_sample, exceeded=exceeded)) + '\n')
                f.flush()
                os.fsync(f.fileno())
        if exceeded:
            raise RuntimeError('resource budget exceeded: ' + ', '.join(exceeded))
    def interrupted(signum, frame):
        raise RuntimeError('supervisor interrupted: ' + str(signum))
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    def release_ports(wanted):
        for sock in reservations:
            if sock.fileno() >= 0 and sock.getsockname()[1] in wanted:
                sock.close()
    def spawn(role, binary, argv, ready_ports):
        release_ports(ready_ports)
        out = (run/'logs'/(role+'.out')).open('wb')
        command = ['stdbuf', '-oL', '-eL', str(run/'bin'/binary)] + argv
        env = {k:v for k,v in os.environ.items() if not k.startswith('HCM_')}
        env['TMPDIR'] = str(run/'logs')
        env['HCM_BENCH_CONFIG_READONLY'] = '1'
        control = run / 'control' / role
        control.mkdir(parents=True, mode=0o700)
        env['HCM_CONTROL_DIR'] = str(control)
        env['HCM_RUN_ID'] = run.name
        env['HCM_CONTROL_TIMEOUT_SECONDS'] = str(args.timeout + 90)
        if role == 'storage':
            env['HCM_FSM_HEAP_PAGES'] = str(args.fsm_heap_pages)
        if args.request_driver and role.startswith('compute_'):
            env['HCM_REQUEST_WORKLOAD'] = '1'
            # P1: StableSnapshot 的 pre-catchup 上限在 compute 进程内 getenv
            # 读取；spawn 默认剥离全部 HCM_ 前缀变量，必须显式透传，
            # 否则 compute 只能看到默认 900s（r1c-natural-load-p2-001 的
            # cp0 pre-catchup 即因此以默认值超时）
            if os.environ.get('HCM_SNAPSHOT_CATCHUP_TIMEOUT_MS'):
                env['HCM_SNAPSHOT_CATCHUP_TIMEOUT_MS'] = os.environ['HCM_SNAPSHOT_CATCHUP_TIMEOUT_MS']
            # P0: lazy.cc 的 IR 等待上限同样在 compute 进程内 getenv 读取
            #（r2-fault-small-004 仅 driver 环境设置了 HCM_IR_WAIT_MAX_MS，
            #  compute 未收到而用默认 120s）
            if os.environ.get('HCM_IR_WAIT_MAX_MS'):
                env['HCM_IR_WAIT_MAX_MS'] = os.environ['HCM_IR_WAIT_MAX_MS']
            # 第 16 层慢路径墙钟总时限（r2-20260923-live-smoke-005）：B/C 双
            # 节点对同一 BLink 索引页（table=10000 page=26）同时 S→X 升级互等
            # （GPLM hold=[B,C] S、queue=[B(X),C(X)]），默认 300s 自愈远晚于
            # driver 的 execute+STATUS 总预算（~105s），必须在 driver 放弃前
            # 触发 ReleaseRemoteForForcedPage 幂等解锁打破死锁环
            if os.environ.get('HCM_FETCH_GRANT_TOTAL_MS'):
                env['HCM_FETCH_GRANT_TOTAL_MS'] = os.environ['HCM_FETCH_GRANT_TOTAL_MS']
        env_path = save_environment(role, env)
        proc = subprocess.Popen(command, cwd=cwds[role], stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                                start_new_session=True, env=env)
        entry = dict(role=role, proc=proc, fd=pidfd_open(proc.pid), meta=None, out=out, argv=command)
        children.append(entry)
        owned.append(entry)
        event(run, 'spawn', name=role, pid=proc.pid, argv=command, cwd=str(cwds[role]), env_path=env_path)
        deadline = min(time.monotonic()+40, started+args.timeout)
        while time.monotonic()<deadline:
            if proc.poll() is not None:
                raise RuntimeError(role+' exited during startup: '+str(proc.returncode))
            try:
                meta = identity(proc.pid)
            except (FileNotFoundError, ProcessLookupError):
                # transient: /proc/PID/exe is briefly unreadable across execve, or
                # the process is inside its exit window; the next poll()/identity()
                # round resolves it with a definite answer
                continue
            if meta['exe'] == str(run/'bin'/binary) and meta['cwd'] == str(cwds[role]):
                if entry['meta'] is None:
                    register(entry, meta)
                supervise()
                if all(listening(proc.pid, p) for p in ready_ports[:1]) and state(role, 'rpc-ready'):
                    event(run, 'listener_owned', name=role, ports=ready_ports)
                    return
            time.sleep(.1)
        raise RuntimeError(role+' readiness timeout')
    def state(role, name):
        path = run / 'control' / role / (name + '.json')
        if not path.exists():
            return None
        data = json.loads(path.read_text())
        e = next(e for e in children if e['role'] == role)
        if (data.get('run_id') != run.name or data.get('pid') != e['proc'].pid or
                data.get('state') != name):
            raise RuntimeError('application state identity/name mismatch: ' + str(path))
        if e['proc'].poll() is None:
            alive(e)
        return data

    def request(role, name):
        path = run / 'control' / role / (name + '.request')
        if path.exists():
            raise RuntimeError('duplicate control request')
        write_json(path.with_suffix('.tmp'), dict(run_id=run.name, time_ns=time.time_ns()))
        path.with_suffix('.tmp').rename(path)
        dirfd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        os.fsync(dirfd)
        os.close(dirfd)
        event(run, 'application_request', name=role, request=name)

    def audit_files():
        for e in owned:
            if e['meta'] is None or not alive(e):
                continue
            opened = []
            try:
                for f in (Path('/proc')/str(e['meta']['pid'])/'fd').iterdir():
                    try:
                        target = os.readlink(f)
                        flags = int((f.parent.parent/'fdinfo'/f.name).read_text().split('flags:\t')[1].splitlines()[0], 8)
                        if target.startswith('/') and flags & os.O_ACCMODE:
                            opened.append(dict(fd=f.name, target=target, flags=flags))
                            if not target.startswith(str(run)+'/') and not target.startswith('/dev/'):
                                raise RuntimeError('write fd outside run: '+target)
                    except FileNotFoundError:
                        pass
            except FileNotFoundError:
                if alive(e):
                    raise
            event(run, 'write_fd_audit', name=e['role'], identity=e['meta'], files=opened)
    driver_proc = None
    driver_out = None
    workload_stop_sent = False
    driver_validated = False
    hold_until = None
    try:
        supervise(force=True)
        spawn('storage','storage',['ycsb'],[storage_rpc,storage_meta,raft0,raft1])
        spawn('remote','remote',['ycsb'],[remote_rpc,remote_meta])
        for i in range(args.nodes):
            spawn('compute_'+chr(65+i),'compute',
                  ['ycsb','lazy',str(args.threads),'0.4','0.9',str(i)],[compute_ports[i]])
        if args.request_driver:
            deadline = min(time.monotonic() + 40, started + args.timeout)
            while time.monotonic() < deadline:
                supervise()
                if all(state('compute_' + chr(65+i), 'worker-' + str(w))
                       for i in range(args.nodes) for w in range(args.threads)):
                    break
                time.sleep(.1)
            else:
                raise RuntimeError('request worker readiness deadline')
            driver_env = dict(os.environ)
            driver_env.update(HCM_RUN_ID=run.name, HCM_RUN_DIR=str(run), HCM_DRIVER_MODE=args.driver_mode,
                              HCM_NODE_COUNT=str(args.nodes), HCM_THREADS_PER_NODE=str(args.threads),
                              HCM_PROJECT_ROOT=str(PROJECT), HCM_TREE_STATS_BIN=str(run / 'bin' / 'tree_stats'),
                              HCM_ABI_JSON=str(run / 'bin' / 'abi.json'),
                              HCM_FAULT_PRESEEDED_RECORDS=str(args.num_record),
                              TMPDIR=str(run / 'logs'), PYTHONPATH=str(run / 'tooling'), PYTHONDONTWRITEBYTECODE='1')
            env_path = save_environment('request_driver', driver_env)
            (run / 'ledger').mkdir(mode=0o700)
            driver_out = (run / 'logs' / 'driver.out').open('wb')
            driver_command = [sys.executable, '-B', str(run / 'tooling' / args.request_driver.name)]
            driver_proc = subprocess.Popen(driver_command, cwd=run, stdout=driver_out,
                                           stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                                           start_new_session=True, env=driver_env)
            driver_entry = dict(role='request_driver', proc=driver_proc, fd=pidfd_open(driver_proc.pid),
                                meta=None, out=driver_out, argv=driver_command, driver_tree=True)
            owned.append(driver_entry)
            meta = identity(driver_proc.pid)
            if meta['exe'] != str(Path(sys.executable).resolve()) or meta['cwd'] != str(run):
                raise RuntimeError('request driver did not exec the expected Python interpreter/cwd')
            register(driver_entry, meta)
            event(run, 'spawn', name='request_driver', pid=driver_proc.pid, argv=driver_command,
                  cwd=str(run), env_path=env_path)
        phase = 'WORKLOAD'
        fault_role = None  # 被注入 SIGKILL 的计算节点角色（其退出被预期、收尾时豁免）
        while time.monotonic()-started < args.timeout:
            supervise()
            # R2 小规模故障注入：driver 在"无在途事务"时机写 fault-inject.request，
            # 监督者验证身份后对目标 compute 发 SIGKILL。仅 --allow-fault 时启用。
            if args.allow_fault and fault_role is None:
                fault_request = run / 'fault-inject.request'
                if fault_request.exists():
                    spec = json.loads(fault_request.read_text())
                    if spec.get('run_id') != run.name:
                        raise RuntimeError('fault injection request identity mismatch')
                    victim = spec.get('victim', '')
                    entry = next((e for e in children if e['role'] == victim), None)
                    if entry is None or not victim.startswith('compute_'):
                        raise RuntimeError('fault injection target invalid: ' + victim)
                    if not process_alive(entry):
                        raise RuntimeError('fault injection target already dead: ' + victim)
                    send_verified(entry['meta'], signal.SIGKILL, entry['fd'])
                    fault_role = victim
                    record = dict(run_id=run.name, victim=victim, identity=entry['meta'],
                                  time_ns=time.time_ns(), signal='SIGKILL',
                                  meaning='R2 small-scale single-node fault; survivor takeover exercised by driver')
                    write_json(run / 'fault-injected.json', record)
                    event(run, 'injected_fault', **record)
                    result['injected_fault'] = record
                    result['R2'] = 'EXECUTED_SMALL_SCALE'
            if any(e['proc'].poll() not in (None, 0) and e['role'] != fault_role for e in children):
                raise RuntimeError('service failed without injected fault')
            if phase != 'CLOSING' and any(e['proc'].poll() is not None and e['role'] != fault_role for e in children):
                raise RuntimeError('service exited before coordinated shutdown')
            computes = [e for e in children[2:] if e['role'] != fault_role]
            if driver_proc is not None and not driver_validated:
                rc = driver_proc.poll()
                if rc is not None:
                    if rc != 0:
                        raise RuntimeError('request driver failed: ' + str(rc))
                    if any(alive(e) for e in owned if e.get('driver_tree') and e is not driver_entry):
                        raise RuntimeError('request driver exited while a checker/child remains alive')
                    model = json.loads((run / 'ledger' / 'final-model.json').read_text())
                    counts = model.get('outcome_counts', {})
                    if (model.get('run_id') != run.name or model.get('all_terminal_classes_accounted') is not True or
                            counts.get('UNKNOWN') != 0 or counts.get('UNFINISHED') != 0):
                        raise RuntimeError('request driver final accounting is incomplete or unresolved')
                    driver_validated = True
                    event(run, 'request_driver_complete', returncode=rc, R1='NOT_ACCEPTED')
                    if args.hold_after_validation:
                        if time.monotonic() + args.hold_after_validation + 30 >= started + args.timeout:
                            raise RuntimeError('insufficient remaining wall budget for hold and healthy drain')
                        hold_until = time.monotonic() + args.hold_after_validation
                        phase = 'HOLD_AFTER_DRIVER_VALIDATION'
                        result['hold'] = dict(seconds=args.hold_after_validation, started_at=time.time(),
                                              meaning='driver contract validated; independent R1 acceptance still required')
                        event(run, 'hold_started', **result['hold'], R1='NOT_ACCEPTED', R2='NOT_RUN')
                        write_json(run / 'hold-state.json', dict(run_id=run.name, state=phase, **result['hold']))
            if driver_validated and not workload_stop_sent and (hold_until is None or time.monotonic() >= hold_until):
                if hold_until is not None:
                    event(run, 'hold_budget_completed', R2='NOT_RUN')
                for e in computes:
                    request(e['role'], 'workload-stop')
                workload_stop_sent = True
                phase = 'WORKLOAD'
            if phase == 'WORKLOAD' and all(state(e['role'], 'drained') for e in computes):
                if driver_proc is not None and not workload_stop_sent:
                    raise RuntimeError('compute drained before request driver completed')
                result['compute_drain'] = {e['role']: state(e['role'], 'drained') for e in computes}
                for certificate in result['compute_drain'].values():
                    verify_compute_drain(certificate, args.threads)
                request('remote', 'drain')
                phase = 'HEARTBEAT_DRAIN'
            if phase == 'HEARTBEAT_DRAIN' and state('remote', 'drained'):
                request('storage', 'drain')
                phase = 'REPLAY_DRAIN'
            if phase == 'REPLAY_DRAIN' and state('storage', 'drained'):
                result['storage_cut'] = state('storage', 'drained')
                verify_storage_drain(result['storage_cut'])
                for e in computes:
                    request(e['role'], 'shutdown')
                request('remote', 'shutdown')
                request('storage', 'shutdown')
                phase = 'CLOSING'
            if phase == 'CLOSING' and all(e['proc'].poll() == 0 for e in children if e['role'] != fault_role):
                if not all(state(e['role'], 'closed') for e in children if e['role'] != fault_role):
                    raise RuntimeError('missing application closed certificate')
                for certificate in result['compute_drain'].values():
                    verify_compute_drain(certificate, args.threads)
                verify_storage_drain(result['storage_cut'])
                supervise(force=True)
                result['status'] = 'HEALTHY_LIFECYCLE'
                result['R0_lifecycle'] = 'PASS'
                result['detail'] = 'all compute tickets acknowledged; replay equals WAL tail; zero active undo; healthy exits; independent main verifier decides R1'
                break
            time.sleep(.1)
        else:
            raise RuntimeError('bounded diagnostic timeout; no recovery injection')
    except Exception as exc:
        failure_trace = traceback.format_exc()
        result['detail'] = str(exc) + '\n' + failure_trace.rstrip()
        event(run, 'failure', error=str(exc), traceback=failure_trace)
    finally:
        for s in reservations:
            s.close()
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        result['cleanup_errors'] = []
        try:
            discover_driver_children()
        except Exception as exc:
            result['status'] = 'FAIL'
            result['cleanup_errors'].append(str(exc))
            event(run, 'cleanup_discovery_failed', error=str(exc), traceback=traceback.format_exc())

        def fully_exited(e):
            return process_fully_exited(e)
        pending = []
        for e in reversed(owned):
            try:
                if alive(e):
                    result['status'] = 'FAIL'
                    send_verified(e['meta'], signal.SIGTERM, e['fd'])
                    event(run, 'failure_cleanup_term_NOT_R2', name=e['role'], identity=e['meta'])
                    pending.append(e)
                elif e.get('proc') is not None and e['proc'].poll() is None:
                    # identity already reports an exit but the child is not reaped
                    # yet: wait below for its natural full exit so the recorded
                    # exit status matches reality instead of guessing
                    event(run, 'cleanup_awaiting_exit', name=e['role'], identity=e['meta'])
                    pending.append(e)
            except (FileNotFoundError, ProcessLookupError):
                # exited exactly while being checked or signalled: log the race
                # and keep waiting for the full exit; never swallow it silently
                event(run, 'cleanup_exit_race', name=e['role'], identity=e['meta'])
                if e.get('proc') is not None and e['proc'].poll() is None:
                    pending.append(e)
            except Exception as exc:
                result['status'] = 'FAIL'
                result['cleanup_errors'].append(e['role'] + ': ' + str(exc))
                event(run, 'cleanup_refused', name=e['role'], error=str(exc), traceback=traceback.format_exc())
        term_deadline = min(time.monotonic() + 5, started + args.timeout - 2)
        while pending and time.monotonic() < term_deadline:
            pending = [e for e in pending if not fully_exited(e)]
            if pending:
                time.sleep(.05)
        for e in pending:
            try:
                if alive(e):
                    send_verified(e['meta'], signal.SIGKILL, e['fd'])
                    event(run, 'failure_cleanup_forced_kill_NOT_R2', name=e['role'], identity=e['meta'])
                elif e.get('proc') is not None and e['proc'].poll() is None:
                    # stuck mid-exit (zombie leader whose group members linger):
                    # the pidfd pins the original process, so a forced kill cannot
                    # hit a recycled pid and resolves the lingering group
                    if LIBC.syscall(424, e['fd'], signal.SIGKILL, 0, 0) < 0:
                        raise OSError(ctypes.get_errno(), 'pidfd_send_signal')
                    event(run, 'failure_cleanup_forced_kill_NOT_R2', name=e['role'], identity=e['meta'],
                          note='pidfd_pinned_kill_for_unreaped_exit')
            except (FileNotFoundError, ProcessLookupError):
                event(run, 'cleanup_exit_race', name=e['role'], identity=e['meta'])
            except Exception as exc:
                result['status'] = 'FAIL'
                result['cleanup_errors'].append(e['role'] + ': ' + str(exc))
                event(run, 'cleanup_refused', name=e['role'], error=str(exc), traceback=traceback.format_exc())
        kill_deadline = min(time.monotonic() + 5, started + args.timeout)
        while pending and time.monotonic() < kill_deadline:
            pending = [e for e in pending if not fully_exited(e)]
            if pending:
                time.sleep(.05)
        result['owned_process_exits'] = {}
        for e in owned:
            rc = e['proc'].poll() if e.get('proc') is not None else None
            pidfd_done = bool(select.select([e['fd']], [], [], 0)[0])
            if e.get('proc') is not None:
                exited = rc is not None or pidfd_done
            else:
                try:
                    exited = pidfd_done or not alive(e)
                except Exception:
                    exited = pidfd_done
            record = dict(identity=e['meta'], exited=exited, returncode=rc,
                          exit_status_known=rc is not None)
            if not exited:
                proc_state = 'unknown'
                if e['meta'] is not None:
                    try:
                        proc_state = (Path('/proc') / str(e['meta']['pid']) / 'stat').read_text().rsplit(')', 1)[1].split()[0]
                    except (OSError, ValueError, IndexError):
                        proc_state = 'gone'
                record['proc_state'] = proc_state
                record['unresolved'] = ('zombie not fully reaped (lingering group members)' if proc_state == 'Z'
                                        else 'still executing (proc state ' + proc_state + ')')
                result['status'] = 'FAIL'
                result['cleanup_errors'].append(e['role'] + ': ' + record['unresolved'] + ', preserved as unresolved')
            result['owned_process_exits'][e['role']] = record
            event(run, 'exit', name=e['role'], **record)
            if e.get('out') is not None:
                e['out'].close()
            os.close(e['fd'])
        result['peak_cluster_rss_bytes'] = peak_rss
        result['rss_scope'] = budget['scope']
        result['wall_seconds'] = time.monotonic() - started
        result['last_resource_sample'] = filesystem_sample
        result['exits'] = {e['role']:e['proc'].poll() for e in children}
        if driver_proc is not None:
            result['request_driver_exit'] = driver_proc.poll()
        if driver_out is not None and not driver_out.closed:
            driver_out.close()
        write_json(run/'summary.json',result)
    print(json.dumps(result), flush=True)
    return 0 if result['status']=='HEALTHY_LIFECYCLE' else 1


if __name__ == '__main__':
    sys.exit(main())
