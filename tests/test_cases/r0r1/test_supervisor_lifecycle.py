#!/usr/bin/env python3
"""Targeted supervisor lifecycle regressions.

Covers the exact race windows fixed previously in scripts/r0r1/cluster.py:
- exit window (process gone while pidfd still unreadable / proc not reaped)
- signalling during exit (identity check races with process disappearance)
- pid reuse refusal (identity mismatch must raise, never signal)
- driver-descendant style entries (proc=None) against zombies/reaped pids
- pidfd-pinned signalling after exit must not fail or hit a recycled pid

These tests import the real cluster.py functions; no cluster is started.
"""
import importlib.util
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import time

PROJECT = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location('hcm_cluster', PROJECT / 'scripts' / 'r0r1' / 'cluster.py')
cluster = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cluster)

PASSED = []
FAILED = []


def check(name, function):
    try:
        function()
    except Exception as exc:  # noqa: BLE001 - regression harness reports all failures
        FAILED.append((name, repr(exc)))
        print(f'FAIL {name}: {exc!r}')
    else:
        PASSED.append(name)
        print(f'PASS {name}')


def spawn_sleep(seconds='60'):
    return subprocess.Popen([sys.executable, '-c', f'import time; time.sleep({seconds})'],
                            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL, start_new_session=True)


def entry_for(proc):
    return dict(role='test', proc=proc, fd=cluster.pidfd_open(proc.pid),
                meta=cluster.identity(proc.pid))


def wait_exit(proc, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return True
        time.sleep(0.02)
    return False


def test_alive_reaped_exit():
    proc = spawn_sleep()
    entry = entry_for(proc)
    assert cluster.process_alive(entry) is True
    proc.terminate()
    assert wait_exit(proc)
    assert proc.poll() is not None
    # reaped direct child: poll() already returns the exit status
    assert cluster.process_alive(entry) is False
    os.close(entry['fd'])


def test_alive_exit_window_unreaped_pidfd():
    proc = spawn_sleep()
    entry = entry_for(proc)
    proc.terminate()
    # do NOT call poll() before the exit is observed: the exit window is
    # covered by the pidfd becoming readable before any reaping
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if select.select([entry['fd']], [], [], 0)[0]:
            break
        time.sleep(0.02)
    assert select.select([entry['fd']], [], [], 0)[0], 'pidfd never became readable after exit'
    assert cluster.process_alive(entry) is False
    assert cluster.process_fully_exited(entry) is True
    proc.poll()
    os.close(entry['fd'])


def test_alive_procless_zombie():
    # driver-descendant style entry: no Popen handle (proc=None). After the
    # descendant exits and is reaped, the entry must read as not-alive through
    # the pidfd/procless path and never raise a supervisor error.
    proc = spawn_sleep()
    meta = cluster.identity(proc.pid)
    fd = cluster.pidfd_open(proc.pid)
    proc.terminate()
    assert wait_exit(proc)
    proc.poll()  # reaped: pidfd readable, /proc entry gone
    entry = dict(role='driver_child', proc=None, fd=fd, meta=meta)
    assert cluster.process_alive(entry) is False
    assert cluster.process_fully_exited(entry) is True
    os.close(fd)


def test_alive_zombie_state_detected():
    # a real unreaped zombie: /proc/<pid> exists with state Z; identity()
    # must raise ProcessLookupError so process_alive reports False instead of
    # treating the stale /proc entry as a live process
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:  # child: signal readiness, then exit and stay a zombie
        os.close(read_fd)
        os.write(write_fd, b'r')
        os.close(write_fd)
        os._exit(0)
    os.close(write_fd)
    os.read(read_fd, 1)
    os.close(read_fd)
    fd = cluster.pidfd_open(pid)
    deadline = time.monotonic() + 10
    state = ''
    while time.monotonic() < deadline:
        try:
            state = (Path('/proc') / str(pid) / 'stat').read_text().rsplit(')', 1)[1].split()[0]
        except FileNotFoundError:
            state = 'gone'
            break
        if state == 'Z':
            break
        time.sleep(0.02)
    assert state == 'Z', f'child did not become a zombie (state={state})'
    meta = dict(pid=pid, starttime='0', exe='x', cwd='x', boot_id='x')  # meta unused on this path
    entry = dict(role='zombie', proc=None, fd=fd, meta=meta)
    assert cluster.process_alive(entry) is False
    os.close(fd)
    os.waitpid(pid, 0)


def test_alive_pid_reuse_refused():
    # identity guard: a recorded meta whose fields no longer match /proc must
    # raise instead of reporting alive (this is what detects a recycled pid
    # during the window where the pidfd is not yet readable)
    x = spawn_sleep()
    fd = cluster.pidfd_open(x.pid)
    stale = cluster.identity(x.pid)
    stale['starttime'] = '1'  # emulate a pre-reuse recorded identity
    entry = dict(role='reused', proc=None, fd=fd, meta=stale)
    try:
        cluster.process_alive(entry)
    except RuntimeError as exc:
        assert 'identity changed' in str(exc)
    else:
        raise AssertionError('pid reuse was not refused')
    finally:
        os.close(fd)
        x.terminate(); x.poll()


def test_send_verified_refuses_mismatch():
    x = spawn_sleep()
    fd = cluster.pidfd_open(x.pid)
    stale = cluster.identity(x.pid)
    stale['exe'] = '/not/the/same/exe'  # recorded identity no longer matches
    try:
        cluster.send_verified(stale, signal.SIGTERM, fd)
    except RuntimeError as exc:
        assert 'identity mismatch' in str(exc)
    else:
        raise AssertionError('mismatched identity signal was not refused')
    finally:
        os.close(fd)
        x.terminate(); x.poll()


def test_send_verified_exit_race():
    # process exits and is fully reaped before the signal: /proc/<pid> is gone.
    # cluster.py catches (FileNotFoundError, ProcessLookupError) for exactly
    # this race (cleanup_exit_race); the verifier must expose the same classes.
    proc = spawn_sleep()
    meta = cluster.identity(proc.pid)
    fd = cluster.pidfd_open(proc.pid)
    proc.terminate()
    assert wait_exit(proc)
    proc.poll()
    deadline = time.monotonic() + 5
    gone = False
    while time.monotonic() < deadline:
        try:
            cluster.identity(meta['pid'])
        except (FileNotFoundError, ProcessLookupError):
            gone = True
            break
        time.sleep(0.02)
    assert gone, 'reaped pid still observable; cannot emulate the exit race'
    try:
        cluster.send_verified(meta, signal.SIGTERM, fd)
    except (FileNotFoundError, ProcessLookupError):
        pass
    else:
        raise AssertionError('signal to a vanished pid did not raise the expected race error')
    os.close(fd)


def test_pidfd_pinned_signal_after_exit():
    # the pinned-kill path (cluster.py: pidfd_pinned_kill_for_unreaped_exit)
    # signals a process that has exited but is NOT yet reaped: the pidfd pins
    # the original pid, the call must succeed and cannot hit a recycled pid.
    # (Once fully reaped, pidfd_send_signal would return ESRCH; the supervisor
    # only uses this path while poll() still reports the child as running.)
    proc = spawn_sleep()
    fd = cluster.pidfd_open(proc.pid)
    proc.terminate()
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if select.select([fd], [], [], 0)[0]:
            break
        time.sleep(0.02)
    assert select.select([fd], [], [], 0)[0], 'pidfd not readable after exit'
    rc = cluster.LIBC.syscall(424, fd, signal.SIGKILL, 0, 0)
    assert rc == 0, f'pidfd_send_signal to unreaped exited pinned process failed: {rc}'
    proc.poll()
    os.close(fd)


def test_fully_exited_matrix():
    live = spawn_sleep()
    fd = cluster.pidfd_open(live.pid)
    live_entry = dict(role='live', proc=live, fd=fd, meta=cluster.identity(live.pid))
    assert cluster.process_fully_exited(live_entry) is False
    live.terminate()
    assert wait_exit(live)
    live.poll()
    assert cluster.process_fully_exited(live_entry) is True
    os.close(fd)

    unreaped = spawn_sleep()
    fd2 = cluster.pidfd_open(unreaped.pid)
    unreaped_entry = dict(role='unreaped', proc=unreaped, fd=fd2, meta=cluster.identity(unreaped.pid))
    unreaped.terminate()
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and not select.select([fd2], [], [], 0)[0]:
        time.sleep(0.02)
    assert select.select([fd2], [], [], 0)[0], 'pidfd not readable for unreaped exit'
    assert cluster.process_fully_exited(unreaped_entry) is True
    unreaped.poll()
    os.close(fd2)


def test_descendant_observation_predicate():
    # discover_driver_children registers descendants with proc=None and a
    # pidfd; when the descendant exits and is reaped by init (orphan path),
    # the entry must read as not-alive, never as a supervisor error
    parent = subprocess.Popen(
        [sys.executable, '-c',
         'import subprocess,sys,time;'
         'subprocess.Popen([sys.executable,"-c","import time;time.sleep(0.5)"]);'
         'time.sleep(60)'],
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL, start_new_session=True)
    try:
        deadline = time.monotonic() + 10
        child_pid = None
        while time.monotonic() < deadline:
            try:
                pids = set()
                for task in (Path('/proc') / str(parent.pid) / 'task').iterdir():
                    pids.update(map(int, (task / 'children').read_text().split()))
                if pids:
                    child_pid = pids.pop()
                    break
            except FileNotFoundError:
                pass
            time.sleep(0.05)
        assert child_pid is not None, 'parent did not spawn the descendant'
        fd = cluster.pidfd_open(child_pid)
        meta = cluster.identity(child_pid)
        entry = dict(role='driver_child_test', proc=None, fd=fd, meta=meta)
        assert cluster.process_alive(entry) is True
        # the short-lived descendant exits on its own; init reaps the orphan
        deadline = time.monotonic() + 15
        result = None
        while time.monotonic() < deadline:
            result = cluster.process_alive(entry)
            if result is False:
                break
            time.sleep(0.1)
        assert result is False, 'orphaned descendant never reported as gone'
        os.close(fd)
    finally:
        parent.terminate(); parent.poll()


def main():
    checks = [
        ('alive_reaped_exit', test_alive_reaped_exit),
        ('alive_exit_window_unreaped_pidfd', test_alive_exit_window_unreaped_pidfd),
        ('alive_procless_zombie', test_alive_procless_zombie),
        ('alive_zombie_state_detected', test_alive_zombie_state_detected),
        ('alive_pid_reuse_refused', test_alive_pid_reuse_refused),
        ('send_verified_refuses_mismatch', test_send_verified_refuses_mismatch),
        ('send_verified_exit_race', test_send_verified_exit_race),
        ('pidfd_pinned_signal_after_exit', test_pidfd_pinned_signal_after_exit),
        ('fully_exited_matrix', test_fully_exited_matrix),
        ('descendant_observation_predicate', test_descendant_observation_predicate),
    ]
    for name, function in checks:
        check(name, function)
    print(f'SUPERVISOR_LIFECYCLE_REGRESSIONS pass={len(PASSED)} fail={len(FAILED)}')
    if FAILED:
        for name, error in FAILED:
            print(f'FAILED {name}: {error}')
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
