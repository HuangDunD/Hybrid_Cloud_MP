#!/usr/bin/env python3
"""P4 multi-node Lazy MVCC integration on one Linux host: 2 compute nodes,
one storage node and one remote/meta node. The driver uses explicit control
commands (barriers, paused flush, held publication) instead of sleeps to decide
correctness. Fresh data, epoch and ports for every run."""
from concurrent.futures import ThreadPoolExecutor
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import threading
import time

p = argparse.ArgumentParser()
p.add_argument('--source', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
p.add_argument('--bench', choices=['ycsb'], default='ycsb')
a = p.parse_args()
a.source = a.source.resolve()
a.output = a.output.resolve()
a.output.mkdir(parents=True, exist_ok=False)
OUT = a.output
shutil.copytree(a.source / 'config', OUT / 'config')
shutil.copytree(a.source / 'workload/tpcc/tpcc_tables', OUT / 'workload/tpcc/tpcc_tables')

# 2 compute RPC + 2 control + remote rpc/meta + storage rpc/meta.
ports = []
sockets = []
for _ in range(8):
    s = socket.socket(); s.bind(('127.0.0.1', 0)); ports.append(s.getsockname()[1]); sockets.append(s)
compute_ports = ports[0:2]
control_ports = ports[2:4]
remote, remote_meta, storage, storage_meta = ports[4:8]
cfg = json.loads((OUT / 'config/compute_node_config.json').read_text())
cfg.update(mvcc_epoch=hashlib.sha256(str(OUT).encode()).hexdigest()[:16],
           undo_buffer_pool_size_pages=1, undo_lock_table_initial_capacity=4,
           buffer_pool_size='64kB', use_rdma=False, push_page_with_scheduler=0,
           log_flush_interval_ms=3, log_flush_batch_trigger=16)
cfg['local_compute_node'].update(machine_id=0, thread_num_per_machine=2, coroutine_num=1,
    txn_system=1, generate_log=1, lock_mode=1, benchmark_run_mode='count', batch_size=16)
cfg['remote_compute_nodes'] = dict(remote_compute_node_ips=['127.0.0.1', '127.0.0.1'],
                                   remote_compute_node_port=compute_ports)
cfg['remote_server_nodes'] = dict(remote_server_node_ips=['127.0.0.1'],
                                  remote_server_node_port=[remote],
                                  remote_server_node_meta_port=[remote_meta])
cfg['remote_storage_nodes'] = dict(remote_storage_node_ips=['127.0.0.1'],
                                   remote_storage_node_rpc_port=[storage],
                                   remote_storage_node_meta_port=[storage_meta])
def put(name, data): (OUT / name).write_text(json.dumps(data, indent=2) + '\n')
put('config/compute_node_config.json', cfg)
put('config/remote_server_config.json', {'local_server_node': {'local_rpc_port': remote, 'local_meta_port': remote_meta}})
put('config/storage_node_config.json', {'local_storage_node': dict(machine_num=1, machine_id=0, local_rpc_port=storage, local_meta_port=storage_meta, random_generate=False, page_read_io='buffered')})
for bench in ['ycsb', 'smallbank', 'tpcc']:
    name = f'config/{bench}_config.json'
    c = json.loads((OUT / name).read_text())
    c[bench]['attempted_num'] = 100
    if bench == 'ycsb': c[bench].update(num_record=2000, num_hot_record=100)
    if bench == 'smallbank': c[bench].update(num_accounts=2000, num_hot_accounts=100)
    put(name, c)
for s in sockets: s.close()

children = {}
files = []
commands = {}
def launch(name, command, cwd):
    directory = OUT / 'run' / cwd
    directory.mkdir(parents=True, exist_ok=True)
    output = open(OUT / (name + '.log'), 'w'); files.append(output)
    commands[name] = command
    children[name] = subprocess.Popen(command, cwd=directory, stdout=output,
                                      stderr=subprocess.STDOUT, start_new_session=True)
def listening(port):
    for path in ['/proc/net/tcp', '/proc/net/tcp6']:
        for line in Path(path).read_text().splitlines()[1:]:
            cols = line.split()
            if int(cols[1].split(':')[1], 16) == port and cols[3] == '0A': return True
    return False
trace_file = open(OUT / 'control-trace.log', 'a', buffering=1)
trace_lock = threading.Lock()
def trace(message):
    with trace_lock:
        trace_file.write(message + '\n')
class Node:
    def __init__(self, port):
        self.port = port
        self.sock = socket.create_connection(('127.0.0.1', port), timeout=120)
        self.file = self.sock.makefile('rb')
    def cmd(self, line):
        trace(f'{self.port} -> {line}')
        self.sock.sendall((line + '\n').encode())
        reply = self.file.readline()
        assert reply, f'control connection closed after {line}'
        text = reply.decode().rstrip('\n')
        trace(f'{self.port} <- {text}')
        return text
    def try_cmd(self, line):
        try: return self.cmd(line)
        except (BrokenPipeError, ConnectionResetError): return None
    def close(self):
        try: self.file.close(); self.sock.close()
        except OSError: pass

result = {'protocol': 'remote_tso_tit', 'ports': ports, 'commands': commands, 'checks': []}
def check(condition, message):
    result['checks'].append({'ok': bool(condition), 'message': message})
    assert condition, message

def parse_kv(reply):
    assert reply.startswith('OK '), reply
    out = {}
    for token in reply[3:].split():
        k, v = token.split('=', 1)
        out[k] = int(v)
    return out

def begin(node):
    reply = node.cmd('BEGIN'); parts = reply.split()
    assert parts[0] == 'OK' and len(parts) == 4, reply
    return {'handle': int(parts[1]), 'snapshot': int(parts[2]), 'local': int(parts[3])}
def read(node, tx, table, key):
    reply = node.cmd(f"READ {tx['handle']} {table} {key}")
    assert reply.startswith('VALUE ') or reply == 'NOTFOUND', reply
    return None if reply == 'NOTFOUND' else int(reply.split()[1])
def write(node, tx, table, key, value):
    reply = node.cmd(f"WRITE {tx['handle']} {table} {key} {value}")
    assert reply == 'OK', reply
def commit(node, tx):
    reply = node.cmd(f"COMMIT {tx['handle']}")
    assert reply.startswith('OK '), reply
    return int(reply.split()[1])
def abort(node, tx):
    assert node.cmd(f"ABORT {tx['handle']}") == 'OK'

def snapshot(node, tx):
    reply = node.cmd(f"SNAPSHOT {tx['handle']}")
    assert reply.startswith('OK '), reply
    return int(reply.split()[1])

def check_tso_tit_order(n0, n1, table, key0, key1):
    base = begin(n0)
    write(n0, base, table, key0, 80)
    write(n0, base, table, key1, 10)
    base_cts = commit(n0, base)
    old = begin(n1)
    check(old['snapshot'] == base_cts, 'old reader starts before both writers')

    writer_a = begin(n0)
    write(n0, writer_a, table, key0, 70)
    assert n0.cmd('HOLD_INSTALL tso_reserved') == 'OK'
    a_result = int(n0.cmd(f"COMMIT_ASYNC {writer_a['handle']}").split()[1])
    assert n0.cmd('WAIT_INSTALL_GATE') == 'OK'
    between = begin(n1)
    a_cts = between['snapshot']
    check(a_cts == base_cts + 1, 'snapshot includes A reservation before its commit WAL')
    check(read(n1, old, table, key0) == 80,
          'snapshot older than A reads undo immediately despite pending commit')
    writer_b = begin(n1)
    write(n1, writer_b, table, key1, 60)
    b_cts = commit(n1, writer_b)
    check(b_cts == a_cts + 1 and n0.cmd(f'TRY_RESULT {a_result}') == 'PENDING',
          'independent writer B returns success while lower CTS A still lacks commit WAL')

    latest = begin(n1)
    check(latest['snapshot'] == b_cts and read(n1, latest, table, key1) == 60,
          'reader started after B success sees B while A remains pending')
    check(read(n1, between, table, key1) == 10, 'snapshot between A and B excludes B')
    # n1's command thread will block in the real read. Observe its waiter at n0
    # before releasing A, so scheduling cannot turn this into a false pass.
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending_read = pool.submit(read, n1, latest, table, key0)
        try:
            deadline = time.monotonic() + 30
            while not parse_kv(n0.cmd('STATS'))['tit_waiters']:
                assert not pending_read.done(), 'reader returned before A commit WAL'
                assert time.monotonic() < deadline, 'reader did not enter A home TIT wait'
                time.sleep(0.01)
            check(not pending_read.done(), 'reader of A at latest snapshot waits at A home')
        finally:
            assert n0.cmd('RELEASE_INSTALL_GATE') == 'OK'
        check(pending_read.result(timeout=30) == 70, 'reader sees A only after its WAL is durable')
    check(n0.cmd(f'RESULT {a_result}') == f'OK {a_cts}', 'A returns its originally reserved CTS')
    rpc_count = parse_kv(n1.cmd('STATS'))['tit_status_rpcs']
    check(read(n1, latest, table, key0) == 70 and read(n1, between, table, key0) == 70,
          'terminal TIT decision is visible at and above A CTS')
    check(parse_kv(n1.cmd('STATS'))['tit_status_rpcs'] == rpc_count,
          'repeat readers reuse the cached remote terminal TIT decision')
    check(read(n1, old, table, key0) == 80 and read(n1, old, table, key1) == 10,
          'old reader keeps its original versions after both commits')
    commit(n1, latest)
    commit(n1, between)

    before = parse_kv(n0.cmd('STATS'))
    read_only = begin(n0)
    commit(n0, read_only)
    after = parse_kv(n0.cmd('STATS'))
    check(after['tso_snapshots'] == before['tso_snapshots'] + 1
          and after['tso_snapshot_ns'] > before['tso_snapshot_ns']
          and after['tso_commits'] == before['tso_commits']
          and after['tso_commit_ns'] == before['tso_commit_ns'],
          'read-only transaction obtains one TSO snapshot and allocates no commit timestamp')
    return old

try:
    launch('remote', [str(a.source / 'build/remote_server/remote_node'), a.bench], 'remote_server')
    launch('storage', [str(a.source / 'build/storage_server/storage_pool'), a.bench], 'storage_server')
    deadline = time.monotonic() + 180
    while not all(listening(port) for port in [remote, remote_meta, storage, storage_meta]):
        assert all(child.poll() is None for child in children.values()), 'server exited before readiness'
        assert time.monotonic() < deadline, 'server readiness timeout'
        time.sleep(0.05)
    # storage_pool's meta service accepts one compute at a time; stagger the
    # handshakes so the second listener exists before the second compute connects.
    for node in [0, 1]:
        launch(f'node{node}', [str(a.source / 'build/tests/lazy_mvcc_cluster_node'),
                               a.bench, str(node), str(control_ports[node])], f'node{node}')
        if node == 0:
            time.sleep(0.5)
    deadline = time.monotonic() + 120
    nodes = [None, None]
    while nodes[0] is None or nodes[1] is None:
        assert all(child.poll() is None for child in children.values()), 'compute exited before readiness'
        assert time.monotonic() < deadline, 'control readiness timeout'
        for n in [0, 1]:
            if nodes[n] is not None: continue
            try: nodes[n] = Node(control_ports[n])
            except OSError: pass
        if nodes[0] is None or nodes[1] is None: time.sleep(0.05)
    n0, n1 = nodes
    T = 0
    key0 = int(n0.cmd(f'KEY {T}').split()[1])
    key0_page = int(n0.cmd(f'PAGE_OF {T} {key0}').split()[1])
    key1 = int(n0.cmd(f'KEY_OTHER_PAGE {T} {key0_page}').split()[1])
    check(key0 != key1 and int(n0.cmd(f'PAGE_OF {T} {key1}').split()[1]) != key0_page,
          'unrelated key is on a different main page')
    check(n1.cmd(f'KEY {T}').split()[1] == str(key0), 'both nodes resolve the same first key')

    # 1. Establish a committed base value on A, then verify B's new/old snapshots.
    a = begin(n0); write(n0, a, T, key0, 100); cts1 = commit(n0, a)
    b = begin(n1); check(snapshot(n1, b) == cts1, 'B begins at A commit timestamp'); check(read(n1, b, T, key0) == 100, 'B reads A commit'); commit(n1, b)
    b_old = begin(n1); old_snapshot = snapshot(n1, b_old)
    a = begin(n0); write(n0, a, T, key0, 90); cts2 = commit(n0, a)
    check(cts2 == cts1 + 1, 'one global position per Lazy commit')
    b_new = begin(n1); check(snapshot(n1, b_new) == cts2, 'post-success B Begin includes commit')
    check(read(n1, b_new, T, key0) == 90, 'B new snapshot reads new value')
    check(read(n1, b_old, T, key0) == 100, 'B old snapshot keeps old value')
    check(read(n1, b_old, T, key0) == 100, 'B old snapshot repeat-read is fixed')
    commit(n1, b_new); commit(n1, b_old)

    b_high_old = check_tso_tit_order(n0, n1, T, key0, key1)

    # 4. Move the main page to B before A's transaction ends; B reads history,
    #    then A aborts after both main and undo caches are evicted.
    a = begin(n0)
    write(n0, a, T, key0, 50)
    n0.cmd('FLUSH')
    n1.cmd(f'FETCH_X_MAIN {T} {key0}')
    status = n1.cmd(f"QUERY_STATUS 0 {a['local']}")
    check(status == 'FOUND ACTIVE 0', 'cross-node creator status is ACTIVE, not a history terminal')
    check(read(n1, b_high_old, T, key0) == 80, 'old snapshot reads history of uncommitted migrated row')
    n0.cmd('CLEAR_UNDO'); n1.cmd('CLEAR_UNDO')
    n1.cmd(f'EVICT_MAIN {T} {key0}'); n0.cmd(f'EVICT_MAIN {T} {key0}')
    abort(n0, a)
    check(read(n1, b_high_old, T, key0) == 80, 'old snapshot still reads old value after abort')
    current = n1.cmd(f'CURRENT {T} {key0}')
    check(current.startswith('OK 70 '), 'abort restored the migrated main page')
    check(n1.cmd(f"QUERY_STATUS 0 {a['local']}") == 'FOUND ABORTED 0',
          'aborted creator is explicitly ABORTED, not unknown/committed')
    commit(n1, b_high_old)

    # 5. Commit after main-page migration and eviction.
    a = begin(n0); write(n0, a, T, key0, 40); n0.cmd('FLUSH')
    n1.cmd(f'FETCH_X_MAIN {T} {key0}')
    n0.cmd('CLEAR_UNDO'); n1.cmd('CLEAR_UNDO')
    cts6 = commit(n0, a)
    b_mig = begin(n1); check(snapshot(n1, b_mig) == cts6, 'commit after migration advances B')
    check(read(n1, b_mig, T, key0) == 40, 'new snapshot sees committed migrated row')
    commit(n1, b_mig)

    # 6. Concurrent undo append on A and old-record history reads on B.
    a = begin(n0); write(n0, a, T, key0, 1000)
    b_conc = begin(n1); check(snapshot(n1, b_conc) == cts6, 'concurrent reader keeps cts6')
    barrier = threading.Barrier(3)
    out = {}
    def writer():
        barrier.wait(); out['write'] = n0.cmd(f'WRITE_LOOP {a["handle"]} {T} {key0} 64 1001 200')
    def reader():
        barrier.wait(); out['read'] = n1.cmd(f'READ_LOOP {b_conc["handle"]} {T} {key0} 64 40')
    t0 = threading.Thread(target=writer); t1 = threading.Thread(target=reader)
    t0.start(); t1.start(); barrier.wait(); t0.join(); t1.join()
    check(out['read'].startswith('OK '), 'concurrent old-record reads complete without half records: ' + out['read'])
    check(out['write'].startswith('OK '), 'concurrent undo appends complete: ' + out['write'])
    cts7 = commit(n0, a)
    check(read(n1, b_conc, T, key0) == 40, 'pre-write snapshot still reads 40 after 64 overwrites')
    b_latest = begin(n1); check(read(n1, b_latest, T, key0) == 1064, 'new snapshot sees last overwrite')
    commit(n1, b_latest)
    stats0 = parse_kv(n0.cmd('STATS')); stats1 = parse_kv(n1.cmd('STATS'))
    check(stats1['undo_remote_fetches'] > 0, 'B performed remote undo page fetches')
    check(stats0['undo_transfers'] + stats1['undo_transfers'] > 0, 'undo permission transfers were observed')
    check(stats0['undo_evictions'] + stats1['undo_evictions'] > 0, 'undo frame evictions were observed')
    check(stats0['tit_completed_waits'] > 0 and stats1['tit_status_rpcs'] > 0,
          'both the home TIT wait and remote status lookup are recorded')
    result['stats'] = {'node0': stats0, 'node1': stats1}

    # 7. Historical read after both main and undo caches are evicted.
    hist = begin(n1); check(snapshot(n1, hist) == cts7, 'historical reader at cts7')
    a = begin(n0); write(n0, a, T, key0, 30); n0.cmd('FLUSH'); cts8 = commit(n0, a)
    check(cts8 == cts7 + 1, 'history test commit gets the next position')
    n0.cmd('CLEAR_UNDO'); n1.cmd('CLEAR_UNDO')
    n1.cmd(f'EVICT_MAIN {T} {key0}'); n0.cmd(f'EVICT_MAIN {T} {key0}')
    check(read(n1, hist, T, key0) == 1064, 'old snapshot reads history after main+undo eviction')
    b_now = begin(n1); check(read(n1, b_now, T, key0) == 30, 'new snapshot reads latest after eviction')
    commit(n1, hist); commit(n1, b_now)
    check(n1.cmd(f'QUERY_STATUS 0 999999999').startswith('UNKNOWN'), 'unknown creator is explicit, never treated as committed')

    # Shutdown both compute nodes together: the remote meta barrier needs both.
    shutdown = {}
    def stop(name, node):
        shutdown[name] = node.try_cmd('SHUTDOWN')
    s0 = threading.Thread(target=stop, args=('n0', n0)); s1 = threading.Thread(target=stop, args=('n1', n1))
    s0.start(); s1.start(); s0.join(); s1.join()
    check(shutdown.get('n0') == 'OK' and shutdown.get('n1') == 'OK', 'both compute nodes shut down cleanly')
    n0.close(); n1.close()
    for name in ['node0', 'node1', 'storage', 'remote']:
        code = children[name].wait(timeout=60)
        result[name + '_exit'] = code
        check(code == 0, f'{name} process exit 0 (got {code})')
    result['passed'] = True
except BaseException as error:
    import traceback
    result['error'] = traceback.format_exc()
    raise
finally:
    for child in children.values():
        if child.poll() is None:
            try: os.killpg(child.pid, signal.SIGTERM)
            except ProcessLookupError: pass
            try: child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                try: os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError: pass
                child.wait()
    result['exits'] = {name: child.returncode for name, child in children.items()}
    (OUT / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
    for f in files: f.close()
    print(json.dumps(result, indent=2), flush=True)
