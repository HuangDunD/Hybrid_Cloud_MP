#!/usr/bin/env python3
"""P5 2PC / mix MVCC integration on one Linux host: two compute nodes, one
storage node and one remote/meta node. The driver uses explicit control events
(install gates, commit timestamp gates, paused WAL flushes) instead of sleeps to
establish ordering. Fresh data, epoch and ports for every run.

Mode 2 covers the ordinary 2PC variant selected by --variant. Mode 4 covers the
mix Lazy and distributed branches; each transaction is forced through
NoteKeyAccess/DecideCommitMode without changing the scheduler itself.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
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
p.add_argument('--mode', type=int, choices=[2, 4], default=2)
p.add_argument('--variant', choices=['2pc', '2pc-cornus', '2pc-cornus-opt'], default='2pc')
p.add_argument('--crash-recovery', action='store_true')
p.add_argument('--bench', choices=['ycsb'], default='ycsb')
p.add_argument('--undo-pages', type=int, default=32)
a = p.parse_args()
a.source = a.source.resolve()
a.output = a.output.resolve()
a.output.mkdir(parents=True, exist_ok=False)
OUT = a.output
shutil.copytree(a.source / 'config', OUT / 'config')
shutil.copytree(a.source / 'workload/tpcc/tpcc_tables', OUT / 'workload/tpcc/tpcc_tables')

ports = []
sockets = []
for _ in range(8):
    s = socket.socket(); s.bind(('127.0.0.1', 0)); ports.append(s.getsockname()[1]); sockets.append(s)
compute_ports = ports[0:2]
control_ports = ports[2:4]
remote, remote_meta, storage, storage_meta = ports[4:8]
cfg = json.loads((OUT / 'config/compute_node_config.json').read_text())
cfg.update(mvcc_epoch=hashlib.sha256(str(OUT).encode()).hexdigest()[:16],
           undo_buffer_pool_size_pages=a.undo_pages, undo_lock_table_initial_capacity=16,
           buffer_pool_size=('16kB' if a.mode == 2 else '128kB'), use_rdma=False, push_page_with_scheduler=0,
           partition_size_per_table=8, log_flush_interval_ms=3, log_flush_batch_trigger=16)
cfg['local_compute_node'].update(machine_id=0, thread_num_per_machine=2, coroutine_num=1,
    txn_system=a.mode, generate_log=1, lock_mode=1, parallel_page_fetch=1, benchmark_run_mode='count', batch_size=16,
    two_pc_variant=a.variant, cornus_prepare_commit=0)
cfg.setdefault('hybrid_cost_model', {})['enabled'] = 0
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
    output = open(OUT / (name + '.log'), 'a'); files.append(output)
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
        self.sock = socket.create_connection(('127.0.0.1', port), timeout=180)
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

result = {'mode': a.mode, 'variant': a.variant, 'ports': ports, 'undo_pages': a.undo_pages,
          'commands': commands, 'checks': []}
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

def begin(node, branch=None):
    reply = node.cmd('BEGIN'); parts = reply.split()
    assert parts[0] == 'OK' and len(parts) == 4, reply
    tx = {'handle': int(parts[1]), 'snapshot': int(parts[2]), 'local': int(parts[3]), 'node': node}
    if branch is not None:
        assert node.cmd(f"FORCE {tx['handle']} {branch}") == 'OK'
    return tx
def read(node, tx, table, key):
    reply = node.cmd(f"READ {tx['handle']} {table} {key}")
    assert reply.startswith('VALUE ') or reply == 'NOTFOUND', reply
    return None if reply == 'NOTFOUND' else int(reply.split()[1])
def write(node, tx, table, key, value):
    return node.cmd(f"WRITE {tx['handle']} {table} {key} {value}")
def delete(node, tx, table, key):
    return node.cmd(f"DELETE {tx['handle']} {table} {key}")
def commit(node, tx):
    reply = node.cmd(f"COMMIT {tx['handle']}")
    assert reply.startswith('OK '), reply
    return int(reply.split()[1])
def commit_async(node, tx):
    reply = node.cmd(f"COMMIT_ASYNC {tx['handle']}")
    assert reply.startswith('OK '), reply
    return int(reply.split()[1])
def abort(node, tx):
    assert node.cmd(f"ABORT {tx['handle']}") == 'OK'
def stats(node):
    return parse_kv(node.cmd('STATS'))
def current(node, table, key):
    reply = node.cmd(f'CURRENT {table} {key}')
    assert reply.startswith('OK '), reply
    parts = reply[3:].split()
    fields = {'value': int(parts[0])}
    for token in parts[1:]:
        k, v = token.split('=', 1)
        fields[k] = int(v)
    return fields
def read_row(node, table, key):
    tx = begin(node)
    value = read(node, tx, table, key)
    abort(node, tx)
    return value

def start_cluster():
    launch('remote', [str(a.source / 'build/remote_server/remote_node'), a.bench], 'remote_server')
    launch('storage', [str(a.source / 'build/storage_server/storage_pool'), a.bench], 'storage_server')
    deadline = time.monotonic() + 180
    while not all(listening(port) for port in [remote, remote_meta, storage, storage_meta]):
        assert all(child.poll() is None for child in children.values()), 'server exited before readiness'
        assert time.monotonic() < deadline, 'server readiness timeout'
        time.sleep(0.05)
    for node in [0, 1]:
        launch(f'node{node}', [str(a.source / 'build/tests/lazy_mvcc_cluster_node'),
                               a.bench, str(node), str(control_ports[node]), str(a.mode)], f'node{node}')
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
    return nodes

try:
    n0, n1 = start_cluster()
    T = 0
    key0 = int(n1.cmd(f'KEY_ON_NODE {T} 0').split()[1])
    key1 = int(n1.cmd(f'KEY_ON_NODE {T} 1').split()[1])
    check(key0 != key1, 'the transfer uses two distinct rows')
    check(int(n1.cmd(f'OWNER {T} {key0}').split()[1]) == 0, 'first key is owned by node 0')
    check(int(n1.cmd(f'OWNER {T} {key1}').split()[1]) == 1, 'second key is owned by node 1')

    if a.crash_recovery:
        for cut in ['one_vote', 'all_votes', 'decided']:
            values = [read_row(n0, T, key0), read_row(n1, T, key1)]
            tx = begin(n0)
            check(write(n0, tx, T, key0, values[0] - 7) == 'OK' and
                  write(n0, tx, T, key1, values[1] + 7) == 'OK', f'{cut} transfer')
            if cut == 'one_vote':
                n1.cmd('HOLD_INSTALL commit')
                n0.cmd('HOLD_INSTALL prepare')
            else:
                n0.cmd(f'HOLD_INSTALL {cut}')
            rid = commit_async(n0, tx)
            check(n0.cmd('WAIT_INSTALL_GATE') == 'OK', f'{cut} reached')
            if cut == 'one_vote':
                n1.cmd('WAIT_INSTALL_GATE')
                n1.cmd('FLUSH')  # persist its provisional row without a YES
            reserved = begin(n1)['snapshot']
            check(n0.cmd(f'TRY_RESULT {rid}') == 'PENDING', f'{cut} before client reply')
            for name in ['node0', 'node1', 'remote', 'storage']:
                os.killpg(children[name].pid, signal.SIGKILL)
            for child in children.values(): child.wait(timeout=15)
            children.clear()
            n0.close(); n1.close()
            n0, n1 = start_cluster()
            won = cut == 'decided' or (cut == 'all_votes' and a.variant != '2pc')
            expected = [values[0] - 7, values[1] + 7] if won else values
            check([read_row(n0, T, key0), read_row(n1, T, key1)] == expected,
                  f'{cut} restart recovered both balances')
            check(n0.cmd(f"TIT 0 {tx['local']}") == f"OK {reserved if won else 0}",
                  f'{cut} restart recovered the decision and exact CTS')
            # Reuse both recovered rows; verify locks, undo pages and ID allocation.
            retry = begin(n0)
            check(retry['local'] > tx['local'] and write(n0, retry, T, key0, expected[0] + 1) == 'OK' and
                  write(n0, retry, T, key1, expected[1] - 1) == 'OK', f'{cut} recovered writer locks')
            check(commit(n0, retry) > retry['snapshot'], f'{cut} post-recovery commit')

    # 1. Read-only: no position, no undo page, fixed snapshot.
    before = stats(n1)
    r = begin(n1)
    check(read(n1, r, T, key0) is not None, 'read-only sees initialized data')
    check(commit(n1, r) == 0, 'read-only commit allocates no position')
    after = stats(n1)
    check(after['tso_snapshots'] == before['tso_snapshots'] + 1, 'Begin takes one timestamp on the home node')
    check(after['tso_commits'] == before['tso_commits'], 'read-only commit does not reserve')
    check(after['undo_pages'] == before['undo_pages'], 'read-only transaction allocates no undo page')

    # 2. Single local writer on node 0.
    base = read_row(n0, T, key0)
    before_first = begin(n1)
    tx = begin(n0); check(write(n0, tx, T, key0, base - 10) == 'OK', 'local single-writer write')
    cts1 = commit(n0, tx); check(cts1 > 0, 'single local writer publishes a position')
    check(read_row(n1, T, key0) == base - 10, 'new snapshot sees the local single-writer commit')
    check(before_first['snapshot'] < cts1, 'old snapshot precedes the commit')
    after_first = begin(n1)

    # 3. Single remote writer: home n1, participant n0.
    tx = begin(n1); check(write(n1, tx, T, key0, base - 20) == 'OK', 'remote single-writer write')
    cts2 = commit(n1, tx); check(cts2 == cts1 + 1, 'one global position for one writer')
    check(read_row(n1, T, key0) == base - 20, 'new snapshot sees the remote single-writer commit')
    seen_old = read(n1, before_first, T, key0)
    check(seen_old == base, f'pre-first-commit snapshot keeps the old value (got {seen_old}, expected {base}; snap={before_first["snapshot"]}, cts1={cts1}, cts2={cts2})')
    check(read(n1, after_first, T, key0) == base - 10, 'post-first snapshot keeps its version')
    abort(n1, after_first)
    row = current(n0, T, key0)
    check(row['creator_home'] == 1, 'row creator is the remote home transaction')
    check(row['previous_writer'] == 0, 'the actual writer node manages the undo subsegment')
    check(row['value'] == base - 20, 'current row holds the installed value')

    # 3b. Self-read prefers the transaction's own private write set.
    tx = begin(n0)
    check(write(n0, tx, T, key0, base - 30) == 'OK', 'private write for self-read')
    check(read(n0, tx, T, key0) == base - 30, 'self-read sees the private write')
    abort(n0, tx)
    check(read_row(n0, T, key0) == base - 20, 'aborted private-only write is discarded')

    # 4. Multi-writer transfer: one transaction writes rows on both nodes.
    va = read_row(n1, T, key0)
    vb = read_row(n1, T, key1)
    tx = begin(n0)
    check(read(n0, tx, T, key0) == va and read(n0, tx, T, key1) == vb, 'transfer reads both balances')
    check(write(n0, tx, T, key0, va - 10) == 'OK', 'transfer debits node 0 row')
    check(write(n0, tx, T, key1, vb + 10) == 'OK', 'transfer credits node 1 row')
    before = stats(n0)
    cts3 = commit(n0, tx)
    after = stats(n0)
    check(after['tso_commits'] == before['tso_commits'] + 1 and after['tso_commit_ns'] > before['tso_commit_ns'],
          'one timed commit timestamp request for both participants')
    check(read_row(n0, T, key0) == va - 10 and read_row(n1, T, key1) == vb + 10,
          'new snapshot sees the complete transfer')
    check(read(n1, before_first, T, key0) == base, 'old snapshot still reads the transfer pre-image')
    abort(n1, before_first)
    check(current(n0, T, key0)['previous_writer'] == 0 and current(n1, T, key1)['previous_writer'] == 1,
          'each participant owns its undo subsegment')
    va, vb = va - 10, vb + 10

    # 5. Pause before the last durable vote. Old snapshots must keep both pre-images.
    held_old = begin(n1)
    n1.cmd('HOLD_INSTALL commit')
    tx = begin(n0)
    check(write(n0, tx, T, key0, va - 5) == 'OK', 'gated transfer debits')
    check(write(n0, tx, T, key1, vb + 5) == 'OK', 'gated transfer credits')
    rid = commit_async(n0, tx)
    check(n1.cmd('WAIT_INSTALL_GATE') == 'OK', 'participant installed and is paused before ACK')
    check(held_old['snapshot'] == cts3, 'snapshot predates the reservation')
    check(read(n1, held_old, T, key0) == va, 'installed row is invisible at the old snapshot (debit)')
    check(read(n1, held_old, T, key1) == vb, 'installed row is invisible at the old snapshot (credit)')
    # A later writer must not pass the unpublished transaction on either row.
    contender = begin(n1, '2pc' if a.mode == 4 else None)
    check(write(n1, contender, T, key0, 1) != 'OK', 'a later writer cannot pass the unpublished row lock')
    abort(n1, contender)
    check(read(n1, held_old, T, key0) == va, 'failed contender did not overwrite the row')
    n1.cmd('RELEASE_INSTALL_GATE')
    reply = n0.cmd(f'RESULT {rid}')
    check(reply.startswith('OK '), 'commit succeeds after the participant ACK is released')
    cts4 = int(reply.split()[1]); check(cts4 == cts3 + 1, 'commit gets one timestamp')
    check(read_row(n0, T, key0) == va - 5 and read_row(n1, T, key1) == vb + 5,
          'only the complete transfer becomes visible')
    check(read(n1, held_old, T, key0) == va, 'pre-commit snapshot remains fixed')
    abort(n1, held_old)
    va, vb = va - 5, vb + 5

    # 6. Abort before commit: both participants stay at the pre-image and locks drain.
    before = stats(n0)
    tx = begin(n0)
    check(write(n0, tx, T, key0, 1) == 'OK' and write(n0, tx, T, key1, 2) == 'OK', 'aborted transfer writes')
    abort(n0, tx)
    after = stats(n0)
    check(after['tso_commits'] == before['tso_commits'], 'abort reserves no position')
    check(read_row(n0, T, key0) == va and read_row(n1, T, key1) == vb, 'abort leaves both rows unchanged')
    tx = begin(n0); check(write(n0, tx, T, key0, va) == 'OK', 'locks were released by the abort')
    abort(n0, tx)

    # 7. Snapshot write conflict: a newer committed version is never overwritten.
    stale = begin(n1)
    tx = begin(n0); check(write(n0, tx, T, key0, va + 100) == 'OK', 'newer commit for conflict test')
    before_wal = commit(n0, tx)
    check(write(n1, stale, T, key0, 7) == 'CONFLICT', 'writer with a stale snapshot aborts with conflict')
    abort(n1, stale)
    check(read_row(n0, T, key0) == va + 100, 'stale writer did not overwrite the newer version')
    va += 100

    # 8. Durable Prepare condition: a paused participant flusher keeps the commit incomplete.
    probe = begin(n1)
    n1.cmd('PAUSE_FLUSH')
    tx = begin(n0)
    check(write(n0, tx, T, key0, va - 1) == 'OK' and write(n0, tx, T, key1, vb + 1) == 'OK',
          'WAL-held transfer writes')
    rid = commit_async(n0, tx)
    check(probe['snapshot'] == before_wal, 'older snapshot predates this commit')
    check(read(n1, probe, T, key0) == va, 'unpublished WAL-held commit is invisible')
    abort(n1, probe)
    check(n0.cmd(f'TRY_RESULT {rid}') == 'PENDING', 'commit is not complete while a prepare flush is paused')
    n1.cmd('FLUSH')
    n1.cmd('RESUME_FLUSH')
    reply = n0.cmd(f'RESULT {rid}')
    check(reply.startswith('OK '), 'commit completes after the participant WAL is durable')
    cts5 = int(reply.split()[1]); check(cts5 == before_wal + 1, 'WAL-held commit gets the next position')
    check(read_row(n0, T, key0) == va - 1 and read_row(n1, T, key1) == vb + 1,
          'WAL-held commit becomes visible as a whole')
    va, vb = va - 1, vb + 1

    # C precedes either install. A reader must find the pending writer via the row lock.
    earlier = begin(n1)
    n0.cmd('HOLD_INSTALL tso_reserved')
    tx = begin(n0)
    check(write(n0, tx, T, key0, va - 3) == 'OK' and write(n0, tx, T, key1, vb + 3) == 'OK',
          'timestamp-gated transfer writes')
    rid = commit_async(n0, tx)
    check(n0.cmd('WAIT_INSTALL_GATE') == 'OK', 'home obtained C before sending either Prepare')
    between = begin(n1)
    check(between['snapshot'] == cts5 + 1, 'Begin includes reserved C while the commit WAL is pending')
    check(read(n1, earlier, T, key0) == va and read(n1, earlier, T, key1) == vb,
          'old snapshot still reads both pre-images after C allocation')
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending_read = pool.submit(read, n1, between, T, key1)
        try:
            deadline = time.monotonic() + 10
            while not stats(n0)['tit_waiters']:
                assert time.monotonic() < deadline, 'remote reader never reached the pending creator'
                time.sleep(0.01)
            check(not pending_read.done() and n0.cmd(f'TRY_RESULT {rid}') == 'PENDING',
                  'C alone cannot make a participant row visible')
        finally:
            n0.cmd('RELEASE_INSTALL_GATE')
        check(pending_read.result(timeout=30) == vb + 3, 'reader sees the new credit after commit WAL')
    reply = n0.cmd(f'RESULT {rid}')
    check(reply.startswith('OK '), 'home returns success after the decision WAL')
    cts6 = int(reply.split()[1]); check(cts6 == between['snapshot'], 'WAL carries the reserved timestamp')
    check(read(n1, between, T, key0) == va - 3, 'same snapshot sees the matching debit')
    abort(n1, earlier); abort(n1, between)
    va, vb = va - 3, vb + 3

    # 10. Evict both main and undo caches; the old snapshot still reconstructs history.
    hist = begin(n1); check(hist['snapshot'] == cts6, 'history reader holds cts6')
    tx = begin(n0)
    check(write(n0, tx, T, key0, va + 9) == 'OK' and write(n0, tx, T, key1, vb - 9) == 'OK',
          'post-history transfer writes')
    commit(n0, tx)
    n0.cmd('CLEAR_UNDO'); n1.cmd('CLEAR_UNDO')
    # For 2PC the page partition owner is fixed; evicting on the owner forces a
    # storage fetch for the history chain. Mix reuses Lazy page access, whose
    # main-page migration/eviction is covered by the P4 cluster test; here the
    # undo-cache eviction is what the cross-branch history read must survive.
    if a.mode == 2:
        n0.cmd(f'EVICT_MAIN {T} {key0}'); n1.cmd(f'EVICT_MAIN {T} {key1}')
    check(read(n1, hist, T, key0) == va and read(n1, hist, T, key1) == vb,
          'old snapshot reads both pre-images after main+undo eviction')
    check(read_row(n1, T, key0) == va + 9 and read_row(n1, T, key1) == vb - 9,
          'new snapshot reads the latest values after eviction')
    abort(n1, hist)

    # 11. A later transaction writes both rows: post-commit lock handoff works.
    tx = begin(n1)
    check(write(n1, tx, T, key0, va + 8) == 'OK' and write(n1, tx, T, key1, vb - 8) == 'OK',
          'post-commit writers acquire both rows')
    commit(n1, tx)
    check(read_row(n0, T, key0) == va + 8 and read_row(n1, T, key1) == vb - 8,
          'later writers see a complete transaction')

    # 12. Mix: exercise the forced Lazy and distributed 2PC branches in one mode.
    if a.mode == 4:
        check(int(n0.cmd(f'OWNER {T} {key0}').split()[1]) == 0, 'mix first key owner')
        tx = begin(n0, 'lazy')
        check(write(n0, tx, T, key0, va + 7) == 'OK', 'mix Lazy write installs during execution')
        lazy_cts = commit(n0, tx)
        check(lazy_cts > 0, 'mix Lazy commit publishes')
        check(read_row(n1, T, key0) == va + 7, 'mix new snapshot sees the Lazy commit')
        hist_mix = begin(n1); check(hist_mix['snapshot'] == lazy_cts, 'mix history reader starts after Lazy')
        tx = begin(n0, '2pc')
        check(write(n0, tx, T, key0, va + 6) == 'OK', 'mix forced 2PC single-writer write')
        commit(n0, tx)
        check(read(n1, hist_mix, T, key0) == va + 7, 'mix 2PC commit keeps the previous Lazy version for old snapshots')
        tx = begin(n0, '2pc')
        check(write(n0, tx, T, key0, va + 5) == 'OK' and write(n0, tx, T, key1, vb - 5) == 'OK',
              'mix forced 2PC multi-writer transfer writes')
        commit(n0, tx)
        check(read_row(n0, T, key0) == va + 5 and read_row(n1, T, key1) == vb - 5,
              'mix 2PC multi-writer transfer publishes as a whole')
        tx = begin(n1, 'lazy')
        check(read(n1, tx, T, key0) == va + 5, 'mix Lazy reads the 2PC-installed version')
        abort(n1, tx)

    # 13. Tombstone: old snapshot retains the pre-delete value, a newer snapshot
    # sees NotFound, and a visible tombstone stops history traversal.
    branch = '2pc' if a.mode == 4 else None
    pre = begin(n1)
    pre_val = read(n1, pre, T, key1)
    check(pre_val is not None, 'tombstone pre-image is readable')
    tx = begin(n0, branch)
    check(delete(n0, tx, T, key1) == 'OK', '2PC tombstone write')
    tomb_cts = commit(n0, tx)
    check(tomb_cts > 0, 'tombstone commit publishes')
    check(read_row(n1, T, key1) is None, 'new snapshot sees the tombstone as not found')
    check(current(n1, T, key1)['deleted'] == 1, 'current row carries the deleted state')
    check(read(n1, pre, T, key1) == pre_val, 'old snapshot reads the pre-delete version')
    post = begin(n1)
    check(post['snapshot'] == tomb_cts, 'post-delete snapshot equals the tombstone commit timestamp')
    check(read(n1, post, T, key1) is None,
          'a visible tombstone returns not found instead of following to an older version')
    check(read(n1, post, T, key0) is not None, 'next probe in the same transaction clears NOTFOUND')
    abort(n1, pre); abort(n1, post)

    # 14. Long snapshot over ten commits: no GC means every old version stays
    # reachable even after both undo and main caches are cleared.
    long = begin(n1)
    long_val = read(n1, long, T, key0)
    check(long_val is not None, 'long snapshot starts on an existing value')
    last_cts = tomb_cts
    for i in range(10):
        tx = begin(n0)
        check(write(n0, tx, T, key0, long_val + i + 1) == 'OK', f'long-history write {i}')
        last_cts = commit(n0, tx)
        check(last_cts == tomb_cts + i + 1, f'long-history commit {i} gets the next position')
    check(read(n1, long, T, key0) == long_val, 'long snapshot still reads its fixed version')
    check(read_row(n1, T, key0) == long_val + 10, 'new snapshot reads the latest long-history value')
    n0.cmd('CLEAR_UNDO'); n1.cmd('CLEAR_UNDO')
    if a.mode == 2:
        n0.cmd(f'EVICT_MAIN {T} {key0}')
    check(read(n1, long, T, key0) == long_val,
          'long snapshot reconstructs ten versions after undo/main cache eviction')
    latest = begin(n1)
    check(latest['snapshot'] == last_cts, 'Begin after commit uses commit timestamp as its snapshot')
    check(read(n1, latest, T, key0) == long_val + 10,
          'commit timestamp == snapshot timestamp committed version is visible')
    abort(n1, latest); abort(n1, long)

    # 15. Repeated 2PC overwrites: one installed version, one pre-image undo;
    # an aborted repeated write leaves the row unchanged.
    tx = begin(n0, branch)
    check(write(n0, tx, T, key0, long_val + 20) == 'OK' and
          write(n0, tx, T, key0, long_val + 21) == 'OK', 'repeated 2PC overwrite')
    repeated_cts = commit(n0, tx)
    check(repeated_cts == last_cts + 1, 'repeated overwrite publishes once')
    check(read_row(n1, T, key0) == long_val + 21, 'repeated overwrite keeps the last value')
    check(current(n0, T, key0)['previous_writer'] == 0,
          'repeated overwrite has one actual-writer undo pre-image')
    tx = begin(n1, branch)
    check(write(n1, tx, T, key0, long_val + 30) == 'OK' and
          write(n1, tx, T, key0, long_val + 31) == 'OK', 'repeated aborted overwrite')
    abort(n1, tx)
    check(read_row(n0, T, key0) == long_val + 21,
          'aborted repeated write leaves the committed value')

    # A refused first lock can already have reserved undo at the home.
    holder = begin(n0, branch)
    check(write(n0, holder, T, key0, long_val + 40) == 'OK', 'local holder acquired first row')
    # Fetch both in one TxExe: the first fails at the home while the remote
    # participant grants the second (even a visible tombstone still takes a lock).
    pair = begin(n0, branch)
    check(n0.cmd(f"LOCK_PAIR {pair['handle']} {T} {key0} {key1}") == 'ABORT', 'mixed fetch result aborts')
    abort(n0, pair)
    remote_probe = begin(n1, branch)
    check(n1.cmd(f"LOCK_PAIR {remote_probe['handle']} {T} {key1} {key1}") == 'OK',
          'abort drained the concurrent remote grant, duplicate RID acquires once')
    abort(n1, remote_probe)
    with_holder = stats(n0)['admitted_reservations']
    for _ in range(3):
        contender = begin(n0, branch)
        check(write(n0, contender, T, key0, long_val + 41) in ('CONFLICT', 'ABORTING'),
              'first local lock attempt conflicts')
        abort(n0, contender)
        check(stats(n0)['admitted_reservations'] == with_holder, 'abort returns its first-write admission')
    abort(n0, holder)

    # 16. Controlled capacity: a new write transaction is refused once the
    # page budget is exhausted; an in-flight transaction keeps its reserved
    # page and can finish, and no old undo page is overwritten.
    before0 = stats(n0); before1 = stats(n1)
    rejects_before = before0['capacity_rejects'] + before1['capacity_rejects']
    # Leave exactly one admission slot, then let the in-flight transaction take
    # it. This is the state a process started with a configured budget would be
    # in after its last admissible new writer.
    n0.cmd(f'UNDO_CAPACITY {max(1, before0["undo_pages"] + 1)}')
    n1.cmd(f'UNDO_CAPACITY {max(1, before1["undo_pages"] + 1)}')
    inflight = begin(n0, branch)
    check(write(n0, inflight, T, key0, long_val + 40) == 'OK', 'capacity in-flight write')
    after_inflight0 = stats(n0); after_inflight1 = stats(n1)
    pages_after_inflight = after_inflight0['undo_pages'] + after_inflight1['undo_pages']
    fresh = begin(n0, branch)
    rejected = write(n0, fresh, T, key0, long_val + 41)
    check(rejected in ('FULL', 'CONFLICT', 'ABORTING'), f'new write rejected at capacity (got {rejected})')
    abort(n0, fresh)
    after_reject = stats(n0)
    after_reject1 = stats(n1)
    check(after_reject['capacity_rejects'] + after_reject1['capacity_rejects'] > rejects_before,
          'capacity rejection is counted')
    check(after_reject['undo_pages'] + after_reject1['undo_pages'] == pages_after_inflight,
          'rejected write allocates no undo page')
    check(read_row(n0, T, key0) == long_val + 21, 'rejected write never overwrites the row')
    check(write(n0, inflight, T, key0, long_val + 42) == 'OK',
          'in-flight transaction continues after capacity is reached')
    n0.cmd('UNDO_CAPACITY 0'); n1.cmd('UNDO_CAPACITY 0')
    check(commit(n0, inflight) > 0, 'in-flight transaction commits after capacity is reset')
    check(read_row(n0, T, key0) == long_val + 42, 'in-flight committed value is visible')

    if a.mode == 4:
        # Capacity is shared by the Lazy leg; an existing page may continue,
        # a fresh Lazy transaction is refused without allocating or overwriting.
        before0 = stats(n0); before1 = stats(n1)
        rejects_before = before0['capacity_rejects'] + before1['capacity_rejects']
        n0.cmd(f'UNDO_CAPACITY {max(1, before0["undo_pages"] + 1)}')
        n1.cmd(f'UNDO_CAPACITY {max(1, before1["undo_pages"] + 1)}')
        inflight = begin(n0, 'lazy')
        check(write(n0, inflight, T, key0, long_val + 50) == 'OK', 'Lazy capacity in-flight write')
        after_inflight0 = stats(n0); after_inflight1 = stats(n1)
        pages_after_inflight = after_inflight0['undo_pages'] + after_inflight1['undo_pages']
        fresh = begin(n0, 'lazy')
        rejected = write(n0, fresh, T, key0, long_val + 51)
        check(rejected in ('FULL', 'CONFLICT', 'ABORTING'), f'Lazy new write rejected at capacity (got {rejected})')
        abort(n0, fresh)
        after0 = stats(n0); after1 = stats(n1)
        check(after0['capacity_rejects'] + after1['capacity_rejects'] > rejects_before,
              'Lazy capacity rejection is counted')
        check(write(n0, inflight, T, key0, long_val + 52) == 'OK',
              'Lazy in-flight transaction continues at capacity')
        n0.cmd('UNDO_CAPACITY 0'); n1.cmd('UNDO_CAPACITY 0')
        check(commit(n0, inflight) > 0, 'Lazy in-flight transaction commits after capacity reset')
        check(read_row(n0, T, key0) == long_val + 52, 'Lazy in-flight committed value is visible')

    final0 = stats(n0); final1 = stats(n1)
    result['stats'] = {'n0': final0, 'n1': final1}
    check(final0['undo_pages'] > 0, 'committed writers allocated undo pages')
    check(final0['history_steps'] + final1['history_steps'] > 0,
          'history traversal steps were observed')
    check(final0['si_conflicts'] + final1['si_conflicts'] > 0,
          'SI conflict counter records the stale writer')
    check(final0['lock_conflicts'] > 0, 'lock conflict counter records the unpublished-row contender')
    check(final0['prepared_contexts'] > 0, 'prepared participant contexts are retained')
    check(final0['single_writes'] > 0, 'single-writer contexts are retained')
    check(final0['finished_participants'] > 0, 'finish-only drains are recorded')
    # Mode 2/2PC uses fixed page partitions, so the undo writer is the page
    # owner and the visibility fetch is local; cross-node undo fetch/migration
    # is covered by the P4 Lazy cluster test.
    if a.undo_pages == 1:
        check(final0['undo_evictions'] > 0, 'one-frame undo pool forces evictions')

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
