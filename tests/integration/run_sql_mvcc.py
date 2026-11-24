#!/usr/bin/env python3
"""Run the SQL listener and real clients in an isolated one/two-member Lazy cluster."""
import argparse
import base64
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
import sqlite3
import json
import re
import shutil
import socket
import subprocess
import time
from contextlib import ExitStack
from pathlib import Path
from urllib.request import build_opener, ProxyHandler, Request

parser = argparse.ArgumentParser()
parser.add_argument('--source', type=Path, required=True)
parser.add_argument('--upgrade-from', type=Path,
                    help='use this build for the initial SQL cluster before a full restart')
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--nodes', type=int, choices=(1, 2), required=True)
parser.add_argument('--regressions', action='store_true')
scenario = parser.add_mutually_exclusive_group()
scenario.add_argument('--short-query-bench', action='store_true', help='measure end-to-end point-read latency and throughput')
scenario.add_argument('--capacity', action='store_true', help='limit new-writer admission after bootstrap')
scenario.add_argument('--wait-writers', action='store_true', help='exercise WAIT_DETECT writer handoff')
scenario.add_argument('--restart', choices=['storage', 'cluster'],
                      help='restore retained SQL WAL and verify committed rows after a process restart')
args = parser.parse_args()
if args.upgrade_from and args.restart != 'cluster':
    parser.error('--upgrade-from requires --restart cluster')
source, output = args.source.resolve(), args.output.resolve()
output.mkdir(parents=True, exist_ok=False)
shutil.copytree(source / 'config', output / 'config')
children = {}
checks = []
metrics = {}
trace = (output / 'sql.log').open('w', buffering=1)


def wait_for(predicate, description, timeout=120):
    deadline = time.monotonic() + timeout
    while not predicate():
        for name, process in children.items():
            assert process.poll() is None, f'{name} exited {process.returncode} during {description}'
        if time.monotonic() > deadline:
            raise TimeoutError(description)
        time.sleep(.05)


def listening(port):
    for protocol in ('tcp', 'tcp6'):
        for line in Path('/proc/net', protocol).read_text().splitlines()[1:]:
            fields = line.split()
            if fields[3] == '0A' and int(fields[1].split(':')[1], 16) == port:
                return True
    return False


def start(name, binary, *argv, restart=False):
    cwd = output / 'run' / name
    cwd.mkdir(parents=True, exist_ok=restart)
    suffix = '.restart' if restart is True else ('.' + restart if restart else '')
    log_name = name + suffix + '.log'
    binaries = args.upgrade_from.resolve() if args.upgrade_from and not restart else source
    with (output / log_name).open('w') as log:
        children[name] = subprocess.Popen([binaries / 'build' / binary, *argv], cwd=cwd,
                                          stdout=log, stderr=subprocess.STDOUT)


def configure():
    with ExitStack() as stack:
        def reserve(port=0):
            sock = stack.enter_context(socket.socket())
            sock.bind(('127.0.0.1', port))
            return sock.getsockname()[1]
        while True:
            base = reserve()
            try:
                if args.nodes == 2:
                    reserve(base + 1)
                break
            except OSError:
                continue
        rpc = [reserve() for _ in range(args.nodes)]
        remote, barrier, storage, meta = [reserve() for _ in range(4)]
        cfgdir = output / 'config'
        cfg = json.loads((cfgdir / 'compute_node_config.json').read_text())
        cfg.update(sql_listen_port_base=base, use_rdma=False, undo_buffer_pool_size_pages=2,
                   undo_lock_table_initial_capacity=4, table_buffer_pool_size_per_table=4,
                   index_buffer_pool_size_per_table=32, fsm_buffer_pool_size_per_table=16,
                   partition_size_per_table=4, push_page_with_scheduler=0, log_flush_interval_ms=3)
        if args.capacity:
            cfg['undo_capacity_pages'] = 1
        cfg['local_compute_node'].update(generate_log=1, lock_mode=int(args.wait_writers), machine_id=0)
        cfg['remote_compute_nodes'] = dict(remote_compute_node_ips=['127.0.0.1'] * args.nodes,
                                           remote_compute_node_port=rpc)
        cfg['remote_server_nodes'] = dict(remote_server_node_ips=['127.0.0.1'],
                                          remote_server_node_port=[remote], remote_server_node_meta_port=[barrier])
        cfg['remote_storage_nodes'] = dict(remote_storage_node_ips=['127.0.0.1'],
                                           remote_storage_node_rpc_port=[storage], remote_storage_node_meta_port=[meta])
        for name, content in [('compute_node', cfg), ('remote_server', dict(local_server_node=dict(
                local_rpc_port=remote, local_meta_port=barrier))), ('storage_node', dict(local_storage_node=dict(
                machine_id=0, machine_num=1, local_rpc_port=storage, local_meta_port=meta, page_read_io='buffered')))]:
            (cfgdir / (name + '_config.json')).write_text(json.dumps(content, indent=2))
    return base, rpc, [remote, barrier, storage]


class Client:
    def __init__(self, port):
        self.sock = socket.create_connection(('127.0.0.1', port), timeout=20)
        self.tag = self.sock.getsockname()[1]

    def sql(self, statement):
        print(self.tag, '>', statement, file=trace)
        self.sock.sendall((statement + ';').encode())
        reply = self.sock.recv(65536)
        assert reply, f'connection closed after {statement}'
        if reply.startswith(b'+'):
            while not re.search(rb'Total record\(s\): \d+\n', reply):
                data = self.sock.recv(65536)
                assert data, f'truncated reply to {statement}'
                reply += data
        text = reply.decode().strip()
        print(self.tag, '<', text, file=trace)
        return text

    def expect(self, sql, expected):
        actual = self.sql(sql)
        assert actual == expected, (sql, expected, actual)
        checks.append(sql)

    def rows(self, sql, *rows):
        result = self.sql(sql)
        assert result.endswith(f'Total record(s): {len(rows)}'), (sql, result, rows)
        values = [tuple(field.strip() for field in line.strip('|').split('|'))
                  for line in result.splitlines() if line.startswith('|')][1:]
        assert sorted(values) == sorted(tuple(map(str, r)) for r in rows), (sql, values, rows)
        checks.append(sql)

    def begin(self):
        self.expect('begin', 'Begin')

    def commit(self):
        self.expect('commit', 'Commit')

    def rollback(self):
        self.expect('rollback', 'RollBack')

    def change(self, sql, count=1):
        self.expect(sql, f'affect raw : {count}')


def counters(port, names=('sql_wal_lsn', 'sql_undo_pages', 'sql_heap_evictions', 'sql_undo_evictions')):
    result = {}
    local = build_opener(ProxyHandler({}))
    for name in names:
        with local.open(f'http://127.0.0.1:{port}/vars/{name}', timeout=5) as response:
            html = response.read().decode()
            match = re.search(fr'id="value-{name}">(\d+)</span>', html)
            assert match, (name, html[-200:])
            result[name] = int(match[1])
    return result


def scan_plan_cases(c):
    lookup = '  PrimaryKeyLookup account key=1 filter=account.balance=100'
    for where in ('id = 1 and balance = 100', 'balance = 100 and id = 1'):
        c.expect('explain select id from account where ' + where, 'Project account.id\n' + lookup)
        c.rows('select id from account where ' + where, (1,))
    c.expect('explain update account set balance = 0 where id = 1 and balance = 100',
             'Update account set balance=0\n' + lookup)
    c.expect('explain delete from account where balance = 100 and id = 1', 'Delete account\n' + lookup)
    c.expect('explain insert into account values (3,0)', 'Insert account values=(3, 0)')
    c.rows('select balance from account where id = 3')
    c.change('update account set balance = 0 where balance = 70 and id = 1', 0)
    c.change('delete from account where id = 1 and balance = 70', 0)
    c.rows('select id from account where id = 1 and id = 2')
    c.rows('select id,balance from account', (1, 100), (2, 70))
    c.expect('explain select id from account where balance = 70',
             'Project account.id\n  SeqScan account filter=account.balance=70')

    join = 'select account.id from account, levels where '
    c.rows(join + 'levels.code = 1', (1,), (2,))
    c.rows(join + 'levels.amount > levels.code', (1,), (2,))
    where = 'levels.amount > account.balance and levels.code = 1 and account.id > levels.code'
    c.expect('explain ' + join + where,
             'Project account.id\n  NestedLoopJoin on=account.balance<levels.amount AND account.id>levels.code'
             '\n    SeqScan account\n    SeqScan levels filter=levels.code=1')
    c.rows(join + where, (2,))
    c.expect('explain select id from account order by id', 'ORDER BY is not supported')

    # A CHAR comparison must not change when the planner swaps the operands.
    c.expect('create table words (shortword char(1), longword char(3))', 'success')
    c.expect('create table labels (label char(3))', 'success')
    c.change("insert into words values ('a','abc')")
    c.change("insert into labels values ('abc')")
    c.rows('select shortword from words, labels where labels.label > words.shortword', ('a',))
    c.rows('select shortword from labels, words where words.shortword < labels.label', ('a',))
    c.rows('select shortword from words where shortword = longword')
    c.expect('drop table labels', 'success')
    c.expect('drop table words', 'success')


def parser_isolation(a, b):
    # EOF in a block comment leaves the old shared scanner in STATE_COMMENT.
    a.expect('/* unterminated comment', 'Syntax error')
    b.rows('select id from account where id = 2', (2,))
    a.rows('select id from account where id = 1', (1,))

    gate = Barrier(2)
    def query(client, key):
        for _ in range(12):
            gate.wait(timeout=10)
            if key == 1:
                client.expect('select from', 'Syntax error')
            client.rows(f'/* same table, distinct AST */ select id from account where id = {key}', (key,))
    with ThreadPoolExecutor(2) as pool:
        tasks = [pool.submit(query, client, key) for client, key in ((a, 1), (b, 2))]
        for task in tasks:
            task.result(timeout=60)


def exercise(w, r, other, rpc):
    w.expect('create table account (id int, balance int, primary key(id))', 'success')
    w.expect('create table levels (amount int, code int)', 'success')
    w.change('insert into account values (1,100)')
    w.change('insert into account values (2,70)')
    w.change('insert into levels values (100,1)')
    parser_isolation(w, r)
    scan_plan_cases(w)
    point = 'select balance from account where id = 1'
    scan = 'select balance from account where id >= 1 and id <= 1'
    r.begin()
    other.begin()
    w.begin()
    w.change('update account set balance = 80 where id = 1')
    w.change('update account set balance = 95 where id = 2')
    w.change('update levels set amount = 80 where code = 1')
    w.rows(point, (80,))
    r.rows(point, (100,))
    w.commit()
    other.rows(point, (100,))  # first table access after acknowledgement
    r.rows('select id,balance from account where balance >= 90', (1, 100))
    r.rows(point + ' and balance >= 90', (100,))
    r.rows('select account.id,levels.amount from account join levels where account.balance = levels.amount', (1, 100))
    join = 'select account.id,levels.amount from account join levels where account.balance '
    r.rows(join + '< levels.amount', (2, 100))
    r.rows(join + '> levels.amount')
    r.commit()
    r.rows('select id,balance from account where balance >= 90', (2, 95))
    r.rows(join + '< levels.amount')
    r.rows(join + '> levels.amount', (2, 80))
    other.expect('update account set balance = 60 where id = 1', 'Abort: SI write conflict')
    other.rows(point, (80,))
    w.begin()
    w.change('update account set balance = 50 where id = 1')
    w.change('update account set balance = 40 where id = 1')
    w.rows(point, (40,))
    w.rollback()
    r.rows(point, (80,))
    r.begin()
    w.change('update account set balance = 97 where id = 2')
    r.expect('update account set balance = 1 where balance >= 0', 'Abort: SI write conflict')
    r.rows(point, (80,))

    r.begin()
    w.begin()
    w.change('delete from account where id = 1')
    w.rows(point)
    w.rollback()
    w.rows(point, (80,))
    w.change('delete from account where id = 1')
    other.begin()
    w.change('insert into account values (1,77)')
    w.begin()
    for read in (point, scan):
        r.rows(read, (80,))
        other.rows(read)
        w.rows(read, (77,))
    w.commit()
    other.commit()
    r.expect('delete from account where id = 1', 'Abort: SI write conflict')

    key3 = 'select balance from account where id = 3'
    r.begin()
    w.begin()
    w.change('insert into account values (3,300)')
    w.rows(key3, (300,))
    r.rows(key3)
    other.expect('insert into account values (3,900)', 'Abort: writer conflict')
    w.commit()
    r.rows(key3)
    r.commit()
    r.rows(key3, (300,))
    w.expect('insert into account values (3,1)', 'Abort: duplicate primary key')
    w.begin()
    w.change('insert into account values (4,4)')
    w.rollback()
    w.rows('select id from account where id = 4')
    w.begin()
    w.change('insert into account values (4,5)')
    w.change('delete from account where id = 4')
    w.commit()
    w.rows('select id from account where id = 4')
    w.change('insert into levels values (80,2)')
    w.begin()
    w.change('update levels set amount = 11', 2)
    w.rollback()
    w.rows('select amount,code from levels', (80, 1), (80, 2))
    r.begin()
    w.expect('drop table levels', 'DDL busy: active SQL transaction or DDL')
    r.commit()
    w.expect('drop table levels', 'success')
    w.expect('create table levels (tag char(100), amount int)', 'success')
    w.change("insert into levels values ('new',40)")
    w.rows('select amount from levels', (40,))
    w.begin()
    w.change('update account set balance = 5 where id = 1')
    w.expect('select missing from account', 'fail')
    w.rows(point, (77,))

    # Evict the actual target data pages and history, then refetch during rollback.
    wide = 'select magic from ycsb_user_table where id = 0'
    r.begin()
    before = counters(rpc[0])
    for k in range(0, 60, 3):
        w.change(f'update ycsb_user_table set magic = 80 where id = {k}')
    w.begin()
    for k in range(0, 60, 3):
        w.change(f'update ycsb_user_table set magic = 30 where id = {k}')
    w.rows(wide, (30,))
    r.rows(wide, (125,))
    w.rollback()
    w.rows(wide, (80,))
    r.rows(wide, (125,))
    r.commit()
    after = counters(rpc[0])
    for name in ('sql_heap_evictions', 'sql_undo_evictions'):
        assert after[name] > before[name], (before, after)
    metrics.update(before_eviction=before, after_eviction=after)
    # No other clients issue writes during the read-only counter interval.
    before = [counters(p) for p in rpc]
    r.begin()
    r.rows(point, (77,))
    r.rows(scan, (77,))
    r.commit()
    after = [counters(p) for p in rpc]
    for first, last in zip(before, after):
        for name in ('sql_wal_lsn', 'sql_undo_pages'):
            assert first[name] == last[name], (first, last)
    metrics.update(before_reads=before, after_reads=after)


def edge_cases(w, r, base):
    r.begin()
    w.change('delete from account where id = 2')
    r.expect('update account set balance = 13 where id = 2', 'Abort: SI write conflict')
    transient = Client(base)
    transient.begin()
    transient.change('update account set balance = 3 where id = 1')
    transient.sock.shutdown(socket.SHUT_WR)
    assert transient.sock.recv(1) == b''  # server has rolled back before closing
    transient.sock.close()
    w.rows('select balance from account where id = 1', (77,))
    w.begin()
    w.change('update account set balance = 63 where id = 1')
    w.expect('select from', 'Syntax error')
    w.rows('select balance from account where id = 1', (77,))
    w.begin()
    w.change('update account set balance = 61 where id = 1')
    w.expect('select amount from levels where tag = 1', 'fail')
    w.rows('select balance from account where id = 1', (77,))

    w.expect('create table oversized (id int, payload char(913), primary key(id))', 'fail')
    w.expect('create table widest (id int, payload char(912), primary key(id))', 'success')
    first, last = 'x' * 911 + 'a', 'x' * 911 + 'z'
    w.change(f"insert into widest values (9,'{first}')")
    r.begin()
    w.change(f"update widest set payload = '{last}' where id = 9")
    r.rows(f"select id from widest where payload = '{first}'", (9,))
    r.rows(f"select id from widest where payload = '{last}'")
    r.commit()
    r.rows(f"select id from widest where payload = '{last}'", (9,))
    # Start simultaneous insert statements with a common old snapshot. Exactly
    # one can own the index entry, even when the clients are on different nodes.
    w.begin()
    r.begin()
    gate = Barrier(2)

    def insert(client):
        gate.wait(timeout=10)
        return client.sql('insert into account values (8,88)')
    with ThreadPoolExecutor(2) as pool:
        futures = [pool.submit(insert, c) for c in (w, r)]
        replies = [future.result(timeout=25) for future in futures]
    assert replies.count('affect raw : 1') == 1 and sum(x.startswith('Abort:') for x in replies) == 1, replies
    (w, r)[replies.index('affect raw : 1')].commit()
    r.rows('select id from account where balance = 88', (8,))
    w.expect('drop table widest', 'success')  # waits for all writers' replay before deleting
    subprocess.run([source / 'build/tests/sql_mvcc_replay_check'],
                   cwd=output / 'run/storage/sql_mvcc_db', check=True)
    checks.append('replayed physical SQL row and both undo references')


def regressions(w, base):
    cases = source / 'tests/test_cases'
    cli = source / 'build/wookongdb-mp-client/WookongDB_client'
    address = ['-h', '127.0.0.1', '-p', str(base)]
    for table in ('account', 'levels', 'ycsb_user_table'):
        w.expect(f'drop table {table}', 'success')

    def run_cli(script):
        run = subprocess.run([cli, *address], input=script.read_text(), stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, text=True, check=True, timeout=300)
        (output / (script.stem + '.out')).write_text(run.stdout)
        return run.stdout

    def normalize(text):
        return Counter(line for line in text.splitlines() if line and line not in ('Bye.', 'WookongDB-MP> Bye.'))

    for script in sorted((cases / 'basic_query_test').glob('*.sql')):
        expected = script.with_name(script.stem.replace('test', 'answer') + '.txt').read_text()
        actual = run_cli(script)
        assert normalize(actual) == normalize(expected), (script, normalize(actual) - normalize(expected),
                                                        normalize(expected) - normalize(actual))
        checks.append(script.stem)

    for group in ('concurrency_test', 'cache_consistency_test'):
        if group == 'cache_consistency_test' and args.nodes < 2:
            continue
        for script in sorted((cases / group).glob('*.sql')):
            actual = output / (script.stem + '.out')
            subprocess.run([source / 'build/tests' / group, *address, script, actual], check=True, timeout=60)
            expected = script.with_name(script.stem + '_output.txt').read_text()
            assert actual.read_text().strip() == expected.strip(), script
            checks.append(script.stem)
    script = cases / 'load_table_test/load_table_test.sql'
    assert normalize(run_cli(script)) == normalize(script.with_name('load_table_output.txt').read_text())
    checks.append(script.stem)
    for name in re.findall(r'create table (\w+)', script.read_text()):
        w.expect(f'drop table {name}', 'success')

    # The historical join golden ends in the middle of a TCP read. Compare
    # all output rows from the native client against the same fixture in SQLite.
    script = cases / 'join_test/join_test.sql'
    sql = script.read_text().splitlines()
    db = sqlite3.connect(':memory:')
    for statement in sql[:-1]:
        db.execute(statement)
    def printed(value):
        value = f'{value:.6f}' if isinstance(value, float) else str(value)
        return value if len(value) <= 16 else value[:13] + '...'
    expected = Counter(tuple(map(printed, row)) for row in db.execute(sql[-1]))
    actual = run_cli(script)
    rows = [tuple(v.strip() for v in line.strip('|').split('|')) for line in actual.splitlines()
            if line.startswith('|')][1:]
    assert Counter(rows) == expected and f'Total record(s): {sum(expected.values())}' in actual
    db.close()
    checks.append('join_test full result')


def restart_sql(base, rpc, ready):
    db_dir = output / 'run/storage/sql_mvcc_db'
    table = db_dir / 'ycsb_user_table'
    query = 'select magic from ycsb_user_table where id = 0'
    with ExitStack() as stack:
        w = Client(base)
        stack.callback(w.sock.close)
        w.expect('create table retired (id int)', 'success')
        w.change('insert into retired values (17)')
        w.expect('drop table retired', 'success')  # drain the bootstrap heap
        with table.open('rb') as data:
            data.seek(4096)
            before = data.read(4096)
        assert len(before) == 4096
        w.expect('create table retired (v char(8))', 'success')
        w.change("insert into retired values ('current')")
        w.change('update ycsb_user_table set magic = 80 where id = 0')
        w.rows(query, (80,))
        w.expect('create table replay_barrier (id int)', 'success')
        w.expect('drop table replay_barrier', 'success')

    checker = [source / 'build/tests/sql_mvcc_replay_check']
    expected_scns = None
    if args.restart == 'cluster' and not args.upgrade_from:
        checker.extend(f'127.0.0.1:{port}' for port in rpc)
        expected_scns = subprocess.check_output(checker, cwd=db_dir, text=True)

    def stop(name):
        process = children.pop(name)
        process.kill()
        process.wait(timeout=10)

    # Force uncommitted undo and row installs into the durable WAL by
    # committing an unrelated update on the same writer. Keep its socket open.
    owner = base + args.nodes - 1
    pending = Client(owner)
    pending.begin()
    pending.change('update ycsb_user_table set magic = 701 where id = 1')
    pending.change('update ycsb_user_table set magic = 702 where id = 1')
    pending.rows('select magic from ycsb_user_table where id = 1', (702,))
    with ExitStack() as stack:
        fence = Client(owner)
        stack.callback(fence.sock.close)
        fence.begin()
        fence.change('update ycsb_user_table set magic = 703 where id = 2')
        fence.rollback()
        fence.change('update ycsb_user_table set magic = 704 where id = 3')

    if args.restart == 'storage':
        # A live compute process treats a failed LogWrite as fatal. Wait for its
        # deferred lock cleanup WAL as well as the earlier acknowledged commit.
        for port in rpc:
            target = counters(port, ['sql_wal_lsn'])['sql_wal_lsn']
            wait_for(lambda: counters(port, ['sql_durable_wal_lsn'])['sql_durable_wal_lsn'] >= target,
                     'quiescent writer before storage-only restart')
    if args.restart == 'cluster':
        for node in range(args.nodes):
            stop(f'node{node}')
        pending.sock.close()
        stop('remote')
    stop('storage')
    assert not list(db_dir.parent.glob('LOG_FILE_*'))
    assert list(db_dir.glob('LOG_FILE_0.0'))
    # Simulate a lagging heap page and reconstructible undo files in this
    # run's private data directory. A cached SQL reply cannot satisfy the probe.
    with table.open('r+b') as data:
        data.seek(4096)
        assert data.read(4096) != before
        data.seek(4096)
        data.write(before)
    undo_files = list(db_dir.glob('UNDO_*.pages'))
    assert undo_files
    for file in undo_files:
        file.unlink()
    if args.restart == 'cluster':
        start('remote', 'remote_server/remote_node', 'sql', restart=True)
    start('storage', 'storage_server/storage_pool', 'sql', restart=True)
    wait_for(lambda: all(listening(p) for p in ready), 'restarted services')
    if args.restart == 'cluster':
        for node in range(args.nodes):
            start(f'node{node}', 'compute_server/compute_server', str(node), 'sql_mvcc_db', restart=True)
        wait_for(lambda: 'skip bootstrap' in (output / 'node0.restart.log').read_text(),
                 'reopened existing SQL catalog')
    else:
        # Live compute nodes already opened this database on the previous
        # storage process. Repeat their normal OpenDb RPC on its replacement.
        req = Request(f'http://127.0.0.1:{ready[-1]}/StorageService/OpenDb',
                      data=json.dumps(dict(db_name=base64.b64encode(b'sql_mvcc_db').decode(), node_id=0)).encode(),
                      headers={'Content-Type': 'application/json'})
        with build_opener(ProxyHandler({})).open(req, timeout=90) as response:
            assert b'ycsb_user_table' in [base64.b64decode(name) for name in json.load(response)['table_names']]
    restored = subprocess.check_output(checker, cwd=db_dir, text=True)
    print(restored, end='')
    if expected_scns is not None:
        assert restored == expected_scns, (expected_scns, restored)
        checks.append('each node retained both exact pre-crash commit SCNs')
    checks.append('WAL restored a stale heap page and both undo references after restart')
    with ExitStack() as stack:
        w, r = Client(base), Client(base + args.nodes - 1)
        stack.callback(w.sock.close)
        stack.callback(r.sock.close)
        if args.restart == 'storage':
            pending.rows('select magic from ycsb_user_table where id = 1', (702,))
            pending.rollback()
            pending.sock.close()
        r.rows('select magic from ycsb_user_table where id = 1', (125,))
        r.rows('select magic from ycsb_user_table where id = 2', (125,))
        r.rows('select magic from ycsb_user_table where id = 3', (704,))
        w.change('update ycsb_user_table set magic = 705 where id = 1')
        r.rows(query, (80,))
        r.rows('select v from retired', ('current',))
        r.begin()
        r.rows(query, (80,))
        w.change('update ycsb_user_table set magic = 81 where id = 0')
        r.rows(query, (80,))
        r.commit()
        r.rows(query, (81,))

    if args.restart == 'cluster':
        for node in range(args.nodes):
            stop(f'node{node}')
        stop('remote')
        stop('storage')
        start('remote', 'remote_server/remote_node', 'sql', restart='second')
        start('storage', 'storage_server/storage_pool', 'sql', restart='second')
        wait_for(lambda: all(listening(p) for p in ready), 'second restart services')
        for node in range(args.nodes):
            start(f'node{node}', 'compute_server/compute_server', str(node), 'sql_mvcc_db', restart='second')
        wait_for(lambda: 'skip bootstrap' in (output / 'node0.second.log').read_text(), 'second SQL recovery')
        with ExitStack() as stack:
            r = Client(owner)
            stack.callback(r.sock.close)
            r.rows(query, (81,))
            r.rows('select magic from ycsb_user_table where id = 1', (705,))
            r.rows('select magic from ycsb_user_table where id = 3', (704,))


def run():
    base, rpc, ready = configure()
    start('remote', 'remote_server/remote_node', 'sql')
    start('storage', 'storage_server/storage_pool', 'sql')
    wait_for(lambda: all(listening(p) for p in ready), 'server readiness')
    for node in range(args.nodes):
        start(f'node{node}', 'compute_server/compute_server', str(node), 'sql_mvcc_db')
    wait_for(lambda: 'initialized ycsb_user_table with 1000 rows' in (output / 'node0.log').read_text(),
             'SQL bootstrap commit')
    if args.restart:
        restart_sql(base, rpc, ready)
        return
    if args.short_query_bench:
        from sql_short_queries import measure
        metrics.update(measure(base))
        return
    with ExitStack() as stack:
        def connect(node):
            client = Client(base + node)
            stack.callback(client.sock.close)
            return client
        writer = connect(0)
        reader = connect(args.nodes - 1)
        other = connect(args.nodes - 1)
        if args.wait_writers:
            read = 'select magic from ycsb_user_table where id = 0'
            update = 'update ycsb_user_table set magic = 80 where id = 0'
            writer.begin()
            reader.begin()
            writer.change(update)
            writer.rows(read, (80,))
            name = 'sql_writer_queue_depth_max'
            assert counters(rpc[0], [name])[name] == 0
            with ThreadPoolExecutor(1) as pool:
                blocked = pool.submit(reader.expect, update, 'Abort: SI write conflict')
                wait_for(lambda: counters(rpc[0], [name])[name] == 1, 'registered row writer')
                other.rows(read, (125,))
                writer.commit()
                blocked.result(timeout=20)
            reader.rows(read, (80,))
        elif args.capacity:
            before = counters(rpc[0])
            writer.expect('update ycsb_user_table set magic = 0 where id = 1', 'Abort: undo capacity exhausted')
            writer.expect("insert into ycsb_user_table values (2000, 0," + ",".join(["'x'"] * 10) + ")",
                          'Abort: undo capacity exhausted')
            reader.rows('select magic from ycsb_user_table where id = 1', (125,))
            reader.rows('select magic from ycsb_user_table where id = 2000')
            after = counters(rpc[0])
            assert before['sql_undo_pages'] == after['sql_undo_pages']
            metrics.update(before_capacity=before, after_capacity=after)
        else:
            exercise(writer, reader, other, rpc)
            edge_cases(writer, reader, base)
            if args.regressions:
                regressions(writer, base)


passed = False
try:
    run()
    passed = True
finally:
    exits = {name: p.poll() for name, p in children.items()}
    for p in children.values():
        if p.poll() is None:
            p.terminate()
    for p in children.values():
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()
    trace.close()
    report = dict(passed=passed and all(rc is None for rc in exits.values()),
                  checks=len(checks), metrics=metrics, before_shutdown=exits,
                  after_shutdown={name: p.returncode for name, p in children.items()})
    (output / 'result.json').write_text(json.dumps(report, indent=2))
    print(json.dumps(report))
    if passed:
        assert report['passed']
