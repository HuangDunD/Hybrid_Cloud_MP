#!/usr/bin/env python3
import hashlib
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import sys
import threading
import time
import weakref

RECV_BUFFERS = weakref.WeakKeyDictionary()
RECV_LOCK = threading.Lock()

RUN_ID = os.environ['HCM_RUN_ID']
RUN = Path(os.environ['HCM_RUN_DIR']).resolve(strict=True)
MODE = os.environ.get('HCM_DRIVER_MODE', 'serial')
NODES = int(os.environ.get('HCM_NODE_COUNT', '1'))
THREADS = int(os.environ.get('HCM_THREADS_PER_NODE', '1'))
LEDGER_PATH = RUN / 'ledger' / 'requests.jsonl'
VALUE_SIZE = 1004


def now_ns():
    return time.time_ns()


def fsync_file(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class Ledger:
    def __init__(self, path):
        self.path = path
        self.lock = threading.Lock()
        self.fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            os.fsync(self.fd)
            fsync_file(path.parent)
        except BaseException:
            os.close(self.fd)
            self.fd = None
            raise

    def write(self, event, **fields):
        record = dict(run_id=RUN_ID, event=event, time_ns=now_ns(), **fields)
        data = (json.dumps(record, sort_keys=True) + '\n').encode()
        with self.lock:
            if self.fd is None:
                raise RuntimeError('ledger is closed')
            view = memoryview(data)
            while view:
                try:
                    n = os.write(self.fd, view)
                except InterruptedError:
                    continue
                if n <= 0:
                    raise OSError('ledger write made no progress')
                view = view[n:]
            os.fsync(self.fd)

    def close(self):
        with self.lock:
            if self.fd is not None:
                fd, self.fd = self.fd, None
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)


def write_artifact(path, value):
    with path.open('x') as f:
        json.dump(value, f, indent=2, sort_keys=True)
        f.write('\n')
        f.flush()
        os.fsync(f.fileno())
    fsync_file(path.parent)


def ledger_accounting():
    planned, terminal, transactions = {}, set(), set()
    counts = dict(CONFIRMED_COMMITTED=0, CONFIRMED_ABORTED=0, UNKNOWN=0, UNFINISHED=0)
    mix = dict(READ=0, UPDATE=0, INSERT=0, DELETE=0)
    with LEDGER_PATH.open() as f:
        for line in f:
            rec = json.loads(line)
            if rec.get('run_id') != RUN_ID:
                raise RuntimeError('ledger accounting run identity mismatch')
            if rec.get('event') == 'planned':
                rid, tx = rec['request_id'], rec['tx_id']
                if rid in planned or tx in transactions:
                    raise RuntimeError('duplicate planned request/transaction')
                transactions.add(tx)
                planned[rid] = tuple(rec.get(k) for k in ('node', 'generation', 'tx_id'))
                for op in rec['operations']:
                    mix[op['op']] += 1
            elif rec.get('event') == 'terminal':
                rid = rec['request_id']
                if rid in terminal or rid not in planned or planned[rid] != tuple(
                        rec.get(k) for k in ('node', 'generation', 'tx_id')):
                    raise RuntimeError('duplicate/orphan/mismatched terminal')
                if rec.get('outcome_class') not in counts or rec['response'].get('outcome') != rec['outcome_class']:
                    raise RuntimeError('terminal outcome accounting mismatch')
                terminal.add(rid)
                counts[rec['outcome_class']] += 1
    if set(planned) != terminal or not planned or counts['UNKNOWN'] or counts['UNFINISHED']:
        raise RuntimeError('ledger has unresolved/missing terminal requests')
    return counts, mix, len(planned)


def value_for(key, version):
    if version <= 0:
        raise ValueError('application version must be positive')
    out = bytearray(struct.pack('<I', 125))
    for field in range(10):
        prefix = f'v{version:06d}|k{key:06d}|f{field}|'.encode()
        body = bytearray(prefix)
        seed = hashlib.sha256(f'{RUN_ID}|{key}|{version}|{field}'.encode()).digest()
        while len(body) < 100:
            body.extend(seed)
            seed = hashlib.sha256(seed).digest()
        out.extend(body[:100])
    assert len(out) == VALUE_SIZE
    return bytes(out)


def load_value_for(key, version=1):
    out = bytearray(struct.pack('<I', 125))
    for field in range(10):
        prefix = f'L{version:06d}|K{key:09d}|F{field}|'.encode()
        fill = (key * 131 + field * 17 + version) % 251 + 1
        out.extend((prefix + bytes([fill]) * 100)[:100])
    assert len(out) == VALUE_SIZE
    return bytes(out)


def recv_line(sock):
    with RECV_LOCK:
        data = RECV_BUFFERS.setdefault(sock, bytearray())
    limit = 8 * 1024 * 1024
    while True:
        end = data.find(b'\n')
        if end >= 0:
            if end > limit:
                raise RuntimeError('oversized response')
            line = bytes(data[:end])
            del data[:end + 1]
            return json.loads(line)
        if len(data) > limit:
            raise RuntimeError('oversized response')
        chunk = sock.recv(min(65536, limit + 1 - len(data)))
        if not chunk:
            raise RuntimeError('connection closed while reading response')
        data.extend(chunk)


def send_json(sock, value):
    data = (json.dumps(value, sort_keys=True) + '\n').encode()
    view = memoryview(data)
    while view:
        n = sock.send(view)
        if n <= 0:
            raise RuntimeError('connection closed while sending')
        view = view[n:]


def load_worker(node, worker=0):
    path = RUN / 'control' / f'compute_{chr(65 + node)}' / f'worker-{worker}.json'
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        if path.exists():
            data = json.loads(path.read_text())
            if (data.get('run_id') != RUN_ID or data.get('node') != node or
                    data.get('worker') != worker or not data.get('generation')):
                raise RuntimeError('worker identity/generation mismatch')
            return data
        time.sleep(.05)
    raise RuntimeError('worker control state not ready')


def result_for(terminal, op, key):
    for item in terminal.get('results', []):
        if item.get('op') == op and item.get('key') == key:
            return item
    raise RuntimeError(f'missing result for {op}/{key}')


class SharedState:
    def __init__(self):
        self.ledger = Ledger(LEDGER_PATH)
        self.lock = threading.Lock()
        self.sequences = {node: 0 for node in range(NODES)}
        self.request_index = 0
        self.requests = set()
        self.transactions = set()

    def reserve(self, tx, rid):
        with self.lock:
            if tx in self.transactions or rid in self.requests:
                raise RuntimeError('duplicate request/transaction; use STATUS, never retry')
            self.transactions.add(tx)
            self.requests.add(rid)


class RecoveryRejectedDuringRecovery(RuntimeError):
    # EXECUTE 被服务端以 RECOVERY_IN_PROGRESS 拒绝（事务从未开始）。
    # 终局已由 execute() 记为 CONFIRMED_ABORTED；上层（probe 循环）
    # 应以新事务重试，绝不能对该 tx 走 STATUS 轮询（事务不存在）。
    pass


class Driver:
    def __init__(self, node=0, worker=0, shared=None):
        self.node = node
        self.worker_id = worker
        self.worker = load_worker(node, worker)
        self.port = int(self.worker['port'])
        self.shared = shared or SharedState()
        self.ledger = self.shared.ledger
        self.model = {}
        self.versions = {}
        self.outcome_counts = {'CONFIRMED_COMMITTED': 0, 'CONFIRMED_ABORTED': 0,
                               'UNKNOWN': 0, 'UNFINISHED': 0}

    def tx_id(self):
        with self.shared.lock:
            self.shared.sequences[self.node] += 1
            sequence = self.shared.sequences[self.node]
        return ((self.node + 1) << 48) | sequence

    def request_id(self):
        with self.shared.lock:
            self.shared.request_index += 1
            index = self.shared.request_index
        return f'{MODE}-{index:04d}'

    def identity(self, tx, request_id):
        return dict(run_id=RUN_ID, request_id=request_id, tx_id=tx, node=self.node,
                    generation=self.worker['generation'])

    def validate_response(self, response, tx, request_id, events):
        if not isinstance(response, dict) or any(response.get(k) != v for k, v in
                                                self.identity(tx, request_id).items()):
            raise RuntimeError('response identity/generation mismatch: ' + repr(response))
        if response.get('event') not in events:
            raise RuntimeError('unexpected response event: ' + repr(response))
        return response

    def record(self, event, tx, request_id, **fields):
        identity = self.identity(tx, request_id)
        identity.pop('run_id')
        self.ledger.write(event, **identity, **fields)

    def status(self, tx, request_id):
        # 恢复窗口内事务可能挂起在旧锁状态（等待 Phase3 SetRecoveryAbort
        # 唤醒后重路由），等待上限参数化：默认 30s 不变，R2 故障测试显式
        # 放大（挂起事务必须等到真实终局，绝不能把 UNKNOWN 记进台账）
        deadline = time.monotonic() + int(os.environ.get('HCM_STATUS_DEADLINE_SECONDS', '30'))
        while time.monotonic() < deadline:
            try:
                with socket.create_connection(('127.0.0.1', self.port), timeout=5) as sock:
                    send_json(sock, dict(self.identity(tx, request_id), kind='STATUS'))
                    response = recv_line(sock)
            except (OSError, RuntimeError, ValueError) as exc:
                self.record('status_error', tx, request_id, error=repr(exc))
                time.sleep(.05)
                continue
            self.record('status', tx, request_id, response=response)
            self.validate_response(response, tx, request_id, {'terminal', 'pending'})
            if response['event'] == 'terminal':
                return response
            time.sleep(.05)
        raise RuntimeError('terminal status deadline; EXECUTE is never retried')

    def control(self, kind, event, **fields):
        rid = self.request_id()
        request = dict(self.identity(0, rid), kind=kind, **fields)
        self.record('control_planned', 0, rid, request=request)
        # 控制请求（QUIESCE/TREE_SNAPSHOT 等）的等待上限。P1 修复后
        # TREE_SNAPSHOT = 显式 replay 追平（内部自有 fail-closed 超时
        # HCM_SNAPSHOT_CATCHUP_TIMEOUT_MS）+ 快速取页；大检查点前的积压
        # 追平可能超过原固定 1800s，参数化等待预算（默认保持 1800s 不变，
        # 仅 R1c 显式放大），不是放宽任何正确性门槛。
        # P0 修复（单位）：变量名以 _MS 结尾，原实现却把值直接传给以秒为
        # 单位的 socket.settimeout——设置 60000 会被当成 60000 秒。改为
        # 毫秒语义（/1000.0 换算），默认 1800000ms = 1800s 行为不变
        #（已冻结 env 均未设置过该变量，无兼容负担）。
        deadline = int(os.environ.get('HCM_SNAPSHOT_DEADLINE_MS', '1800000')) / 1000.0
        with socket.create_connection(('127.0.0.1', self.port), timeout=10) as sock:
            sock.settimeout(deadline)
            send_json(sock, request)
            response = recv_line(sock)
        self.record('compute_' + event, 0, rid, response=response)
        return self.validate_response(response, 0, rid, {event})

    def tree_stats(self):
        return self.control('TREE_STATS', 'tree-stats')

    def execute(self, purpose, operations, decision='COMMIT', expect='CONFIRMED_COMMITTED', disconnect=False,
                track_model=True, ledger_full_values=True, expected_ok=True, expected_error='',
                after_executed=None, fault=None):
        tx = self.tx_id()
        rid = self.request_id()
        self.shared.reserve(tx, rid)
        if not operations or len(operations) > 128 or tx & ((1 << 48) - 1) == 0:
            raise RuntimeError('invalid transaction operation/identity budget')
        planned_ops = []
        request_ops = []
        for op in operations:
            item = dict(op=op['op'], key=op['key'])
            request_item = dict(item)
            if 'value' in op:
                item['value_sha256'] = hashlib.sha256(op['value']).hexdigest()
                if ledger_full_values:
                    item['value_hex'] = op['value'].hex()
                else:
                    item['value_model'] = 'load_value_for(key,app_version)'
                request_item['value_hex'] = op['value'].hex()
            if 'app_version' in op:
                item['app_version'] = op['app_version']
                request_item['app_version'] = op['app_version']
            planned_ops.append(item)
            request_ops.append(request_item)
        step_mode = fault in ('cancel_during_execution', 'disconnect_during_execution')
        if step_mode:
            decision = 'CANCEL' if fault == 'cancel_during_execution' else 'DISCONNECTED'
        elif disconnect:
            decision = 'DISCONNECTED'
        self.record('planned', tx, rid, worker=self.worker_id, purpose=purpose,
                    decision=decision, operations=planned_ops, fault=fault,
                    expected_ok=expected_ok, expected_error=expected_error,
                    outcome_class='UNFINISHED')
        request = dict(self.identity(tx, rid), kind='EXECUTE', operations=request_ops, step_mode=step_mode)
        sock = None
        executed = terminal = None
        failure = None
        try:
            sock = socket.create_connection(('127.0.0.1', self.port), timeout=10)
            sock.settimeout(30)
            send_json(sock, request)
            accepted = recv_line(sock)
            self.record('accepted', tx, rid, response=accepted)
            # R2 契约修复：恢复窗口内服务端拒绝受理新事务（rejected +
            # RECOVERY_IN_PROGRESS，事务从未开始）。这是确定性终局
            #（CONFIRMED_ABORTED，服务端确认），绝不能落入 STATUS 轮询——
            # 事务不存在，轮询必然超时记 UNKNOWN 污染台账（fault-016
            # 实测 fault-0059：恢复完成前 0.04s 进入即触发）。记终局后向
            # 上抛可重试软错误（不含硬门槛关键词），probe 循环以新事务重试。
            if accepted.get('event') == 'rejected':
                reason = accepted.get('error', '')
                if reason != 'RECOVERY_IN_PROGRESS':
                    raise RuntimeError('unexpected rejection: ' + repr(accepted))
                rejected_terminal = dict(self.identity(tx, rid), event='terminal',
                                         outcome='CONFIRMED_ABORTED', decision='REJECTED',
                                         completed_ops=0, error=reason,
                                         confirmation='SERVER_REJECTED')
                self.record('terminal', tx, rid, response=rejected_terminal,
                            outcome_class='CONFIRMED_ABORTED')
                self.outcome_counts['CONFIRMED_ABORTED'] += 1
                raise RecoveryRejectedDuringRecovery(
                    'server rejected (RECOVERY_IN_PROGRESS); '
                    'transaction never started, safe to retry with a new transaction')
            self.validate_response(accepted, tx, rid, {'accepted'})
            if step_mode:
                step = recv_line(sock)
                self.record('step', tx, rid, response=step)
                self.validate_response(step, tx, rid, {'step'})
                if step.get('completed_ops') != 1 or len(operations) < 2:
                    raise RuntimeError('fault requires exactly one executed step and remaining operations')
                self.record('decision', tx, rid, decision=decision, completed_ops=1)
                if decision == 'DISCONNECTED':
                    sock.shutdown(socket.SHUT_WR)
                    sock.close()
                    sock = None
                    terminal = self.status(tx, rid)
                    evidence = RUN / 'control' / f'compute_{chr(65 + self.node)}' / f'txn-{tx}-executed.json'
                    executed = terminal.get('executed') or json.loads(evidence.read_text())
                else:
                    send_json(sock, dict(self.identity(tx, rid), decision='CANCEL'))
                    executed = recv_line(sock)
                    terminal = recv_line(sock)
                self.validate_response(executed, tx, rid, {'executed'})
                self.record('executed', tx, rid, response=executed,
                            source='durable_compute_evidence' if sock is None else 'response')
                if executed.get('completed_ops') != 1 or terminal.get('completed_ops') != 1:
                    raise RuntimeError('execution continued beyond cancellation point')
            else:
                executed = recv_line(sock)
                self.record('executed', tx, rid, response=executed)
                self.validate_response(executed, tx, rid, {'executed'})
                if after_executed is not None:
                    after_executed(executed)
                self.record('decision', tx, rid, decision=decision)
                if disconnect:
                    sock.shutdown(socket.SHUT_WR)
                    sock.close()
                    sock = None
                    terminal = self.status(tx, rid)
                else:
                    send_json(sock, dict(self.identity(tx, rid), decision=decision))
                    if fault == 'commit_response_loss':
                        sock.shutdown(socket.SHUT_WR)
                        sock.close()
                        sock = None
                        terminal = self.status(tx, rid)
                    else:
                        terminal = recv_line(sock)
            self.validate_response(terminal, tx, rid, {'terminal'})
            if terminal.get('outcome') not in self.outcome_counts:
                raise RuntimeError('invalid terminal outcome class')
        except Exception as exc:
            failure = exc
            if sock is not None:
                sock.close()
                sock = None
            self.record('request_error', tx, rid, error=repr(exc))
            # rejected(RECOVERY_IN_PROGRESS) 已在上方记 CONFIRMED_ABORTED
            # 终局（事务从未开始），直接上抛供上层以新事务重试；落入下方
            # STATUS 轮询只会超时记 UNKNOWN 污染台账
            if isinstance(exc, RecoveryRejectedDuringRecovery):
                raise
            try:
                terminal = self.status(tx, rid)
                if terminal.get('outcome') not in self.outcome_counts:
                    raise RuntimeError('invalid recovered outcome')
            except Exception as status_error:
                terminal = dict(self.identity(tx, rid), event='terminal', outcome='UNKNOWN',
                                error=repr(status_error), confirmation='UNTRUSTED')
        finally:
            if sock is not None:
                sock.close()
        outcome = terminal['outcome']
        self.record('terminal', tx, rid, response=terminal, outcome_class=outcome)
        self.outcome_counts[outcome] += 1
        if outcome in ('UNKNOWN', 'UNFINISHED'):
            raise RuntimeError(f'unresolved outcome for {purpose}: {terminal}')
        if failure is not None:
            raise RuntimeError(f'{purpose}: request failed, STATUS confirmed {outcome}; no retry') from failure
        if executed is None or not isinstance(executed.get('ok'), bool):
            raise RuntimeError('missing executed ok/error evidence')
        if expected_ok is not None and executed['ok'] is not expected_ok:
            raise RuntimeError(f'{purpose}: unexpected executed ok: {executed}')
        allowed_errors = {expected_error} if isinstance(expected_error, str) else set(expected_error)
        if executed.get('error') not in allowed_errors:
            raise RuntimeError(f'{purpose}: unexpected executed error: {executed}')
        if terminal.get('results') != executed.get('results'):
            raise RuntimeError(f'{purpose}: terminal/executed result mismatch')
        if terminal.get('error') != executed.get('error'):
            raise RuntimeError(f'{purpose}: terminal overwrote executed error')
        if [(item.get('op'), item.get('key')) for item in executed.get('results', [])] != [
                (op['op'], op['key']) for op in operations]:
            raise RuntimeError(f'{purpose}: executed operation sequence mismatch')
        if terminal.get('decision') != decision:
            raise RuntimeError(f'{purpose}: terminal decision mismatch')
        expected_set = {expect} if isinstance(expect, str) else set(expect)
        if outcome not in expected_set:
            raise RuntimeError(f'{purpose}: expected {sorted(expected_set)}, got {outcome}: {terminal}')
        if outcome == 'CONFIRMED_COMMITTED' and (not executed['ok'] or decision != 'COMMIT'):
            raise RuntimeError('commit contradicts executed result/decision')
        if outcome == 'CONFIRMED_COMMITTED' and track_model:
            for op in operations:
                key = op['key']
                if op['op'] == 'DELETE':
                    self.model.pop(key, None)
                    self.versions.pop(key, None)
                elif op['op'] in ('INSERT', 'UPDATE'):
                    self.model[key] = op['value']
                    self.versions[key] = op['app_version']
        return terminal

    def verify_read(self, key, expected=None, missing=False, version=None):
        terminal = self.execute(f'verify-read-{key}', [dict(op='READ', key=key)],
                                decision='COMMIT', expected_ok=not missing,
                                expected_error='NOT_FOUND' if missing else '',
                                expect='CONFIRMED_COMMITTED' if not missing else 'CONFIRMED_ABORTED')
        result = result_for(terminal, 'READ', key)
        if missing:
            if result.get('found'):
                raise RuntimeError(f'{key} unexpectedly visible')
            return terminal
        if not result.get('found'):
            raise RuntimeError(f'{key} not found')
        actual = bytes.fromhex(result['value_hex'])
        if expected is not None and actual != expected:
            raise RuntimeError(f'{key} full value mismatch')
        if version is not None and self.versions.get(key) != version:
            raise RuntimeError(f'{key} application version mismatch')
        return terminal

    def serial_contract(self):
        missing = 900001
        self.verify_read(missing, missing=True)
        self.execute('update-missing', [dict(op='UPDATE', key=missing, value=value_for(missing, 1), app_version=1)],
                     expect='CONFIRMED_ABORTED', expected_ok=False, expected_error='NOT_FOUND')
        self.execute('delete-missing', [dict(op='DELETE', key=missing)],
                     expect='CONFIRMED_ABORTED', expected_ok=False, expected_error='NOT_FOUND')

        key = 100
        v1 = value_for(key, 1)
        self.execute('insert-read', [dict(op='INSERT', key=key, value=v1, app_version=1)])
        self.verify_read(key, v1, version=1)

        self.execute('duplicate-key-reject', [dict(op='INSERT', key=key, value=value_for(key, 99), app_version=99)],
                     expect='CONFIRMED_ABORTED', expected_ok=False, expected_error='DUPLICATE_KEY')
        self.verify_read(key, v1, version=1)

        v2 = value_for(key, 2)
        self.execute('update-read', [dict(op='UPDATE', key=key, value=v2, app_version=2)])
        self.verify_read(key, v2, version=2)

        rollback_insert_key = 101
        self.execute('explicit-rollback-insert', [dict(op='INSERT', key=rollback_insert_key,
                     value=value_for(rollback_insert_key, 1), app_version=1)],
                     decision='ROLLBACK', expect='CONFIRMED_ABORTED')
        self.verify_read(rollback_insert_key, missing=True)

        self.execute('explicit-rollback-update', [dict(op='UPDATE', key=key,
                     value=value_for(key, 3), app_version=3)],
                     decision='ROLLBACK', expect='CONFIRMED_ABORTED')
        self.verify_read(key, v2, version=2)

        self.execute('explicit-rollback-delete', [dict(op='DELETE', key=key)],
                     decision='ROLLBACK', expect='CONFIRMED_ABORTED')
        self.verify_read(key, v2, version=2)

        cancel_key = 102
        self.execute('disconnect-cancel-insert', [dict(op='INSERT', key=cancel_key,
                     value=value_for(cancel_key, 1), app_version=1)],
                     expect='CONFIRMED_ABORTED', disconnect=True)
        self.verify_read(cancel_key, missing=True)

        multi_key = 103
        self.execute('multi-operation-rollback', [
            dict(op='INSERT', key=multi_key, value=value_for(multi_key, 1), app_version=1),
            dict(op='UPDATE', key=key, value=value_for(key, 4), app_version=4),
        ], decision='ROLLBACK', expect='CONFIRMED_ABORTED')
        self.verify_read(multi_key, missing=True)
        self.verify_read(key, v2, version=2)

        second_undo = 104
        self.execute('second-undo-setup', [dict(op='INSERT', key=107, value=value_for(107, 1), app_version=1)])
        self.execute('multi-operation-rollback-delete-update-insert', [
            dict(op='DELETE', key=key),
            dict(op='UPDATE', key=107, value=value_for(107, 2), app_version=2),
            dict(op='INSERT', key=second_undo, value=value_for(second_undo, 1), app_version=1),
        ], decision='ROLLBACK', expect='CONFIRMED_ABORTED')
        self.verify_read(key, v2, version=2)
        self.verify_read(107, value_for(107, 1), version=1)
        self.verify_read(second_undo, missing=True)

        lost_key = 105
        self.execute('commit-response-loss', [dict(op='INSERT', key=lost_key,
                     value=value_for(lost_key, 1), app_version=1)], fault='commit_response_loss')
        self.verify_read(lost_key, value_for(lost_key, 1), version=1)
        for fault, keys, error in (
                ('cancel_during_execution', (108, 109), 'CANCELLED'),
                ('disconnect_during_execution', (110, 111),
                 ('CLIENT_DISCONNECTED', 'EXECUTION_CONTROL_TIMEOUT_OR_DISCONNECT'))):
            self.execute(fault, [dict(op='INSERT', key=k, value=value_for(k, 1), app_version=1) for k in keys],
                         fault=fault, expected_ok=False, expected_error=error, expect='CONFIRMED_ABORTED')
            for cancelled_key in keys:
                self.verify_read(cancelled_key, missing=True)

        self.execute('delete-commit', [dict(op='DELETE', key=key)])
        self.verify_read(key, missing=True)

        # 同键不同 RID：删除 k1 释放其槽位后，空闲槽的分配是实现自由，不能假设
        # 恰好是下一个插入占用它——持续插入探针键直到某个键真正占用该槽，随后
        # 重插 k1 必须落到不同 RID，并验证索引指向新位置（RID 是物理位置而非键身份）。
        keys = [2000, 2001, 2002]
        rids = {}
        for i, k in enumerate(keys, 1):
            terminal = self.execute(f'rid-setup-insert-{k}', [dict(op='INSERT', key=k,
                                    value=value_for(k, i), app_version=i)])
            r = result_for(terminal, 'INSERT', k)
            rids[k] = (r['rid_page'], r['rid_slot'])
        self.execute('rid-delete-original', [dict(op='DELETE', key=2000)])
        freed = rids[2000]
        occupier = None
        probe = 2003
        for _ in range(24):
            terminal = self.execute(f'rid-reuse-probe-insert-{probe}', [dict(op='INSERT', key=probe,
                                    value=value_for(probe, 1), app_version=1)])
            r = result_for(terminal, 'INSERT', probe)
            if (r['rid_page'], r['rid_slot']) == freed:
                occupier = probe
                break
            probe += 1
        if occupier is None:
            raise RuntimeError('freed slot was never reused; cannot exercise different-RID reinsert')
        reinserted = self.execute('same-key-new-rid-insert', [dict(op='INSERT', key=2000,
                                  value=value_for(2000, 7), app_version=7)])
        new = result_for(reinserted, 'INSERT', 2000)
        new_rid = (new['rid_page'], new['rid_slot'])
        if new_rid == freed:
            raise RuntimeError('same-key reinsert did not exercise a different RID')
        self.verify_read(2000, value_for(2000, 7), version=7)

        for live_key, expected in sorted(self.model.items()):
            self.verify_read(live_key, expected, version=self.versions[live_key])
        ledger_counts, _, _ = ledger_accounting()
        if ledger_counts != self.outcome_counts:
            raise RuntimeError('serial ledger/driver terminal accounting mismatch')
        summary = dict(run_id=RUN_ID, mode=MODE, requests=self.shared.request_index,
                       outcome_counts=ledger_counts, final_model={
                           str(k): dict(app_version=self.versions[k],
                                        value_sha256=hashlib.sha256(v).hexdigest())
                           for k, v in sorted(self.model.items())},
                       all_terminal_classes_accounted=True)
        write_artifact(RUN / 'ledger' / 'final-model.json', summary)
        self.ledger.write('contract_complete', summary=summary, outcome_class='CONFIRMED_COMMITTED')
        return summary


def owned_worker(shared, node, worker):
    driver = Driver(node, worker, shared)
    base = 100000 + node * 10000 + worker * 1000
    live = []
    for i in range(3):
        key = base + i
        driver.execute(f'owned-preload-n{node}-w{worker}-{i}',
                       [dict(op='INSERT', key=key, value=value_for(key, 1), app_version=1)])
        live.append(key)
    next_key = base + 100
    pattern = ['READ', 'READ', 'UPDATE', 'INSERT', 'DELETE'] * 4
    version = 2
    for i, op in enumerate(pattern):
        if op == 'INSERT':
            key = next_key
            next_key += 1
            driver.execute(f'owned-insert-n{node}-w{worker}-{i}',
                           [dict(op='INSERT', key=key, value=value_for(key, 1), app_version=1)])
            live.append(key)
        else:
            if not live:
                raise RuntimeError('owned workload ran out of live keys')
            key = live[0]
            if op == 'READ':
                driver.verify_read(key, driver.model[key], version=driver.versions[key])
            elif op == 'UPDATE':
                value = value_for(key, version)
                driver.execute(f'owned-update-n{node}-w{worker}-{i}',
                               [dict(op='UPDATE', key=key, value=value, app_version=version)])
                version += 1
            elif op == 'DELETE':
                driver.execute(f'owned-delete-n{node}-w{worker}-{i}', [dict(op='DELETE', key=key)])
                live.pop(0)
    if not live:
        raise RuntimeError('owned workload ended without a visible key')
    return driver, live[0]


def mixed_contract(shared):
    if NODES != 3 or THREADS < 1:
        raise RuntimeError('mixed contract requires three nodes and at least one worker per node')
    jobs = [(node, worker) for node in range(NODES) for worker in range(THREADS)]
    results = {}
    errors = []

    def run_owned(job):
        node, worker = job
        try:
            results[job] = owned_worker(shared, node, worker)
        except Exception as exc:
            errors.append((job, repr(exc)))

    threads = [threading.Thread(target=run_owned, args=(job,)) for job in jobs]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    if errors:
        raise RuntimeError('owned multinode workload failed: ' + json.dumps(errors))

    # 跨节点可见性：每个所有者的一个已提交键，必须能从全部计算节点读到同一完整值。
    cross_reads = 0
    for (owner_node, owner_worker), (owner, key) in sorted(results.items()):
        expected = owner.model[key]
        for node in range(NODES):
            reader = Driver(node, 0, shared)
            reader.verify_read(key, expected)
            cross_reads += 1

    shared_key = 190000
    coordinator = Driver(0, 0, shared)
    coordinator.execute('shared-key-insert', [dict(op='INSERT', key=shared_key,
                        value=value_for(shared_key, 1), app_version=1)])
    conflict_results = []
    rounds = int(os.environ.get('HCM_CONFLICT_ROUNDS', '3'))
    if rounds < 3:
        raise RuntimeError('shared competition requires at least three rounds')
    for round_index in range(rounds):
        holder = Driver(round_index % NODES, 0, shared)
        rival = Driver((round_index + 1) % NODES, 0, shared)
        version = 10 + 2 * round_index

        def conflict_operations(kinds):
            def while_held(executed):
                if not executed.get('ok'):
                    raise RuntimeError('conflict holder did not acquire the key')
                for kind in kinds:
                    op = dict(op=kind, key=shared_key)
                    if kind in ('INSERT', 'UPDATE'):
                        op.update(value=value_for(shared_key, 999), app_version=999)
                    terminal = rival.execute(f'shared-r{round_index}-{kind}-KEY_CONFLICT', [op],
                        expected_ok=False, expected_error='KEY_CONFLICT', expect='CONFIRMED_ABORTED')
                    conflict_results.append(dict(round=round_index, op=kind, outcome=terminal['outcome'],
                                                 error=terminal['error']))
            return while_held

        holder.execute(f'shared-r{round_index}-update-holder',
            [dict(op='UPDATE', key=shared_key, value=value_for(shared_key, version), app_version=version)],
            after_executed=conflict_operations(('UPDATE', 'READ', 'INSERT', 'DELETE')))
        for node in range(NODES):
            Driver(node, 0, shared).verify_read(shared_key, value_for(shared_key, version))
            cross_reads += 1
        rival.execute(f'shared-r{round_index}-duplicate-insert',
            [dict(op='INSERT', key=shared_key, value=value_for(shared_key, 999), app_version=999)],
            expected_ok=False, expected_error='DUPLICATE_KEY', expect='CONFIRMED_ABORTED')
        holder.execute(f'shared-r{round_index}-delete-holder', [dict(op='DELETE', key=shared_key)],
                       after_executed=conflict_operations(('DELETE', 'INSERT')))
        for node in range(NODES):
            Driver(node, 0, shared).verify_read(shared_key, missing=True)
            cross_reads += 1
        shared_winner_version = version + 1
        shared_winner_value = value_for(shared_key, shared_winner_version)
        holder.execute(f'shared-r{round_index}-reinsert-holder',
            [dict(op='INSERT', key=shared_key, value=shared_winner_value, app_version=shared_winner_version)],
            after_executed=conflict_operations(('INSERT', 'UPDATE')))
        for node in range(NODES):
            Driver(node, 0, shared).verify_read(shared_key, shared_winner_value)
            cross_reads += 1

    final_model = {}
    final_versions = {}
    operation_mix = {'READ': 8 * len(jobs), 'UPDATE': 4 * len(jobs),
                     'INSERT': 4 * len(jobs), 'DELETE': 4 * len(jobs)}
    for driver, _ in results.values():
        for key, value in driver.model.items():
            if key in final_model:
                raise RuntimeError('owned worker key ranges overlap')
            final_model[key] = value
            final_versions[key] = driver.versions[key]
    final_model[shared_key] = shared_winner_value
    final_versions[shared_key] = shared_winner_version
    # 最终再从每个节点读取全部存活键，作为稳定截面前的跨节点全量可见性检查。
    final_cross_reads = 0
    for key, expected in sorted(final_model.items()):
        for node in range(NODES):
            Driver(node, 0, shared).verify_read(key, expected)
            final_cross_reads += 1
    ledger_counts, planned_mix, _ = ledger_accounting()
    summary = dict(run_id=RUN_ID, mode=MODE, requests=shared.request_index,
                   nodes=NODES, threads_per_node=THREADS, outcome_counts=ledger_counts,
                   operation_mix=planned_mix, owned_operation_mix=operation_mix,
                   cross_node_reads=cross_reads + final_cross_reads,
                   owned_setup_inserts=3 * len(jobs), owned_mix_percent={'READ': 40, 'UPDATE': 20, 'INSERT': 20, 'DELETE': 20},
                   shared_key_conflict={'rounds': rounds, 'committed': rounds * 3,
                                        'aborted': len(conflict_results), 'cases': conflict_results,
                                        'winner_version': shared_winner_version},
                   final_model={str(k): dict(app_version=final_versions[k],
                                             value_sha256=hashlib.sha256(v).hexdigest())
                                for k, v in sorted(final_model.items())},
                   all_terminal_classes_accounted=True)
    write_artifact(RUN / 'ledger' / 'final-model.json', summary)
    shared.ledger.write('contract_complete', summary=summary, outcome_class='CONFIRMED_COMMITTED')
    return summary, shared


def control_request(role, name):
    path = RUN / 'control' / role / (name + '.request')
    if path.exists():
        raise RuntimeError('duplicate checkpoint request')
    tmp = path.with_suffix('.tmp')
    with tmp.open('x') as f:
        json.dump(dict(run_id=RUN_ID, time_ns=now_ns()), f)
        f.write('\n')
        f.flush()
        os.fsync(f.fileno())
    tmp.rename(path)
    dirfd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(dirfd)
    finally:
        os.close(dirfd)


def wait_control_state(role, name, timeout=1800):
    path = RUN / 'control' / role / (name + '.json')
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            data = json.loads(path.read_text())
            if data.get('run_id') != RUN_ID or data.get('state') != name:
                raise RuntimeError('checkpoint run/state identity mismatch')
            return data
        time.sleep(.1)
    raise RuntimeError('storage checkpoint deadline')


def service_rss_bytes():
    total = 0
    for path in (RUN / 'pids').glob('*.json'):
        pid = json.loads(path.read_text())['pid']
        try:
            pages = int((Path('/proc') / str(pid) / 'statm').read_text().split()[1])
            total += pages * os.sysconf('SC_PAGE_SIZE')
        except (FileNotFoundError, ProcessLookupError):
            pass
    return total


def strict_tree_check(db, target, artifact):
    binary = Path(os.environ['HCM_TREE_STATS_BIN']).resolve(strict=True)
    command = [str(binary), str(db), 'ycsb_user_table', '--json-only', '--model-range', f'0:{target}:1']
    started = time.monotonic()
    proc, failure = None, None
    with artifact.with_suffix('.out').open('x') as output:
        try:
            proc = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, text=True, timeout=1800)
        except Exception as exc:
            failure = repr(exc)
        finally:
            output.flush()
            os.fsync(output.fileno())
    fsync_file(artifact.parent)
    text = artifact.with_suffix('.out').read_text()
    try:
        result = json.loads(text.split('JSON_BEGIN', 1)[1].split('JSON_END', 1)[0].strip())
        if not isinstance(result, dict):
            raise ValueError('checker result is not an object')
    except (ValueError, IndexError) as exc:
        result = {'pass': False, 'error': str(exc), 'output_tail': text[-4000:]}
    result['command'] = command
    result['exit_code'] = proc.returncode if proc is not None else None
    result['wall_seconds'] = time.monotonic() - started
    if failure:
        result.update({'pass': False, 'execution_error': failure})
    write_artifact(artifact, result)
    if (result['exit_code'] != 0 or result.get('pass') is not True or result.get('structure_errors') or
            result.get('index_total_keys') != target or result.get('model_checked_records') != target or
            result.get('model_expected_records') != target or result.get('order') != 253 or
            not result.get('content_sha256')):
        raise RuntimeError('strict index/heap/FSM/full-value check failed: ' + str(artifact))
    return result


def preserve_storage_checkpoint(destination):
    source = RUN / 'storage' / 'build' / 'storage_server'
    destination.mkdir()
    for name in ('ycsb_user_table', 'ycsb_user_table_bl', 'ycsb_user_table_fsm'):
        with (source / name).open('rb') as src, (destination / name).open('xb') as dst:
            before = os.fstat(src.fileno())
            while True:
                chunk = src.read(1024 * 1024)
                if not chunk:
                    break
                dst.write(chunk)
            after = os.fstat(src.fileno())
            if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                raise RuntimeError('storage file changed during stable snapshot')
            dst.flush()
            os.fsync(dst.fileno())
    fsync_file(destination)
    fsync_file(destination.parent)
    return destination


def insert_chunk(shared, worker, start, count, batch_size=128):
    driver = Driver(0, worker, shared)
    done = 0
    while done < count:
        n = min(batch_size, count - done)
        operations = [dict(op='INSERT', key=start + done + i,
                           value=load_value_for(start + done + i, 1), app_version=1)
                      for i in range(n)]
        driver.execute(f'load-insert-w{worker}-{start + done}', operations,
                       track_model=False, ledger_full_values=False)
        done += n


def load_contract(shared):
    if NODES != 1 or THREADS < 1:
        raise RuntimeError('natural load stage uses one compute node after R1b multinode acceptance')
    checkpoints = ([int(item) for item in os.environ['HCM_LOAD_TARGETS'].split(',')]
                   if os.environ.get('HCM_LOAD_TARGETS') else
                   [1000, 32000, 128000, 524288, 1048576, 2097152, 4200000])
    if not checkpoints or any(item <= 0 for item in checkpoints) or \
       any(b <= a for a, b in zip(checkpoints, checkpoints[1:])):
        raise RuntimeError('load checkpoints must be positive and strictly increasing')
    results = []
    inserted = 0
    phase_started = time.monotonic()
    for checkpoint_index, target in enumerate(checkpoints):
        chunk = target - inserted
        if chunk <= 0:
            continue
        base = inserted
        workers = THREADS
        per_worker = chunk // workers
        remainder = chunk % workers
        threads = []
        errors = []

        def run_worker(worker, start, count):
            try:
                if count:
                    insert_chunk(shared, worker, start, count)
            except Exception as exc:
                errors.append((worker, start, count, repr(exc)))

        cursor = base
        for worker in range(workers):
            count = per_worker + (1 if worker < remainder else 0)
            thread = threading.Thread(target=run_worker, args=(worker, cursor, count))
            threads.append(thread)
            thread.start()
            cursor += count
        for thread in threads:
            thread.join()
        if errors:
            raise RuntimeError('load phase failed: ' + json.dumps(errors))
        inserted = target

        artifacts = RUN / 'ledger' / f'checkpoint-{checkpoint_index}'
        artifacts.mkdir()
        fsync_file(artifacts.parent)
        idle = []
        for worker in range(THREADS):
            state = Driver(0, worker, shared).control('QUIESCE', 'quiescent')
            if state.get('ok') is not True or state.get('active_transactions') != 0:
                raise RuntimeError('all-worker quiescence not confirmed')
            idle.append(state)
        write_artifact(artifacts / 'workers-idle.json', idle)
        snapshot = Driver(0, 0, shared).control('TREE_SNAPSHOT', 'tree-snapshot')
        if snapshot.get('ok') is not True or snapshot.get('active_transactions') != 0:
            raise RuntimeError('compute actual snapshot failed')
        compute_dir = Path(snapshot['snapshot_dir']).resolve(strict=True)
        control_dir = (RUN / 'control' / 'compute_A').resolve(strict=True)
        if compute_dir.parent != control_dir:
            raise RuntimeError('compute snapshot is outside this run/node')
        manifest = json.loads((compute_dir / 'manifest.json').read_text())
        if (manifest.get('run_id') != RUN_ID or manifest.get('node') != 0 or
                manifest.get('source') != 'compute-lazy-fetch' or manifest.get('complete') is not True or
                manifest.get('page_size') != 4096):
            raise RuntimeError('compute snapshot manifest identity/source/layout mismatch')
        write_artifact(artifacts / 'compute-snapshot.json', snapshot)
        compute_stats = strict_tree_check(compute_dir, target, artifacts / 'compute-check.json')
        compute_stats['root_page'] = manifest['root_page']
        compute_stats['total_keys'] = compute_stats['index_total_keys']
        control_request('storage', f'checkpoint-{checkpoint_index}')
        storage_cut = wait_control_state('storage', f'checkpoint-{checkpoint_index}')
        try:
            if (storage_cut.get('checkpoint') != checkpoint_index or
                    storage_cut.get('active_undo_transactions') != 0 or
                    storage_cut.get('wal_tail_inclusive') != storage_cut.get('replay_inclusive') or
                    any(storage_cut.get(field) is not True for field in
                        ('wal_checkpoint_fdatasync', 'database_checkpoint_fdatasync', 'physical_writes_frozen'))):
                raise RuntimeError('storage cut is not fully drained/durable/frozen')
            write_artifact(artifacts / 'storage-cut.json', storage_cut)
            storage_dir = preserve_storage_checkpoint(artifacts / 'storage')
            storage_stats = strict_tree_check(storage_dir, target, artifacts / 'storage-check.json')
            if compute_stats['content_sha256'] != storage_stats['content_sha256']:
                raise RuntimeError('compute/storage logical full-content digest mismatch')
        finally:
            control_request('storage', f'checkpoint-{checkpoint_index}.release')
            released = wait_control_state('storage', f'checkpoint-{checkpoint_index}-released')
            write_artifact(artifacts / 'storage-released.json', released)
        db_dir = RUN / 'storage' / 'build' / 'storage_server'
        wal_bytes = sum(p.stat().st_size for p in (db_dir / 'log_v2').glob('seg_*.log'))
        undo_bytes = sum(p.stat().st_size for p in (db_dir / 'undo').glob('*') if p.is_file()) if (db_dir / 'undo').exists() else 0
        heap_pages = (db_dir / 'ycsb_user_table').stat().st_size // 4096
        index_pages = (db_dir / 'ycsb_user_table_bl').stat().st_size // 4096
        fsm_pages = (db_dir / 'ycsb_user_table_fsm').stat().st_size // 4096
        checkpoint = dict(checkpoint=checkpoint_index, records=inserted,
                          phase_wall_seconds=time.monotonic() - phase_started,
                          service_rss_bytes=service_rss_bytes(),
                          wal_bytes=wal_bytes, undo_bytes=undo_bytes,
                          heap_bytes=(db_dir / 'ycsb_user_table').stat().st_size,
                          index_bytes=(db_dir / 'ycsb_user_table_bl').stat().st_size,
                          fsm_bytes=(db_dir / 'ycsb_user_table_fsm').stat().st_size,
                          storage_cut=storage_cut, workers_idle=idle,
                          compute_snapshot=str(compute_dir), storage_snapshot=str(storage_dir),
                          content_sha256=storage_stats['content_sha256'],
                          full_value_bytes_verified=target * VALUE_SIZE,
                          compute_tree=compute_stats, storage_tree=storage_stats,
                          artifacts_bytes=sum(p.stat().st_size for p in RUN.rglob('*') if p.is_file()),
                          disk_free_bytes=os.statvfs(RUN).f_bavail * os.statvfs(RUN).f_frsize,
                          cache_capacity={'heap_frames': 3000, 'index_frames': 3000, 'fsm_frames': 2000},
                          workset_to_cache={'heap_pages': heap_pages, 'index_pages': index_pages,
                                            'fsm_pages': fsm_pages,
                                            'heap_ratio': heap_pages / 3000,
                                            'index_ratio': index_pages / 3000,
                                            'fsm_ratio': fsm_pages / 2000},
                          splits_observed={'compute_internal_pages': sum(compute_stats['level_pages'][:-1]),
                                           'storage_internal_pages': sum(storage_stats['level_pages'][:-1])},
                          height_requirement_met=False)
        if compute_stats['total_keys'] != inserted or storage_stats['index_total_keys'] != inserted:
            raise RuntimeError('load key count mismatch')
        if storage_stats['height'] < 1 or not storage_stats['pass']:
            raise RuntimeError('storage structure invalid at checkpoint')
        is_final = checkpoint_index == len(checkpoints) - 1
        required_height = int(os.environ.get('HCM_LOAD_REQUIRED_HEIGHT', '4'))
        if required_height < 1:
            raise RuntimeError('required height must be positive')
        checkpoint['required_height'] = required_height if is_final else None
        checkpoint['height_requirement_met'] = (compute_stats['height'] >= required_height and
                                                  storage_stats['height'] >= required_height) if is_final else None
        checkpoint['acceptance'] = 'FINAL_HEIGHT_CHECK' if is_final else 'INTERMEDIATE_NOT_ACCEPTANCE'
        results.append(checkpoint)
        write_artifact(RUN / 'ledger' / f'checkpoint-{checkpoint_index}.json', checkpoint)
        if checkpoint['artifacts_bytes'] > int(os.environ.get('HCM_MAX_ARTIFACT_BYTES', str(64 * 1024**3))) or \
                checkpoint['service_rss_bytes'] > int(os.environ.get('HCM_MAX_RSS_BYTES', str(32 * 1024**3))):
            raise RuntimeError('checkpoint resource budget exceeded')
        if is_final and not checkpoint['height_requirement_met']:
            raise RuntimeError('natural required-height requirement not met: ' + json.dumps(checkpoint))
        if not is_final:
            resumed = Driver(0, 0, shared).control('RESUME', 'resumed')
            if resumed.get('ok') is not True or resumed.get('active_transactions') != 0:
                raise RuntimeError('compute resume not confirmed')
        phase_started = time.monotonic()

    final = results[-1]
    ledger_counts, _, business_requests = ledger_accounting()
    if ledger_counts['CONFIRMED_COMMITTED'] != business_requests or ledger_counts['CONFIRMED_ABORTED']:
        raise RuntimeError('load terminal accounting failed')
    summary = dict(run_id=RUN_ID, mode=MODE, requests=shared.request_index,
                   business_transactions=ledger_counts['CONFIRMED_COMMITTED'],
                   outcome_counts=ledger_counts,
                   model_kind='sequential_insert_range', start_key=0, key_count=inserted,
                   app_version=1, value_model='load_value_for(key,app_version)',
                   checkpoints=results, compute_height=final['compute_tree']['height'],
                   storage_height=final['storage_tree']['height'], height_5='NOT_RUN',
                   natural_layout={'page_size': 4096, 'fanout_order': 253, 'fanout_modified': False},
                   all_terminal_classes_accounted=True)
    write_artifact(RUN / 'ledger' / 'final-model.json', summary)
    shared.ledger.write('contract_complete', summary=summary, outcome_class='CONFIRMED_COMMITTED')
    return summary, shared


def fault_contract(shared):
    """R2 小规模单节点故障恢复验证。

    流程：三节点各播种一批已提交数据（kill 前跨节点读对照）→ 通过监督者
    注入 compute_A SIGKILL（无在途事务时机）→ 探测恢复完成 → 由存活节点
    B/C 对被接管分区（victim 的全部键）执行读/写/删/插并做全量值校验 →
    最终跨节点全量可见性检查 → 普通 final-model（独立验证链兼容）。

    lazy 模式下新到请求由"接收节点本地执行 + 锁路由到新 GPLM 管理者 +
    数据从存储取"实现接管；本契约验证的正是这条链路。
    """
    if NODES != 3:
        raise RuntimeError('fault contract requires exactly three nodes')
    victim_node = 0
    survivors = (1, 2)
    model, versions = {}, {}

    def note(key, value, version):
        model[key] = value
        versions[key] = version

    def drop(key):
        model.pop(key, None)
        versions.pop(key, None)

    def insert_keys(node, keys, version=1, purpose='insert'):
        value = {k: value_for(k, version) for k in keys}
        driver = Driver(node, 0, shared)
        for i in range(0, len(keys), 10):
            batch = keys[i:i + 10]
            driver.execute(f'{purpose}-n{node}-{i}',
                           [dict(op='INSERT', key=k, value=value[k], app_version=version)
                            for k in batch], track_model=False)
            for k in batch:
                note(k, value[k], version)

    def read_on(node, key, expected, purpose='read'):
        terminal = Driver(node, 0, shared).execute(
            f'{purpose}-n{node}-k{key}', [dict(op='READ', key=key)],
            track_model=False)
        result = result_for(terminal, 'READ', key)
        if not result.get('found'):
            raise RuntimeError(f'{key} not found on node {node} ({purpose})')
        actual = bytes.fromhex(result['value_hex'])
        if actual != expected:
            raise RuntimeError(f'{key} full value mismatch on node {node} ({purpose})')

    # —— 阶段 1：三节点播种（victim 也播种；kill 后这些键构成被接管数据集）——
    seeded = {}
    for node in range(NODES):
        base = 400000 + node * 1000
        keys = [base + i for i in range(30)]
        insert_keys(node, keys, purpose='fault-seed')
        seeded[node] = keys
    victim_keys = seeded[victim_node]

    # kill 前跨节点对照：victim 与存活节点的键从任意节点可读、全值正确。
    for node in range(NODES):
        for owner in range(NODES):
            for key in seeded[owner][::6]:
                read_on(node, key, model[key], purpose='fault-precross')

    # —— 阶段 2：无在途事务时触发监督者注入 SIGKILL ——
    request_path = RUN / 'fault-inject.request'
    if request_path.exists():
        raise RuntimeError('duplicate fault injection request')
    with request_path.open('x') as f:
        json.dump(dict(run_id=RUN_ID, victim=f'compute_{chr(65 + victim_node)}',
                       time_ns=now_ns()), f)
        f.flush()
        os.fsync(f.fileno())
    fsync_file(RUN)
    injected = RUN / 'fault-injected.json'
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline and not injected.exists():
        time.sleep(.1)
    if not injected.exists():
        raise RuntimeError('supervisor did not execute the fault injection')
    injected_record = json.loads(injected.read_text())
    if injected_record.get('run_id') != RUN_ID or injected_record.get('victim') != 'compute_A':
        raise RuntimeError('fault injection record identity mismatch')
    injected_at = time.monotonic()

    # —— 阶段 3：恢复完成探测。逐键（victim 全部键 + 存活节点全部键）
    # 在两个存活节点上读取并重试：恢复窗口内事务可能被 tainted abort、
    # 挂起在旧锁状态（status 长等待直到 Phase3 唤醒后的真实终局）或撞
    # 未释放 IR 锁；重试直到每个键在两个存活节点都读出正确全值——
    # 以"全键可读且全值正确"作为恢复实际完成的判定信号。
    all_keys = sorted(model)
    probe_deadline = time.monotonic() + int(os.environ.get('HCM_RECOVERY_PROBE_SECONDS', '600'))
    retry_stats = []
    pending = {(node, key) for node in survivors for key in all_keys}
    while pending and time.monotonic() < probe_deadline:
        for node, key in sorted(pending):
            try:
                read_on(node, key, model[key], purpose='fault-probe')
                pending.discard((node, key))
            except RuntimeError as exc:
                message = str(exc)
                # P0 修复（探测错误分类）：全值错配 / 已提交键 NOT_FOUND 是
                # 正确性硬门槛违反（错值、非法 NOT_FOUND 即 FAIL），不是可
                # 重试的瞬态错误——立即失败样本，保留证据，不以重试掩盖
                if 'mismatch' in message or 'not found on node' in message:
                    raise
                retry_stats.append(dict(node=node, key=key, error=message[:120]))
                time.sleep(1.0)
    if pending:
        raise RuntimeError('takeover probe incomplete after fault: ' +
                           json.dumps(sorted(pending)[:10]) + '; last errors: ' +
                           json.dumps(retry_stats[-5:]))
    recovery_seconds = time.monotonic() - injected_at

    # —— 阶段 4：存活节点对被接管数据集的完整读写接管验证 ——
    # 4a. victim 全部键从两个存活节点读取（全值经 READ 结果隐式校验）
    for node in survivors:
        for key in victim_keys:
            read_on(node, key, model[key], purpose='fault-takeover-read')
    # 4b. victim 键 UPDATE（写路径走新 GPLM + 存储回写）
    update_version = 2
    for key in victim_keys[:6]:
        value = value_for(key, update_version)
        Driver(survivors[0], 0, shared).execute(
            f'fault-takeover-update-k{key}',
            [dict(op='UPDATE', key=key, value=value, app_version=update_version)],
            track_model=False)
        note(key, value, update_version)
    # 4c. 新键 INSERT（被接管分区的 heap/FSM 页分配走接管路径）
    takeover_insert_base = 500000
    takeover_keys = [takeover_insert_base + i for i in range(12)]
    insert_keys(survivors[0], takeover_keys[:6], purpose='fault-takeover-insert')
    insert_keys(survivors[1], takeover_keys[6:], purpose='fault-takeover-insert')
    # 4d. victim 键 DELETE + 重插（删除页回收与重分配也经过接管路径）
    recycle_keys = victim_keys[-4:]
    for key in recycle_keys:
        Driver(survivors[1], 0, shared).execute(
            f'fault-takeover-delete-k{key}', [dict(op='DELETE', key=key)], track_model=False)
        drop(key)
    for key in recycle_keys[:2]:
        value = value_for(key, 9)
        Driver(survivors[0], 0, shared).execute(
            f'fault-takeover-reinsert-k{key}',
            [dict(op='INSERT', key=key, value=value, app_version=9)], track_model=False)
        note(key, value, 9)
    # 4e. 存活节点自身的键读写（确认既有数据未被恢复过程破坏）
    for node in survivors:
        for key in seeded[node][::6]:
            read_on(node, key, model[key], purpose='fault-survivor-read')

    # —— 阶段 5：最终跨节点全量可见性（两个存活节点视角读全部存活键）——
    for key, expected in sorted(model.items()):
        for node in survivors:
            read_on(node, key, expected, purpose='fault-final-cross')

    ledger_counts, _, _ = ledger_accounting()
    if ledger_counts['UNKNOWN'] or ledger_counts['UNFINISHED']:
        raise RuntimeError('fault contract has unresolved outcomes')
    summary = dict(run_id=RUN_ID, mode=MODE, requests=shared.request_index,
                   nodes=NODES, threads_per_node=THREADS,
                   outcome_counts=ledger_counts,
                   fault=dict(victim='compute_A', signal='SIGKILL',
                              injected_at_ns=injected_record['time_ns'],
                              recovery_probe_seconds=round(recovery_seconds, 2),
                              probe_retries=len(retry_stats),
                              probed_keys=len(all_keys) * len(survivors),
                              injected_record=injected_record),
                   takeover=dict(victim_keys=len(victim_keys),
                                 reads_on_survivors=2 * len(victim_keys),
                                 updates=6, new_inserts=12, delete_reinsert=2),
                   final_model={str(k): dict(app_version=versions[k],
                                             value_sha256=hashlib.sha256(v).hexdigest())
                                for k, v in sorted(model.items())},
                   all_terminal_classes_accounted=True)
    write_artifact(RUN / 'ledger' / 'final-model.json', summary)
    shared.ledger.write('contract_complete', summary=summary, outcome_class='CONFIRMED_COMMITTED')
    return summary, shared


def main():
    if MODE not in ('serial', 'mixed', 'load', 'fault'):
        raise RuntimeError('unsupported driver mode: ' + MODE)
    shared = SharedState()
    try:
        if MODE == 'serial':
            summary = Driver(shared=shared).serial_contract()
        elif MODE == 'fault':
            summary, _ = fault_contract(shared)
        else:
            summary, _ = mixed_contract(shared) if MODE == 'mixed' else load_contract(shared)
        print(json.dumps(summary, sort_keys=True), flush=True)
        return 0
    except Exception as exc:
        shared.ledger.write('contract_failed', error=repr(exc), acceptance=False)
        print(json.dumps({'pass': False, 'error': str(exc), 'run_id': RUN_ID}), flush=True)
        return 1
    finally:
        shared.ledger.close()


if __name__ == '__main__':
    sys.exit(main())
