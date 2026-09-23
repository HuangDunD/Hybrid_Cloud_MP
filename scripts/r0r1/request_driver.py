#!/usr/bin/env python3
import concurrent.futures
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
        # R2b 执行中注入（29.1 任务 1-3）：进度锚点（第 N 个已确认提交后注入）、
        # 在途登记（事件驱动写入台账，禁止 sleep 定时）、victim 在途改走
        # 恢复后重读核定（绝不记 UNKNOWN）、计划内 EXECUTED 响应丢失计数。
        self.anchor_commits = int(os.environ.get('HCM_FAULT_ANCHOR_COMMITS', '0'))
        self.live_attempted = int(os.environ.get('HCM_FAULT_LIVE_ATTEMPTED', '0'))
        self.reply_loss_every = int(os.environ.get('HCM_FAULT_REPLY_LOSS_EVERY', '0'))
        self.victim_node = 0
        self.victim_mode = self.anchor_commits > 0
        self.anchor_armed = False
        self.victim_killed = False
        self.injected_ns = None
        self.confirmed_commits = 0
        self.reply_loss_counter = 0
        self.inflight = {}
        self.terminal_ns = {}
        self.pending_adjudication = {}
        self.inflight_adjudication_set = set()

    def snapshot_inflight_locked(self):
        # 当前仍未终结的在途请求（anchor_armed 时刻快照用）
        return [dict(item, still_in_flight=True) for rid, item in self.inflight.items()
                if item.get('terminal_ns') is None]

    def snapshot_inflight_at_locked(self, wall_ns):
        # kill 时刻（time_ns 墙钟）在途：created_ns <= kill 且（至今未终结 或
        # terminal_ns > kill——kill 之后才拿到服务端终局/核定终局的请求在
        # kill 时刻必然在途；kill 之后才发出的请求（created_ns > kill）排除）。
        items = []
        for rid, item in self.inflight.items():
            if item['created_ns'] > wall_ns:
                continue
            tns = item.get('terminal_ns')
            if tns is None or tns > wall_ns:
                items.append(dict(item, request_id=rid, in_flight_at_kill=True))
        return items

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


class VictimPendingAdjudication(RuntimeError):
    # R2b 执行中注入（29.1 任务 1）：victim 节点已按锚点计划被监督者 SIGKILL，
    # 发往 victim 的在途请求的 STATUS 通道随节点死亡不可达。该请求绝不记
    # UNKNOWN/UNFINISHED 终局：转入 pending_adjudication，待恢复完成后以
    # 「重读写效果」核定真实终局（COMMITTED/ABORTED 二选一），并与存储
    # WAL 终局交叉核对。纯读事务无写效果不可核定，负载设计禁止向 victim
    # 发纯读事务（如实记录的实验边界）。
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
            if (self.shared.victim_mode and self.shared.victim_killed
                    and self.node == self.shared.victim_node):
                # R2b：victim 已按锚点计划死亡，STATUS 通道不可达；绝不轮询到
                # 超时记 UNKNOWN，改走恢复后重读核定（execute 的 except 捕获转移）。
                raise VictimPendingAdjudication(
                    f'status unavailable: victim node {self.node} killed by anchor injection')
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

    def finalize_terminal(self, tx, rid, outcome, terminal):
        # R2b：统一终局记账——在途条目标记终结墙钟（kill 时刻快照依据）；
        # 已确认提交计数驱动锚点（纯进度事件，事件驱动无 sleep 定时）。
        self.record('terminal', tx, rid, response=terminal, outcome_class=outcome)
        self.outcome_counts[outcome] += 1
        trigger_anchor = False
        with self.shared.lock:
            entry = self.shared.inflight.get(rid)
            if entry is not None:
                entry['terminal_ns'] = now_ns()
                entry['outcome'] = outcome
            # 锚点计数语义（29.1）：仅 victim 节点上含写操作事务的
            # CONFIRMED_COMMITTED——precross/探针的只读 COMMIT 与存活节点
            # 的提交都不推进 victim 负载进度（live-smoke-001 实证：读事务
            # 也计数会使锚点在 precross 期间触发，victim 死后对照读永远
            # 无法完成）。
            if (outcome == 'CONFIRMED_COMMITTED' and self.shared.anchor_commits > 0
                    and entry is not None
                    and entry.get('node') == self.shared.victim_node
                    and entry.get('write_ops')):
                self.shared.confirmed_commits += 1
                if (not self.shared.anchor_armed
                        and self.shared.confirmed_commits >= self.shared.anchor_commits):
                    self.shared.anchor_armed = True
                    trigger_anchor = True
        if trigger_anchor:
            self.arm_and_await_injection(tx, rid)

    def arm_and_await_injection(self, trigger_tx, trigger_rid):
        """R2b 锚点注入（29.1 任务 1）：第 N 个已确认提交落账即请求监督者注入
        SIGKILL。触发条件是纯进度事件（提交确认记账），等待的是监督者注入
        回执文件事件；arm 与 kill 两个时刻的在途请求快照均写入台账，kill
        时刻快照 = created_ns <= kill 且（未终结 或 terminal_ns > kill）。"""
        shared = self.shared
        request_path = RUN / 'fault-inject.request'
        if request_path.exists():
            raise RuntimeError('duplicate fault injection request')
        with shared.lock:
            armed_snapshot = shared.snapshot_inflight_locked()
            confirmed = shared.confirmed_commits
        self.record('anchor_armed', trigger_tx, trigger_rid,
                    anchor_commits=shared.anchor_commits, confirmed_commits=confirmed,
                    inflight=armed_snapshot)
        with request_path.open('x') as f:
            json.dump(dict(run_id=RUN_ID, victim=f'compute_{chr(65 + shared.victim_node)}',
                           trigger=dict(request_id=trigger_rid, tx_id=trigger_tx,
                                        confirmed_commits=confirmed),
                           time_ns=now_ns()), f)
            f.flush()
            os.fsync(f.fileno())
        fsync_file(RUN)
        injected_path = RUN / 'fault-injected.json'
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and not injected_path.exists():
            time.sleep(.02)
        if not injected_path.exists():
            raise RuntimeError('supervisor did not execute the anchor injection')
        injected_record = json.loads(injected_path.read_text())
        if (injected_record.get('run_id') != RUN_ID
                or injected_record.get('victim') != f'compute_{chr(65 + shared.victim_node)}'):
            raise RuntimeError('anchor injection record identity mismatch')
        with shared.lock:
            shared.victim_killed = True
            shared.injected_ns = injected_record['time_ns']
            fired_snapshot = shared.snapshot_inflight_at_locked(shared.injected_ns)
            shared.inflight_adjudication_set = {item['request_id'] for item in fired_snapshot}
        self.record('anchor_fired', trigger_tx, trigger_rid,
                    injected_time_ns=injected_record['time_ns'],
                    inflight_at_kill=fired_snapshot,
                    inflight_count=len(fired_snapshot),
                    injected_record=injected_record)

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
        # R2b（29.1 任务 1）：在途登记——锚点注入时刻的快照依据；写效果快照
        # 供 victim 在途请求恢复后重读核定（INSERT 前置不存在、UPDATE/DELETE
        # 前置为负载独占键的已知版本，重读结果无歧义）。
        with self.shared.lock:
            self.shared.inflight[rid] = dict(
                run_id=RUN_ID, request_id=rid, node=self.node,
                generation=self.worker['generation'], tx_id=tx,
                worker=self.worker_id, purpose=purpose,
                decision=decision, fault=fault, created_ns=now_ns(), stage='EXECUTE_SENT',
                write_ops=[dict(op=item['op'], key=item['key'],
                                value_hex=item.get('value_hex'),
                                value_sha256=item.get('value_sha256'),
                                app_version=item.get('app_version'))
                           for item in planned_ops
                           if item['op'] in ('INSERT', 'UPDATE', 'DELETE')])
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
                self.finalize_terminal(tx, rid, 'CONFIRMED_ABORTED', rejected_terminal)
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
                self.validate_response(executed, tx, rid, {'executed'})
                drop_planned = False
                if self.shared.reply_loss_every > 0 and self.node != self.shared.victim_node:
                    # R2b（29.1 任务 2）：计划内丢弃 EXECUTED 响应——驱动先验
                    # 证并留存响应原文（证据不丢），随后表现为"未收到"标记
                    # UNKNOWN，经 STATUS 核定真实结局；已提交不丢、未提交不可
                    # 见；UNKNOWN 绝不记作终局。victim 节点不丢（其 STATUS 通道
                    # 会随锚点注入死亡，核定路径另由重读兜底）。
                    with self.shared.lock:
                        self.shared.reply_loss_counter += 1
                        drop_planned = (self.shared.reply_loss_counter
                                        % self.shared.reply_loss_every == 0)
                if drop_planned:
                    with self.shared.lock:
                        loss_index = self.shared.reply_loss_counter
                    self.record('executed_dropped', tx, rid, response=executed,
                                reply_loss_index=loss_index)
                    self.record('response_lost', tx, rid, stage='executed', planned=True,
                                interim_state='UNKNOWN', decision_to_send=decision)
                    if after_executed is not None:
                        after_executed(executed)
                    self.record('decision', tx, rid, decision=decision, sent_after='executed_dropped')
                    send_json(sock, dict(self.identity(tx, rid), decision=decision))
                    # 不 shutdown/close 原连接：半关闭会让服务端把 EOF 误判为
                    # 客户端断连（与决策送达竞态，live-early4 实测触发
                    # DISCONNECTED abort）。连接保持打开，另开 STATUS 连接
                    # 核定真实结局；原连接由函数尾部统一关闭。
                    terminal = self.status(tx, rid)
                    evidence = RUN / 'control' / f'compute_{chr(65 + self.node)}' / f'txn-{tx}-executed.json'
                    executed = terminal.get('executed') or json.loads(evidence.read_text())
                    self.validate_response(executed, tx, rid, {'executed'})
                    self.record('executed', tx, rid, response=executed,
                                source='post_loss_status_recovery')
                else:
                    self.record('executed', tx, rid, response=executed)
                    if after_executed is not None:
                        after_executed(executed)
                    self.record('decision', tx, rid, decision=decision)
                if disconnect:
                    sock.shutdown(socket.SHUT_WR)
                    sock.close()
                    sock = None
                    terminal = self.status(tx, rid)
                elif drop_planned:
                    pass  # 丢失分支已完成 decision 发送与 STATUS 终局核定
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
            # R2b（29.1 任务 1）：victim 已按锚点死亡且本请求发往 victim——
            # STATUS 通道不可达，绝不记 UNKNOWN，转入挂起核定（恢复后重读）。
            # live-smoke-010 补充：锚点触发前的健康窗口，victim 请求也可能
            # 确定性中止（页分裂推送竞争 → fail-closed 回源拒绝，同 survivor
            # 侧错误串），STATUS 轮询 30s 超时即伪 UNKNOWN（fault-0070 实证）；
            # 该场景同样转挂起（锚点触发后由核定循环统一重读裁定）。
            if (self.shared.victim_mode
                    and self.node == self.shared.victim_node
                    and (self.shared.victim_killed
                         or any(marker in repr(exc) for marker in
                                ('unverified storage fallback',
                                 'remote grant stalled',
                                 'IR lock wait deadline exceeded')))):
                with self.shared.lock:
                    entry = self.shared.inflight.get(rid)
                    if entry is not None:
                        entry['victim_pending'] = True
                        entry['stage'] = 'VICTIM_PENDING_ADJUDICATION'
                        self.shared.pending_adjudication[rid] = entry
                self.record('victim_pending', tx, rid,
                            executed_known=(executed if isinstance(executed, dict) else None),
                            planned_decision=decision,
                            note='victim killed in flight; deferred to post-recovery reread')
                pending_exc = VictimPendingAdjudication(
                    f'{rid}: victim killed while request in flight')
                pending_exc.request_id = rid
                pending_exc.tx_id = tx
                pending_exc.pre_kill = not self.shared.victim_killed
                raise pending_exc from exc
            # R2b（29.1 任务 1）：survivor 上的确定性执行中止（fail-closed
            # 拒绝未验证存储回源 / 授权等待超时——live-smoke-003 实证
            # 'unverified storage fallback ...'）。live-smoke-010 实证锚点
            # 触发前的健康窗口同样成立：页分裂/推送竞争导致推送丢失走
            # 存储兜底被防御拒绝（RequireStorageSource），事务确定性中止。
            # 故错误串匹配不依赖 victim_killed：事务未达决策点必未提交，
            # STATUS 轮询查不到（end_ticket=0）只会产生伪 UNKNOWN。写事务
            # （独占键域）转挂起核定（恢复后重读写效果）；只读事务无效果
            # 可核定，记显式只读中止终局，均不落入 UNKNOWN。
            if (self.shared.victim_mode
                    and self.node != self.shared.victim_node
                    and any(marker in repr(exc) for marker in
                            ('unverified storage fallback', 'remote grant stalled',
                             'IR lock wait deadline exceeded'))):
                entry = None
                with self.shared.lock:
                    entry = self.shared.inflight.get(rid)
                if entry is not None and entry.get('write_ops'):
                    with self.shared.lock:
                        entry['victim_pending'] = True
                        entry['stage'] = 'SURVIVOR_PENDING_ADJUDICATION'
                        entry['source'] = 'survivor_deterministic_abort'
                        self.shared.pending_adjudication[rid] = entry
                    self.record('victim_pending', tx, rid,
                                executed_known=(executed if isinstance(executed, dict) else None),
                                planned_decision=decision,
                                note='survivor deterministic exec abort during recovery '
                                     'window; deferred to post-recovery reread')
                    pending_exc = VictimPendingAdjudication(
                        f'{rid}: survivor deterministic exec abort during recovery window')
                    pending_exc.request_id = rid
                    pending_exc.tx_id = tx
                    raise pending_exc from exc
                if entry is not None:
                    terminal = dict(self.identity(tx, rid), event='terminal',
                                    outcome='CONFIRMED_ABORTED', decision='ABORT',
                                    completed_ops=0, results=[], error='',
                                    confirmation='SURVIVOR_EXEC_ABORT_READ_ONLY',
                                    note='read-only request deterministically aborted '
                                         'during recovery window (no effects)')
                    self.finalize_terminal(tx, rid, 'CONFIRMED_ABORTED', terminal)
                    raise RecoveryRejectedDuringRecovery(
                        f'{rid}: read-only request aborted during recovery window') from exc
            try:
                terminal = self.status(tx, rid)
                if terminal.get('outcome') not in self.outcome_counts:
                    raise RuntimeError('invalid recovered outcome')
            except VictimPendingAdjudication as pending_exc:
                # status 循环内发现 victim 死亡（disconnect/response-loss 路径）：
                # 同样转挂起核定，绝不落入 UNKNOWN。
                with self.shared.lock:
                    entry = self.shared.inflight.get(rid)
                    if entry is not None:
                        entry['victim_pending'] = True
                        entry['stage'] = 'VICTIM_PENDING_ADJUDICATION'
                        self.shared.pending_adjudication[rid] = entry
                self.record('victim_pending', tx, rid,
                            executed_known=(executed if isinstance(executed, dict) else None),
                            planned_decision=decision,
                            note='victim detected during status polling')
                pending_exc.request_id = rid
                pending_exc.tx_id = tx
                raise pending_exc
            except Exception as status_error:
                # R2b（29.1 任务 2 兜底，live-smoke-009/010 实证）：STATUS
                # 核定通道超时不限于恢复窗口——健康窗口的页分裂推送风暴
                # 同样让事务在服务端挂起 30s+（smoke-010 fault-0070：纯
                # timeout 无确定性中止标记）。UNKNOWN 终局会让 worker 线程
                # 死于 'unresolved outcome'（锚点永远凑不齐，run 挂死）。
                # 统一转挂起：写事务恢复后重读写效果；只读事务恢复完成后
                # STATUS 重查（事务表届时定局）。锚点最终未触发则由主流程
                # 'load exhausted' 如实 FAIL。
                if self.shared.victim_mode:
                    entry = None
                    with self.shared.lock:
                        entry = self.shared.inflight.get(rid)
                    if entry is not None and entry.get('write_ops'):
                        with self.shared.lock:
                            entry['victim_pending'] = True
                            entry['stage'] = 'REPLY_LOSS_PENDING_ADJUDICATION'
                            entry['source'] = 'reply_loss'
                            self.shared.pending_adjudication[rid] = entry
                        self.record('victim_pending', tx, rid,
                                    executed_known=(executed if isinstance(executed, dict) else None),
                                    planned_decision=decision,
                                    note='reply/status loss during recovery window; '
                                         'deferred to post-recovery reread')
                        pending_exc = VictimPendingAdjudication(
                            f'{rid}: reply/status loss during recovery window')
                        pending_exc.request_id = rid
                        pending_exc.tx_id = tx
                        pending_exc.pre_kill = not self.shared.victim_killed
                        raise pending_exc from status_error
                    if entry is not None:
                        with self.shared.lock:
                            entry['victim_pending'] = True
                            entry['stage'] = 'REPLY_LOSS_STATUS_RECHECK'
                            entry['source'] = 'reply_loss'
                            self.shared.pending_adjudication[rid] = entry
                        self.record('victim_pending', tx, rid,
                                    executed_known=(executed if isinstance(executed, dict) else None),
                                    planned_decision=decision,
                                    note='reply/status loss on read-only request; '
                                         'deferred to post-recovery STATUS recheck')
                        pending_exc = VictimPendingAdjudication(
                            f'{rid}: read-only status recheck deferred after recovery')
                        pending_exc.request_id = rid
                        pending_exc.tx_id = tx
                        pending_exc.pre_kill = not self.shared.victim_killed
                        raise pending_exc from status_error
                terminal = dict(self.identity(tx, rid), event='terminal', outcome='UNKNOWN',
                                error=repr(status_error), confirmation='UNTRUSTED')
        finally:
            if sock is not None:
                sock.close()
        outcome = terminal['outcome']
        # R2b（live-smoke-013 补充）：服务端在执行中确定性中止（45s 授权
        # 超时等）返回 outcome=UNKNOWN + error 带中止标记的终局响应——走
        # 正常响应路径（recv_line），上方 except 分支的串匹配接不到
        #（fault-0063 实证 'remote grant stalled past deadline (X)'，
        # end_ticket=0 即未达决策点必未提交）。与 except 路径同款核定：
        # 写事务转挂起（恢复后重读）；只读事务无效果可核定，记显式只读
        # 中止终局后上抛供上层以新事务重试。均不落入 UNKNOWN。
        if (self.shared.victim_mode
                and outcome == 'UNKNOWN'
                and isinstance(terminal.get('error'), str)
                and any(marker in terminal['error'] for marker in (
                        'unverified storage fallback', 'remote grant stalled',
                        'IR lock wait deadline exceeded'))):
            if self.node == self.shared.victim_node:
                # victim 写事务同款场景：锚点前服务端确定性中止——转挂起
                #（锚点触发后核定循环统一重读），pre_kill 继续凑锚点。
                with self.shared.lock:
                    entry = self.shared.inflight.get(rid)
                if entry is not None:
                    with self.shared.lock:
                        entry['victim_pending'] = True
                        entry['stage'] = 'VICTIM_PENDING_ADJUDICATION'
                        self.shared.pending_adjudication[rid] = entry
                    self.record('victim_pending', tx, rid, executed_known=executed,
                                note='server-side deterministic abort on victim '
                                     '(terminal UNKNOWN + stall marker)')
                    pending_exc = VictimPendingAdjudication(
                        f'{rid}: server-side deterministic abort on victim')
                    pending_exc.request_id = rid
                    pending_exc.tx_id = tx
                    pending_exc.pre_kill = not self.shared.victim_killed
                    raise pending_exc
            else:
                with self.shared.lock:
                    entry = self.shared.inflight.get(rid)
                if entry is not None and entry.get('write_ops'):
                    with self.shared.lock:
                        entry['victim_pending'] = True
                        entry['stage'] = 'SURVIVOR_PENDING_ADJUDICATION'
                        entry['source'] = 'survivor_deterministic_abort'
                        self.shared.pending_adjudication[rid] = entry
                    self.record('victim_pending', tx, rid, executed_known=executed,
                                note='server-side deterministic abort '
                                     '(terminal UNKNOWN + stall marker)')
                    pending_exc = VictimPendingAdjudication(
                        f'{rid}: server-side deterministic abort')
                    pending_exc.request_id = rid
                    pending_exc.tx_id = tx
                    pending_exc.pre_kill = not self.shared.victim_killed
                    raise pending_exc
                if entry is not None:
                    self.finalize_terminal(tx, rid, 'CONFIRMED_ABORTED',
                        dict(self.identity(tx, rid), event='terminal',
                             outcome='CONFIRMED_ABORTED',
                             confirmation='SURVIVOR_EXEC_ABORT_READ_ONLY'))
                    raise RecoveryRejectedDuringRecovery(
                        f'{rid}: read-only server-side deterministic abort')
        self.finalize_terminal(tx, rid, outcome, terminal)
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


def fault_tree_structure_check(db, artifact, expected_keys):
    """R2b 未提交不可见的结构侧断言（29.1 任务 3）：对恢复后的存储做离线
    tree_stats（不带 --model-range——fault 契约键域不连续、版本不一，键集
    对照由调用方以台账 final model 完成）。硬门槛：idx/heap 双侧均无孤儿键
    （未提交 INSERT 的物化残留会表现为 idx_only_keys/heap_only_keys 非零）、
    无 RID 错配、无重复键、无锁残留，键数与 expected_keys 一致。"""
    binary = Path(os.environ['HCM_TREE_STATS_BIN']).resolve(strict=True)
    command = [str(binary), str(db), 'ycsb_user_table', '--json-only']
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
    if (result['exit_code'] != 0 or result.get('pass') is not True
            or result.get('structure_errors')
            or result.get('idx_only_keys') != 0 or result.get('heap_only_keys') != 0
            or result.get('rid_mismatch') != 0 or result.get('dup_keys_idx') != 0
            or result.get('dup_keys_heap') != 0 or result.get('heap_lock_residual') != 0
            or result.get('order') != 253
            or result.get('index_total_keys') != expected_keys
            or result.get('heap_distinct_keys') != expected_keys
            or not result.get('content_sha256')):
        raise RuntimeError('uncommitted-invisibility tree structure check failed: ' + str(artifact))
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


def fault_live_contract(shared):
    """R2b 执行中注入最小闭环（29.1 任务 1-3）。

    三节点混合负载并发（victim 线程只发独占新键 INSERT 事务保证在途核定
    无歧义；存活节点线程 CRUD+READ 混合并按计划丢弃 EXECUTED 响应）→
    第 N 个已确认提交落账即锚点注入 compute_A SIGKILL（纯进度事件驱动，
    arm/kill 两时刻在途快照均入台账）→ 恢复完成后：kill 时刻全部在途
    请求逐一核定终局（存活节点服务端 STATUS 终局；发往 victim 的挂起
    请求以重读写效果核定，绝不记 UNKNOWN）→ 未提交写键双存活节点显式
    不可见 → 存活节点接管读写 → 最终全量跨节点可见性。
    """
    if NODES != 3:
        raise RuntimeError('fault live contract requires exactly three nodes')
    victim_node = shared.victim_node
    survivors = (1, 2)
    if victim_node != 0 or survivors != (1, 2):
        raise RuntimeError('live contract pins victim=compute_A for now')
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

    def read_missing_on(node, key, purpose='read-missing'):
        terminal = Driver(node, 0, shared).execute(
            f'{purpose}-n{node}-k{key}', [dict(op='READ', key=key)],
            expected_ok=False, expected_error='NOT_FOUND', expect='CONFIRMED_ABORTED',
            track_model=False)
        result = result_for(terminal, 'READ', key)
        if result.get('found'):
            raise RuntimeError(f'{key} unexpectedly visible on node {node} ({purpose})')

    # —— 阶段 1：播种（与受控窗口模式同一数据集）+ kill 前跨节点对照 ——
    seeded = {}
    for node in range(NODES):
        base = 400000 + node * 1000
        keys = [base + i for i in range(30)]
        insert_keys(node, keys, purpose='live-seed')
        seeded[node] = keys
    for node in range(NODES):
        for owner in range(NODES):
            for key in seeded[owner][::6]:
                # 锚点注入可能落在 precross 进行中：恢复窗口内该读事务可能被
                # 服务端 tainted abort（终局已定 ABORTED）或防御拒绝——以新事
                # 务重试直到恢复完成；错值/缺键仍是硬门槛立即失败。
                deadline = time.monotonic() + 600
                while True:
                    try:
                        read_on(node, key, model[key], purpose='live-precross')
                        break
                    except VictimPendingAdjudication as pending_exc:
                        # 防御分支（锚点计数限定 victim 写提交后，正常锚点
                        # 配置下 precross 必先于注入完成；仅锚点 1..3 落在
                        # 播种/precross 的边界形态可达）：发往 victim 的对照
                        # 读随节点死亡永不可能完成，纯读无写效果不可重读核
                        # 定——从挂起核定集合移除，显式记 SKIPPED 终局（不进
                        # 四类终局计数，verify 侧白名单放行），跳过该键；
                        # 该键正确性覆盖转移至阶段 7 全量跨节点可见性。
                        rid = getattr(pending_exc, 'request_id', None)
                        tx = getattr(pending_exc, 'tx_id', None)
                        if rid is None or tx is None:
                            raise
                        with shared.lock:
                            entry = dict(shared.inflight.get(rid) or {})
                            shared.pending_adjudication.pop(rid, None)
                            shared.inflight_adjudication_set.discard(rid)
                            inflight_entry = shared.inflight.get(rid)
                            if inflight_entry is not None:
                                inflight_entry['terminal_ns'] = now_ns()
                                inflight_entry['outcome'] = 'SKIPPED_VICTIM_DEAD'
                        if not entry:
                            raise
                        shared.ledger.write(
                            'terminal', node=node, request_id=rid, tx_id=tx,
                            generation=entry.get('generation'), worker=entry.get('worker'),
                            outcome_class='SKIPPED_VICTIM_DEAD',
                            response=dict(run_id=RUN_ID, request_id=rid, tx_id=tx,
                                          node=node, generation=entry.get('generation'),
                                          event='terminal', outcome='SKIPPED_VICTIM_DEAD',
                                          decision='SKIP', completed_ops=0,
                                          confirmation='VICTIM_PRECROSS_SKIPPED'))
                        break
                    except (RecoveryRejectedDuringRecovery, RuntimeError) as exc:
                        message = str(exc)
                        if 'mismatch' in message or 'not found on node' in message:
                            raise
                        if time.monotonic() > deadline:
                            raise
                        time.sleep(1.0)

    # —— 阶段 2：混合负载 + 锚点注入（进度事件驱动，无 sleep 定时）——
    attempted = shared.live_attempted if shared.live_attempted > 0 else 300
    budget = max(1, attempted // NODES)

    def victim_worker(budget_n, base):
        # victim 线程：仅独占新键 INSERT（每 3 笔一笔双 INSERT）。前置
        # 状态=不存在，恢复后重读核定无歧义；纯读/改写在 victim 上不可
        # 核定（STATUS 通道随节点死亡），负载设计如实回避（实验边界）。
        driver = Driver(victim_node, 0, shared)
        committed_keys = []
        j = 0
        while j < budget_n and not shared.victim_killed:
            j += 1
            k1 = base + j * 2
            ops = [dict(op='INSERT', key=k1, value=value_for(k1, 1), app_version=1)]
            keys = [k1]
            if j % 3 == 0:
                k2 = base + j * 2 + 1
                ops.append(dict(op='INSERT', key=k2, value=value_for(k2, 1), app_version=1))
                keys.append(k2)
            try:
                driver.execute(f'live-n{victim_node}-i{j}', ops, track_model=False)
                # smoke-016：execute 正常返回即 CONFIRMED_COMMITTED，键效果
                # 必在存储——记录以供主线程并入 model（树检查 expected_keys）
                committed_keys.extend(keys)
            except RecoveryRejectedDuringRecovery:
                continue
            except VictimPendingAdjudication:
                break
            except RuntimeError as exc:
                if 'STATUS confirmed' in str(exc):
                    continue
                raise
        return victim_node, j, sorted(committed_keys)

    def survivor_worker(node, budget_n, base):
        # 存活节点线程：CRUD+READ 混合；execute 内按 HCM_FAULT_REPLY_LOSS_EVERY
        # 计划丢弃 EXECUTED 响应（证据留台账、STATUS 核定真实结局）。
        driver = Driver(node, 0, shared)
        committed = {}
        j = 0
        while j < budget_n:
            j += 1
            mode = j % 5
            try:
                if mode in (0, 1) or not committed:
                    key = base + j
                    driver.execute(f'live-n{node}-i{j}',
                                   [dict(op='INSERT', key=key, value=value_for(key, 1), app_version=1)],
                                   track_model=False)
                    committed[key] = 1
                elif mode == 2:
                    key = min(committed)
                    version = committed[key] + 1
                    driver.execute(f'live-n{node}-u{j}',
                                   [dict(op='UPDATE', key=key, value=value_for(key, version),
                                         app_version=version)], track_model=False)
                    committed[key] = version
                elif mode == 3:
                    key = max(committed)
                    driver.execute(f'live-n{node}-d{j}', [dict(op='DELETE', key=key)],
                                   track_model=False)
                    del committed[key]
                else:
                    key = min(committed)
                    driver.execute(f'live-n{node}-r{j}', [dict(op='READ', key=key)],
                                   track_model=False)
            except RecoveryRejectedDuringRecovery:
                continue
            except VictimPendingAdjudication as exc:
                # survivor 自身事务在恢复窗口内确定性中止（execute 已登记
                # 挂起核定，阶段 4 重读写效果）——预算继续；来自 victim 的
                # 挂起核定绝不允许出现在 survivor 线程（防御断言）。
                rid = getattr(exc, 'request_id', None)
                entry = None
                if rid is not None:
                    with shared.lock:
                        entry = shared.pending_adjudication.get(rid)
                if entry is not None and entry.get('source') in (
                        'survivor_deterministic_abort', 'reply_loss'):
                    continue
                raise RuntimeError(f'survivor node {node} must not enter victim adjudication') from exc
            except RuntimeError as exc:
                if 'STATUS confirmed' in str(exc):
                    continue
                raise
        # smoke-016：返回键→最新版本映射（DELETE 已移除），供主线程并入
        # model（树检查 expected_keys = len(model) + 预装载）
        return node, j, sorted(committed.items())

    bases = {node: 600000 + node * 100000 for node in range(NODES)}
    with concurrent.futures.ThreadPoolExecutor(max_workers=NODES) as pool:
        futures = [pool.submit(victim_worker, budget, bases[victim_node])]
        futures += [pool.submit(survivor_worker, node, budget, bases[node])
                    for node in survivors]
        worker_results = [f.result() for f in futures]
    # smoke-016 修复：worker 的已提交写效果并入 model。此前 worker 均
    # track_model=False 且不回写共享 model，树检查 expected_keys 少算
    # worker 存活键（victim 域/存活域 INSERT/UPDATE 存活集），导致
    # index_total_keys(len(model)+preseeded) != 实际树键数而 FAIL——
    # 权威重放（verify_storage_model.expected_from_ledger）与树一致，
    # storage 并无数据丢失，是 driver 内存 model 缺口。
    # victim 挂起项（break 时在途）不在此并入——由阶段 4 核定循环
    # REREAD 定局后逐键并入（COMMITTED→note）。
    for rnode, _j, committed_payload in worker_results:
        if rnode == victim_node:
            for k in committed_payload:
                note(k, value_for(k, 1), 1)
        else:
            for k, ver in committed_payload:
                note(k, value_for(k, ver), ver)
    if not shared.victim_killed:
        raise RuntimeError('load exhausted before anchor injection; '
                           'increase HCM_FAULT_LIVE_ATTEMPTED')
    injected_at = time.monotonic()
    with shared.lock:
        inflight_at_kill = list(shared.inflight_adjudication_set)

    # —— 阶段 3：恢复完成探测（全键双存活节点可读且全值正确）——
    all_keys = sorted(model)
    probe_deadline = time.monotonic() + int(os.environ.get('HCM_RECOVERY_PROBE_SECONDS', '600'))
    retry_stats = []
    pending_probes = {(node, key) for node in survivors for key in all_keys}
    while pending_probes and time.monotonic() < probe_deadline:
        for node, key in sorted(pending_probes):
            try:
                read_on(node, key, model[key], purpose='live-probe')
                pending_probes.discard((node, key))
            except RuntimeError as exc:
                message = str(exc)
                if 'mismatch' in message or 'not found on node' in message:
                    raise
                retry_stats.append(dict(node=node, key=key, error=message[:120]))
                time.sleep(1.0)
    if pending_probes:
        raise RuntimeError('takeover probe incomplete after live fault: ' +
                           json.dumps(sorted(pending_probes)[:10]) + '; last errors: ' +
                           json.dumps(retry_stats[-5:]))
    recovery_seconds = time.monotonic() - injected_at

    # —— 阶段 4：victim 在途请求核定（重读写效果；绝不记 UNKNOWN）——
    # survivor 恢复窗口内确定性中止的写事务（source='survivor_deterministic_abort'）
    # 同样以重读写效果核定（键域独占无歧义）；其余非 victim 请求进入核定
    # 视为协议违例。
    adjudications = []
    for rid in sorted(shared.pending_adjudication):
        entry = shared.pending_adjudication[rid]
        if (entry.get('node') != victim_node
                and entry.get('source') not in ('survivor_deterministic_abort',
                                                'reply_loss')):
            raise RuntimeError(f'{rid}: non-victim request entered adjudication')
        if entry.get('stage') == 'REPLY_LOSS_STATUS_RECHECK':
            # A1（29.1 任务 2）纯读兜底：无写效果可重读。本阶段在 takeover
            # probe 之后（恢复已完成），服务端事务表已被恢复流程定局，
            # STATUS 重查必得真实终局；仍超时即核定失败 FAIL（绝不记 UNKNOWN）。
            driver = Driver(entry['node'], 0, shared)
            terminal_response = driver.status(entry['tx_id'], rid)
            verdict = terminal_response.get('outcome')
            if verdict not in ('CONFIRMED_COMMITTED', 'CONFIRMED_ABORTED'):
                raise RuntimeError(f'{rid}: status recheck returned {verdict!r}')
            shared.ledger.write('terminal', request_id=rid, node=entry['node'],
                                generation=entry['generation'], tx_id=entry['tx_id'],
                                response=terminal_response, outcome_class=verdict,
                                adjudication='POST_RECOVERY_STATUS_RECHECK')
            with shared.lock:
                inflight_entry = shared.inflight.get(rid)
                if inflight_entry is not None:
                    inflight_entry['terminal_ns'] = now_ns()
                    inflight_entry['outcome'] = verdict
                shared.terminal_ns[rid] = now_ns()
            adjudications.append(dict(request_id=rid, tx_id=entry['tx_id'],
                                      purpose=entry.get('purpose'), verdict=verdict,
                                      write_checks=[]))
            continue
        checks, verdicts = [], set()
        for op in entry['write_ops']:
            key = op['key']
            expected_value = bytes.fromhex(op['value_hex'])
            try:
                read_on(survivors[0], key, expected_value, purpose='live-adjudicate')
                checks.append(dict(op=op['op'], key=key, observed='NEW_VALUE_VISIBLE',
                                   value_sha256=op.get('value_sha256')))
                verdicts.add('CONFIRMED_COMMITTED')
            except RuntimeError as exc:
                if 'not found on node' in str(exc):
                    checks.append(dict(op=op['op'], key=key, observed='NOT_FOUND'))
                    verdicts.add('CONFIRMED_ABORTED')
                else:
                    raise
        if len(verdicts) != 1:
            raise RuntimeError(f'{rid}: inconsistent write effects after recovery: {checks}')
        verdict = verdicts.pop()
        if verdict == 'CONFIRMED_COMMITTED':
            # smoke-016：重读可见即已提交——写效果并入 model（victim 挂起
            # INSERT / survivor 确定性中止重试后提交的键，树检查需计入）
            for op in entry['write_ops']:
                if op['op'] in ('INSERT', 'UPDATE'):
                    note(op['key'], bytes.fromhex(op['value_hex']),
                         op.get('app_version', 1))
            for op in entry['write_ops']:
                if op['op'] == 'DELETE':
                    drop(op['key'])
        if verdict == 'CONFIRMED_ABORTED':
            # 未提交不可见（任务 3）：对第二个存活节点显式断言不可见（防删除复活）
            for op in entry['write_ops']:
                read_missing_on(survivors[1], op['key'], purpose='live-abort-invisible')
        terminal_response = dict(run_id=RUN_ID, request_id=rid, node=entry['node'],
                                 generation=entry['generation'], tx_id=entry['tx_id'],
                                 event='terminal', outcome=verdict,
                                 decision='COMMIT' if verdict == 'CONFIRMED_COMMITTED' else 'ADJUDICATED_ABORT',
                                 confirmation='REREAD_AFTER_RECOVERY',
                                 completed_ops=len(entry['write_ops'])
                                 if verdict == 'CONFIRMED_COMMITTED' else 0,
                                 executed_known=entry.get('executed_known'),
                                 write_checks=checks)
        shared.ledger.write('terminal', request_id=rid, node=entry['node'],
                            generation=entry['generation'], tx_id=entry['tx_id'],
                            response=terminal_response, outcome_class=verdict,
                            adjudication='REREAD_AFTER_RECOVERY')
        with shared.lock:
            inflight_entry = shared.inflight.get(rid)
            if inflight_entry is not None:
                inflight_entry['terminal_ns'] = now_ns()
                inflight_entry['outcome'] = verdict
            shared.terminal_ns[rid] = now_ns()
        adjudications.append(dict(request_id=rid, tx_id=entry['tx_id'],
                                  purpose=entry.get('purpose'), verdict=verdict,
                                  write_checks=checks))

    # kill 时刻在途请求终局完备性核对（任务 1 硬门槛：任何 UNKNOWN/缺终局即 FAIL）
    terminal_outcomes = {}
    with LEDGER_PATH.open() as f:
        for line in f:
            rec = json.loads(line)
            if rec.get('run_id') == RUN_ID and rec.get('event') == 'terminal':
                terminal_outcomes[rec['request_id']] = rec.get('outcome_class')
    unresolved_inflight = []
    for rid in inflight_at_kill:
        outcome = terminal_outcomes.get(rid)
        # SKIPPED_VICTIM_DEAD：锚点 1..3 边界形态下 precross 纯读被显式
        # 跳过（见 precross 重试循环防御分支），属可解释非终局核定路径
        if outcome not in ('CONFIRMED_COMMITTED', 'CONFIRMED_ABORTED',
                           'SKIPPED_VICTIM_DEAD'):
            unresolved_inflight.append((rid, outcome))
    if unresolved_inflight:
        raise RuntimeError('in-flight requests without final adjudication: ' +
                           json.dumps(unresolved_inflight[:20]))
    write_artifact(RUN / 'inflight-adjudication.json', dict(
        anchor_commits=shared.anchor_commits,
        injected_time_ns=shared.injected_ns,
        inflight_at_kill=inflight_at_kill,
        adjudicated=[a for a in adjudications],
        reply_loss_count=shared.reply_loss_counter,
        unresolved=[]))

    # 未提交不可见专项断言（任务 3）：kill 时刻在途（created_ns<=kill 且
    # terminal_ns>kill）且终局 ABORTED 的全部写键——即"故障瞬间未提交且最终
    # 未提交"（arm→kill 间新发起的在途同样被覆盖；arm 时刻在途但 kill 前已
    # 提交者不属于未提交，由其终局 COMMITTED 与阶段 4/7 新值可见断言覆盖），
    # 双存活节点重读均不可见（victim 挂起项已在阶段 4 断言；此处覆盖存活
    # 节点服务端核定的 tainted abort）。
    aborted_inflight_keys = []
    with shared.lock:
        entries = [dict(item) for item in shared.inflight.values()]
    for item in entries:
        if item.get('created_ns', 0) > (shared.injected_ns or 0):
            continue
        tns = item.get('terminal_ns')
        if tns is None or tns <= (shared.injected_ns or 0):
            continue
        if item.get('outcome') != 'CONFIRMED_ABORTED':
            continue
        for op in item['write_ops']:
            if op['op'] == 'INSERT':
                aborted_inflight_keys.append(op['key'])
    for key in sorted(set(aborted_inflight_keys)):
        # 只断言从未提交成功的独占新键（victim 挂起项 + 存活线程 kill 窗口在途
        # INSERT）；UPDATE/DELETE 键的前置状态属于线程内版本链，已由各线程
        # 后续提交与最终全量可见性覆盖。
        if key >= bases[0]:
            for node in survivors:
                read_missing_on(node, key, purpose='live-uncommitted-invisible')

    # —— 阶段 5：存活节点接管读写（与受控窗口模式同型）——
    victim_keys = seeded[victim_node]
    for node in survivors:
        for key in victim_keys:
            read_on(node, key, model[key], purpose='live-takeover-read')
    update_version = 2
    for key in victim_keys[:6]:
        value = value_for(key, update_version)
        Driver(survivors[0], 0, shared).execute(
            f'live-takeover-update-k{key}',
            [dict(op='UPDATE', key=key, value=value, app_version=update_version)],
            track_model=False)
        note(key, value, update_version)
    takeover_insert_base = 510000
    takeover_keys = [takeover_insert_base + i for i in range(12)]
    insert_keys(survivors[0], takeover_keys[:6], purpose='live-takeover-insert')
    for key in takeover_keys[:6]:
        note(key, value_for(key, 1), 1)
    for key in victim_keys[6:12]:
        Driver(survivors[1], 0, shared).execute(
            f'live-takeover-delete-k{key}', [dict(op='DELETE', key=key)],
            track_model=False)
        drop(key)
    for node in survivors:
        for key in victim_keys:
            if key in model:
                read_on(node, key, model[key], purpose='live-takeover-cross')

    # —— 阶段 6：victim 负载已提交独占键的接管可见性已在阶段 4 核定时
    # 逐一全值验证（COMMITTED 判定即"新值在存活节点可见且全值正确"）——

    # —— 阶段 7：最终全量跨节点可见性 ——
    for node in survivors:
        for key in sorted(model):
            read_on(node, key, model[key], purpose='live-final-cross')

    # —— 阶段 8：未提交不可见的结构侧断言（29.1 任务 3 兜底）——
    # NOT_FOUND 探针（上方）证明"读路径不可见"；此处冻结存储物理写
    # （checkpoint-0，本 run 首个存储检查点）后复制三件套，离线 tree_stats
    # 断言 idx/heap 双侧无孤儿键（idx_only_keys/heap_only_keys=0）、无 RID
    # 错配/重复键/锁残留，键数与台账 final model 一致——证明"物理结构无
    # 未提交残留"。检查点必然释放（finally），不影响后续健康关闭。
    control_request('storage', 'checkpoint-0')
    fault_tree_cut = wait_control_state('storage', 'checkpoint-0')
    try:
        if (fault_tree_cut.get('checkpoint') != 0
                or fault_tree_cut.get('active_undo_transactions') != 0
                or fault_tree_cut.get('wal_tail_inclusive') != fault_tree_cut.get('replay_inclusive')
                or any(fault_tree_cut.get(field) is not True for field in
                       ('wal_checkpoint_fdatasync', 'database_checkpoint_fdatasync',
                        'physical_writes_frozen'))):
            raise RuntimeError('fault tree checkpoint cut is not drained/durable/frozen')
        tree_artifacts = RUN / 'ledger' / 'fault-tree-checkpoint'
        tree_artifacts.mkdir()
        fsync_file(tree_artifacts.parent)
        write_artifact(tree_artifacts / 'storage-cut.json', fault_tree_cut)
        storage_dir = preserve_storage_checkpoint(tree_artifacts / 'storage')
        # 存储预装载键补偿（live-smoke-013 起）：num_record>0 时存储按
        # ycsb 预装载 num_record 个键（键域与驱动事务键域 400000+/600000+
        # 不重叠，live-smoke-008 实证 3144 = 预装载 3000 + 台账 model 144）。
        # tree_stats 统计整树（含预装载），期望键数 = len(model) + 预装载。
        # 空表（num_record=0）则退化为原语义。预装载键也会走正常写路径进
        # WAL，verify_storage_model 的 WAL 重放模型不受此补偿影响。
        preseeded = int(os.environ.get('HCM_FAULT_PRESEEDED_RECORDS', '0') or 0)
        tree_structure = fault_tree_structure_check(
            storage_dir, tree_artifacts / 'tree-stats.json', len(model) + preseeded)
    finally:
        control_request('storage', 'checkpoint-0.release')
        released = wait_control_state('storage', 'checkpoint-0-released')
        write_artifact(tree_artifacts / 'storage-released.json', released)

    aborted_inflight = sum(1 for rid in inflight_at_kill
                           if terminal_outcomes.get(rid) == 'CONFIRMED_ABORTED')
    committed_inflight = len(inflight_at_kill) - aborted_inflight
    # R2b 收尾（smoke-019 实证）：fault_live 曾是唯一漏写 final-model.json
    # 的契约（serial/mixed/load/fault 均有），cluster.py 收尾在 driver rc==0
    # 后读该文件做终局核算（run_id / all_terminal_classes_accounted /
    # outcome_counts 的 UNKNOWN/UNFINISHED==0）。ledger_accounting() 自带
    # planned==terminal 与 UNKNOWN/UNFINISHED 强校验——任一不满足 driver
    # 非零退出，绝不把未核定终局写进 final-model。
    ledger_counts, planned_mix, _ = ledger_accounting()
    summary = dict(
        run_id=RUN_ID, mode=MODE, requests=shared.request_index,
        outcome_counts=ledger_counts, operation_mix=planned_mix,
        contract='fault_live',
        anchor_commits=shared.anchor_commits,
        attempted_budget_per_worker=budget,
        confirmed_commits=shared.confirmed_commits,
        injected_time_ns=shared.injected_ns,
        inflight_at_kill=len(inflight_at_kill),
        inflight_committed=committed_inflight,
        inflight_aborted=aborted_inflight,
        victim_pending_adjudicated=len(adjudications),
        victim_pending_committed=sum(1 for a in adjudications
                                     if a['verdict'] == 'CONFIRMED_COMMITTED'),
        victim_pending_aborted=sum(1 for a in adjudications
                                   if a['verdict'] == 'CONFIRMED_ABORTED'),
        reply_loss_planned=shared.reply_loss_every,
        reply_loss_counter=shared.reply_loss_counter,
        uncommitted_invisible_keys=sorted(set(aborted_inflight_keys)),
        tree_structure=dict(
            idx_only_keys=tree_structure['idx_only_keys'],
            heap_only_keys=tree_structure['heap_only_keys'],
            rid_mismatch=tree_structure['rid_mismatch'],
            index_total_keys=tree_structure['index_total_keys'],
            final_model_keys=len(model),
            content_sha256=tree_structure['content_sha256'],
        ),
        recovery_seconds=round(recovery_seconds, 3),
        worker_results=worker_results,
        final_model={str(k): dict(app_version=versions[k],
                                  value_sha256=hashlib.sha256(v).hexdigest())
                     for k, v in sorted(model.items())},
        all_terminal_classes_accounted=True,
    )
    write_artifact(RUN / 'ledger' / 'final-model.json', summary)
    shared.ledger.write('contract_complete', summary=summary,
                        outcome_class='CONFIRMED_COMMITTED')
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
    if shared.victim_mode:
        return fault_live_contract(shared)
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
