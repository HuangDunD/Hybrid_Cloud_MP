#!/usr/bin/env python3
import argparse
from array import array
import hashlib
import json
import os
from pathlib import Path
import struct


def read_exact(f, offset, size):
    f.seek(offset)
    data = f.read(size)
    if len(data) != size:
        raise ValueError(f'short read at {offset}: {len(data)}/{size}')
    return data


def bitmap_get(data, slot):
    return bool(data[slot // 8] & (1 << (7 - slot % 8)))


def bitmap_count(data, slots):
    return sum(bitmap_get(data, i) for i in range(slots))


def load_value_for(key, version):
    if key < 0 or version <= 0:
        raise ValueError('invalid deterministic value identity')
    out = bytearray(struct.pack('<I', 125))
    for field in range(10):
        prefix = f'L{version:06d}|K{key:09d}|F{field}|'.encode()
        fill = (key * 131 + field * 17 + version) % 251 + 1
        out.extend((prefix + bytes([fill]) * 100)[:100])
    return bytes(out)


class RangeModel:
    def __init__(self, begin, count, version):
        if begin < 0 or count <= 0 or version <= 0:
            raise ValueError('invalid model range')
        self.begin, self.end, self.version = begin, begin + count, version
        self.seen = bytearray(count)

    def __len__(self):
        return self.end - self.begin

    def __contains__(self, key):
        return self.begin <= key < self.end

    def __getitem__(self, key):
        if key not in self:
            raise KeyError(key)
        return load_value_for(key, self.version)

    def insert(self, key, version):
        if key not in self or version != self.version or self.seen[key - self.begin]:
            raise ValueError('range ledger duplicate/key/version mismatch')
        self.seen[key - self.begin] = 1


def check_identity(record, plan):
    if any(record.get(k) != plan.get(k) for k in
           ('run_id', 'request_id', 'node', 'generation', 'tx_id')):
        raise ValueError('ledger response/transaction identity mismatch')


def expected_from_ledger(path, run_id, range_model=None):
    pending, terminal, seen_requests, seen_tx = {}, {}, set(), set()
    counts = {'CONFIRMED_COMMITTED': 0, 'CONFIRMED_ABORTED': 0, 'UNKNOWN': 0, 'UNFINISHED': 0,
              'SKIPPED_VICTIM_DEAD': 0}
    model, versions = range_model if range_model is not None else {}, {}
    with path.open() as f:
        for line in f:
            rec = json.loads(line)
            if rec.get('run_id') != run_id:
                raise ValueError('ledger run_id mismatch')
            event, rid = rec.get('event'), rec.get('request_id')
            if event == 'planned':
                tx, node = rec['tx_id'], rec['node']
                if rid in seen_requests or tx in seen_tx or not rid or not rec.get('generation'):
                    raise ValueError('duplicate request/transaction or absent generation')
                if tx >> 48 != node + 1 or tx & ((1 << 48) - 1) == 0:
                    raise ValueError('transaction node prefix mismatch')
                seen_requests.add(rid)
                seen_tx.add(tx)
                pending[rid] = dict(plan=rec, stages={})
                if not rec.get('operations') or rec.get('outcome_class') != 'UNFINISHED':
                    raise ValueError('invalid planned event')
                continue
            if event not in ('accepted', 'executed', 'decision', 'terminal', 'status'):
                continue
            if rid not in pending:
                raise ValueError('event without pending request or duplicate terminal')
            state = pending[rid]
            plan, stages = state['plan'], state['stages']
            check_identity(rec, plan)
            if event == 'status':
                check_identity(rec['response'], plan)
                continue
            if event in stages:
                raise ValueError('duplicate ledger stage')
            if event in ('accepted', 'executed', 'terminal'):
                check_identity(rec['response'], plan)
                if rec['response'].get('event') != event:
                    if event == 'accepted' and rec['response'].get('event') == 'rejected':
                        # R2 fault 契约（28.2/29.2）：恢复窗口内服务端防御中止——accepted 已入账但响应为
                        # rejected，合法短路，终局 CONFIRMED_ABORTED，须有 RECOVERY_IN_PROGRESS 证据。
                        if rec['response'].get('error') != 'RECOVERY_IN_PROGRESS':
                            raise ValueError('rejected accept lacks RECOVERY_IN_PROGRESS evidence')
                        stages['rejected_short_circuit'] = True
                    elif (event == 'executed' and rec['response'].get('event') == 'terminal'
                          and rec['response'].get('decision') == 'DISCONNECTED'):
                        # R2 fault 契约（28.4/29.2）：disconnect_during_execution——客户端断连后经 STATUS
                        # 核定，executed 事件承载 terminal 响应且内嵌 executed 证据（ok 可为 false 的窗口
                        # 瞬态失败），无独立 decision 事件，终局 CONFIRMED_ABORTED。
                        inner = rec['response'].get('executed')
                        if not isinstance(inner, dict) or inner.get('event') != 'executed':
                            raise ValueError('disconnect executed evidence missing embedded executed record')
                        check_identity(inner, plan)
                        stages['disconnect_short_circuit'] = True
                    else:
                        raise ValueError('response event mismatch')
            if event == 'executed' and 'accepted' not in stages:
                raise ValueError('execution lacks accepted evidence')
            stages[event] = rec
            if event != 'terminal':
                continue
            outcome = rec.get('outcome_class')
            if outcome not in counts or rec['response'].get('outcome') != outcome:
                raise ValueError('invalid/inconsistent outcome class')
            counts[outcome] += 1
            if outcome in ('UNKNOWN', 'UNFINISHED'):
                raise ValueError('unresolved ledger outcome; never treated as abort')
            if stages.get('rejected_short_circuit'):
                # 防御中止短路：无 executed/decision 阶段，completed_ops=0，计划决策被服务端 REJECTED
                # 覆盖（uncertain 提交被拒，不允许记 UNKNOWN）。
                resp = rec['response']
                if outcome != 'CONFIRMED_ABORTED':
                    raise ValueError('rejected short-circuit must end CONFIRMED_ABORTED')
                if (resp.get('decision') != 'REJECTED' or resp.get('confirmation') != 'SERVER_REJECTED'
                        or resp.get('completed_ops') != 0):
                    raise ValueError('rejected short-circuit terminal lacks REJECTED/SERVER_REJECTED/zero-ops evidence')
                terminal[rid] = {k: rec[k] for k in
                                 ('run_id', 'request_id', 'node', 'generation', 'tx_id', 'outcome_class')}
                terminal[rid]['confirmation'] = 'SERVER_REJECTED'
                del pending[rid]
                continue
            if stages.get('disconnect_short_circuit'):
                # disconnect 短路：无独立 decision 事件；executed 证据取自内嵌记录；窗口瞬态失败
                # （ok=false 覆盖 expected_ok）合法，但 terminal 必须与内嵌证据完全一致且终局 ABORTED。
                resp = rec['response']
                executed = stages['executed']['response']['executed']
                if outcome != 'CONFIRMED_ABORTED' or resp.get('decision') != 'DISCONNECTED':
                    raise ValueError('disconnect short-circuit must end DISCONNECTED CONFIRMED_ABORTED')
                if not isinstance(executed.get('ok'), bool) or 'error' not in executed:
                    raise ValueError('disconnect executed lacks explicit ok/error')
                if resp.get('error', '') != executed['error'] or resp.get('completed_ops') != executed.get('completed_ops'):
                    raise ValueError('disconnect terminal contradicts embedded executed evidence')
                if [(item.get('op'), item.get('key')) for item in executed.get('results', [])] != [
                        (op['op'], op['key']) for op in plan['operations']]:
                    raise ValueError('disconnect execution results do not match planned operation sequence')
                terminal[rid] = {k: rec[k] for k in
                                 ('run_id', 'request_id', 'node', 'generation', 'tx_id', 'outcome_class')}
                terminal[rid]['confirmation'] = 'SERVER_DISCONNECT_ABORT'
                del pending[rid]
                continue
            if rec['response'].get('confirmation') == 'REREAD_AFTER_RECOVERY':
                # R2b（29.1 任务 1）：victim 锚点注入时在途、STATUS 通道随节点死亡不可达，
                # 恢复完成后以重读写效果核定的终局（绝不记 UNKNOWN）。executed/decision
                # 阶段可有可无（由 kill 时刻所处协议阶段决定）。
                resp = rec['response']
                if 'accepted' not in stages:
                    raise ValueError('reread adjudication lacks accepted evidence')
                if outcome == 'CONFIRMED_COMMITTED':
                    if resp.get('decision') != 'COMMIT' or resp.get('completed_ops') != len(plan['operations']):
                        raise ValueError('reread commit lacks COMMIT decision/full-op visibility')
                    executed = stages.get('executed')
                    if executed is not None and executed['response'].get('ok') is False:
                        raise ValueError('reread commit contradicts failed execution evidence')
                    for op in plan['operations']:
                        kind = op['op']
                        if kind not in ('READ', 'DELETE', 'INSERT', 'UPDATE'):
                            raise ValueError('unknown ledger operation')
                        if kind == 'DELETE':
                            if op['key'] not in model:
                                raise ValueError('committed delete of absent model key')
                            del model[op['key']]
                            del versions[op['key']]
                        elif kind in ('INSERT', 'UPDATE'):
                            value = (bytes.fromhex(op['value_hex']) if 'value_hex' in op else None)
                            if value is None or len(value) != 1004 or hashlib.sha256(value).hexdigest() != op['value_sha256']:
                                raise ValueError('reread ledger full value/hash mismatch')
                            model[op['key']] = value
                            versions[op['key']] = op['app_version']
                elif outcome == 'CONFIRMED_ABORTED':
                    if resp.get('decision') != 'ADJUDICATED_ABORT' or resp.get('completed_ops') != 0:
                        raise ValueError('reread abort lacks ADJUDICATED_ABORT/zero-ops evidence')
                else:
                    raise ValueError('reread adjudication must end COMMITTED/ABORTED')
                terminal[rid] = {k: rec[k] for k in
                                 ('run_id', 'request_id', 'node', 'generation', 'tx_id', 'outcome_class')}
                terminal[rid]['confirmation'] = 'REREAD_AFTER_RECOVERY'
                del pending[rid]
                continue
            if rec['response'].get('confirmation') == 'VICTIM_PRECROSS_SKIPPED':
                # R2b 边界形态（锚点落在播种/precross 窗口）：发往 victim 的
                # precross 纯对照读随节点死亡无法完成，也无写效果可重读核
                # 定——显式 SKIPPED 终局。合法性：仅纯 READ、有 victim_pending
                # 证据、零完成操作；键不可见性由全量可见性断言另行覆盖。
                resp = rec['response']
                if outcome != 'SKIPPED_VICTIM_DEAD' or resp.get('decision') != 'SKIP':
                    raise ValueError('precross skip terminal lacks SKIP evidence')
                if 'victim_pending' not in stages:
                    raise ValueError('precross skip without victim_pending evidence')
                if any(op['op'] != 'READ' for op in plan['operations']):
                    raise ValueError('precross skip applies to pure reads only')
                if resp.get('completed_ops') != 0:
                    raise ValueError('precross skip must have zero completed ops')
                terminal[rid] = {k: rec[k] for k in
                                 ('run_id', 'request_id', 'node', 'generation', 'tx_id', 'outcome_class')}
                terminal[rid]['confirmation'] = 'VICTIM_PRECROSS_SKIPPED'
                del pending[rid]
                continue
            if not all(stage in stages for stage in ('accepted', 'executed', 'decision')):
                raise ValueError('terminal lacks durable accepted/executed/decision evidence')
            executed = stages['executed']['response']
            decision = stages['decision']['decision']
            if rec['response'].get('decision') != decision or plan['decision'] != decision:
                raise ValueError('planned/decision/terminal mismatch')
            if not isinstance(executed.get('ok'), bool) or 'error' not in executed:
                raise ValueError('executed lacks explicit ok/error')
            if plan.get('expected_ok') is not None and executed['ok'] is not plan['expected_ok']:
                raise ValueError('unexpected execution success/failure')
            errors = plan.get('expected_error', '')
            if executed['error'] not in ({errors} if isinstance(errors, str) else set(errors)):
                raise ValueError('unexpected execution error code')
            if rec['response'].get('results') != executed.get('results'):
                raise ValueError('executed/terminal results mismatch')
            if rec['response'].get('error', '') != executed['error']:
                raise ValueError('terminal overwrote execution error')
            if [(item.get('op'), item.get('key')) for item in executed.get('results', [])] != [
                    (op['op'], op['key']) for op in plan['operations']]:
                raise ValueError('execution results do not match planned operation sequence')
            if plan.get('fault') in ('cancel_during_execution', 'disconnect_during_execution'):
                if executed.get('completed_ops') != 1 or rec['response'].get('completed_ops') != 1:
                    raise ValueError('execution cancellation did not stop after first operation')
            if outcome == 'CONFIRMED_COMMITTED':
                if not executed['ok'] or decision != 'COMMIT':
                    raise ValueError('commit contradicts execution/decision')
                for op in plan['operations']:
                    key, kind = op['key'], op['op']
                    if kind not in ('READ', 'DELETE', 'INSERT', 'UPDATE'):
                        raise ValueError('unknown ledger operation')
                    if range_model is not None and kind != 'INSERT':
                        raise ValueError('range model requires committed inserts only')
                    if kind == 'DELETE':
                        if key not in model:
                            raise ValueError('committed delete of absent model key')
                        del model[key]
                        del versions[key]
                    elif kind in ('INSERT', 'UPDATE'):
                        value = (bytes.fromhex(op['value_hex']) if 'value_hex' in op else
                                 load_value_for(key, op['app_version']) if
                                 op.get('value_model') == 'load_value_for(key,app_version)' else None)
                        if value is None or len(value) != 1004 or hashlib.sha256(value).hexdigest() != op['value_sha256']:
                            raise ValueError('ledger full value/hash/model mismatch')
                        if range_model is not None:
                            if value != range_model[key]:
                                raise ValueError('range model full value mismatch')
                            range_model.insert(key, op['app_version'])
                        else:
                            if (kind == 'INSERT') == (key in model):
                                raise ValueError('committed insert/update contradicts model membership')
                            model[key], versions[key] = value, op['app_version']
            terminal[rid] = {k: rec[k] for k in
                             ('run_id', 'request_id', 'node', 'generation', 'tx_id', 'outcome_class')}
            del pending[rid]
    if pending or not terminal or len(seen_requests) != len(terminal):
        raise ValueError('planned/terminal request set mismatch')
    if range_model is not None and not all(range_model.seen):
        raise ValueError('ledger range coverage incomplete')
    return model, versions, counts, terminal


def parse_index(path, range_model=None):
    with path.open('rb') as f:
        size = f.seek(0, 2)
        if size % 4096 or size < 3 * 4096:
            raise ValueError('partial/short index file')
        pages = size // 4096
        root, first, last = struct.unpack('<iii', read_exact(f, 4096, 12))
        if any(p < 2 or p >= pages for p in (root, first, last)):
            raise ValueError('invalid BLink header')
        sentinel = struct.unpack('<iiiiQBBBB4x', read_exact(f, 0, 32))
        if sentinel[:4] != (last, first, -1, 0) or sentinel[5:8] != (1, 0, 0):
            raise ValueError('invalid leaf sentinel endpoints/flags')
        result = {} if range_model is None else array('q', [-1]) * len(range_model)
        visited = set()
        cur, previous, previous_key, previous_high = first, 0, None, None
        count = 0
        while cur != 0:
            if cur < 2 or cur >= pages or cur in visited:
                raise ValueError('leaf link out of bounds/cycle')
            visited.add(cur)
            page = read_exact(f, cur * 4096, 4096)
            prev_leaf, next_leaf, right, nkey, high, is_leaf, has_high, is_root, _ = struct.unpack('<iiiiQBBBB4x', page[:32])
            if is_leaf != 1 or not 0 <= nkey <= 253 or prev_leaf != previous:
                raise ValueError('leaf chain hit invalid node/backlink')
            if has_high not in (0, 1) or is_root not in (0, 1) or is_root != int(cur == root):
                raise ValueError('invalid leaf flags')
            if right != (next_leaf if next_leaf else -1) or (next_leaf == 0) != (cur == last):
                raise ValueError('leaf right/tail mismatch')
            if has_high != int(next_leaf != 0):
                raise ValueError('leaf high-key/tail mismatch')
            rids_raw = struct.unpack_from('<508i', page, 32 + 254 * 8)
            for i in range(nkey):
                key = struct.unpack_from('<Q', page, 32 + 8 * i)[0]
                rid = (rids_raw[2 * i], rids_raw[2 * i + 1])
                if previous_key is not None and key <= previous_key:
                    raise ValueError('leaf/global keys not strictly ordered')
                if has_high and key >= high:
                    raise ValueError('leaf key exceeds exclusive high boundary')
                if i == 0 and previous_high is not None and key < previous_high:
                    raise ValueError('leaf crosses previous high boundary')
                if rid[0] < 1 or rid[1] < 0:
                    raise ValueError('invalid leaf RID')
                if range_model is None:
                    result[key] = rid
                else:
                    if key not in range_model:
                        raise ValueError('index key outside expected range')
                    result[key - range_model.begin] = (rid[0] << 32) | rid[1]
                previous_key = key
                count += 1
            previous, cur = cur, next_leaf
            previous_high = high if has_high else None
        if previous != last or (range_model is not None and count != len(range_model)):
            raise ValueError('leaf chain tail/key coverage mismatch')
        return result


def parse_heap(path, abi, range_model=None, index=None):
    page_size = abi.get('PAGE_SIZE', abi.get('page_size'))
    dataitem_size = abi.get('sizeof_DataItem', abi.get('dataitem_size'))
    lock_off = abi.get('DataItem.lock', abi.get('dataitem_lock_offset'))
    valid_off = abi.get('DataItem.valid', abi.get('dataitem_valid_offset'))
    value_size_off = abi.get('DataItem.value_size', 24)
    user_insert_off = abi.get('DataItem.user_insert', valid_off + 1)
    if 'PAGE_SIZE' in abi and (abi['sizeof_RmPageHdr'] != 24 or abi['sizeof_itemkey_t'] != 8 or
                              abi['order'] != 253 or abi['BL_RIDS_OFFSET'] != 2064):
        raise ValueError('unsupported compiled page layout')
    if page_size != 4096 or not 0 <= lock_off <= dataitem_size - 8 or not 0 <= valid_off < dataitem_size - 1:
        raise ValueError('unsupported heap ABI')
    records = {}
    seen = bytearray(len(range_model)) if range_model is not None else None
    verified = 0
    free_bad = []
    with path.open('rb') as f:
        f.seek(0, 2)
        total_pages = f.tell() // page_size
        if f.tell() % page_size:
            raise ValueError('heap file has partial page')
        page0 = read_exact(f, 0, page_size)
        record_size, num_pages, per_page, first_free, bitmap_size = struct.unpack_from('<5i', page0, 24)
        if record_size != dataitem_size + 1004 or num_pages != total_pages or per_page <= 0 or \
           bitmap_size < (per_page + 7) // 8 or \
           24 + bitmap_size + per_page * (record_size + 8) > page_size:
            raise ValueError('invalid heap header')
        for page_no in range(1, total_pages):
            page = read_exact(f, page_no * page_size, page_size)
            next_free, num_records, llsn, prev_llsn = struct.unpack_from('<iiQQ', page, 0)
            bitmap = page[24:24 + bitmap_size]
            live_count = bitmap_count(bitmap, per_page)
            if live_count != num_records:
                raise ValueError(f'heap page {page_no} count {num_records} != bitmap {live_count}')
            slots = 24 + bitmap_size
            stride = record_size + 8
            for slot in range(per_page):
                base = slots + slot * stride
                key = struct.unpack_from('<Q', page, base)[0]
                item = base + 8
                table_id = struct.unpack_from('<i', page, item)[0]
                lock = struct.unpack_from('<Q', page, item + lock_off)[0]
                value_size = struct.unpack_from('<i', page, item + value_size_off)[0]
                valid = page[item + valid_off]
                user_insert = page[item + user_insert_off]
                if bitmap_get(bitmap, slot):
                    if table_id != 0 or lock != 0 or value_size != record_size - dataitem_size or \
                       valid != 1 or user_insert != 0:
                        raise ValueError(f'live heap slot invalid at {page_no}/{slot}')
                    value = page[item + dataitem_size:item + record_size]
                    if range_model is None:
                        if key in records:
                            raise ValueError('duplicate heap key')
                        records[key] = dict(rid=(page_no, slot), value=value)
                    else:
                        if key not in range_model or seen[key - range_model.begin]:
                            raise ValueError('heap range key missing/duplicate')
                        offset = key - range_model.begin
                        if index[offset] != (page_no << 32) | slot or value != range_model[key]:
                            raise ValueError('heap full value or index RID mismatch')
                        seen[offset] = 1
                        verified += 1
                elif lock != 0 or valid != 0 or user_insert != 0:
                    raise ValueError(f'free heap slot retains markers: {page_no}/{slot}')
    if range_model is not None:
        if verified != len(range_model) or not all(seen):
            raise ValueError('heap range coverage incomplete')
        return verified
    return records


def reconcile_wal(audit, terminals):
    if (audit.get('crc_and_framing_pass') is not True or audit['duplicate_terminal_transactions'] or
            audit['transactions_without_terminal']):
        raise ValueError('WAL framing/terminal accounting failed')
    wal_terminal = {}
    for identity, kinds in audit['terminal_types'].items():
        node, tx = map(int, identity.split(':', 1))
        key = (node, tx)
        if len(kinds) != 1 or kinds[0] not in ('BATCHEND', 'ABORTEND') or key in wal_terminal:
            raise ValueError('ambiguous/duplicate WAL terminal identity')
        if tx >> 48 != node + 1:
            raise ValueError('WAL transaction node prefix mismatch')
        wal_terminal[key] = kinds[0]
    expected = {}
    optional_abort = {}
    optional_any = {}
    for rec in terminals.values():
        identity = (rec['node'], rec['tx_id'])
        if rec['outcome_class'] not in ('CONFIRMED_COMMITTED', 'CONFIRMED_ABORTED',
                                        'SKIPPED_VICTIM_DEAD'):
            raise ValueError('unresolved outcome cannot be reconciled as abort')
        if identity in expected or identity in optional_abort:
            raise ValueError('duplicate ledger WAL identity')
        if rec.get('confirmation') == 'SERVER_REJECTED':
            # 防御中止不入存储批次日志：SERVER_REJECTED 终局无 WAL 对应合法（29.2；017/018/019c/020
            # 实测 ledger 终局与 WAL 终局差额恰为 rejected 短路事务数）。
            if rec['outcome_class'] != 'CONFIRMED_ABORTED':
                raise ValueError('server-rejected terminal must be CONFIRMED_ABORTED')
            continue
        if (rec.get('confirmation') == 'REREAD_AFTER_RECOVERY'
                and rec['outcome_class'] == 'CONFIRMED_ABORTED'):
            # R2b（29.1）：victim 在途经重读核定中止——事务可能从未抵达存储
            # （WAL 无终局合法），也可能已被恢复流程 Undo（ABORTEND）；若 WAL
            # 出现 BATCHEND 即矛盾（重读不可见但 WAL 声称已提交）。
            optional_abort[identity] = 'ABORTEND'
            continue
        if rec.get('confirmation') == 'VICTIM_PRECROSS_SKIPPED':
            # R2b 边界形态：precross 纯读被跳过——读事务可能从未送达（WAL
            # 无终局）也可能已提交（BATCHEND），无写效果可裁定，双向豁免。
            optional_any[identity] = None
            continue
        if rec.get('confirmation') == 'SURVIVOR_EXEC_ABORT_READ_ONLY':
            # R2b（29.1）：恢复窗口内 survivor 只读请求确定性中止——事务未
            # 达决策点必未提交，无写效果；WAL 可能有 BEGIN 无终局（回滚后
            # 亦可能 ABORTEND），双向豁免。合法性：仅纯 READ。
            if any(op['op'] != 'READ' for op in plan['operations']):
                raise ValueError('read-only exec abort applies to pure reads only')
            optional_any[identity] = None
            continue
        expected[identity] = 'BATCHEND' if rec['outcome_class'] == 'CONFIRMED_COMMITTED' else 'ABORTEND'
    missing = {k: v for k, v in expected.items() if wal_terminal.get(k) != v}
    surplus = {k: v for k, v in wal_terminal.items() if k not in expected}
    if missing:
        raise ValueError('ledger/WAL terminal sets differ: missing/mismatched WAL terminals')
    bad_surplus = {k: v for k, v in surplus.items()
                   if (optional_abort.get(k) != 'ABORTEND' or v != 'ABORTEND')
                   and k not in optional_any}
    if bad_surplus:
        raise ValueError('ledger/WAL terminal sets differ: unexpected WAL terminals')


def verify(run, abi):
    summary = json.loads((run / 'summary.json').read_text())
    cut = summary['storage_cut']
    run_id = cut['run_id']
    if (cut.get('state') != 'drained' or cut.get('active_undo_transactions') != 0 or
            cut.get('wal_tail_inclusive') != cut.get('replay_inclusive') or
            any(cut.get(field) is not True for field in
                ('rpc_stopped_and_joined', 'wal_final_fdatasync', 'database_final_fdatasync'))):
        raise ValueError('final storage cut is not stopped/drained/durable')
    if json.loads((run / 'control' / 'storage' / 'drained.json').read_text()) != cut:
        raise ValueError('summary/control final storage cut mismatch')
    final = json.loads((run / 'ledger' / 'final-model.json').read_text())
    if final.get('run_id') != run_id:
        raise ValueError('final model run identity mismatch')
    range_model = None
    if final.get('model_kind') == 'sequential_insert_range':
        if final.get('value_model') != 'load_value_for(key,app_version)':
            raise ValueError('unsupported range value model')
        range_model = RangeModel(final['start_key'], final['key_count'], final['app_version'])
    model, versions, counts, terminals = expected_from_ledger(
        run / 'ledger' / 'requests.jsonl', run_id, range_model)
    generations = {}
    for terminal in terminals.values():
        node = terminal['node']
        if node not in generations:
            ready = json.loads((run / 'control' / f'compute_{chr(65 + node)}' / 'worker-0.json').read_text())
            if ready.get('run_id') != run_id or ready.get('node') != node or ready.get('worker') != 0:
                raise ValueError('worker control identity mismatch')
            generations[node] = ready.get('generation')
        if not generations[node] or terminal['generation'] != generations[node]:
            raise ValueError('ledger/actual compute generation mismatch')
    if counts != final['outcome_counts'] or final.get('all_terminal_classes_accounted') is not True:
        raise ValueError('final model terminal accounting mismatch')
    if range_model is None:
        if set(map(str, model)) != set(final['final_model']):
            raise ValueError('ledger-derived model differs from final model keys')
        for key, value in model.items():
            item = final['final_model'][str(key)]
            if item['app_version'] != versions[key] or item['value_sha256'] != hashlib.sha256(value).hexdigest():
                raise ValueError('ledger-derived model differs from final model content')
    db = run / 'storage' / 'build' / 'storage_server'
    index = parse_index(db / 'ycsb_user_table_bl', range_model)
    heap = parse_heap(db / 'ycsb_user_table', abi, range_model, index)
    if range_model is None:
        if set(index) != set(heap) or set(index) != set(model):
            raise ValueError('index/heap/ledger key sets differ')
        for key, expected_value in model.items():
            if index[key] != heap[key]['rid'] or heap[key]['value'] != expected_value:
                raise ValueError('offline heap full value/index RID mismatch')
            if struct.unpack_from('<I', heap[key]['value'])[0] != 125:
                raise ValueError('offline value magic mismatch')
    audit = json.loads((run / 'wal-audit.json').read_text())
    reconcile_wal(audit, terminals)
    result = dict(run_id=run_id, outcome_counts=counts, ledger_requests=len(terminals),
                  wal_records=audit['records'], index_keys=len(index),
                  heap_records=heap if range_model is not None else len(heap),
                  full_value_bytes_verified=1004 * len(model),
                  app_versions={str(k): versions[k] for k in sorted(versions)},
                  storage_cut=summary['storage_cut'])
    if range_model is not None:
        result['model_range'] = dict(begin=range_model.begin, end=range_model.end, version=range_model.version)
    result['pass'] = True
    return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('run_dir', type=Path)
    ap.add_argument('abi', type=Path)
    ap.add_argument('output', type=Path)
    args = ap.parse_args()
    try:
        result = verify(args.run_dir.resolve(strict=True), json.loads(args.abi.read_text()))
    except Exception as exc:
        result = {'pass': False, 'error': str(exc), 'error_type': type(exc).__name__}
    try:
        with args.output.open('x') as f:
            json.dump(result, f, indent=2, sort_keys=True)
            f.write('\n')
            f.flush()
            os.fsync(f.fileno())
        fd = os.open(args.output.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except Exception as exc:
        result = {'pass': False, 'error': str(exc), 'error_type': type(exc).__name__}
    print(json.dumps(result, sort_keys=True), flush=True)
    return 0 if result['pass'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
