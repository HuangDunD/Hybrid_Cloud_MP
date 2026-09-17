#!/usr/bin/env python3
"""Short, isolated checker tests; fixtures are never acceptance workload data."""
import argparse
import hashlib
import json
from pathlib import Path
import struct
import subprocess
import sys
import tempfile


TABLE = 'checker_fixture'
DIGEST_FORMAT = 'key-le64,size-le64,version-le64,sha256(value);sorted-key;v2'


def value_for(key, version=1, run_id=None):
    result = bytearray(struct.pack('<I', 125))
    for field in range(10):
        if run_id is None:
            prefix = f'L{version:06d}|K{key:09d}|F{field}|'.encode()
            fill = (key * 131 + field * 17 + version) % 251 + 1
            body = prefix + bytes([fill]) * 100
        else:
            body = f'v{version:06d}|k{key:06d}|f{field}|'.encode()
            seed = hashlib.sha256(f'{run_id}|{key}|{version}|{field}'.encode()).digest()
            while len(body) < 100:
                body += seed
                seed = hashlib.sha256(seed).digest()
        result.extend(body[:100])
    return bytes(result)


class Fixture:
    def __init__(self, abi, *, height=3, keys=None, omit=(), reverse_rids=False,
                 run_id=None, versions=None, commit_version=37):
        self.abi = abi
        self.page_size = abi['PAGE_SIZE']
        self.keys = list(range(5)) if keys is None else list(keys)
        self.live = [key for key in self.keys if key not in omit]
        self.versions = versions or {}
        self.run_id = run_id
        self.commit_version = commit_version
        self.rids = {}
        self.values = {}
        self.files = {}
        self.record_size = abi['sizeof_DataItem'] + 1004
        self.slot_size = abi['sizeof_itemkey_t'] + self.record_size
        self.slots = (8 * (self.page_size - 1 - abi['sizeof_RmFileHdr']) + 1) // (1 + self.slot_size * 8)
        self.bitmap_size = (self.slots + 7) // 8
        self.heap_pages = 1 + (len(self.live) + self.slots - 1) // self.slots
        self.files['heap'] = bytearray(self.heap_pages * self.page_size)
        self.header('heap', 0, abi['sizeof_RmPageHdr'], 'RmFileHdr', {
            'record_size_': self.record_size, 'num_pages_': self.heap_pages,
            'num_records_per_page_': self.slots, 'first_free_page_no_': -1,
            'bitmap_size_': self.bitmap_size,
        })
        for pid in range(self.heap_pages):
            self.header('heap', pid, 0, 'RmPageHdr', {'next_free_page_no_': -1, 'num_records_': 0})
        order = list(reversed(self.live)) if reverse_rids else self.live
        counts = [0] * self.heap_pages
        for position, key in enumerate(order):
            pid, slot = position // self.slots + 1, position % self.slots
            self.rids[key] = (pid, slot)
            counts[pid] += 1
            bitmap = pid * self.page_size + abi['sizeof_RmPageHdr']
            self.files['heap'][bitmap + slot // 8] |= 0x80 >> (slot % 8)
            offset = self.tuple_offset(key)
            self.put('heap', offset, 'Q', key)
            item = offset + abi['sizeof_itemkey_t']
            for field, fmt, val in [('table_id', 'i', 0), ('value_size', 'i', 1004),
                                    ('version', 'Q', commit_version), ('valid', 'B', 1)]:
                self.put('heap', item + abi['DataItem.' + field], fmt, val)
            value = value_for(key, self.versions.get(key, 1), run_id)
            self.values[key] = value
            start = item + abi['sizeof_DataItem']
            self.files['heap'][start:start + len(value)] = value
        for pid in range(1, self.heap_pages):
            self.heap_header(pid, 'num_records_', counts[pid])
        self.counts = counts
        self.build_index(height)
        self.build_fsm()

    def put(self, file, offset, fmt, value):
        struct.pack_into('<' + fmt, self.files[file], offset, value)

    def header(self, file, page, base, type_name, values):
        for field, value in values.items():
            fmt = 'I' if type_name.startswith('FSM') else 'i'
            if type_name == 'FSMPageHeader' and field == 'page_type':
                fmt = 'B'
            self.put(file, page * self.page_size + base + self.abi[type_name + '.' + field], fmt, value)

    def heap_header(self, page, name, value):
        self.header('heap', page, 0, 'RmPageHdr', {name: value})

    def tuple_offset(self, key):
        pid, slot = self.rids[key]
        return pid * self.page_size + self.abi['sizeof_RmPageHdr'] + self.bitmap_size + slot * self.slot_size

    def item(self, key, field, fmt, value):
        self.put('heap', self.tuple_offset(key) + self.abi['sizeof_itemkey_t'] + self.abi['DataItem.' + field], fmt, value)

    def node(self, page, field, value):
        fmt = 'B' if field in ('is_leaf', 'is_root', 'has_high_key') else 'Q' if field == 'high_key' else 'i'
        self.put('index', page * self.page_size + self.abi['BLNodeHdr.' + field], fmt, value)

    def key(self, page, index, value):
        self.put('index', page * self.page_size + self.abi['BL_KEYS_OFFSET'] + index * 8, 'Q', value)

    def rid(self, page, index, page_no, slot_no):
        offset = page * self.page_size + self.abi['BL_RIDS_OFFSET'] + index * self.abi['sizeof_Rid']
        self.put('index', offset + self.abi['Rid.page_no_'], 'i', page_no)
        self.put('index', offset + self.abi['Rid.slot_no_'], 'i', slot_no)

    def fill_node(self, page, leaf, keys, rids, *, root=False, high=None, right=-1, prev=-1, next=-1):
        for field, value in dict(is_leaf=leaf, is_root=root, num_key=len(keys),
                                 has_high_key=high is not None, high_key=high or 0,
                                 right_sibling=right, prev_leaf=prev, next_leaf=next).items():
            self.node(page, field, value)
        for i, key in enumerate(keys):
            self.key(page, i, key)
            self.rid(page, i, *rids[i])

    def build_index(self, height):
        if height == 1:
            root, first, last, pages = 2, 2, 2, 3
        else:
            assert len(self.keys) == 5
            root, first, last, pages = (6 if height == 2 else 8), 2, 5, (7 if height == 2 else 9)
        self.root = root
        self.files['index'] = bytearray(pages * self.page_size)
        self.header('index', 1, 0, 'BLFileHdr', dict(root_page_id=root, first_leaf=first, last_leaf=last))
        self.fill_node(0, True, [], [], prev=last, next=first)
        if height == 1:
            self.fill_node(2, True, self.live, [self.rids[k] for k in self.live], root=True, prev=0, next=0)
            return
        groups = [self.keys[:2], self.keys[2:3], self.keys[3:4], self.keys[4:]]
        boundaries = [self.keys[2], self.keys[3], self.keys[4]]
        for i, group in enumerate(groups):
            live = [key for key in group if key in self.live]
            self.fill_node(i + 2, True, live, [self.rids[k] for k in live],
                           high=boundaries[i] if i < 3 else None, right=i + 3 if i < 3 else -1,
                           prev=i + 1 if i else 0, next=i + 3 if i < 3 else 0)
        if height == 2:
            self.fill_node(6, False, [0] + boundaries, [(pid, -1) for pid in range(2, 6)], root=True)
        else:
            self.fill_node(6, False, [0, boundaries[0]], [(2, -1), (3, -1)], high=boundaries[1], right=7)
            self.fill_node(7, False, [0, boundaries[2]], [(4, -1), (5, -1)])
            self.fill_node(8, False, [0, boundaries[1]], [(6, -1), (7, -1)], root=True)

    @staticmethod
    def category(free_bytes):
        if free_bytes == 0:
            return 0
        if free_bytes >= 4096 * 9 // 10:
            return 255
        if free_bytes >= 4096 * 2 // 3:
            return 192
        if free_bytes >= 4096 // 3:
            return 128
        return 64

    def fsm(self, page, name, value):
        type_name = 'FSMMetaData' if page == 1 else 'FSMPageHeader'
        self.header('fsm', page, 0, type_name, {name: value})

    def fsm_node(self, page, index, value):
        self.files['fsm'][page * self.page_size + self.abi['sizeof_FSMPageHeader'] + index] = value

    def build_fsm(self):
        self.files['fsm'] = bytearray(5 * self.page_size)
        self.files['fsm'][:self.page_size] = self.files['heap'][:self.page_size]
        self.header('fsm', 1, 0, 'FSMMetaData', dict(magic_number=0x46534D54, version=1,
                    total_heap_pages=1025, total_fsm_pages=3, tree_height=2, root_page_id=4,
                    next_fsm_page_id=4, leaves_per_page=1024, children_per_page=512, table_id=20000))
        for pid, first, count in [(2, 0, 1024), (3, 1024, 1)]:
            capacity = 1 << (count - 1).bit_length()
            leaf_start = capacity - 1
            nodes = [0] * (leaf_start + count)
            for heap_page in range(max(1, first), min(self.heap_pages, first + count)):
                nodes[leaf_start + heap_page - first] = self.category((self.slots - self.counts[heap_page]) * self.slot_size)
            for index in range(leaf_start - 1, -1, -1):
                nodes[index] = max((nodes[child] if child < len(nodes) else 0) for child in (2 * index + 1, 2 * index + 2))
            self.header('fsm', pid, 0, 'FSMPageHeader', dict(magic_number=0x46535047, page_id=pid,
                        page_type=2, parent_page=4, level=0, first_heap_page=first, heap_pages_count=count,
                        first_child_page=0, child_count=0, node_count=len(nodes), leaf_start=leaf_start))
            start = pid * self.page_size + self.abi['sizeof_FSMPageHeader']
            self.files['fsm'][start:start + len(nodes)] = bytes(nodes)
        roots = [self.files['fsm'][pid * self.page_size + self.abi['sizeof_FSMPageHeader']] for pid in (2, 3)]
        self.header('fsm', 4, 0, 'FSMPageHeader', dict(magic_number=0x46535047, page_id=4, page_type=1,
                    parent_page=0, level=1, first_heap_page=0, heap_pages_count=0,
                    first_child_page=2, child_count=2, node_count=3, leaf_start=1))
        start = 4 * self.page_size + self.abi['sizeof_FSMPageHeader']
        self.files['fsm'][start:start + 3] = bytes([max(roots)] + roots)
        self.put('fsm', start + 3, 'I', 2)
        self.put('fsm', start + 7, 'I', 3)

    def digest(self):
        digest = hashlib.sha256()
        for key in sorted(self.live):
            digest.update(struct.pack('<QQQ', key, len(self.values[key]), self.commit_version))
            digest.update(hashlib.sha256(self.values[key]).digest())
        return digest.hexdigest()

    def write(self, directory):
        for kind, suffix in [('index', '_bl'), ('heap', ''), ('fsm', '_fsm')]:
            (directory / (TABLE + suffix)).write_bytes(self.files[kind])


def run(checker, fixture, parent, args=(), success=True, error_contains=None):
    with tempfile.TemporaryDirectory(prefix='tree-stats-case-', dir=parent) as temporary:
        directory = Path(temporary)
        fixture.write(directory)
        result = subprocess.run([str(checker), str(directory), TABLE, '--json-only', *args],
                                capture_output=True, text=True, timeout=20)
        if result.returncode < 0:
            raise AssertionError(f'checker crashed: {result.returncode}: {result.stderr}')
        try:
            report = json.loads(result.stdout.split('JSON_BEGIN\n', 1)[1].split('\nJSON_END', 1)[0])
        except (IndexError, ValueError) as error:
            raise AssertionError(f'missing valid report: {result.stdout!r} {result.stderr!r}') from error
        assert (result.returncode == 0) == success, (result.returncode, report)
        assert report['pass'] is success, report
        if success:
            assert report['structure_errors'] == [], report
            assert report['content_digest_format'] == DIGEST_FORMAT, report
            assert report['content_sha256'] == fixture.digest(), report
        else:
            assert report['structure_errors'] and report['content_sha256'] is None, report
            if error_contains:
                assert any(error_contains in text for text in report['structure_errors']), report
        return report, result.stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--checker', type=Path, required=True)
    parser.add_argument('--work-dir', type=Path, help='existing directory for isolated temporary test copies')
    args = parser.parse_args()
    checker = args.checker.resolve(strict=True)
    if sys.byteorder != 'little':
        raise RuntimeError('fixture serialization currently requires little-endian host ABI')
    result = subprocess.run([str(checker), '--abi-json'], capture_output=True, text=True, check=True, timeout=20)
    abi = json.loads(result.stdout)
    assert abi['PAGE_SIZE'] == 4096 and abi['order'] == 253, abi
    assert abi['LOG_HEADER_SIZE'] == abi['log_header_size'] == 44
    assert abi['sizeof_DataItem'] == abi['dataitem_size']
    assert abi['DELETE_FIXED_SIZE'] == 66 and abi['UPDATE_FIXED_SIZE'] == 77
    cases = 0
    model_args = ['--model-range', '0:5:1']
    for height in (1, 2, 3):
        fixture = Fixture(abi, height=height)
        report, _ = run(checker, fixture, args.work_dir, model_args)
        assert report['height'] == height and report['index_total_keys'] == 5
        assert report['model_records_verified'] == report['model_checked_records'] == 5
        assert report['full_value_bytes_verified'] == 5 * 1004
        cases += 1
    for options in [dict(height=1, keys=[]), dict(omit=(2,)), dict(reverse_rids=True),
                    dict(keys=[2**63 + i for i in range(5)]), dict(commit_version=999)]:
        fixture = Fixture(abi, **options)
        run(checker, fixture, args.work_dir)
        cases += 1
    fixture = Fixture(abi, versions={key: 2 for key in range(2, 5)})
    run(checker, fixture, args.work_dir, ['--model-range', '0:2:1', '--model-range', '2:5:2'])
    cases += 1
    fixture = Fixture(abi, run_id='isolated-checker-test')
    run(checker, fixture, args.work_dir, model_args + ['--model-run-id', fixture.run_id])
    cases += 1
    fixture = Fixture(abi)
    for pid in (2, 3, 4):
        count = 2047 if pid == 2 else 1 if pid == 3 else 3
        for i in range(count):
            fixture.fsm_node(pid, i, 0)
    report, _ = run(checker, fixture, args.work_dir, model_args)
    assert report['fsm_conservative_pages'] > 0
    cases += 1
    fixture = Fixture(abi)
    # SQL's FSM page zero stores the raw RmFileHdr, unlike the workload wrapper.
    offset = abi['sizeof_RmPageHdr']
    raw = fixture.files['heap'][offset:offset + abi['sizeof_RmFileHdr']]
    fixture.files['fsm'][:4096] = bytes(4096)
    fixture.files['fsm'][:len(raw)] = raw
    run(checker, fixture, args.work_dir, model_args)
    cases += 1
    fixture = Fixture(abi)
    _, output = run(checker, fixture, args.work_dir, model_args + ['--dump-content'])
    rows = [json.loads(line) for line in output.split('CONTENT_BEGIN\n')[1].split('CONTENT_END')[0].splitlines()]
    assert rows == [dict(key=key, version=fixture.commit_version, value_hex=fixture.values[key].hex()) for key in fixture.live]
    cases += 1

    def mutate_heap_header(fixture, name, value):
        fixture.header('heap', 0, abi['sizeof_RmPageHdr'], 'RmFileHdr', {name: value})

    def root_id(fixture, value):
        fixture.header('index', 1, 0, 'BLFileHdr', {'root_page_id': value})

    def heap_bitmap(fixture, key, occupied):
        pid, slot = fixture.rids[key]
        offset = pid * 4096 + abi['sizeof_RmPageHdr'] + slot // 8
        mask = 0x80 >> (slot % 8)
        fixture.files['heap'][offset] = (fixture.files['heap'][offset] | mask) if occupied else (fixture.files['heap'][offset] & ~mask)

    def alter_value(fixture):
        offset = fixture.tuple_offset(4) + abi['sizeof_itemkey_t'] + abi['sizeof_DataItem'] + 1003
        fixture.files['heap'][offset] ^= 1

    def overestimate(fixture, heap_page):
        fixture.fsm_node(2, 1023 + heap_page, 64)
        for index in range(1022, -1, -1):
            base = 2 * 4096 + abi['sizeof_FSMPageHeader']
            fixture.fsm_node(2, index, max(fixture.files['fsm'][base + 2 * index + 1], fixture.files['fsm'][base + 2 * index + 2]))
        root = fixture.files['fsm'][2 * 4096 + abi['sizeof_FSMPageHeader']]
        fixture.fsm_node(4, 0, root)
        fixture.fsm_node(4, 1, root)

    mutations = [
        ('root-sentinel', lambda f: root_id(f, 0), 'header page ids'),
        ('root-header', lambda f: root_id(f, 1), 'header page ids'),
        ('root-negative', lambda f: root_id(f, -1), 'header page ids'),
        ('root-out-of-range', lambda f: root_id(f, 900), 'header page ids'),
        ('extra-root', lambda f: f.node(2, 'is_root', 1), 'root flag'),
        ('invalid-bool', lambda f: f.node(2, 'is_leaf', 2), 'boolean'),
        ('empty-internal', lambda f: f.node(8, 'num_key', 0), 'empty internal'),
        ('negative-count', lambda f: f.node(2, 'num_key', -1), 'key count'),
        ('oversized-count', lambda f: f.node(2, 'num_key', 254), 'key count'),
        ('internal-first-not-neg', lambda f: f.key(7, 0, 1), 'NEG_KEY'),
        ('child-header', lambda f: f.rid(8, 0, 1, -1), 'child RID'),
        ('child-negative', lambda f: f.rid(8, 0, -1, -1), 'child RID'),
        ('child-slot', lambda f: f.rid(8, 0, 6, 0), 'child RID'),
        ('child-cycle', lambda f: f.rid(8, 0, 8, -1), 'duplicate reachable'),
        ('duplicate-child', lambda f: f.rid(8, 1, 6, -1), 'duplicate reachable'),
        ('orphan-page', lambda f: f.files['index'].extend(bytes(4096)), 'unreachable'),
        ('duplicate-key', lambda f: f.key(2, 1, 0), 'duplicate'),
        ('unsigned-ancestor-range', lambda f: f.key(3, 0, 2**63), 'parent interval'),
        ('high-key', lambda f: f.node(6, 'high_key', 4), 'parent interval'),
        ('missing-high-key', lambda f: f.node(6, 'has_high_key', 0), 'parent interval'),
        ('wrong-sibling-layer', lambda f: f.node(6, 'right_sibling', 4), 'right sibling'),
        ('leaf-forward-skip', lambda f: f.node(2, 'next_leaf', 4), 'chain'),
        ('leaf-backward-skip', lambda f: f.node(4, 'prev_leaf', 2), 'chain'),
        ('sentinel-prev', lambda f: f.node(0, 'prev_leaf', 4), 'sentinel'),
        ('internal-leaf-link', lambda f: f.node(6, 'next_leaf', 3), 'leaf links'),
        ('truncated-index', lambda f: f.files['index'].pop(), 'length'),
        ('truncated-heap', lambda f: f.files['heap'].pop(), 'length'),
        ('truncated-fsm', lambda f: f.files['fsm'].pop(), 'length'),
        ('heap-record-size', lambda f: mutate_heap_header(f, 'record_size_', 1), 'record size'),
        ('heap-bitmap-size', lambda f: mutate_heap_header(f, 'bitmap_size_', 4096), 'geometry'),
        ('heap-slot-count', lambda f: mutate_heap_header(f, 'num_records_per_page_', 2147483647), 'slots per page'),
        ('heap-num-pages', lambda f: mutate_heap_header(f, 'num_pages_', 900), 'num_pages'),
        ('heap-num-records', lambda f: f.heap_header(1, 'num_records_', 2), 'num_records'),
        ('bitmap-valid-clear', lambda f: f.item(0, 'valid', 'B', 0), 'bitmap/valid'),
        ('bitmap-valid-set', lambda f: heap_bitmap(f, 0, False), 'bitmap/valid'),
        ('invalid-valid', lambda f: f.item(0, 'valid', 'B', 2), 'bitmap/valid'),
        ('tuple-lock', lambda f: f.item(0, 'lock', 'Q', 1), 'lock'),
        ('delete-marker', lambda f: f.item(0, 'user_insert', 'B', 1), 'deletion'),
        ('value-size', lambda f: f.item(0, 'value_size', 'i', 2147483647), 'value_size'),
        ('mixed-table-id', lambda f: f.item(1, 'table_id', 'i', 1), 'table IDs'),
        ('rid-zero-page', lambda f: f.rid(2, 0, 0, 0), 'RID outside'),
        ('rid-out-of-slot', lambda f: f.rid(2, 0, 1, 3), 'RID outside'),
        ('rid-wrong-slot', lambda f: f.rid(2, 0, 1, 1), 'key-to-RID'),
        ('heap-duplicate-key', lambda f: f.put('heap', f.tuple_offset(1), 'Q', 0), 'duplicate heap'),
        ('heap-only-key', lambda f: f.put('heap', f.tuple_offset(1), 'Q', 99), 'absent from index'),
        ('fsm-magic', lambda f: f.fsm(1, 'magic_number', 0), 'magic'),
        ('fsm-page-id', lambda f: f.fsm(2, 'page_id', 3), 'identity'),
        ('fsm-level', lambda f: f.fsm(2, 'level', 1), 'level'),
        ('fsm-parent', lambda f: f.fsm(2, 'parent_page', 3), 'parent'),
        ('fsm-empty', lambda f: f.fsm(4, 'child_count', 0), 'count'),
        ('fsm-node-bounds', lambda f: f.fsm(2, 'node_count', 0xffffffff), 'geometry'),
        ('fsm-category', lambda f: f.fsm_node(4, 0, 23), 'category'),
        ('fsm-aggregate', lambda f: f.fsm_node(4, 0, 0), 'aggregate'),
        ('fsm-range-gap', lambda f: f.fsm(3, 'first_heap_page', 1023), 'ranges'),
        ('fsm-capacity-small', lambda f: f.fsm(1, 'total_heap_pages', 1), 'capacity'),
        ('fsm-next-id', lambda f: f.fsm(1, 'next_fsm_page_id', 9), 'allocation'),
        ('fsm-table-id', lambda f: f.fsm(1, 'table_id', 20001), 'table_id'),
        ('fsm-child-blob', lambda f: f.put('fsm', 4 * 4096 + abi['sizeof_FSMPageHeader'] + 3, 'I', 1), 'blob'),
        ('fsm-full-page-overestimate', lambda f: overestimate(f, 1), 'overestimates'),
        ('fsm-unallocated-page', lambda f: overestimate(f, 10), 'overestimates'),
        ('fsm-heap-header-as-free', lambda f: overestimate(f, 0), 'overestimates'),
        ('full-value-last-byte', alter_value, 'model value'),
    ]
    for name, mutation, message in mutations:
        fixture = Fixture(abi)
        mutation(fixture)
        try:
            run(checker, fixture, args.work_dir, model_args, success=False, error_contains=message)
        except Exception as error:
            raise AssertionError(f'corruption case {name} failed') from error
        cases += 1
    for invalid_args in [['--unknown'], ['--model-range', '0:0:1'], ['--model-range', '0:6:1'],
                         ['--model-range', '0:5:2'], ['--model-range', '0:5:1', '--model-range', '4:6:1']]:
        run(checker, Fixture(abi), args.work_dir, invalid_args, success=False)
        cases += 1
    print(json.dumps(dict(pass_=True, cases=cases, fixture_scope='isolated-checker-unit-tests-only',
                          abi_source=str(checker), content_digest_format=DIGEST_FORMAT), sort_keys=True))


if __name__ == '__main__':
    main()
