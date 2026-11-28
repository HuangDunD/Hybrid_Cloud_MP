"""Scan the RMV2 heap files of a drained modulo-partitioned benchmark run."""
import json
import struct

TABLES = {
    'ycsb': ('user_table',),
    'smallbank': ('savings', 'checking'),
    'tatp': ('subscriber', 'access_info', 'special_facility', 'call_forwarding'),
    'sysbench': ('sbtest1', 'k_index'),
    'tpcc': ('warehouse', 'district', 'customer', 'customerhistory', 'ordernew', 'order',
             'orderline', 'item', 'stock', 'customerindex', 'orderindex'),
}

PAGE_SIZE = 4096
PAGE_HEADER = struct.Struct('=iiQQ')       # RmPageHdr
FILE_HEADER = struct.Struct('=iiiiiI')     # RmFileHdr follows the page-zero header
KEY = struct.Struct('=Q')


def warehouse_id(table, key, scale):
    if table in ('warehouse', 'item'):
        return key                        # Items use their one-based catalog ID.
    if table == 'stock':
        return (key - 1) // scale['stock']
    if table == 'district':
        district = key
    elif table in ('customer', 'ordernew', 'order', 'customerindex'):
        district = key >> 32
    elif table == 'customerhistory':
        district = key % (1 << 20)
    elif table == 'orderindex':
        district = ((key >> 32) - 1) // scale['customer']
    elif table == 'orderline':
        district = (key - 1) // 15 // 10000000
    else:
        raise ValueError(table)
    return (district - 1) // scale['district']


def verify_modulo_pages(servers, workload, nodes, pages_per_owner):
    directory = servers / 'run/storage_server'
    tables = TABLES[workload]
    manifest = (directory / f'{workload}_{tables[0]}.layout').read_text().split()
    assert manifest[0] == 'key-modulo-v1' and list(map(int, manifest[-2:])) == [nodes, pages_per_owner]
    scale = {}
    if workload == 'tpcc':
        for name in ('stock', 'customer', 'district'):
            path = servers / 'workload/tpcc/tpcc_tables' / (name + '.json')
            scale[name] = json.loads(path.read_text())['table']['bkt_num']

    results = {}
    for table in tables:
        name = workload + '_' + table
        index = {}
        with (directory / (name + '_index.txt')).open() as mapping:
            for line in mapping:
                key, page, slot = map(int, line.split())
                assert key not in index, (name, 'duplicate index key', key)
                index[key] = (page, slot)

        occupied_pages = 0
        rows_by_node = [0] * nodes
        sample = {}
        with (directory / name).open('rb') as heap:
            header = heap.read(PAGE_SIZE)
            width, pages, capacity, _, bitmap, fmt = FILE_HEADER.unpack_from(header, PAGE_HEADER.size)
            assert fmt == 0x524D5632 and bitmap == (capacity + 7) // 8, name
            for p in range(1, pages):
                data = heap.read(PAGE_SIZE)
                assert len(data) == PAGE_SIZE, (name, p, 'truncated page')
                owner = (p - 1) // pages_per_owner % nodes
                slots = 0
                for slot in range(capacity):
                    if not data[PAGE_HEADER.size + slot // 8] & (128 >> (slot % 8)):
                        continue
                    offset = PAGE_HEADER.size + bitmap + slot * (width + KEY.size)
                    key, = KEY.unpack_from(data, offset)
                    unit = warehouse_id(table, key, scale) - 1 if workload == 'tpcc' else key
                    assert unit >= 0 and unit % nodes == owner, (name, p, slot, key, unit, owner)
                    assert index.pop(key) == (p, slot), (name, p, slot, key)
                    if not sample:
                        sample = dict(page=p, owner=owner, keys=[])
                    if sample['page'] == p and len(sample['keys']) < 8:
                        sample['keys'].append(key)
                    slots += 1
                assert slots == PAGE_HEADER.unpack_from(data)[1], (name, p, 'slot count')
                occupied_pages += bool(slots)
                rows_by_node[owner] += slots
            assert not heap.read(1), (name, 'unaccounted pages')
        assert not index, (name, 'index entries without heap rows', len(index))
        results[name] = dict(pages=pages - 1, occupied_pages=occupied_pages,
                             rows_by_node=rows_by_node, first_page_sample=sample)
    return results
