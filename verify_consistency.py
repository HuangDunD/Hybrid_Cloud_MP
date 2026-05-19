#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SmallBank 数据一致性验证脚本
直接读取存储节点的数据库文件，验证故障恢复后的数据一致性。

验证项目：
1. 所有记录的 lock 字段 == 0（无残留锁）
2. 所有记录的 value_size 正确
3. 所有记录的 valid == 1（未被错误删除）
4. savings 表的 magic == 97 (SmallBank_MAGIC)
5. checking 表的 magic == 98 (SmallBank_MAGIC + 1)
6. 索引文件中的所有 key 都能在数据文件中找到对应记录

使用方法：
    python3 verify_consistency.py [storage_dir]
    默认 storage_dir 为 build/storage_server/
"""

import struct
import sys
import os

# ============================================================================
# 常量定义（与 C++ 代码保持一致）
# ============================================================================
PAGE_SIZE = 4096

# DataItem 在磁盘上的序列化布局（不含 value 数据）
# struct DataItem {
#   table_id_t table_id;    // int32_t  (4 bytes)
#   lock_t lock;            // uint64_t (8 bytes)
#   uint8_t *value;         // pointer  (8 bytes, 在磁盘上无意义)
#   int value_size;         // int32_t  (4 bytes)
#   uint64_t version;       // uint64_t (8 bytes)
#   lsn_t prev_lsn;        // uint64_t (8 bytes)
#   uint8_t valid;          // uint8_t  (1 byte)
#   uint8_t user_insert;    // uint8_t  (1 byte)
#   // 可能有 padding
# };
# sizeof(DataItem) 在 64 位系统上需要实际测量

# RmPageHdr 布局
# struct RmPageHdr {
#   int next_free_page_no_;  // int32_t (4 bytes)
#   int num_records_;        // int32_t (4 bytes)
#   LLSN LLSN_;              // uint64_t (8 bytes)
#   LLSN pre_LLSN_;          // uint64_t (8 bytes)
# };
SIZEOF_RM_PAGE_HDR = 24  # 4 + 4 + 8 + 8

# RmFileHdr 布局
# struct RmFileHdr {
#   int record_size_;           // int32_t (4 bytes)
#   int num_pages_;             // int32_t (4 bytes)
#   int num_records_per_page_;  // int32_t (4 bytes)
#   int first_free_page_no_;    // int32_t (4 bytes)
#   int bitmap_size_;           // int32_t (4 bytes)
# };
SIZEOF_RM_FILE_HDR = 20  # 5 * 4

SIZEOF_ITEMKEY = 8  # sizeof(itemkey_t) = sizeof(uint64_t)

# SmallBank magic numbers
SMALLBANK_SAVINGS_MAGIC = 97
SMALLBANK_CHECKING_MAGIC = 98

# Lock 常量
UNLOCKED = 0
EXCLUSIVE_LOCKED = 0xFF00000000000000

OFFSET_PAGE_HDR = 0

# ============================================================================
# 颜色输出
# ============================================================================
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    CYAN = '\033[96m'
    RESET = '\033[0m'
    BOLD = '\033[1m'

def log_info(msg):
    print(f"{Colors.GREEN}[INFO]{Colors.RESET} {msg}")

def log_warn(msg):
    print(f"{Colors.YELLOW}[WARN]{Colors.RESET} {msg}")

def log_error(msg):
    print(f"{Colors.RED}[ERROR]{Colors.RESET} {msg}")

def log_success(msg):
    print(f"{Colors.CYAN}[SUCCESS]{Colors.RESET} {msg}")

# ============================================================================
# 读取文件头
# ============================================================================
def read_file_header(filepath):
    """读取数据文件的 Page 0，解析 RmPageHdr + RmFileHdr"""
    with open(filepath, 'rb') as f:
        page0 = f.read(PAGE_SIZE)

    if len(page0) < SIZEOF_RM_PAGE_HDR + SIZEOF_RM_FILE_HDR:
        raise ValueError(f"文件 {filepath} 太小，无法读取文件头")

    # 解析 RmPageHdr
    page_hdr = struct.unpack_from('<iiQQ', page0, OFFSET_PAGE_HDR)
    # 解析 RmFileHdr（紧跟在 RmPageHdr 之后）
    file_hdr_offset = SIZEOF_RM_PAGE_HDR
    file_hdr = struct.unpack_from('<iiiii', page0, file_hdr_offset)

    return {
        'record_size': file_hdr[0],
        'num_pages': file_hdr[1],
        'num_records_per_page': file_hdr[2],
        'first_free_page_no': file_hdr[3],
        'bitmap_size': file_hdr[4],
    }

# ============================================================================
# 读取索引文件
# ============================================================================
def read_index_file(filepath):
    """读取索引文件，返回 [(key, page_no, slot_no), ...]"""
    entries = []
    with open(filepath, 'r') as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) == 3:
                key = int(parts[0])
                page_no = int(parts[1])
                slot_no = int(parts[2])
                entries.append((key, page_no, slot_no))
    return entries

# ============================================================================
# 从数据页中读取记录
# ============================================================================
def read_record_from_page(page_data, slot_no, file_hdr):
    """从页面数据中读取指定 slot 的记录"""
    bitmap_offset = SIZEOF_RM_PAGE_HDR + OFFSET_PAGE_HDR
    bitmap_size = file_hdr['bitmap_size']
    record_size = file_hdr['record_size']
    slots_offset = bitmap_offset + bitmap_size

    # 检查 bitmap 中该 slot 是否有效
    bitmap = page_data[bitmap_offset:bitmap_offset + bitmap_size]
    byte_idx = slot_no // 8
    bit_idx = slot_no % 8
    if byte_idx < len(bitmap):
        is_set = (bitmap[byte_idx] >> bit_idx) & 1
    else:
        is_set = 0

    # 每个 slot 的大小 = record_size + sizeof(itemkey_t)
    slot_size = record_size + SIZEOF_ITEMKEY
    slot_start = slots_offset + slot_no * slot_size

    if slot_start + slot_size > len(page_data):
        return None, False

    slot_data = page_data[slot_start:slot_start + slot_size]

    # 解析 itemkey_t (uint64_t, 8 bytes)
    item_key = struct.unpack_from('<Q', slot_data, 0)[0]

    # 解析 DataItem（紧跟在 itemkey 之后）
    # DataItem 布局（64位系统）:
    #   table_id (int32_t, 4B) + padding(4B) + lock (uint64_t, 8B) +
    #   value_ptr (8B) + value_size (int32_t, 4B) + padding(4B) +
    #   version (uint64_t, 8B) + prev_lsn (uint64_t, 8B) +
    #   valid (uint8_t, 1B) + user_insert (uint8_t, 1B) + padding(6B)
    # 总计约 56 字节（含对齐）
    #
    # 但实际上我们需要根据编译器的对齐来解析
    # 最可靠的方式是按偏移量逐个字段读取
    di_offset = SIZEOF_ITEMKEY  # DataItem 在 slot 中的偏移

    # 使用紧凑解析：先读 table_id(4B)，然后考虑对齐
    # struct DataItem 的内存布局（gcc x86_64, 默认对齐）:
    #   offset 0:  table_id (int32_t, 4B)
    #   offset 4:  padding (4B, 因为 lock 是 uint64_t 需要 8B 对齐)
    #   offset 8:  lock (uint64_t, 8B)
    #   offset 16: value (uint8_t*, 8B)
    #   offset 24: value_size (int, 4B)
    #   offset 28: padding (4B, 因为 version 是 uint64_t)
    #   offset 32: version (uint64_t, 8B)
    #   offset 40: prev_lsn (uint64_t, 8B)
    #   offset 48: valid (uint8_t, 1B)
    #   offset 49: user_insert (uint8_t, 1B)
    #   offset 50: padding (6B, 对齐到 8B 边界)
    #   total: 56 bytes
    SIZEOF_DATA_ITEM = 56

    if di_offset + SIZEOF_DATA_ITEM > len(slot_data):
        return None, False

    table_id = struct.unpack_from('<i', slot_data, di_offset + 0)[0]
    lock_val = struct.unpack_from('<Q', slot_data, di_offset + 8)[0]
    value_size = struct.unpack_from('<i', slot_data, di_offset + 24)[0]
    version = struct.unpack_from('<Q', slot_data, di_offset + 32)[0]
    prev_lsn = struct.unpack_from('<Q', slot_data, di_offset + 40)[0]
    valid = struct.unpack_from('<B', slot_data, di_offset + 48)[0]
    user_insert = struct.unpack_from('<B', slot_data, di_offset + 49)[0]

    # value 数据紧跟在 DataItem 之后
    value_offset = di_offset + SIZEOF_DATA_ITEM
    value_data = slot_data[value_offset:value_offset + value_size] if value_size > 0 else b''

    return {
        'item_key': item_key,
        'table_id': table_id,
        'lock': lock_val,
        'value_size': value_size,
        'version': version,
        'prev_lsn': prev_lsn,
        'valid': valid,
        'user_insert': user_insert,
        'value_data': value_data,
        'bitmap_set': is_set,
    }, True

# ============================================================================
# 验证单个表
# ============================================================================
def verify_table(data_filepath, index_filepath, table_name, expected_magic, expected_value_size):
    """验证单个表的数据一致性"""
    log_info(f"验证表: {table_name}")

    # 读取文件头
    file_hdr = read_file_header(data_filepath)
    log_info(f"  文件头: record_size={file_hdr['record_size']}, num_pages={file_hdr['num_pages']}, "
             f"records_per_page={file_hdr['num_records_per_page']}, bitmap_size={file_hdr['bitmap_size']}")

    # 读取索引
    index_entries = read_index_file(index_filepath)
    log_info(f"  索引记录数: {len(index_entries)}")

    # 读取整个数据文件
    with open(data_filepath, 'rb') as f:
        file_data = f.read()

    total_records = len(index_entries)
    errors = []
    lock_errors = 0
    valid_errors = 0
    magic_errors = 0
    size_errors = 0
    missing_records = 0
    verified_count = 0

    for key, page_no, slot_no in index_entries:
        # 读取对应页面
        page_start = page_no * PAGE_SIZE
        page_end = page_start + PAGE_SIZE

        if page_end > len(file_data):
            errors.append(f"Key {key}: 页面 {page_no} 超出文件范围")
            missing_records += 1
            continue

        page_data = file_data[page_start:page_end]

        # 读取记录
        record, ok = read_record_from_page(page_data, slot_no, file_hdr)
        if not ok or record is None:
            errors.append(f"Key {key}: 无法从页面 {page_no} slot {slot_no} 读取记录")
            missing_records += 1
            continue

        # 验证 1: lock == 0
        if record['lock'] != UNLOCKED:
            lock_errors += 1
            if lock_errors <= 5:  # 只打印前5个
                lock_hex = f"0x{record['lock']:016X}"
                errors.append(f"Key {key}: lock 不为 0 (lock={lock_hex})")

        # 验证 2: valid == 1
        if record['valid'] != 1:
            valid_errors += 1
            if valid_errors <= 5:
                errors.append(f"Key {key}: valid != 1 (valid={record['valid']})")

        # 验证 3: value_size 正确
        if record['value_size'] != expected_value_size:
            size_errors += 1
            if size_errors <= 5:
                errors.append(f"Key {key}: value_size 不正确 (expected={expected_value_size}, got={record['value_size']})")

        # 验证 4: magic number
        if len(record['value_data']) >= 4:
            magic = struct.unpack_from('<I', record['value_data'], 0)[0]
            if magic != expected_magic:
                magic_errors += 1
                if magic_errors <= 5:
                    errors.append(f"Key {key}: magic 不正确 (expected={expected_magic}, got={magic})")

        verified_count += 1

    # 输出结果
    result = {
        'table_name': table_name,
        'total_records': total_records,
        'verified_count': verified_count,
        'lock_errors': lock_errors,
        'valid_errors': valid_errors,
        'magic_errors': magic_errors,
        'size_errors': size_errors,
        'missing_records': missing_records,
        'passed': (lock_errors == 0 and valid_errors == 0 and magic_errors == 0
                   and size_errors == 0 and missing_records == 0),
    }

    if result['passed']:
        log_success(f"  ✅ {table_name} 验证通过: {verified_count}/{total_records} 条记录全部正确")
    else:
        log_error(f"  ❌ {table_name} 验证失败:")
        log_error(f"     lock 错误: {lock_errors}")
        log_error(f"     valid 错误: {valid_errors}")
        log_error(f"     magic 错误: {magic_errors}")
        log_error(f"     size 错误: {size_errors}")
        log_error(f"     缺失记录: {missing_records}")
        for err in errors[:10]:
            log_error(f"     - {err}")
        if len(errors) > 10:
            log_error(f"     ... 还有 {len(errors) - 10} 个错误")

    return result

# ============================================================================
# 主函数
# ============================================================================
def main():
    # 确定存储目录
    if len(sys.argv) > 1:
        storage_dir = sys.argv[1]
    else:
        # 默认路径
        script_dir = os.path.dirname(os.path.abspath(__file__))
        storage_dir = os.path.join(script_dir, 'build', 'storage_server')

    log_info(f"存储目录: {storage_dir}")

    # 检查文件是否存在
    required_files = [
        'smallbank_savings',
        'smallbank_checking',
        'smallbank_savings_index.txt',
        'smallbank_checking_index.txt',
    ]
    for fname in required_files:
        fpath = os.path.join(storage_dir, fname)
        if not os.path.exists(fpath):
            log_error(f"文件不存在: {fpath}")
            sys.exit(1)

    print()
    print("=" * 60)
    print("  SmallBank 数据一致性验证")
    print("=" * 60)
    print()

    # smallbank_savings_val_t 和 smallbank_checking_val_t 都是 8 字节
    EXPECTED_VALUE_SIZE = 8

    # 验证 savings 表
    savings_result = verify_table(
        data_filepath=os.path.join(storage_dir, 'smallbank_savings'),
        index_filepath=os.path.join(storage_dir, 'smallbank_savings_index.txt'),
        table_name='smallbank_savings',
        expected_magic=SMALLBANK_SAVINGS_MAGIC,
        expected_value_size=EXPECTED_VALUE_SIZE,
    )

    print()

    # 验证 checking 表
    checking_result = verify_table(
        data_filepath=os.path.join(storage_dir, 'smallbank_checking'),
        index_filepath=os.path.join(storage_dir, 'smallbank_checking_index.txt'),
        table_name='smallbank_checking',
        expected_magic=SMALLBANK_CHECKING_MAGIC,
        expected_value_size=EXPECTED_VALUE_SIZE,
    )

    # 汇总结果
    print()
    print("=" * 60)
    print("  验证结果汇总")
    print("=" * 60)

    all_passed = savings_result['passed'] and checking_result['passed']

    for result in [savings_result, checking_result]:
        status = "✅ 通过" if result['passed'] else "❌ 失败"
        print(f"  {result['table_name']}: {status} "
              f"({result['verified_count']}/{result['total_records']} 条记录)")
        if not result['passed']:
            print(f"    lock错误={result['lock_errors']}, "
                  f"valid错误={result['valid_errors']}, "
                  f"magic错误={result['magic_errors']}, "
                  f"size错误={result['size_errors']}, "
                  f"缺失={result['missing_records']}")

    print()
    if all_passed:
        total = savings_result['verified_count'] + checking_result['verified_count']
        log_success(f"🎉 所有验证通过！共验证 {total} 条记录")
        print()
        sys.exit(0)
    else:
        log_error("❌ 数据一致性验证失败！")
        print()
        sys.exit(1)

if __name__ == '__main__':
    main()
