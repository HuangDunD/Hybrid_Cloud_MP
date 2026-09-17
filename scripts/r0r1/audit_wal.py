#!/usr/bin/env python3
import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path
import struct


def crc32c(data):
    crc = 0xffffffff
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82f63b78 if crc & 1 else 0)
    return crc ^ 0xffffffff


def main():
    p = argparse.ArgumentParser(description='Read-only WAL framing/CRC/end-record audit, not an external request ledger')
    p.add_argument('log_dir', type=Path)
    p.add_argument('abi', type=Path)
    p.add_argument('output', type=Path)
    args = p.parse_args()
    abi = json.loads(args.abi.read_text())
    expected = dict(log_header_size=44,batch_size=8,tx_size=8,node_size=4,table_size=4,lsn_size=8,
                    offset_type=8,offset_length=20,offset_tx=24,offset_node=32,offset_prev_lsn=36,offset_data=44)
    if any(abi[k] != v for k,v in expected.items()):
        raise ValueError('unsupported compiled WAL ABI')
    counts = Counter(); ends = Counter(); per_tx = defaultdict(Counter); deletes = []
    blocks = 0; records = 0; total_bytes = 0
    files = sorted(args.log_dir.glob('seg_*.log'))
    if not files: raise ValueError('no WAL segments')
    names = ['UPDATE','INSERT','DELETE','BEGIN','COMMIT','ABORT','NEWPAGE','FSMUPDATE','BATCHEND','BLINKINSERT','BLINKDELETE','ABORTEND']
    for path in files:
        with path.open('rb') as f:
            header = f.read(abi['segment_header_size'])
            magic,version,seg = struct.unpack_from('<IIQ',header)
            if magic != 0x4c534547 or version != 2 or seg != int(path.stem[4:],16):
                raise ValueError('segment header mismatch')
            seq = 0
            while True:
                data = f.read(abi['block_size'])
                if not data: break
                if len(data) != abi['block_size']: raise ValueError('partial block')
                crc,actual_seq,num,flags,length = struct.unpack_from('<IIHHI',data)
                if actual_seq != seq or length > abi['block_size']-16 or flags != 0:
                    raise ValueError('unsupported/corrupt block header')
                actual_crc = crc32c(data[4:12]+data[16:16+length])
                masked = (((actual_crc >> 15) | (actual_crc << 17)) + 0xa282ead8) & 0xffffffff
                if crc != masked: raise ValueError('CRC32C mismatch')
                pos = 16
                for _ in range(num):
                    if pos+44 > 16+length: raise ValueError('short record header')
                    batch,kind,lsn,size,tx,node,prev = struct.unpack_from('<QiQIQiQ',data,pos)
                    if kind not in range(len(names)) or size<44 or pos+size>16+length:
                        raise ValueError('invalid record length/type')
                    key = '%s:%s' % (node,tx)
                    counts[names[kind]]+=1; per_tx[key][names[kind]]+=1
                    if kind in (8, 11): ends[key]+=1
                    if kind == 2:
                        if size<64: raise ValueError('short DELETE')
                        table,nlen=struct.unpack_from('<iQ',data,pos+44)
                        if nlen>size-64: raise ValueError('DELETE name out of bounds')
                        name=data[pos+56:pos+56+nlen].decode()
                        page,slot=struct.unpack_from('<ii',data,pos+56+nlen)
                        deletes.append(dict(node=node,tx=tx,table=table,name=name,page=page,slot=slot))
                    pos+=size; records+=1
                if pos != 16+length: raise ValueError('record count/payload mismatch')
                total_bytes += len(data); blocks+=1; seq+=1
    summary=dict(scope='WAL framing, CRC and transaction identity audit ONLY; no external response/value reconciliation',
        abi=abi, blocks=blocks, records=records, block_bytes=total_bytes, types=counts,
        transactions=len(per_tx), unique_terminal_transactions=len(ends),
        duplicate_terminal_transactions={k:v for k,v in ends.items() if v!=1},
        transactions_without_terminal=[k for k in per_tx if k not in ends],
        terminal_types={k:[name for name,count in per_tx[k].items() if name in ('BATCHEND','ABORTEND') and count] for k in per_tx},
        delete_records=deletes, crc_and_framing_pass=True, R1_acceptance=False)
    with args.output.open('x') as f: json.dump(summary,f,indent=2)
    print(json.dumps({k:v for k,v in summary.items() if k not in ('delete_records','abi','transactions_without_batchend')}))


if __name__ == '__main__':
    main()
