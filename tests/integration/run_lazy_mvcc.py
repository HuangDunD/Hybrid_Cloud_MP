#!/usr/bin/env python3
"""Fresh data/ports, real storage + directory + compute processes. Linux host."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import time

p = argparse.ArgumentParser()
p.add_argument('--source', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
p.add_argument('--bench', choices=['ycsb', 'smallbank', 'tpcc'], required=True)
p.add_argument('--workload', action='store_true')
a = p.parse_args()
a.source = a.source.resolve()
a.output = a.output.resolve()
a.output.mkdir(parents=True, exist_ok=False)
shutil.copytree(a.source / 'config', a.output / 'config')
shutil.copytree(a.source / 'workload/tpcc/tpcc_tables', a.output / 'workload/tpcc/tpcc_tables')
ports = []
sockets = []
for _ in range(5):
    s = socket.socket(); s.bind(('127.0.0.1', 0)); ports.append(s.getsockname()[1]); sockets.append(s)
compute, remote, remote_meta, storage, storage_meta = ports
cfg = json.loads((a.output / 'config/compute_node_config.json').read_text())
cfg.update(mvcc_epoch=hashlib.sha256(str(a.output).encode()).hexdigest()[:16],
           undo_buffer_pool_size_pages=1, table_buffer_pool_size_per_table=16,
           index_buffer_pool_size_per_table=128, fsm_buffer_pool_size_per_table=32,
           use_rdma=False, push_page_with_scheduler=0)
cfg['local_compute_node'].update(machine_id=0, thread_num_per_machine=2, coroutine_num=1,
    txn_system=1, generate_log=1, lock_mode=1 if a.workload else 0,
    benchmark_run_mode='count', batch_size=16)
cfg['remote_compute_nodes'] = dict(remote_compute_node_ips=['127.0.0.1'], remote_compute_node_port=[compute])
cfg['remote_server_nodes'] = dict(remote_server_node_ips=['127.0.0.1'], remote_server_node_port=[remote], remote_server_node_meta_port=[remote_meta])
cfg['remote_storage_nodes'] = dict(remote_storage_node_ips=['127.0.0.1'], remote_storage_node_rpc_port=[storage], remote_storage_node_meta_port=[storage_meta])
def put(name, data): (a.output / name).write_text(json.dumps(data, indent=2) + '\n')
put('config/compute_node_config.json', cfg)
put('config/remote_server_config.json', {'local_server_node': {'local_rpc_port': remote, 'local_meta_port': remote_meta}})
put('config/storage_node_config.json', {'local_storage_node': dict(machine_num=1, machine_id=0, local_rpc_port=storage, local_meta_port=storage_meta, random_generate=False, page_read_io='buffered')})
for bench in ['ycsb', 'smallbank', 'tpcc']:
    name = f'config/{bench}_config.json'
    cfg = json.loads((a.output / name).read_text())
    cfg[bench]['attempted_num'] = 100
    if bench == 'ycsb': cfg[bench].update(num_record=2000, num_hot_record=100)
    if bench == 'smallbank': cfg[bench].update(num_accounts=2000, num_hot_accounts=100)
    put(name, cfg)
for name, count in dict(warehouse=2, district=5, customer=100, item=1000, stock=1000).items():
    path = f'workload/tpcc/tpcc_tables/{name}.json'
    data = json.loads((a.output / path).read_text()); data['table']['bkt_num'] = count; put(path, data)
for s in sockets: s.close()
children = {}
files = []
commands = {}
def launch(name, command):
    directory = a.output / 'run' / (name + '_server'); directory.mkdir(parents=True)
    output = open(a.output / (name + '.log'), 'w'); files.append(output)
    commands[name] = command
    children[name] = subprocess.Popen(command, cwd=directory, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
def listening(port):
    for path in ['/proc/net/tcp', '/proc/net/tcp6']:
        for line in Path(path).read_text().splitlines()[1:]:
            cols = line.split()
            if int(cols[1].split(':')[1], 16) == port and cols[3] == '0A': return True
    return False
result = {'ports': ports, 'commands': commands, 'case': a.bench, 'workload': a.workload}
try:
    launch('remote', [str(a.source / 'build/remote_server/remote_node'), a.bench])
    launch('storage', [str(a.source / 'build/storage_server/storage_pool'), a.bench])
    deadline = time.monotonic() + 180
    while not all(listening(port) for port in [remote, remote_meta, storage, storage_meta]):
        assert all(child.poll() is None for child in children.values()), 'server exited before readiness'
        assert time.monotonic() < deadline, 'server readiness timeout'
        time.sleep(0.05)  # readiness observation only; no correctness assertion depends on elapsed sleep
    if a.workload:
        cmd = [str(a.source / 'build/compute_server/compute_server'), a.bench, 'lazy', '2', '0.5', '1', '0']
    else:
        cmd = [str(a.source / 'build/tests/lazy_mvcc_test'), a.bench]
    launch('compute', cmd)
    code = children['compute'].wait(timeout=240)
    result['compute_exit'] = code
    assert code == 0, f'compute exited {code}'
    for name in ['storage', 'remote']:
        result[name + '_exit'] = children[name].wait(timeout=30)
        assert result[name + '_exit'] == 0, f'{name} failed'
    result['passed'] = True
finally:
    for child in children.values():
        if child.poll() is None:
            os.killpg(child.pid, signal.SIGTERM)
            try: child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL); child.wait()
    result['exits'] = {name: child.returncode for name, child in children.items()}
    put('result.json', result)
    for f in files: f.close()
    print(json.dumps(result, indent=2), flush=True)
