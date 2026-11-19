#!/usr/bin/env python3
"""Small real Lazy benchmark on two compute processes. Each process has its own
config root so the benchmark CLI's machine_id rewrite cannot race. Fresh data,
epoch and ports; the storage/remote servers are shared."""
import argparse, hashlib, json, os, shutil, signal, socket, subprocess, time
from pathlib import Path
p = argparse.ArgumentParser()
p.add_argument('--source', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
p.add_argument('--attempts', type=int, default=100)
a = p.parse_args(); a.source = a.source.resolve(); a.output = a.output.resolve()
a.output.mkdir(parents=True, exist_ok=False)
ports = []
socks = []
for _ in range(6):
    s = socket.socket(); s.bind(('127.0.0.1', 0)); ports.append(s.getsockname()[1]); socks.append(s)
compute_ports = ports[0:2]; remote, remote_meta, storage, storage_meta = ports[2:6]
for s in socks: s.close()
epoch = hashlib.sha256(str(a.output).encode()).hexdigest()[:16]
def craft(root):
    shutil.copytree(a.source / 'config', root / 'config')
    cfg = json.loads((root / 'config/compute_node_config.json').read_text())
    cfg.update(mvcc_epoch=epoch, undo_buffer_pool_size_pages=8, undo_lock_table_initial_capacity=8,
               table_buffer_pool_size_per_table=16, index_buffer_pool_size_per_table=128,
               fsm_buffer_pool_size_per_table=32, use_rdma=False, push_page_with_scheduler=0,
               log_flush_interval_ms=3, log_flush_batch_trigger=16)
    cfg['local_compute_node'].update(thread_num_per_machine=2, coroutine_num=1, txn_system=1,
        generate_log=1, lock_mode=1, benchmark_run_mode='count', benchmark_run_seconds=30,
        batch_size=16)
    cfg['remote_compute_nodes'] = dict(remote_compute_node_ips=['127.0.0.1', '127.0.0.1'],
                                       remote_compute_node_port=compute_ports)
    cfg['remote_server_nodes'] = dict(remote_server_node_ips=['127.0.0.1'],
                                      remote_server_node_port=[remote],
                                      remote_server_node_meta_port=[remote_meta])
    cfg['remote_storage_nodes'] = dict(remote_storage_node_ips=['127.0.0.1'],
                                       remote_storage_node_rpc_port=[storage],
                                       remote_storage_node_meta_port=[storage_meta])
    (root / 'config/compute_node_config.json').write_text(json.dumps(cfg, indent=2) + '\n')
    (root / 'config/remote_server_config.json').write_text(json.dumps(
        {'local_server_node': {'local_rpc_port': remote, 'local_meta_port': remote_meta}}, indent=2) + '\n')
    (root / 'config/storage_node_config.json').write_text(json.dumps(
        {'local_storage_node': dict(machine_num=1, machine_id=0, local_rpc_port=storage,
                                    local_meta_port=storage_meta, random_generate=False,
                                    page_read_io='buffered')}, indent=2) + '\n')
    y = json.loads((root / 'config/ycsb_config.json').read_text())
    y['ycsb'].update(num_record=2000, num_hot_record=100, attempted_num=a.attempts)
    (root / 'config/ycsb_config.json').write_text(json.dumps(y, indent=2) + '\n')
for root in [a.output / 'node0', a.output / 'node1', a.output / 'servers']:
    craft(root)
    (root / 'run').mkdir()
for root, sub in [(a.output / 'servers', 'remote_server'), (a.output / 'servers', 'storage_server'),
                  (a.output / 'node0', 'compute_server'), (a.output / 'node1', 'compute_server')]:
    (root / 'run' / sub).mkdir()
commands = {}; children = {}; logs = []
def launch(name, args, cwd):
    f = open(a.output / (name + '.log'), 'w'); logs.append(f)
    commands[name] = args
    children[name] = subprocess.Popen(args, cwd=cwd, stdout=f, stderr=subprocess.STDOUT,
                                      start_new_session=True)
def listening(port):
    for path in ['/proc/net/tcp', '/proc/net/tcp6']:
        for line in Path(path).read_text().splitlines()[1:]:
            cols = line.split()
            if int(cols[1].split(':')[1], 16) == port and cols[3] == '0A': return True
    return False
result = {'ports': ports, 'commands': commands}
try:
    launch('remote', [str(a.source / 'build/remote_server/remote_node'), 'ycsb'],
           a.output / 'servers/run/remote_server')
    launch('storage', [str(a.source / 'build/storage_server/storage_pool'), 'ycsb'],
           a.output / 'servers/run/storage_server')
    deadline = time.monotonic() + 180
    while not all(listening(port) for port in [remote, remote_meta, storage, storage_meta]):
        assert all(c.poll() is None for c in children.values()), 'server exited before readiness'
        assert time.monotonic() < deadline, 'server readiness timeout'
        time.sleep(0.05)
    for node in [0, 1]:
        launch(f'node{node}', [str(a.source / 'build/compute_server/compute_server'),
                               'ycsb', 'lazy', '2', '0.5', '1', str(node)],
               a.output / f'node{node}/run/compute_server')
        time.sleep(1.0)
    deadline = time.monotonic() + 300
    while any(c.poll() is None for name, c in children.items() if name.startswith('node')):
        assert time.monotonic() < deadline, 'workload timeout'
        time.sleep(0.1)
    for name in ['node0', 'node1']:
        result[name + '_exit'] = children[name].wait()
    # server shutdown is requested by the minimum compute node after both finish
    for name in ['storage', 'remote']:
        try: result[name + '_exit'] = children[name].wait(timeout=60)
        except subprocess.TimeoutExpired:
            result[name + '_exit'] = 'timeout'
    result['result_txt'] = {}
    for node in [0, 1]:
        path = a.output / f'node{node}/run/compute_server/result.txt'
        result['result_txt'][f'node{node}'] = path.read_text() if path.exists() else 'missing'
    result['passed'] = all(result.get(k) == 0 for k in ['node0_exit', 'node1_exit'])
    assert result['passed'], result
finally:
    for c in children.values():
        if c.poll() is None:
            try: os.killpg(c.pid, signal.SIGTERM)
            except ProcessLookupError: pass
            try: c.wait(timeout=10)
            except subprocess.TimeoutExpired:
                try: os.killpg(c.pid, signal.SIGKILL)
                except ProcessLookupError: pass
                c.wait()
    result['exits'] = {n: c.returncode for n, c in children.items()}
    (a.output / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
    for f in logs: f.close()
    print(json.dumps(result, indent=2), flush=True)
