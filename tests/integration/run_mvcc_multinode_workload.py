#!/usr/bin/env python3
"""Small real benchmark on up to three compute processes for Lazy, ordinary 2PC or mix.
Each process has its own config root so the CLI machine_id rewrite cannot race.
Fresh data, epoch, ports and independent compute/storage directories."""
import argparse, hashlib, json, os, shutil, signal, socket, subprocess, time
from pathlib import Path
from verify_modulo_pages import verify_modulo_pages
p = argparse.ArgumentParser()
p.add_argument('--source', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
p.add_argument('--bench', choices=['ycsb', 'smallbank', 'tpcc', 'tatp', 'sysbench'], required=True)
p.add_argument('--system', choices=['lazy', '2pc', 'mix'], required=True)
p.add_argument('--variant', choices=['2pc', '2pc-cornus', '2pc-cornus-opt'], default='2pc')
p.add_argument('--attempts', type=int, default=100)
p.add_argument('--local-ratio', type=float, default=0.5)
p.add_argument('--wr-rate', type=float, default=0.5)
p.add_argument('--gdb', action='store_true')
p.add_argument('--nodes', type=int, choices=[1, 2, 3], default=2)
p.add_argument('--undo-pages', type=int, default=64)
p.add_argument('--table-pages', type=int, default=16, help='shared heap/index/FSM cache frames per node')
p.add_argument('--prewarm-seconds', type=float, default=0)
p.add_argument('--wal-preallocate-size', default='0MB', help='reserved capacity per WAL stream')
p.add_argument('--partition', type=int, default=8)
p.add_argument('--shuffle', action='store_true')
p.add_argument('--warehouses', type=int)
p.add_argument('--range-size', type=int, default=5)
p.add_argument('--auto-epoch', action='store_true')
a = p.parse_args(); a.source = a.source.resolve(); a.output = a.output.resolve()
a.output.mkdir(parents=True, exist_ok=False)
ports = []
socks = []
for _ in range(4 + a.nodes):
    s = socket.socket(); s.bind(('127.0.0.1', 0)); ports.append(s.getsockname()[1]); socks.append(s)
compute_ports = ports[0:a.nodes]; remote, remote_meta, storage, storage_meta = ports[a.nodes:4 + a.nodes]
for s in socks: s.close()
epoch = '' if a.auto_epoch else hashlib.sha256(str(a.output).encode()).hexdigest()[:16]
system_mode = {'lazy': 1, '2pc': 2, 'mix': 4}[a.system]
def craft(root):
    shutil.copytree(a.source / 'config', root / 'config')
    shutil.copytree(a.source / 'workload/tpcc/tpcc_tables', root / 'workload/tpcc/tpcc_tables')
    cfg = json.loads((root / 'config/compute_node_config.json').read_text())
    cfg.update(mvcc_epoch=epoch, undo_buffer_pool_size_pages=a.undo_pages,
               undo_lock_table_initial_capacity=16,
               buffer_pool_size=f'{4 * a.table_pages}kB', use_rdma=False, push_page_with_scheduler=0,
               partition_size_per_table=a.partition, log_flush_interval_ms=3,
               log_flush_batch_trigger=16)
    cfg['local_compute_node'].update(thread_num_per_machine=2, coroutine_num=1,
        txn_system=system_mode, generate_log=1, lock_mode=1, benchmark_run_mode='count',
        benchmark_run_seconds=30, benchmark_prewarm_seconds=a.prewarm_seconds, batch_size=16, two_pc_variant=a.variant,
        cornus_prepare_commit=0)
    cfg.setdefault('hybrid_cost_model', {})['enabled'] = 0
    cfg['remote_compute_nodes'] = dict(remote_compute_node_ips=['127.0.0.1'] * a.nodes,
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
                                    local_meta_port=storage_meta, random_generate=a.shuffle,
                                    page_read_io='buffered', wal_preallocate_size=a.wal_preallocate_size)}, indent=2) + '\n')
    bench = a.bench
    name = f'config/{bench}_config.json'
    c = json.loads((root / name).read_text())
    c[bench]['attempted_num'] = a.attempts
    if bench == 'ycsb': c[bench].update(num_record=2000, num_hot_record=100)
    if bench == 'sysbench': c[bench].update(table_size=2000, range_size=a.range_size)
    if bench == 'tatp': c[bench]['subscribers'] = 2000
    if bench == 'smallbank': c[bench].update(num_accounts=2000, num_hot_accounts=100)
    (root / name).write_text(json.dumps(c, indent=2) + '\n')
    for name, count in dict(warehouse=a.warehouses if a.warehouses is not None else max(2, a.nodes), district=5, customer=100, item=1000, stock=1000).items():
        path = f'workload/tpcc/tpcc_tables/{name}.json'
        data = json.loads((root / path).read_text()); data['table']['bkt_num'] = count
        (root / path).write_text(json.dumps(data, indent=2) + '\n')
for root in [a.output / f'node{n}' for n in range(a.nodes)] + [a.output / 'servers']:
    craft(root)
for root, sub in [(a.output / 'servers', 'remote_server'), (a.output / 'servers', 'storage_server')] + [
        (a.output / f'node{n}', 'compute_server') for n in range(a.nodes)]:
    (root / 'run' / sub).mkdir(parents=True)
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
result = {'passed': False, 'bench': a.bench, 'system': a.system, 'variant': a.variant, 'mode': system_mode,
          'nodes': a.nodes, 'attempts': a.attempts, 'local_ratio': a.local_ratio,
          'wr_rate': a.wr_rate, 'undo_pages': a.undo_pages, 'table_pages': a.table_pages,
          'prewarm_seconds': a.prewarm_seconds, 'wal_preallocate_size': a.wal_preallocate_size,
          'partition': a.partition, 'auto_epoch': a.auto_epoch, 'ports': ports,
          'commands': commands}
try:
    launch('remote', [str(a.source / 'build/remote_server/remote_node'), a.bench],
           a.output / 'servers/run/remote_server')
    launch('storage', [str(a.source / 'build/storage_server/storage_pool'), a.bench],
           a.output / 'servers/run/storage_server')
    deadline = time.monotonic() + 180
    while not all(listening(port) for port in [remote, remote_meta, storage, storage_meta]):
        assert all(c.poll() is None for c in children.values()), 'server exited before readiness'
        assert time.monotonic() < deadline, 'server readiness timeout'
        time.sleep(0.05)
    for node in range(a.nodes):
        command = [str(a.source / 'build/compute_server/compute_server'),
                   a.bench, a.system, '2', str(a.wr_rate), str(a.local_ratio), str(node)]
        if a.gdb and node == 0:
            command = ['gdb', '-batch', '-return-child-result', '-ex', 'run', '-ex', 'bt full',
                       '-ex', 'frame 1', '-ex', 'info locals', '-ex', 'p this',
                       '-ex', 'p this->node_', '-ex', 'p ComputeNodeCount',
                       '-ex', 'p nodes_channel', '-ex', 'p node_id',
                       '-ex', 'thread apply all bt', '--args'] + command
        launch(f'node{node}', command, a.output / f'node{node}/run/compute_server')
        time.sleep(1.0)
    deadline = time.monotonic() + 300
    while any(c.poll() is None for name, c in children.items() if name.startswith('node')):
        assert time.monotonic() < deadline, 'workload timeout'
        time.sleep(0.1)
    for node in range(a.nodes):
        result[f'node{node}_exit'] = children[f'node{node}'].wait()
    for name in ['storage', 'remote']:
        try: result[name + '_exit'] = children[name].wait(timeout=60)
        except subprocess.TimeoutExpired: result[name + '_exit'] = 'timeout'
    result['result_txt'] = {}
    for node in range(a.nodes):
        path = a.output / f'node{node}/run/compute_server/result.txt'
        result['result_txt'][f'node{node}'] = path.read_text() if path.exists() else 'missing'
    if not (all(result[name + '_exit'] == 0 for name in children) and
            all(report.strip() and report != 'missing' for report in result['result_txt'].values())):
        raise RuntimeError('Workload or service failed; see result.json for exit codes')
    if a.system == '2pc':
        result['heap_layout'] = verify_modulo_pages(a.output / 'servers', a.bench, a.nodes, a.partition)
    result['passed'] = True
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
