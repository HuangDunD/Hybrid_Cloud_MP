# 这个文件的目的是，测试多种条件下的性能对比，能够实现多物理机上自动化的脚本运行
# 需要先在预定的机器的指定目录下，安装本项目，然后配置好环境，如 brpc，boost 等，编译通过后，再跑这个脚本

import os
import sys
import argparse
import time
import json
import shlex
import subprocess
import threading
import logging
import hashlib
from pathlib import Path
from tools.benchmark.report import redact, fingerprint, write_stats, write_comparison
import csv
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import io
import itertools
import math
from collections import deque
from tools.benchmark.report import atomic_text, atomic_json, result_complete


# ==================== 实验配置：日常修改这里，然后直接运行 ====================
# 运行方式：count 按每线程事务尝试数停止；time 按运行秒数停止。
run_mode = 'time'
run_seconds = 60               # time 模式下，每个节点运行多少秒
# TPS 固定每 2 秒输出一次。

# 负载与参数扫描：列表中的组合会生成 case plan。
modes = ['lazy']                # 可选 eager、lazy、2pc、single、mix
bench_names = ['ycsb' , 'smallbank']
thread_num = 10
#1：全都是写，0：全都是读
# write_txn_ratios = [0.1 , 0.5 , 0.9]
write_txn_ratios = [0.9 , 0.5]
local_ratios = [0.5 , 0.1 , 0.9] #本地访问的比例
use_zipfian = True
zipfian_theta = [0.95 , 0.50]
tx_hot_list = [90 , 50 , 10]  #热点访问比例
hybrid_skew_thresholds = [0.2]   # mix 模式下判断是否走 2PC 的偏斜阈值
hot_key_top_ns = [100]           # 热点 key 统计 Top-N
# 锁冲突处理策略：0 = NO_WAIT（冲突立刻回滚本事务），1 = WAIT_DETECT（登记等待 + 链式死锁检测）
# 多个值会作为一层扫描维度依次跑；注意等待路径还要满足 parallel_page_fetch = 0 且 holder 时间戳有效，
# 否则 WAIT_DETECT 会退化成"冲突即回滚"，跟 NO_WAIT 没区别。
lock_modes = [1]
# count 模式下，每个工作线程的事务尝试数；time 模式忽略此上限。
attempt_num_by_bench = {
    "ycsb":10000,
    "smallbank": 100000,
    "tpcc": 10000,
}
# 为了避免存储端一次性元信息发送的监听被并发连接挤爆，分节点顺序错峰启动
handshake_stagger_sec = 1

# 路径与连接配置：节点 IP 从 config/compute_node_config.json 读取。
workspace = str(Path(__file__).resolve().parent)
remote_workspace = '/usr/local/exper/Hybrid_Cloud_MP'
remote_build_dir = '/usr/local/exper/Hybrid_Cloud_MP/build'
# 结果根目录名，可用 RESULT_ROOT 切换（单节点实验用 result_single_node，避免与多节点同名 combo 混淆）
result_root_name = os.environ.get('RESULT_ROOT', 'result')

compute_server_build_dir = '/usr/local/exper/Hybrid_Cloud_MP/build/compute_server'
compute_default_ssh_port = int(os.environ.get('COMPUTE_SSH_PORT', 22))
compute_default_ssh_user = os.environ.get('COMPUTE_SSH_USER', 'root')
compute_default_ssh_passwd = os.environ.get('COMPUTE_SSH_PASS', 'wljwlj123Wlj.')

remote_server_host = os.environ.get('REMOTE_HOST')  # 留空时从 config/compute_node_config.json 读取
remote_server_port = int(os.environ.get('REMOTE_PORT', 22))
remote_server_user = os.environ.get('REMOTE_USER', 'root')
remote_server_passwd = os.environ.get('REMOTE_PASS', 'wljwlj123Wlj.')
remote_key_path = os.environ.get('REMOTE_KEY', None)

# ==================== 以下为实现代码，无需修改 ====================

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logging.getLogger("paramiko").setLevel(logging.WARNING)

# Planning works without Paramiko and never installs packages or contacts hosts.
paramiko = None


def load_ssh_dependency():
    global paramiko
    if paramiko is None:
        try:
            import paramiko as module
        except ImportError:
            sys.path.insert(0, str(Path(__file__).resolve().parent / 'third_party'))
            import paramiko as module
        paramiko = module


def load_compute_server_ssh_config():
    config_path = os.path.join(workspace, 'config', 'compute_node_config.json')
    with open(config_path, 'r', encoding='utf-8') as f:
        config_data = json.load(f)
    remote_compute_nodes = config_data.get('remote_compute_nodes', {})
    hostnames = remote_compute_nodes.get('remote_compute_node_ips', [])
    if not isinstance(hostnames, list) or len(hostnames) == 0:
        raise ValueError("config/compute_node_config.json remote_compute_nodes.remote_compute_node_ips is empty")
    node_count = len(hostnames)

    def normalize(raw_values, default_value, cast_fn):
        if isinstance(raw_values, list):
            if len(raw_values) == node_count:
                return [cast_fn(v) for v in raw_values]
            if len(raw_values) == 1:
                return [cast_fn(raw_values[0]) for _ in range(node_count)]
        return [cast_fn(default_value) for _ in range(node_count)]

    ssh_ports = normalize(remote_compute_nodes.get('remote_compute_node_ssh_ports', []), compute_default_ssh_port, int)
    ssh_users = normalize(remote_compute_nodes.get('remote_compute_node_ssh_usernames', []), compute_default_ssh_user, str)
    ssh_passwords = normalize(remote_compute_nodes.get('remote_compute_node_ssh_passwords', []), compute_default_ssh_passwd, str)
    return hostnames, ssh_ports, ssh_users, ssh_passwords

def resolve_attempt_num(bench_name: str) -> int:
    if bench_name in attempt_num_by_bench:
        return int(attempt_num_by_bench[bench_name])
    raise ValueError(f"unsupported bench_name for attempt_num: {bench_name}")

def run_settings():
    """Fingerprint the source/config used to plan a run, excluding credentials."""
    config = {p.name: redact(json.loads(p.read_text()))
              for p in sorted(Path(workspace, 'config').glob('*.json'))}
    source = hashlib.sha256()
    for directory in ('core', 'compute_server', 'storage_server', 'remote_server', 'common', 'workload'):
        for p in sorted(Path(workspace, directory).rglob('*')):
            if p.is_file() and (p.suffix in ('.h', '.cc', '.proto') or p.name == 'CMakeLists.txt') and '.pb.' not in p.name:
                source.update(str(p.relative_to(workspace)).encode())
                source.update(p.read_bytes())
    for name in ('CMakeLists.txt', 'config.h'):
        source.update(Path(workspace, name).read_bytes())
    revision = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=workspace,
                              capture_output=True, text=True).stdout.strip() or 'unknown'
    return dict(config=config, code_revision=revision, source_fingerprint=source.hexdigest(),
                modes=modes, bench_names=bench_names, thread_num=thread_num,
                local_ratios=local_ratios, write_txn_ratios=write_txn_ratios,
                use_zipfian=use_zipfian, zipfian_theta=zipfian_theta, tx_hot_list=tx_hot_list,
                hybrid_skew_thresholds=hybrid_skew_thresholds, hot_key_top_ns=hot_key_top_ns,
                lock_modes=lock_modes, attempts=attempt_num_by_bench, run_mode=run_mode,
                run_seconds=run_seconds)

# remote_server 和 storage_server 在一个服务器上
def detect_remote_server_host():
    config_path = os.path.join(workspace, 'config', 'compute_node_config.json')
    with open(config_path, 'r', encoding='utf-8') as f:
        config_data = json.load(f)

    remote_server_nodes = config_data.get('remote_server_nodes', {})
    remote_server_ips = remote_server_nodes.get('remote_server_node_ips', [])
    if isinstance(remote_server_ips, list) and remote_server_ips:
        return str(remote_server_ips[0])

    # remote_server 和 storage_server 部署在同一台机器时，回退使用 storage 配置
    remote_storage_nodes = config_data.get('remote_storage_nodes', {})
    remote_storage_ips = remote_storage_nodes.get('remote_storage_node_ips', [])
    if isinstance(remote_storage_ips, list) and remote_storage_ips:
        return str(remote_storage_ips[0])

    raise ValueError(
        "cannot detect remote server host from config/compute_node_config.json; "
        "please set remote_server_nodes.remote_server_node_ips or "
        "remote_storage_nodes.remote_storage_node_ips"
    )


def load_cluster_deploy_hosts():
    config_path = os.path.join(workspace, 'config', 'compute_node_config.json')
    with open(config_path, 'r', encoding='utf-8') as f:
        config_data = json.load(f)
    host_list = []
    seen = set()

    def append_host(host):
        if host and host not in seen:
            seen.add(host)
            host_list.append(host)

    for host in compute_server_hostnames:
        append_host(host)
    append_host(remote_server_host)

    remote_storage_nodes = config_data.get('remote_storage_nodes', {})
    for host in remote_storage_nodes.get('remote_storage_node_ips', []):
        append_host(host)

    return host_list

def bootstrap_cluster():
    deploy_hosts = load_cluster_deploy_hosts()
    if not deploy_hosts:
        raise ValueError("no deploy hosts found from config")

    deploy_user = os.environ.get('DEPLOY_SSH_USER', remote_server_user)
    deploy_passwd = os.environ.get('DEPLOY_SSH_PASS', remote_server_passwd)
    remote_parent_dir = os.environ.get('REMOTE_PROJECT_PARENT', '/usr/local/exper')
    remote_project_dir = os.path.join(remote_parent_dir, 'Hybrid_Cloud_MP')
    config_path = os.path.join(workspace, 'config', 'compute_node_config.json')
    with open(config_path, 'r', encoding='utf-8') as f:
        config_data = json.load(f)
    host_port_map = {}
    for idx, host in enumerate(compute_server_hostnames):
        if idx < len(compute_server_ports):
            host_port_map[host] = int(compute_server_ports[idx])
    host_port_map[remote_server_host] = remote_server_port
    remote_storage_nodes = config_data.get('remote_storage_nodes', {})
    storage_hosts = set(remote_storage_nodes.get('remote_storage_node_ips', []))
    storage_ssh_ports = remote_storage_nodes.get('remote_storage_node_ssh_ports', [])
    if isinstance(storage_ssh_ports, list):
        for idx, host in enumerate(remote_storage_nodes.get('remote_storage_node_ips', [])):
            if idx < len(storage_ssh_ports):
                host_port_map[host] = int(storage_ssh_ports[idx])

    def run_parallel_hosts(hosts, worker, phase):
        errors = []
        lock = threading.Lock()
        threads = []
        logging.info(f"phase start: {phase}")

        def task(host):
            try:
                t0 = time.time()
                worker(host)
                cost = time.time() - t0
                logging.info(f"{phase} done: {host} ({cost:.2f}s)")
            except Exception as e:
                with lock:
                    errors.append(f"{host}: {e}")

        for host in hosts:
            t = threading.Thread(target=task, args=(host,))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        if errors:
            raise RuntimeError(f"{phase} failed: {'; '.join(errors)}")
        logging.info(f"phase done: {phase}")

    def cleanup_host(host):
        ssh_port = host_port_map.get(host, 22)
        pre_kill_cmd = [
            "sshpass", "-p", deploy_passwd,
            "ssh", "-p", str(ssh_port), "-o", "StrictHostKeyChecking=no",
            f"{deploy_user}@{host}",
            "bash -lc 'pkill -f compute_server >/dev/null 2>&1 || true; pkill -f remote >/dev/null 2>&1 || true; pkill -f storage >/dev/null 2>&1 || true'"
        ]
        cleanup_cmd = [
            "sshpass", "-p", deploy_passwd,
            "ssh", "-p", str(ssh_port), "-o", "StrictHostKeyChecking=no",
            f"{deploy_user}@{host}",
            f"rm -rf {remote_project_dir}"
        ]
        subprocess.run(pre_kill_cmd, check=False)
        logging.info(f"cleanup remote project start: {host}")
        subprocess.run(cleanup_cmd, check=True)

    def copy_host(host):
        ssh_port = host_port_map.get(host, 22)
        copy_cmd = [
            "sshpass", "-p", deploy_passwd,
            "scp", "-P", str(ssh_port), "-r", workspace,
            f"{deploy_user}@{host}:{remote_parent_dir}/"
        ]
        logging.info(f"copy project to remote start: {host}")
        subprocess.run(copy_cmd, check=True)

    def build_host(host):
        ssh_port = host_port_map.get(host, 22)
        build_cmd = storage_build_cmd if host in storage_hosts else default_build_cmd
        logging.info(f"build remote project start: {host}")
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            client.connect(hostname=host, port=ssh_port, username=deploy_user, password=deploy_passwd, timeout=30, banner_timeout=60)
            stdin, stdout, stderr = client.exec_command(build_cmd)
            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                err = stderr.read().decode('utf-8', errors='ignore')
                raise RuntimeError(f"remote build failed, exit={exit_status}, err={err}")
        finally:
            client.close()

    default_build_cmd = f"cd {remote_build_dir} && rm -rf * && cmake .. && make -j10"
    storage_build_cmd = f"cd {remote_build_dir} && rm -rf * && cmake .. && make storage_pool remote_node -j10"
    run_parallel_hosts(deploy_hosts, cleanup_host, "cleanup")
    run_parallel_hosts(deploy_hosts, copy_host, "copy")
    run_parallel_hosts(deploy_hosts, build_host, "build")


def ssh_client(host, port , user, passwd):
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        if remote_key_path and os.path.exists(remote_key_path):
            key = paramiko.RSAKey.from_private_key_file(remote_key_path)
            c.connect(hostname=host, port=port, username=user, pkey=key, timeout=10)
        else:
            c.connect(hostname=host, port=port, username=user, password=passwd, timeout=10)
    except paramiko.ssh_exception.AuthenticationException:
        logging.error(f'SSH authentication failed for {user}@{host}:{port}. Set REMOTE_HOST/REMOTE_PORT/REMOTE_USER/REMOTE_PASS or REMOTE_KEY env correctly.')
        raise
    except Exception as e:
        logging.error(f'SSH connection to {host}:{port} failed: {e}')
        raise
    return c

def ssh_exec(client, cmds, verbose=True):
    outs = []       #存储每个命令的执行结果(输出)
    for cmd in cmds:
        stdin, stdout, stderr = client.exec_command(cmd)
        try:
            out = stdout.read().decode()
            err = stderr.read().decode()
            if verbose:
                logging.info(out.strip())
                if err.strip():
                    logging.info(err.strip())
            outs.append((out, err))
        finally:
            try:
                stdin.close()
                stdout.close()
                stderr.close()
            except Exception:
                pass
    return outs

def sftp_put(client, local_path, remote_path):
    import os, posixpath
    if not os.path.exists(local_path):
        logging.error(f"Local missing: {local_path}")
        raise FileNotFoundError(local_path)
    remote_dir = posixpath.dirname(remote_path)
    try:
        ssh_exec(client, [f"mkdir -p {remote_dir}"], verbose=False)
    except Exception as e:
        logging.error(f"Remote mkdir failed for {remote_dir}: {e}")
    try:
        sftp = client.open_sftp()
        try:
            sftp.put(local_path, remote_path)
        finally:
            sftp.close()
    except Exception as e:
        logging.error(f"sftp_put failed local={local_path} remote={remote_path} err={e}")
        try:
            ssh_exec(client, [f"ls -ld {remote_dir}", f"ls -l {remote_path}"], verbose=True)
        except Exception:
            pass
        raise

def sftp_get(client, remote_path, local_path):
    sftp = client.open_sftp()
    sftp.get(remote_path, local_path)
    sftp.close()


def distribute_config_to_node(client):
    configs = ['smallbank_config.json', 'ycsb_config.json', 'compute_node_config.json' , 'storage_node_config.json' , 'remote_server_config.json']
    remote_cfg_dir = os.path.join(remote_workspace, 'config')
    ssh_exec(client, [f"mkdir -p {remote_cfg_dir}"], verbose=False)
    sftp = client.open_sftp()
    try:
        for cfg in configs:
            remote_cfg = os.path.join(remote_cfg_dir, cfg)
            local_cfg = os.path.join(workspace, 'config', cfg)
            if not os.path.exists(local_cfg):
                raise FileNotFoundError(local_cfg)
            sftp.put(local_cfg, remote_cfg)
    finally:
        sftp.close()

def rebuild_compute_server(client, build_dir):
    cmds = [
        f"cd {build_dir} && cmake ..",
        f"cd {build_dir} && make -j14"
    ]
    ssh_exec(client, cmds , verbose=True)

def kill_remote_services(client, build_dir):
    cmds = [
        f"pkill -f remote_node || true",
        f"pkill -f storage_pool || true"
    ]
    ssh_exec(client, cmds, verbose=False)

# 检查服务名为 name 的服务有没有真的跑起来
def check_service_running(client, name):
    stdin, stdout, stderr = client.exec_command(f"pgrep {name}")
    try:
        out = stdout.read().decode().strip()
        return out != ''
    finally:
        try:
            stdin.close()
            stdout.close()
            stderr.close()
        except Exception:
            pass

def remote_file_contains(client, file_path, keyword):
    cmd = f"test -f {shlex.quote(file_path)} && grep -Fq -- {shlex.quote(keyword)} {shlex.quote(file_path)}"
    stdin, stdout, stderr = client.exec_command(cmd)
    try:
        return stdout.channel.recv_exit_status() == 0
    finally:
        try:
            stdin.close()
            stdout.close()
            stderr.close()
        except Exception:
            pass

def read_remote_file_tail(client, file_path, lines=80):
    cmd = f"if test -f {shlex.quote(file_path)}; then tail -n {int(lines)} {shlex.quote(file_path)}; fi"
    stdin, stdout, stderr = client.exec_command(cmd)
    try:
        out = stdout.read().decode('utf-8', errors='ignore')
        return out
    finally:
        try:
            stdin.close()
            stdout.close()
            stderr.close()
        except Exception:
            pass

def wait_remote_log_keyword(client, file_path, keyword, timeout_sec=90, poll_interval_sec=1):
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if remote_file_contains(client, file_path, keyword):
            return True
        time.sleep(poll_interval_sec)
    return False

def wait_remote_services_ready(client, storage_log, remote_log, timeout_sec=180, poll_interval_sec=1):
    storage_keyword = "Storage Server Start Over , Ready For Connect...."
    remote_keyword = "Remote Server Started at port 31508"
    deadline = time.time() + timeout_sec
    last_report_ts = time.time()
    while time.time() < deadline:
        storage_ready = remote_file_contains(client, storage_log, storage_keyword)
        remote_ready = remote_file_contains(client, remote_log, remote_keyword)
        if storage_ready and remote_ready:
            return True, True
        now = time.time()
        if now - last_report_ts >= 5:
            logging.info(
                f"Waiting remote ready... storage_ready={storage_ready} remote_ready={remote_ready}"
            )
            last_report_ts = now
        time.sleep(poll_interval_sec)
    storage_ready = remote_file_contains(client, storage_log, storage_keyword)
    remote_ready = remote_file_contains(client, remote_log, remote_keyword)
    return storage_ready, remote_ready

def launch_remote_background(cmd, name):
    c = ssh_client(remote_server_host, remote_server_port, remote_server_user, remote_server_passwd)
    try:
        stdin, stdout, stderr = c.exec_command(cmd, timeout=10)
        time.sleep(0.2)
        if stdout.channel.exit_status_ready():
            exit_code = stdout.channel.recv_exit_status()
            err = ""
            if stderr.channel.recv_stderr_ready():
                try:
                    err = stderr.channel.recv_stderr(4096).decode('utf-8', errors='ignore')
                except Exception:
                    err = ""
            if exit_code != 0:
                logging.error(f"launch {name} failed with code={exit_code}, err={err}")
                return False
        try:
            stdin.close()
            stdout.close()
            stderr.close()
        except Exception:
            pass
        return True
    finally:
        c.close()

# 启动 remote_sver 和 storage_server
def start_remote_services_checked(client, primary_build_dir, workload_name, fallback_build_dir=None):
    # 先把之前的 remote_server 和 storage_server 进程给关了
    ssh_exec(client, ["pkill -f remote_node"], verbose=True)
    ssh_exec(client, ["pkill -f storage_pool"], verbose=True)
    logging.info('Close Remote Service Success')
    time.sleep(1)

    storage_log = f"{primary_build_dir}/storage_server/storage_boot.log"
    remote_log = f"{primary_build_dir}/remote_server/remote_boot.log"
    ssh_exec(client, [f"rm -f {storage_log}", f"rm -f {remote_log}"], verbose=False)
    # A missing snapshot must never be mistaken for the previous trial's stats.
    ssh_exec(client, [f"rm -f {shlex.quote(primary_build_dir + '/storage_server/storage_timing_stats.txt')}"], verbose=False)

    cmd_storage = (
        f"bash -lc 'cd {shlex.quote(primary_build_dir)}/storage_server && "
        f"rm -f LOG_FILE && nohup ./storage_pool {shlex.quote(workload_name)} "
        f"> {shlex.quote(storage_log)} 2>&1 < /dev/null &'"
    )
    cmd_remote = (
        f"bash -lc 'cd {shlex.quote(primary_build_dir)}/remote_server && "
        f"nohup ./remote_node {shlex.quote(workload_name)} "
        f"> {shlex.quote(remote_log)} 2>&1 < /dev/null &'"
    )
    
    logging.info(f"Storage Command: {cmd_storage}")
    logging.info(f"Remote Command: {cmd_remote}")

    logging.info("Launching storage_pool ...")
    storage_started = launch_remote_background(cmd_storage, "storage_pool")
    time.sleep(1)
    logging.info("Launching remote_node ...")
    remote_started = launch_remote_background(cmd_remote, "remote_node")
    if not storage_started or not remote_started:
        logging.error("failed to launch remote background services")
        exit(-1)
    logging.info("Remote services launch command sent, waiting for ready keywords...")

    storage_ready, remote_ready = wait_remote_services_ready(
        client,
        storage_log,
        remote_log,
        timeout_sec=1800  # Increase timeout to 30 minutes for large data generation
    )

    if not storage_ready:
        storage_tail = read_remote_file_tail(client, storage_log, lines=120)
        logging.warning(f"storage_pool ready keyword not found in {storage_log} after 1800s, continuing anyway.\n{storage_tail}")
    if not remote_ready:
        remote_tail = read_remote_file_tail(client, remote_log, lines=120)
        logging.warning(f"remote_node ready keyword not found in {remote_log} after 1800s, continuing anyway.\n{remote_tail}")
    
    # 检查是否启动成功
    ok_storage = check_service_running(client, "storage_pool")
    ok_remote = check_service_running(client, "remote_node")
    
    if not ok_storage:
        logging.error("storage_pool failed to start")
        exit(-1)
    if not ok_remote:
        logging.error("remote_node failed to start")
        exit(-1)
    if not storage_ready or not remote_ready:
        logging.warning("remote services process exists but startup ready keywords not observed, proceeding as processes are running")
        
    return True

def kill_compute(client):
    ssh_exec(client, ["pkill -f compute_server"], verbose=False)

def ensure_compute_killed(client):
    ssh_exec(client, ["pkill compute_server"], verbose=False)

def remove_remote_compute_outputs(client, build_dir):
    compute_dir = os.path.join(build_dir, "compute_server")
    ssh_exec(client, [f"rm -f {compute_dir}/result.txt {compute_dir}/delay_fetch_remote.txt {compute_dir}/tps.log"], verbose=False)

def remove_remote_compute_logs(client, build_dir):
    compute_dir = os.path.join(build_dir, "compute_server")
    ssh_exec(client, [f"rm -f {compute_dir}/computeserver.log*"], verbose=False)

def remove_local_compute_logs():
    compute_dir = os.path.join(workspace, "build", "compute_server")
    if not os.path.isdir(compute_dir):
        return
    for file_name in os.listdir(compute_dir):
        if not file_name.startswith("computeserver.log"):
            continue
        file_path = os.path.join(compute_dir, file_name)
        try:
            os.remove(file_path)
        except FileNotFoundError:
            pass
        except Exception as e:
            logging.warning(f"remove local compute log failed: {file_path}, err={e}")

def cleanup_compute_logs_all_nodes(build_dir):
    for i, host in enumerate(compute_server_hostnames):
        client = None
        try:
            client = ssh_client(host, compute_server_ports[i], compute_server_usernames[i], compute_server_passwords[i])
            remove_remote_compute_logs(client, build_dir)
        except Exception as e:
            logging.warning(f"cleanup remote compute logs failed: node={i} host={host} err={e}")
        finally:
            if client:
                client.close()
    remove_local_compute_logs()

def collect_compute_debug_snapshot(client, build_dir):
    compute_dir = os.path.join(build_dir, "compute_server")
    cmd = (
        "bash -lc '"
        "echo \"==== pgrep compute_server ====\"; "
        "pgrep -af compute_server || true; "
        "echo \"==== top compute threads ====\"; "
        "PIDS=$(pgrep -d, compute_server || true); "
        "if [ -n \"$PIDS\" ]; then top -b -n1 -H -p \"$PIDS\" | head -n 80; fi; "
        "for p in $(pgrep compute_server); do "
        "echo \"==== pid:$p state ====\"; "
        "cat /proc/$p/wchan 2>/dev/null || true; "
        "echo \"==== pid:$p stack ====\"; "
        "cat /proc/$p/stack 2>/dev/null || true; "
        "if command -v gdb >/dev/null 2>&1; then "
        "echo \"==== pid:$p gdb bt (short) ====\"; "
        "timeout 15s gdb -q -batch -p $p -ex \"set pagination off\" -ex \"info threads\" -ex \"thread apply all bt 8\" 2>/dev/null | head -n 260; "
        "fi; "
        "done; "
        f"echo \"==== ls {compute_dir} ====\"; ls -l {compute_dir} || true; "
        f"echo \"==== tail compute stdout log ====\"; tail -n 80 {compute_dir}/compute_server_*.out 2>/dev/null || true; "
        "'"
    )
    stdin, stdout, stderr = client.exec_command(cmd, timeout=30)
    out = stdout.read().decode(errors='replace')
    err = stderr.read().decode(errors='replace')
    return out + ("\n" + err if err else "")

def start_compute_blocking(client, build_dir, args, log_path, tps_output, cancel_event=None):
    compute_dir = os.path.join(build_dir, "compute_server")
    inner = (f"set -o pipefail; cd {shlex.quote(compute_dir)} && "
             f"./compute_server {args} 2>&1 | tee {shlex.quote(log_path)}")
    stdin, stdout, stderr = client.exec_command('bash -lc ' + shlex.quote(inner))
    out_buf, err_buf = deque(maxlen=256), deque(maxlen=256)
    pending_line = ''
    def consume(chunk):
        nonlocal pending_line
        out_buf.append(chunk)
        pending_line += chunk
        while '\n' in pending_line:
            line, pending_line = pending_line.split('\n', 1)
            if line.startswith('[TPS] '):
                tps_output.write(line[len('[TPS] '):] + '\n')
                tps_output.flush()
    while not stdout.channel.exit_status_ready():
        if cancel_event is not None and cancel_event.is_set():
            ensure_compute_killed(client)
            raise RuntimeError('Another node failed; cancelled this case on all nodes')
        if stdout.channel.recv_ready():
            consume(stdout.channel.recv(4096).decode(errors='replace'))
        if stderr.channel.recv_ready():
            err_buf.append(stderr.channel.recv(4096).decode(errors='replace'))
        time.sleep(0.2)
    consume(stdout.read().decode(errors='replace'))
    out = ''.join(out_buf).strip()
    err = (''.join(err_buf) + stderr.read().decode(errors='replace')).strip()
    exit_code = stdout.channel.recv_exit_status()

    logging.info(f'Compute Server {args} exit with {exit_code}')

    if exit_code != 0:
        if out:
            logging.error(f"STDOUT:\n{out}")
        if err:
            logging.error(f"STDERR:\n{err}")
    else:
        # 成功时也可以选择打印部分日志，或者保持静默
        if err:
            logging.warning(f"STDERR (warning):\n{err}")
    return exit_code, out, err

def wait_compute_finish(client, timeout_sec, build_dir):
    return True

def fetch_runtime_logs(client, node_idx, result_base_dir, build_dir):
    node_dir = Path(result_base_dir, f'node{node_idx}')
    node_dir.mkdir(parents=True, exist_ok=True)
    for remote_name, local_name in (('tps.log', 'tps.log'), (f'compute_server_{node_idx}.out', 'compute.out')):
        try:
            sftp_get(client, f'{build_dir}/compute_server/{remote_name}', str(node_dir / local_name))
        except OSError:
            logging.warning('Missing runtime log: node%s/%s', node_idx, remote_name)


def fetch_node_results(client, node_idx, result_base_dir, build_dir, header=None):
    node_dir = os.path.join(result_base_dir, f"node{node_idx}")
    os.makedirs(node_dir, exist_ok=True)
    rp1 = f"{build_dir}/compute_server/result.txt"
    rp2 = f"{build_dir}/compute_server/delay_fetch_remote.txt"
    lp1 = os.path.join(node_dir, "result.txt")
    lp2 = os.path.join(node_dir, "delay_fetch_remote.txt")
    try:
        # 把远程的 result.txt 上传到本地来
        sftp_get(client, rp1, lp1)
    except Exception as e:
        raise FileNotFoundError(f"fetch node{node_idx} result failed: {rp1}") from e
    try:
        sftp_get(client, rp2, lp2)
    except Exception:
        pass

    # Read back configuration AFTER CLI overrides; never save SSH credentials.
    sftp = client.open_sftp()
    try:
        effective = {}
        for name in ('compute_node_config.json', f"{header['bench_name']}_config.json"):
            with sftp.open(f"{remote_workspace}/config/{name}") as source:
                effective[name] = redact(json.loads(source.read()))
        Path(node_dir, 'effective_config.json').write_text(json.dumps(effective, indent=2) + '\n')
    finally:
        sftp.close()


def fetch_storage_stats(result_base_dir, build_dir):
    out_dir = Path(result_base_dir, 'storage')
    out_dir.mkdir(exist_ok=True)
    client = ssh_client(remote_server_host, remote_server_port, remote_server_user, remote_server_passwd)
    try:
        try:
            sftp_get(client, f"{build_dir}/storage_server/storage_timing_stats.txt",
                     str(out_dir / 'storage_timing_stats.txt'))
        except (OSError, IOError):
            logging.warning('No fresh storage stats; stats.log will show N/A')
    finally:
        client.close()
    
def update_hot_rate(client, bench_name, hot_rate):
    if bench_name == "smallbank":
        cfg_name = "smallbank_config.json"
        section = "smallbank"
        key = "num_hot_rate"
    elif bench_name == "ycsb":
        cfg_name = "ycsb_config.json"
        section = "ycsb"
        key = "TX_HOT"
    else:
        return
        
    remote_cfg = os.path.join(remote_workspace, 'config', cfg_name)
    sftp = client.open_sftp()
    
    try:
        rf = sftp.open(remote_cfg, 'r')
        content = rf.read().decode('utf-8')
        rf.close()
    except Exception:
        sftp.close()
        return
        
    data = json.loads(content)
    if section in data:
        data[section][key] = int(hot_rate)
    
    tmp_remote = os.path.join(remote_workspace, 'config', f'.{cfg_name}.tmp')
    wf = sftp.open(tmp_remote, 'w')
    wf.write(json.dumps(data, indent=2))
    wf.flush()
    wf.close()
    sftp.close()
    ssh_exec(client, [f"mv {tmp_remote} {remote_cfg}"], verbose=False)

def update_access_pattern(client, bench_name, use_zipfian_mode, pattern_value):
    if bench_name == "smallbank":
        cfg_name = "smallbank_config.json"
        section = "smallbank"
        hot_key = "num_hot_rate"
    elif bench_name == "ycsb":
        cfg_name = "ycsb_config.json"
        section = "ycsb"
        hot_key = "TX_HOT"
    else:
        return

    remote_cfg = os.path.join(remote_workspace, 'config', cfg_name)
    sftp = client.open_sftp()

    try:
        rf = sftp.open(remote_cfg, 'r')
        content = rf.read().decode('utf-8')
        rf.close()
    except Exception:
        sftp.close()
        return

    data = json.loads(content)
    if section in data:
        data[section]["use_zipfian"] = 1 if use_zipfian_mode else 0
        if use_zipfian_mode:
            data[section]["zipf_theta"] = float(pattern_value)
        else:
            data[section][hot_key] = int(pattern_value)

    tmp_remote = os.path.join(remote_workspace, 'config', f'.{cfg_name}.tmp')
    wf = sftp.open(tmp_remote, 'w')
    wf.write(json.dumps(data, indent=2))
    wf.flush()
    wf.close()
    sftp.close()
    ssh_exec(client, [f"mv {tmp_remote} {remote_cfg}"], verbose=False)

def update_attempt_num(client, bench_name, attempt_num):
    if bench_name == "smallbank":
        cfg_name = "smallbank_config.json"
        section = "smallbank"
        key = "attempted_num"
    elif bench_name == "ycsb":
        cfg_name = "ycsb_config.json"
        section = "ycsb"
        key = "attempted_num"
    elif bench_name == "tpcc":
        cfg_name = "tpcc_config.json"
        section = "tpcc"
        key = "attempted_num"
    else:
        return
        
    remote_cfg = os.path.join(remote_workspace, 'config', cfg_name)
    sftp = client.open_sftp()
    
    try:
        rf = sftp.open(remote_cfg, 'r')
        content = rf.read().decode('utf-8')
        rf.close()
    except Exception:
        sftp.close()
        return
        
    data = json.loads(content)
    if section in data:
        data[section][key] = int(attempt_num)
    
    tmp_remote = os.path.join(remote_workspace, 'config', f'.{cfg_name}.tmp')
    wf = sftp.open(tmp_remote, 'w')
    wf.write(json.dumps(data, indent=2))
    wf.flush()
    wf.close()
    sftp.close()
    ssh_exec(client, [f"mv {tmp_remote} {remote_cfg}"], verbose=False)

def update_remote_compute_config(client, machine_id, hybrid_skew_threshold=None, hot_key_top_n=None, lock_mode=None, run_case=None):
    remote_cfg = os.path.join(remote_workspace, 'config', 'compute_node_config.json')
    sftp = client.open_sftp()
    rf = sftp.open(remote_cfg, 'r')
    content = rf.read().decode('utf-8')
    rf.close()
    data = json.loads(content)
    if 'local_compute_node' not in data:
        data['local_compute_node'] = {}
    data['local_compute_node']['machine_id'] = int(machine_id)
    if run_case:
        for key in ('run_mode', 'run_seconds'):
            data['local_compute_node']['benchmark_' + key] = run_case[key]
    if lock_mode is not None:
        # handler.cc 的 LoadLockWaitConfig 从 local_compute_node.lock_mode 读取，非法值会 assert
        data['local_compute_node']['lock_mode'] = int(lock_mode)
    if hybrid_skew_threshold is not None:
        data['hybrid_skew_threshold'] = float(hybrid_skew_threshold)
    if hot_key_top_n is not None:
        data['hot_key_top_n'] = int(hot_key_top_n)
    tmp_remote = os.path.join(remote_workspace, 'config', '.compute_node_config.json.tmp')
    wf = sftp.open(tmp_remote, 'w')
    wf.write(json.dumps(data, indent=2))
    wf.flush()
    wf.close()
    sftp.close()
    ssh_exec(client, [f"mv {tmp_remote} {remote_cfg}"], verbose=False)

def read_node_matrix_kv(path):
    """
    Reads result file and returns a dictionary of {key: value} and a list of keys in order.
    Also returns a list of values for backward compatibility if needed, but we prefer KV.
    """
    if not os.path.exists(path):
        return {}, []
    
    data = {}
    keys_order = []
    
    with open(path, 'r', encoding='utf-8') as f:
        for line_idx, line in enumerate(f):
            line = line.strip()
            if not line: continue
            
            key = None
            val = None
            
            # Handle key=value format
            if '=' in line:
                parts = line.split('=')
                if len(parts) == 2:
                    key = parts[0].strip()
                    val_str = parts[1].strip()
                    try:
                        # Handle multiple values if present (though usually one)
                        sub_parts = val_str.split()
                        if len(sub_parts) == 1:
                            val = float(sub_parts[0])
                        else:
                            val = [float(x) for x in sub_parts]
                    except Exception:
                        pass
            else:
                # Handle raw value format (fallback using line index as implicit key if needed, or just "line_N")
                # But for aggregation, we need alignment. 
                # If we mix raw and kv, it's messy.
                # Let's assume if it's raw numbers, we treat them as sequential values
                try:
                    parts = line.split()
                    if len(parts) == 1:
                        val = float(parts[0])
                    else:
                        val = [float(x) for x in parts]
                    # Generate a key based on line number for alignment if no key
                    key = f"line_{line_idx+1}" 
                except Exception:
                    pass
            
            if key and val is not None:
                data[key] = val
                keys_order.append(key)
                
    return data, keys_order

def aggregate_results(result_base_dir, node_count):
    # Mapping of known keys that should be SUMMED
    # All other keys will be AVERAGED by default
    known_sum_keys = {
        'throughput', 
        'fetch_from_remote_count',
        'fetch_from_storage_count',
        'fetch_from_local_count',
        'evicted_pages_count',
        'wait_log_flush_count', 'ownership_transfer_count', 'ownership_transfer_time_total',
        'notify_push_page_count', 'notify_push_page_time',
        'wait_log_flush_time', 'wait_log_flush_push_page_time', 'wait_log_flush_evict_page_time', 'wait_log_flush_tx_over_time',
        'log_flush_count', 'log_flush_time', 'log_flush_total_batch',

        'commit_log_count', 'prepare_log_count', 'backup_log_count',
        'update_log_count',
        'lazy_getpage_dire', 'lazy_getpage_wait', 'lazy_2RTT_count', 'lazy_3RTT_count',
        'tx_begin_time', 'tx_exe_time', 'tx_fetch_exe_time', 'tx_commit_time', 'tx_abort_time', 'TxWaitAbortLogTime',
        'wait_commit_log_time', 'wait_prepare_log_time', 'wait_backup_log_time',
        'tx_write_commit_log_time', 'tx_write_commit_log_time2',
        'tx_write_prepare_log_time', 'tx_write_backup_log_time',
        'tx_get_timestamp_time1', 'tx_get_timestamp_time2',
        'twopc_remote_fetch_time', 'twopc_remote_fetch_count', 'fetch_storage_page_time',
        'single_txn_count', 'distribute_txn_count',
        'hybrid_2pc_commit_count', 'hybrid_lazy_commit_count'
    }
    
    # We will collect all data into a structure: {key: [val_node0, val_node1, ...]}
    aggregated_data = {}
    all_keys_order = [] # To preserve output order from the first node
    
    for i in range(node_count):
        p = os.path.join(result_base_dir, f"node{i}", "result.txt")
        node_data, node_keys = read_node_matrix_kv(p)
        
        if i == 0:
            all_keys_order = node_keys
            
        for k, v in node_data.items():
            if k not in aggregated_data:
                aggregated_data[k] = []
            aggregated_data[k].append(v)
            
    if not aggregated_data:
        return [], []

    # Now aggregate
    final_rows = []
    final_keys = []
    
    # Use the order from the first node (or collected keys)
    # If some nodes miss keys, they will just contribute fewer values (we can handle len < node_count)
    
    for k in all_keys_order:
        vals = aggregated_data.get(k, [])
        if not vals:
            continue
            
        # Check if values are lists (like per-type try/commit counts)
        is_list_val = isinstance(vals[0], list)
        
        if is_list_val:
            # For lists (like [try, commit]), we usually SUM them element-wise
            val_len = len(vals[0])
            summed_list = [0.0] * val_len
            
            for v_list in vals:
                if isinstance(v_list, list) and len(v_list) == val_len:
                    for idx, v in enumerate(v_list):
                        summed_list[idx] += v
            
            final_rows.append(summed_list)
            final_keys.append(k)
            
        else:
            # Single value
            # Decide SUM or AVG
            # Check partial match for keys ending with known sum keys (e.g. "something_throughput")
            # or just exact match. Given we control run.cc, exact match is preferred, 
            # but legacy keys might need care.
            should_sum = False
            if k in known_sum_keys:
                should_sum = True
            elif k.startswith('line_'):
                # Legacy line_X keys: check our hardcoded map
                # But wait, we moved away from line_X in run.cc
                # This is only for reading OLD result files.
                # If reading old files, keys are line_2, line_4 etc.
                if k in {'line_2', 'line_4', 'line_5', 'line_6', 'line_7'}:
                    should_sum = True
            
            if should_sum:
                val = sum(vals)
            else:
                val = sum(vals) / len(vals)
            final_rows.append([val])
            final_keys.append(k)

    return final_rows, final_keys

def write_summary(result_base_dir, summary_tuple, header=None):
    # summary_tuple is (rows, keys)
    if isinstance(summary_tuple, list):
        # Backward compatibility if someone passes just rows
        summary = summary_tuple
        keys = []
    else:
        summary, keys = summary_tuple

    p = os.path.join(result_base_dir, "result.txt")
    with open(p, 'w', encoding='utf-8') as f:
        if header:
            for k, v in header.items():
                f.write(f"{k}={v}\n")
        
        for i, row in enumerate(summary):
            val_str = " ".join(str(x) for x in row)
            if i < len(keys) and keys[i] and not keys[i].startswith('line_'):
                f.write(f"{keys[i]}={val_str}\n")
            else:
                f.write(f"{val_str}\n")


def write_header_to_path(file_path, header):
    if not os.path.exists(file_path):
        return
    with open(file_path, 'r', encoding='utf-8') as fr:
        content = fr.read()
    with open(file_path, 'w', encoding='utf-8') as fw:
        for k, v in header.items():
            fw.write(f"{k}={v}\n")
        fw.write(content)

def run_case_mode(result_dir, case, mode, settings, run_fingerprint):
    bench_name = case['workload']
    current_attempt_num = case['attempts']
    thread_num = case['threads']
    local_ratio = cr = case['local_ratio']
    use_zipfian = case['access_pattern'] == 'theta'
    pattern_value = case['pattern_value']
    write_txn_ratio = case['write_txn_ratio']
    hybrid_skew_threshold = case['hybrid_skew_threshold']
    hot_key_top_n = case['hot_key_top_n']
    lock_mode = case['lock_mode']
    combo_dir_name = case['directory']
    combo_dir = str(plan.result_path(result_dir, case, mode))
    build_dir = remote_build_dir
    logging.info(f"Set Account Success for {bench_name}")
    try:
        cfg_clients = [ssh_client(h, compute_server_ports[i], compute_server_usernames[i], compute_server_passwords[i]) for i, h in enumerate(compute_server_hostnames)]
        rs_client = ssh_client(remote_server_host, remote_server_port , remote_server_user, remote_server_passwd)
    except Exception:
        logging.error("SSH connect failed; check credentials or REMOTE_* env vars.")
        raise
    cfg_clients.append(rs_client)

    for c in cfg_clients:
        distribute_config_to_node(c)
        # rebuild_compute_server(c, build_dir)
        c.close()
    logging.info("Config Transfer And Build Over")

    # 重新连接 rs_client 用于启动服务
    try:
        rs_client = ssh_client(remote_server_host, remote_server_port , remote_server_user, remote_server_passwd)
    except Exception:
        logging.error("SSH connect failed when starting remote services; aborting this run.")
        raise
    # 启动 remote_server 和 storage_server
    ok = start_remote_services_checked(rs_client, remote_build_dir, bench_name, fallback_build_dir=os.path.join("/usr/local/exper/Hybrid_Cloud_MP", "build"))
    logging.info("Start Remote Over")
    rs_client.close()
    if not ok:
        logging.error("remote services failed to start; check build_dir paths and binaries")
        raise RuntimeError('remote services failed to start')

    os.makedirs(combo_dir, exist_ok=True)

    logging.info(f"Creating Dir , {combo_dir}")
    threads = []
    thread_errors = []
    thread_error_lock = threading.Lock()
    cancel_event = threading.Event()
    def run_node(i, host, port, out_dir):
        try:
            remote_client = ssh_client(remote_server_host, remote_server_port, remote_server_user, remote_server_passwd)
            ok_storage = check_service_running(remote_client, "storage_pool")
            ok_remote = check_service_running(remote_client, "remote_node")
            remote_client.close()
            if (not ok_remote or not ok_storage):
                raise RuntimeError("try to starting computeserver , but remote not ok")

            client = ssh_client(host, port, compute_server_usernames[i], compute_server_passwords[i])
            try:
                ensure_compute_killed(client)
                remove_remote_compute_outputs(client, build_dir)
                update_remote_compute_config(client, i, hybrid_skew_threshold=hybrid_skew_threshold, hot_key_top_n=hot_key_top_n, lock_mode=lock_mode, run_case=case)
                update_access_pattern(client, bench_name, use_zipfian, pattern_value)
                update_attempt_num(client, bench_name, current_attempt_num)

                args = f"{bench_name} {mode} {thread_num} {write_txn_ratio} {local_ratio} {i}"
                log_path = f"{build_dir}/compute_server/compute_server_{i}.out"
                time.sleep(handshake_stagger_sec * (i))
                logging.info(f"Starting ComputeServer , hostname = {host} , args = {args}")
                node_dir = Path(out_dir, f'node{i}')
                node_dir.mkdir(parents=True, exist_ok=True)
                try:
                    with (node_dir / 'tps.log').open('w') as tps_output:
                        exit_code, _, err = start_compute_blocking(client, build_dir, args, log_path, tps_output, cancel_event=cancel_event)
                finally:
                    fetch_runtime_logs(client, i, out_dir, build_dir)
                if exit_code != 0:
                    raise RuntimeError(f"compute_server exited with code {exit_code}, detail={err[-1200:]}")
                logging.info("Running ComputeServer Over")
                header = {
                    "round": 0,  # Retained only in the legacy summary schema.
                    "case_id": case["case_id"],
                    "bench_name": bench_name,
                    "system_name": mode,
                    "local_ratio": cr,
                    "local_txn_ratio": local_ratio,
                    "use_zipfian": use_zipfian,
                    "zipf_theta": pattern_value if use_zipfian else "",
                    "tx_hot": pattern_value if not use_zipfian else "",
                    "thread_num": thread_num,
                    "write_txn_ratio": write_txn_ratio,
                    "hybrid_skew_threshold": hybrid_skew_threshold,
                    "hot_key_top_n": hot_key_top_n,
                    "node_count": len(compute_server_hostnames),
                    "combo_path": out_dir
                }
                fetch_node_results(client, i, out_dir, build_dir, header)
            finally:
                client.close()
        except Exception as e:
            cancel_event.set()
            logging.error(f"node {i} failed for {combo_dir_name} mode={mode}: {e}")
            with thread_error_lock:
                thread_errors.append(f"node{i}:{e}")

    # 让所有的计算节点，都去跑 computeserver
    for idx, host in enumerate(compute_server_hostnames):
        t = threading.Thread(target=run_node, args=(idx, host, compute_server_ports[idx], combo_dir))
        threads.append(t)
        t.start()

    try:
        for t in threads:
            t.join()
    except BaseException:
        cancel_event.set()
        for t in threads:
            t.join()
        raise

    if thread_errors:
        raise RuntimeError(f"{combo_dir_name} mode={mode} failed: {'; '.join(thread_errors)}")

    fetch_storage_stats(combo_dir, build_dir)

    combo_summary, combo_keys = aggregate_results(combo_dir, len(compute_server_hostnames))
    combo_header = {
        "round": 0,  # Retained only in the legacy summary schema.
        "case_id": case["case_id"],
        "bench_name": bench_name,
        "system_name": mode,
        "local_ratio": cr,
        "local_txn_ratio": local_ratio,
        "use_zipfian": use_zipfian,
        "zipf_theta": pattern_value if use_zipfian else "",
        "tx_hot": pattern_value if not use_zipfian else "",
        "thread_num": thread_num,
        "write_txn_ratio": write_txn_ratio,
        "hybrid_skew_threshold": hybrid_skew_threshold,
        "hot_key_top_n": hot_key_top_n,
        "lock_mode": lock_mode,
        "node_count": len(compute_server_hostnames),
        "combo_path": combo_dir
    }
    # write dual outputs: human-friendly and machine-friendly
    mat_path = os.path.join(combo_dir, "summary_matrix.txt")
    with open(mat_path, 'w', encoding='utf-8') as mf:
        for row in combo_summary:
            mf.write(" ".join(str(x) for x in row) + "\n")
    # human
    human_path = os.path.join(combo_dir, "summary_human.txt")

    # Convert aggregated results to a dictionary for easier lookup
    summary_dict = {}
    for i, k in enumerate(combo_keys):
        summary_dict[k] = combo_summary[i]

    with open(human_path, 'w', encoding='utf-8') as hf:
        for k, v in combo_header.items():
            hf.write(f"{k}={v}\n")

        # Write all metrics from the dictionary directly, or filter/order if needed
        # The previous logic had hardcoded order. Let's try to preserve it but use keys.

        # Standard metrics
        std_keys = [
            'total_time_seconds', 'throughput', 'lock_ratio',
            'fetch_from_remote_count', 'fetch_from_storage_count', 'fetch_from_local_count',
            'evicted_pages_count'
        ]

        for k in std_keys:
            # Fallback to index-based if key not found (legacy support)
            val = 0
            if k in summary_dict:
                val = summary_dict[k][0]
            else:
                # Try finding it in combo_keys with line_ prefix fallback?
                # Or just skip/zero.
                pass
            hf.write(f"{k}={val}\n")

        # Transaction Types
        if bench_name == 'smallbank':
            types = ['Amalgamate','Balance','DepositChecking','SendPayment','TransactSaving','WriteCheck']
            type_names = ['Amalgamate','Balance','DepositChecking','SendPayment','TransactSaving','WriteCheck']
        elif bench_name == 'ycsb':
            types = ['ycsb_tx']
            type_names = ['ycsb_tx0']
        elif bench_name == 'tpcc':
            types = ['NewOrder','Payment','OrderStatus','Delivery','StockLevel']
            type_names = ['NewOrder','Payment','OrderStatus','Delivery','StockLevel']
        else:
            types = []
            type_names = []

        for t in type_names:
            key_try_commit = f"{t}_try_commit"
            if key_try_commit in summary_dict:
                vals = summary_dict[key_try_commit]
                if len(vals) >= 2:
                    hf.write(f"{t}_try={vals[0]}\n")
                    hf.write(f"{t}_commit={vals[1]}\n")
            else:
                hf.write(f"{t}_try=0\n")
                hf.write(f"{t}_commit=0\n")

        for t in type_names:
            key_rr = f"{t}_rollback_rate"
            val = 0
            if key_rr in summary_dict:
                val = summary_dict[key_rr][0]
            hf.write(f"{t}_rollback_rate={val}\n")

        # Stages
        stages = [
            'tx_begin_time','tx_exe_time','wait_log_flush_time',
            'wait_log_flush_push_page_time','wait_log_flush_evict_page_time','wait_log_flush_tx_over_time',
            'wait_commit_log_time','wait_prepare_log_time','wait_backup_log_time',
            'wait_log_flush_count','ownership_transfer_count','ownership_transfer_time_total','notify_push_page_count','notify_push_page_time','log_flush_count','log_flush_time','log_flush_avg_batch',
            'log_flush_max_batch','log_flush_total_batch',
            'tx_commit_time','tx_abort_time','TxWaitAbortLogTime',
            'fetch_storage_page_time',
            'tx_fetch_exe_time',
            'tx_commit_fetch_page_time',

            'commit_log_count','prepare_log_count','backup_log_count',
            'tx_get_timestamp_time1','tx_get_timestamp_time2',
            'tx_write_commit_log_time', 'tx_write_commit_log_time2',
            'tx_write_prepare_log_time', 'tx_write_backup_log_time',
            'update_log_count',
            'single_txn_count', 'distribute_txn_count',
            'hybrid_2pc_commit_count', 'hybrid_lazy_commit_count',
            'ownership_transfer_time_avg_ms',
            'lazy_getpage_dire', 'lazy_getpage_wait', 'lazy_2RTT_count', 'lazy_3RTT_count',
            'twopc_remote_fetch_time', 'twopc_remote_fetch_count'
        ]

        for k in stages:
            val = 0
            if k in summary_dict:
                val = summary_dict[k][0]
            hf.write(f"{k}={val}\n")

    local_config = settings['config']['compute_node_config.json']['local_compute_node']
    metadata = dict(combo_header, status='complete', case_name=combo_dir_name,
                    case_fingerprint=fingerprint(case), planned_case=case,
                    run_mode=case['run_mode'], run_seconds=case['run_seconds'],
                    attempts_per_thread=current_attempt_num,
                    generate_log=local_config.get('generate_log', 1),
                    parallel_page_fetch=local_config.get('parallel_page_fetch', 0),
                    run_fingerprint=run_fingerprint,
                    code_revision=settings['code_revision'],
                    source_fingerprint=settings['source_fingerprint'])
    write_stats(combo_dir, metadata)
    plan.atomic_json(Path(combo_dir, 'metadata.json'), metadata)
    logging.info(f"{combo_dir_name}/{mode} done; see stats.log")
    cleanup_compute_logs_all_nodes(build_dir)


class BenchmarkPlan:
    MODES = ('eager', 'lazy', '2pc', 'single', 'mix')
    FIELDS = ('case_id', 'workload', 'threads', 'nodes', 'attempts', 'local_ratio', 'access_pattern', 'pattern_value', 'write_txn_ratio', 'hybrid_skew_threshold', 'hot_key_top_n', 'lock_mode', 'run_mode', 'run_seconds', 'modes', 'directory')
    INT_FIELDS = ('case_id', 'threads', 'nodes', 'attempts', 'hot_key_top_n', 'lock_mode')
    FLOAT_FIELDS = ('local_ratio', 'pattern_value', 'write_txn_ratio', 'hybrid_skew_threshold', 'run_seconds')
    atomic_text = staticmethod(atomic_text)
    atomic_json = staticmethod(atomic_json)

    @staticmethod
    def case_name(case):
        value = lambda key: format(case[key], 'g')
        return f"case_{case['case_id']:03d}-{case['workload'].lower()}-t{case['threads']}-{case['access_pattern']}{value('pattern_value')}-lr{value('local_ratio')}-wr{value('write_txn_ratio')}-mix_thres{value('hybrid_skew_threshold')}-topn{case['hot_key_top_n']}-lock{case['lock_mode']}"

    @staticmethod
    def validate(cases):
        if not cases:
            raise ValueError('Case plan is empty')
        for (expected_id, case) in enumerate(cases, 1):
            if set(case) != set(BenchmarkPlan.FIELDS) or case['case_id'] != expected_id:
                raise ValueError('Case plan requires consecutive case IDs starting at 1 and the supported schema')
            if case['workload'] not in ('ycsb', 'smallbank', 'tpcc'):
                raise ValueError('Unsupported workload in case plan')
            if any((type(case[k]) is not int or case[k] < 1 for k in BenchmarkPlan.INT_FIELDS if k != 'lock_mode')):
                raise ValueError('Case IDs, threads, nodes, attempts and top-N must be positive integers')
            if case['run_mode'] not in ('count', 'time'):
                raise ValueError('run_mode must be count or time')
            if not (0 < case['run_seconds'] <= 86400):
                raise ValueError('Invalid benchmark run time')
            if case['lock_mode'] not in (0, 1):
                raise ValueError('lock_mode must be 0 or 1')
            if any((not math.isfinite(case[k]) for k in BenchmarkPlan.FLOAT_FIELDS)):
                raise ValueError('Non-finite parameter in case plan')
            if any((not 0 <= case[k] <= 1 for k in ('local_ratio', 'write_txn_ratio', 'hybrid_skew_threshold'))):
                raise ValueError('Ratios must be between 0 and 1')
            if case['access_pattern'] not in ('theta', 'txhot') or case['pattern_value'] < 0:
                raise ValueError('Invalid access pattern')
            if not case['modes'] or len(set(case['modes'])) != len(case['modes']) or any((m not in BenchmarkPlan.MODES for m in case['modes'])):
                raise ValueError('Invalid or duplicate mode in case plan')
            # 已保存的计划沿用旧目录名。
            suffix = f"-seconds{case['run_seconds']:g}" if case['run_mode'] == 'time' else f"-tx{case['attempts']}"
            directory = case['directory'].lower().removesuffix(suffix).replace('-skew', '-mix_thres')
            if directory != BenchmarkPlan.case_name(case):
                raise ValueError('Case directory does not match its ID and parameters')
        return cases

    @staticmethod
    def generate(settings, node_count):
        cases = []
        pattern = 'theta' if settings['use_zipfian'] else 'txhot'
        values = settings['zipfian_theta'] if settings['use_zipfian'] else settings['tx_hot_list']
        for (bench, value, local, writes, skew, topn, lock) in itertools.product(settings['bench_names'], values, settings['local_ratios'], settings['write_txn_ratios'], settings['hybrid_skew_thresholds'], settings['hot_key_top_ns'], settings['lock_modes']):
            case = dict(case_id=len(cases) + 1, workload=bench, threads=settings['thread_num'], nodes=node_count, attempts=settings['attempts'][bench], local_ratio=float(local), access_pattern=pattern, pattern_value=float(value), write_txn_ratio=float(writes), hybrid_skew_threshold=float(skew), hot_key_top_n=topn, lock_mode=lock, modes=list(settings['modes']), run_mode=settings['run_mode'], run_seconds=float(settings['run_seconds']))
            case['directory'] = BenchmarkPlan.case_name(case)
            cases.append(case)
        return BenchmarkPlan.validate(cases)

    @staticmethod
    def write_plan(root, cases):
        BenchmarkPlan.validate(cases)
        out = io.StringIO()
        writer = csv.DictWriter(out, fieldnames=BenchmarkPlan.FIELDS, delimiter='\t', lineterminator='\n')
        writer.writeheader()
        rows = [dict(c, modes=','.join(c['modes'])) for c in cases]
        writer.writerows(rows)
        BenchmarkPlan.atomic_text(Path(root) / 'case_plan.tsv', out.getvalue())
        lines = ['# Benchmark case plan', '', f"Cases: {len(cases)}; mode executions: {sum((len(c['modes']) for c in cases))}", '', '| ' + ' | '.join(BenchmarkPlan.FIELDS) + ' |', '| ' + ' | '.join(('---' for _ in BenchmarkPlan.FIELDS)) + ' |']
        lines += ['| ' + ' | '.join((str(c[k]) for k in BenchmarkPlan.FIELDS)) + ' |' for c in rows]
        BenchmarkPlan.atomic_text(Path(root) / 'case_plan.md', '\n'.join(lines) + '\n')

    @staticmethod
    def read_plan(root):
        with (Path(root) / 'case_plan.tsv').open(newline='') as source:
            reader = csv.DictReader(source, delimiter='\t')
            if reader.fieldnames != list(BenchmarkPlan.FIELDS):
                raise ValueError('Unsupported case_plan.tsv schema')
            cases = list(reader)
        for case in cases:
            for key in BenchmarkPlan.INT_FIELDS:
                case[key] = int(case[key])
            for key in BenchmarkPlan.FLOAT_FIELDS:
                case[key] = float(case[key])
            case['modes'] = case['modes'].split(',')
        return BenchmarkPlan.validate(cases)

    @staticmethod
    def result_path(root, case, mode):
        if mode not in case['modes']:
            raise ValueError('Mode is not in the saved case plan')
        return Path(root) / case['directory'] / mode

    @staticmethod
    def manifest(directory):
        try:
            return json.loads((Path(directory) / 'metadata.json').read_text())
        except (OSError, ValueError):
            return {}

    @staticmethod
    def complete(root, case, mode, run_fingerprint):
        return result_complete(BenchmarkPlan.result_path(root, case, mode), case, mode, run_fingerprint)

    @staticmethod
    def set_state(root, case, mode, run_fingerprint, status, error=None):
        directory = BenchmarkPlan.result_path(root, case, mode)
        data = BenchmarkPlan.manifest(directory)
        data.update(status=status, case_id=case['case_id'], case_name=case['directory'], bench_name=case['workload'], system_name=mode, node_count=case['nodes'], planned_case=case, run_fingerprint=run_fingerprint, case_fingerprint=fingerprint(case), updated_at=datetime.now(timezone.utc).isoformat())
        data.pop('error', None)
        if error:
            data['error'] = error
        BenchmarkPlan.atomic_json(directory / 'metadata.json', data)

    @staticmethod
    def begin(root, case, mode, run_fingerprint):
        BenchmarkPlan.set_state(root, case, mode, run_fingerprint, 'running')
        directory = BenchmarkPlan.result_path(root, case, mode)
        for pattern in ('summary_human.txt', 'summary_matrix.txt', 'stats.log', 'node*/result.txt', 'node*/effective_config.json', 'node*/delay_fetch_remote.txt', 'node*/tps.log', 'node*/compute.out', 'storage/storage_timing_stats.txt'):
            for old in directory.glob(pattern):
                old.unlink()

    @staticmethod
    def write_status(root, cases, run_fingerprint):
        out = io.StringIO()
        writer = csv.writer(out, delimiter='\t', lineterminator='\n')
        writer.writerow(('case_id', 'workload', 'mode', 'status', 'directory'))
        for case in cases:
            for mode in case['modes']:
                directory = BenchmarkPlan.result_path(root, case, mode)
                status = BenchmarkPlan.manifest(directory).get('status', 'pending')
                if status == 'complete' and (not BenchmarkPlan.complete(root, case, mode, run_fingerprint)):
                    status = 'incomplete'
                writer.writerow((case['case_id'], case['workload'], mode, status, str(directory.relative_to(root))))
        BenchmarkPlan.atomic_text(Path(root) / 'case_status.tsv', out.getvalue())

    @staticmethod
    @contextmanager
    def execution_lock(root):
        with (Path(root) / '.run.lock').open('a') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RuntimeError('This case plan is already running in another process') from None
            try:
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

plan = BenchmarkPlan


def parse_options(argv=None):
    parser = argparse.ArgumentParser(description='Persist a case plan, execute it, or resume interrupted cases.')
    parser.add_argument('--plan-only', action='store_true', help='save case_plan.tsv/md without deployment or SSH')
    parser.add_argument('--dry-run', action='store_true', help='print the plan without running or writing files')
    parser.add_argument('--result-dir', type=Path, help='directory for a NEW plan; must not already exist')
    parser.add_argument('--resume-result-dir', type=Path, default=os.environ.get('RESUME_FROM'),
                        help='reuse an existing case_plan.tsv and skip verified completed modes')
    parser.add_argument('--resume-from-case', type=int, default=1, help='start at this case ID (inclusive)')
    parser.add_argument('--rerun-completed', action='store_true', help='rerun completed modes in the selected range')
    parser.add_argument('--run-mode', choices=('count', 'time'), help='count: attempts per worker; time: run seconds per node')
    parser.add_argument('--run-seconds', type=float, help='seconds to run each node in time mode')
    args = parser.parse_args(argv)
    if args.resume_from_case < 1:
        parser.error('--resume-from-case must be positive')
    if args.result_dir and args.resume_result_dir:
        parser.error('--result-dir and --resume-result-dir are mutually exclusive')
    return args


def prepare_plan(args, current):
    if args.resume_result_dir:
        root = Path(args.resume_result_dir).resolve()
        saved_path = root / 'run.json'
        if not saved_path.is_file() or not (root / 'case_plan.tsv').is_file():
            raise ValueError('Cannot resume: run.json and case_plan.tsv are required')
        saved = json.loads(saved_path.read_text())
        cases = plan.read_plan(root)
        if saved.get('plan_fingerprint') != fingerprint(cases):
            raise ValueError('Saved case plan changed; create a new plan')
        if saved.get('config') != current['config'] or saved.get('source_fingerprint') != current['source_fingerprint']:
            raise ValueError('Source or base config differs from the saved plan; restore it or create a new plan')
        return root, cases, saved, saved['run_fingerprint']
    cases = plan.generate(current, len(compute_server_hostnames))
    root = (args.result_dir or Path(workspace, result_root_name,
            'benchmark_results_' + time.strftime('%Y%m%d_%H%M%S', time.localtime()))).resolve()
    run_fingerprint = fingerprint(dict(config=current['config'], source=current['source_fingerprint'], cases=cases))
    saved = dict(current, plan_fingerprint=fingerprint(cases),
                 run_fingerprint=run_fingerprint)
    if not args.dry_run:
        root.mkdir(parents=True, exist_ok=False)
        plan.write_plan(root, cases)
        plan.atomic_json(root / 'run.json', saved)
        plan.write_status(root, cases, run_fingerprint)
    return root, cases, saved, run_fingerprint


def main(argv=None):
    args = parse_options(argv)
    current = run_settings()
    for name in ('run_mode', 'run_seconds'):
        value = getattr(args, name)
        if value is not None:
            if args.resume_result_dir:
                raise ValueError('Resume uses the saved stop policy; overrides require a new plan')
            current[name] = value
    result_dir, cases, settings, run_fingerprint = prepare_plan(args, current)
    if args.resume_from_case > len(cases):
        raise ValueError('--resume-from-case exceeds the last case ID')
    logging.info('Case plan: %s (%d cases, %d mode executions)', result_dir / 'case_plan.tsv',
                 len(cases), sum(len(c['modes']) for c in cases))
    if args.dry_run:
        for case in cases:
            print(f"{case['directory']}: {','.join(case['modes'])}")
        return
    with plan.execution_lock(result_dir):
        plan.write_status(result_dir, cases, run_fingerprint)
        if args.plan_only:
            logging.info('Plan saved; no deployment or SSH performed.')
            return
        rerun = args.rerun_completed or os.environ.get('SKIP_COMPLETED', '1') == '0'
        pending = [(c, m) for c in cases if c['case_id'] >= args.resume_from_case for m in c['modes']
                   if rerun or not plan.complete(result_dir, c, m, run_fingerprint)]
        write_comparison(result_dir)
        if not pending:
            logging.info('All selected cases are complete; no cluster changes needed.')
            return
        node_count = len(compute_server_hostnames)
        if not node_count or any(len(v) != node_count for v in
                (compute_server_ports, compute_server_usernames, compute_server_passwords)):
            raise ValueError('Compute SSH configuration does not match the planned nodes')
        if any(c['nodes'] != node_count for c, _ in pending):
            raise ValueError('Node count differs from the saved plan')
        load_ssh_dependency()
        if os.environ.get('AUTO_BOOTSTRAP', '1') != '0':
            bootstrap_cluster()
        for case, mode in pending:
            logging.info('Running case %03d/%03d, mode=%s', case['case_id'], len(cases), mode)
            plan.begin(result_dir, case, mode, run_fingerprint)
            plan.write_status(result_dir, cases, run_fingerprint)
            write_comparison(result_dir)
            try:
                run_case_mode(result_dir, case, mode, settings, run_fingerprint)
                if not plan.complete(result_dir, case, mode, run_fingerprint):
                    raise RuntimeError('Case output is incomplete or transaction counts do not match the plan')
            except BaseException as error:
                status = 'interrupted' if isinstance(error, (KeyboardInterrupt, SystemExit)) else 'failed'
                plan.set_state(result_dir, case, mode, run_fingerprint, status, type(error).__name__)
                raise
            finally:
                plan.write_status(result_dir, cases, run_fingerprint)
                write_comparison(result_dir)
        logging.info('Selected cases completed; results: %s', result_dir)


# 从配置文件加载节点信息；以下无需手动修改。
compute_server_hostnames, compute_server_ports, compute_server_usernames, compute_server_passwords = load_compute_server_ssh_config()
remote_server_host = remote_server_host or detect_remote_server_host()


if __name__ == '__main__':
    main()
