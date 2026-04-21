# 这个文件的目的是，测试多种条件下的性能对比，能够实现多物理机上自动化的脚本运行
# 需要先在预定的机器的指定目录下，安装本项目，然后配置好环境，如 brpc，boost 等，编译通过后，再跑这个脚本

import os
import sys
import io
import time
import json
import shlex
import subprocess
import threading
import logging
try:
    import paramiko
except ImportError:
    import subprocess, sys, os
    _tp = os.path.join(os.path.dirname(__file__), 'third_party')
    os.makedirs(_tp, exist_ok=True)
    try:
        subprocess.run(
            [sys.executable, '-m', 'pip', 'install', '-q',
             '--disable-pip-version-check', '--no-warn-script-location',
             '--target', _tp, 'paramiko'],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
    except Exception as e:
        print('paramiko install failed; please ensure network/pip available or preinstall paramiko', file=sys.stderr)
        raise
    sys.path.insert(0, _tp)
    import paramiko
    
modes = ['lazy' , '2pc']
bench_names = ['ycsb' , 'smallbank']
thread_num = 10
#1：全都是写，0：全都是读
write_txn_ratios = [0.5 , 0.2 , 0.8]
attempt_num = 100000
repeats = 1
local_ratios = [0.2 , 0.5, 0.8] #本地访问的比例
use_zipfian = True
zipfian_theta = [0.90 , 0.80 , 0.60 , 0.10]
tx_hot_list = [80 , 50 , 20]  #热点访问比例
# 为了避免存储端一次性元信息发送的监听被并发连接挤爆，分节点顺序错峰启动
handshake_stagger_sec = 1

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logging.getLogger("paramiko").setLevel(logging.WARNING)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

workspace = os.getcwd()
remote_workspace = '/usr/local/exper/Hybrid_Cloud_MP'
remote_build_dir = '/usr/local/exper/Hybrid_Cloud_MP/build'

compute_server_build_dir = '/usr/local/exper/Hybrid_Cloud_MP/build/compute_server'
compute_default_ssh_port = int(os.environ.get('COMPUTE_SSH_PORT', 22))
compute_default_ssh_user = os.environ.get('COMPUTE_SSH_USER', 'root')
compute_default_ssh_passwd = os.environ.get('COMPUTE_SSH_PASS', 'wljwlj123Wlj.')

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

compute_server_hostnames, compute_server_ports, compute_server_usernames, compute_server_passwords = load_compute_server_ssh_config()

# remote_server 和 storage_server 在一个服务器上
remote_server_host = os.environ.get('REMOTE_HOST', '172.16.0.40')
remote_server_port = int(os.environ.get('REMOTE_PORT', 22))
remote_server_user = os.environ.get('REMOTE_USER', 'root')
remote_server_passwd = os.environ.get('REMOTE_PASS', 'wljwlj123Wlj.')
remote_key_path = os.environ.get('REMOTE_KEY', None)

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
        timeout_sec=180
    )

    if not storage_ready:
        storage_tail = read_remote_file_tail(client, storage_log, lines=120)
        logging.error(f"storage_pool ready keyword not found in {storage_log}\n{storage_tail}")
    if not remote_ready:
        remote_tail = read_remote_file_tail(client, remote_log, lines=120)
        logging.error(f"remote_node ready keyword not found in {remote_log}\n{remote_tail}")
    
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
        logging.error("remote services process exists but startup ready keywords not observed")
        exit(-1)
        
    return True

def kill_compute(client):
    ssh_exec(client, ["pkill -f compute_server"], verbose=False)

def ensure_compute_killed(client):
    ssh_exec(client, ["pkill compute_server"], verbose=False)

def remove_remote_compute_outputs(client, build_dir):
    compute_dir = os.path.join(build_dir, "compute_server")
    ssh_exec(client, [f"rm -f {compute_dir}/result.txt {compute_dir}/delay_fetch_remote.txt"], verbose=False)

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

def start_compute_blocking(client, build_dir, args, log_path):
    compute_dir = os.path.join(build_dir, "compute_server")
    cmd = f"bash -lc 'cd {compute_dir} && {compute_dir}/compute_server {args}'"
    stdin, stdout, stderr = client.exec_command(cmd)
    out_buf = []
    err_buf = []
    while not stdout.channel.exit_status_ready():
        if stdout.channel.recv_ready():
            out_buf.append(stdout.channel.recv(4096).decode(errors='replace'))
        if stderr.channel.recv_ready():
            err_buf.append(stderr.channel.recv(4096).decode(errors='replace'))
        time.sleep(0.2)

    out = ("".join(out_buf) + stdout.read().decode(errors='replace')).strip()
    err = ("".join(err_buf) + stderr.read().decode(errors='replace')).strip()
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
    
def update_hot_rate(client, bench_name, hot_rate):
    if bench_name in ("smallbank", "smallbank_aff"):
        cfg_name = f"{bench_name}_config.json"
        section = bench_name
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
    if bench_name in ("smallbank", "smallbank_aff"):
        cfg_name = f"{bench_name}_config.json"
        section = bench_name
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
    if bench_name in ("smallbank", "smallbank_aff"):
        cfg_name = f"{bench_name}_config.json"
        section = bench_name
        key = "attempted_num"
    elif bench_name == "ycsb":
        cfg_name = "ycsb_config.json"
        section = "ycsb"
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

def update_remote_compute_config(client, machine_id):
    remote_cfg = os.path.join(remote_workspace, 'config', 'compute_node_config.json')
    sftp = client.open_sftp()
    rf = sftp.open(remote_cfg, 'r')
    content = rf.read().decode('utf-8')
    rf.close()
    data = json.loads(content)
    if 'local_compute_node' not in data:
        data['local_compute_node'] = {}
    data['local_compute_node']['machine_id'] = int(machine_id)
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
        'single_txn_count', 'distribute_txn_count'
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

def aggregate_round_summaries(base_dir, repeats):
    data = []
    # To support KV, we need to collect keys too, but this function aggregates round summaries which are usually raw matrices.
    # However, round_summary from aggregate_round_from_combos now returns matrix + keys?
    # Wait, aggregate_round_from_combos writes "result.txt" in round_dir.
    # If we change aggregate_round_from_combos to write KV, then we need to read KV here.
    
    # Let's fix aggregate_round_from_combos first to output KV if possible, or just raw.
    # The user wants robustness.
    
    # For now, let's assume this function reads whatever format (KV or raw) and aggregates.
    # If read_node_matrix_kv works on the file, we get a dict.
    
    aggregated_data = {}
    all_keys_order = []
    
    for r in range(repeats):
        p = os.path.join(base_dir, f"round_{r:02d}", "result.txt")
        if not os.path.exists(p):
            continue
        
        node_data, node_keys = read_node_matrix_kv(p)
        
        if not all_keys_order:
            all_keys_order = node_keys
            
        for k, v in node_data.items():
            if k not in aggregated_data:
                aggregated_data[k] = []
            aggregated_data[k].append(v)
            
    if not aggregated_data:
        return [], []
        
    final_rows = []
    final_keys = []
    
    for k in all_keys_order:
        vals = aggregated_data.get(k, [])
        if not vals:
            continue
            
        # For round aggregation, we usually average everything?
        # Or do we sum throughput again?
        # NO. Round aggregation is averaging across repetitions (e.g. 3 runs of same experiment).
        # So we always Average.
        
        is_list_val = isinstance(vals[0], list)
        
        if is_list_val:
            val_len = len(vals[0])
            summed_list = [0.0] * val_len
            for v_list in vals:
                if isinstance(v_list, list) and len(v_list) == val_len:
                    for idx, v in enumerate(v_list):
                        summed_list[idx] += v
            
            # Average the sum
            avg_list = [x / len(vals) for x in summed_list]
            final_rows.append(avg_list)
            final_keys.append(k)
        else:
            val = sum(vals) / len(vals)
            final_rows.append([val])
            final_keys.append(k)
            
    return final_rows, final_keys

def aggregate_round_from_combos(round_dir):
    # This function aggregates results from different combos (e.g. cr_0.1, cr_0.3) into one matrix for the round?
    # Wait, looking at usage:
    # It iterates os.listdir(round_dir). 
    # Usually round_dir contains "ycsb_lazy", "tpcc_2pc" etc. directories.
    # Then inside those, it looks for "cr_X_txhot_Y".
    # And it reads "summary_matrix.txt".
    
    # This function seems to be creating a big summary of ALL experiments in this round.
    # It just concatenates or averages?
    # The original code:
    # 585: max_rows = max(len(r) for r in data)
    # ...
    # 595: cols.append(sum(vals) / len(vals) if vals else 0.0)
    # It AVERAGES across all combos? That sounds weird if combos are different parameters (e.g. different contention).
    # But that's what the code does. It seems to produce a "grand average" of the round.
    
    # We should support KV here too.
    
    data_list = [] # List of (data_dict, keys_list)
    
    for name in os.listdir(round_dir):
        first = os.path.join(round_dir, name)
        if not os.path.isdir(first):
            continue
            
        # Try direct summary_matrix (if structure is flat)
        p_direct = os.path.join(first, "summary_matrix.txt") # This is purely raw matrix
        # But wait, we want to read the KV result if available to be robust?
        # The previous step wrote "summary_matrix.txt" as raw matrix, and "summary_human.txt" as KV.
        # Maybe we should read summary_human.txt?
        # Or we can stick to reading summary_matrix.txt which is raw, but we lose keys.
        
        # Actually, in the main loop:
        # 741: round_summary = aggregate_round_from_combos(round_dir)
        # 744: rf.write(...)
        
        # If we want to maintain the key info, we should try to read keys from somewhere.
        # But summary_matrix.txt doesn't have keys.
        # Let's check if we can read summary_human.txt? 
        # summary_human.txt has "key=value".
        
        # Let's try to read summary_human.txt from the combos.
        
        # Recursive search for combo dirs
        combo_dirs = []
        if os.path.exists(os.path.join(first, "summary_human.txt")):
             combo_dirs.append(first)
        else:
            for subname in os.listdir(first):
                second = os.path.join(first, subname)
                if os.path.isdir(second) and os.path.exists(os.path.join(second, "summary_human.txt")):
                    combo_dirs.append(second)
        
        for c_dir in combo_dirs:
            p = os.path.join(c_dir, "summary_human.txt")
            d, k = read_node_matrix_kv(p)
            if d:
                data_list.append((d, k))

    if not data_list:
        return [], []
        
    # Aggregate (Average)
    aggregated_data = {}
    all_keys_order = []
    
    for d, k in data_list:
        if not all_keys_order:
            all_keys_order = k
        for key, val in d.items():
            if key not in aggregated_data:
                aggregated_data[key] = []
            aggregated_data[key].append(val)
            
    final_rows = []
    final_keys = []
    
    for k in all_keys_order:
        vals = aggregated_data.get(k, [])
        if not vals:
            continue
        
        # Average
        is_list_val = isinstance(vals[0], list)
        if is_list_val:
            val_len = len(vals[0])
            summed_list = [0.0] * val_len
            for v_list in vals:
                if isinstance(v_list, list) and len(v_list) == val_len:
                    for idx, v in enumerate(v_list):
                        summed_list[idx] += v
            avg_list = [x / len(vals) for x in summed_list]
            final_rows.append(avg_list)
            final_keys.append(k)
        else:
            val = sum(vals) / len(vals)
            final_rows.append([val])
            final_keys.append(k)
            
    return final_rows, final_keys

def main():
    auto_bootstrap = os.environ.get("AUTO_BOOTSTRAP", "1")
    if auto_bootstrap != "0":
        bootstrap_cluster()

    if not compute_server_hostnames or not compute_server_ports or not compute_server_usernames or not compute_server_passwords:
        logging.info("not configure compute_server_hostnames, compute_server_ports, compute_server_usernames, compute_server_passwords, remote_server_host , break")
        return
    node_count = len(compute_server_hostnames)
    if len(compute_server_ports) != node_count or len(compute_server_usernames) != node_count or len(compute_server_passwords) != node_count:
        logging.error("compute ssh config length mismatch with remote_compute_node_ips")
        return
    try:
        import socket
        s = socket.socket()
        s.settimeout(3)
        s.connect((remote_server_host, remote_server_port))
        s.close()
    except Exception:
        logging.error(f"Cannot reach {remote_server_host}:{remote_server_port}. Set REMOTE_HOST/PORT env or ensure network/VPN.")
        return
    ts = time.strftime("%Y%m%d%H%M%S", time.localtime())
    # workspace 就是当前运行这个脚本的目录，目前就是 workspace/result/时间戳
    result_dir = os.path.join(workspace, "result", ts)
    os.makedirs(result_dir, exist_ok=True)
    build_dir = remote_build_dir

    for r in range(repeats):
        # 把 round 格式化为 2 位，比如目前 round = 31，那文件名就是 workspace/result/时间戳/rounnd_32，注意这是一个目录
        round_dir = os.path.join(result_dir, f"round_{r:02d}")
        os.makedirs(round_dir, exist_ok=True)

        for bench_name in bench_names:
            access_pattern_values = zipfian_theta if use_zipfian else tx_hot_list
            for pattern_value in access_pattern_values:
                for cr in local_ratios:
                    for write_txn_ratio in write_txn_ratios:
                        for mode in modes:
                            mode_dir = os.path.join(round_dir, f"{bench_name}_{mode}")
                            os.makedirs(mode_dir, exist_ok=True)
                            local_ratio = cr

                            logging.info(f"Set Account Success for {bench_name}")
                            try:
                                cfg_clients = [ssh_client(h, compute_server_ports[i], compute_server_usernames[i], compute_server_passwords[i]) for i, h in enumerate(compute_server_hostnames)]
                                rs_client = ssh_client(remote_server_host, remote_server_port , remote_server_user, remote_server_passwd)
                            except Exception:
                                logging.error("SSH connect failed; check credentials or REMOTE_* env vars.")
                                return
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
                                return
                            # 启动 remote_server 和 storage_server
                            ok = start_remote_services_checked(rs_client, remote_build_dir, bench_name, fallback_build_dir=os.path.join("/usr/local/exper/Hybrid_Cloud_MP", "build"))
                            logging.info("Start Remote Over")
                            rs_client.close()
                            if not ok:
                                logging.error("remote services failed to start; check build_dir paths and binaries")
                                exit(-1)

                            # 构建一个字符串，表示各个参数的名字，例如 local_txn_0.9_txhot_39
                            pattern_tag = "theta" if use_zipfian else "txhot"
                            combo_dir_name = f"lr{cr}_{pattern_tag}_{pattern_value}_wr_{write_txn_ratio}"
                            # 在 round_dir 目录下再搞一个文件夹，表示当前参数
                            combo_dir = os.path.join(mode_dir, combo_dir_name)
                            os.makedirs(combo_dir, exist_ok=True)

                            logging.info(f"Creating Dir , {combo_dir}")
                            threads = []
                            thread_errors = []
                            thread_error_lock = threading.Lock()
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
                                        update_remote_compute_config(client, i)
                                        update_access_pattern(client, bench_name, use_zipfian, pattern_value)
                                        update_attempt_num(client, bench_name, attempt_num)
                                        
                                        args = f"{bench_name} {mode} {thread_num} {write_txn_ratio} {local_ratio} {i}"
                                        log_path = f"{build_dir}/compute_server/compute_server_{i}.out"
                                        time.sleep(handshake_stagger_sec * (i))
                                        logging.info(f"Starting ComputeServer , hostname = {host} , args = {args}")
                                        exit_code, _, err = start_compute_blocking(client, build_dir, args, log_path)
                                        if exit_code != 0:
                                            raise RuntimeError(f"compute_server exited with code {exit_code}, detail={err[-1200:]}")
                                        logging.info("Running ComputeServer Over")
                                        header = {
                                            "round": r,
                                            "bench_name": bench_name,
                                            "system_name": mode,
                                            "local_ratio": cr,
                                            "local_txn_ratio": local_ratio,
                                            "use_zipfian": use_zipfian,
                                            "zipf_theta": pattern_value if use_zipfian else "",
                                            "tx_hot": pattern_value if not use_zipfian else "",
                                            "thread_num": thread_num,
                                            "write_txn_ratio": write_txn_ratio,
                                            "node_count": len(compute_server_hostnames),
                                            "combo_path": out_dir
                                        }
                                        fetch_node_results(client, i, out_dir, build_dir, header)
                                    finally:
                                        client.close()
                                except Exception as e:
                                    logging.error(f"node {i} failed for {combo_dir_name} mode={mode}: {e}")
                                    with thread_error_lock:
                                        thread_errors.append(f"node{i}:{e}")

                            # 让所有的计算节点，都去跑 computeserver
                            for idx, host in enumerate(compute_server_hostnames):
                                t = threading.Thread(target=run_node, args=(idx, host, compute_server_ports[idx], combo_dir))
                                threads.append(t)
                                t.start()

                            for t in threads:
                                t.join()

                            if thread_errors:
                                raise RuntimeError(f"{combo_dir_name} mode={mode} failed: {'; '.join(thread_errors)}")

                            combo_summary, combo_keys = aggregate_results(combo_dir, len(compute_server_hostnames))
                            combo_header = {
                                "round": r,
                                "bench_name": bench_name,
                                "system_name": mode,
                                "local_ratio": cr,
                                "local_txn_ratio": local_ratio,
                                "use_zipfian": use_zipfian,
                                "zipf_theta": pattern_value if use_zipfian else "",
                                "tx_hot": pattern_value if not use_zipfian else "",
                                "thread_num": thread_num,
                                "write_txn_ratio": write_txn_ratio,
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
                                if bench_name in ('smallbank', 'smallbank_aff'):
                                    types = ['Amalgamate','Balance','DepositChecking','SendPayment','TransactSaving','WriteCheck']
                                    type_names = ['Amalgamate','Balance','DepositChecking','SendPayment','TransactSaving','WriteCheck']
                                elif bench_name == 'ycsb':
                                    types = ['Tx1']
                                    type_names = ['Tx1'] # or YCSB_TX_NAME from run.cc? run.cc uses "Tx1"
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
                                    'ownership_transfer_time_avg_ms',
                                    'lazy_getpage_dire', 'lazy_getpage_wait', 'lazy_2RTT_count', 'lazy_3RTT_count',
                                    'twopc_remote_fetch_time', 'twopc_remote_fetch_count'
                                ]
                                
                                for k in stages:
                                    val = 0
                                    if k in summary_dict:
                                        val = summary_dict[k][0]
                                    hf.write(f"{k}={val}\n")
                                    
                            logging.info(f"round {r} {combo_dir_name} done")
                            cleanup_compute_logs_all_nodes(build_dir)

        # after all combos in this round, write round-level matrix for final aggregation
        round_summary, round_keys = aggregate_round_from_combos(round_dir)
        round_result_path = os.path.join(round_dir, "result.txt")
        with open(round_result_path, 'w', encoding='utf-8') as rf:
            for i, row in enumerate(round_summary):
                val_str = " ".join(str(x) for x in row)
                if i < len(round_keys) and round_keys[i] and not round_keys[i].startswith('line_'):
                     rf.write(f"{round_keys[i]}={val_str}\n")
                else:
                     rf.write(f"{val_str}\n")

    final_summary, final_keys = aggregate_round_summaries(result_dir, repeats)
    final_header = {
        "type": "final_summary",
        "bench_name": bench_name,
        "system_name": ",".join(modes),
        "repeats": repeats,
        "local_ratios": ",".join(str(x) for x in local_ratios),
        "use_zipfian": use_zipfian,
        "zipfian_theta": ",".join(str(x) for x in zipfian_theta),
        "tx_hot_list": ",".join(str(x) for x in tx_hot_list),
        "thread_num": thread_num,
        "write_txn_ratios": ",".join(str(x) for x in write_txn_ratios),
        "node_count": len(compute_server_hostnames)
    }
    # final matrix
    final_mat = os.path.join(result_dir, "final_matrix.txt")
    with open(final_mat, 'w', encoding='utf-8') as mf:
        for row in final_summary:
            mf.write(" ".join(str(x) for x in row) + "\n")
    # final human
    final_human = os.path.join(result_dir, "final_human.txt")
    
    # Convert aggregated results to a dictionary for easier lookup
    summary_dict = {}
    for i, k in enumerate(final_keys):
        summary_dict[k] = final_summary[i]

    with open(final_human, 'w', encoding='utf-8') as hf:
        for k, v in final_header.items():
            hf.write(f"{k}={v}\n")
        # metrics keys mapping
        names = [
            'total_time_seconds','throughput','lock_ratio',
            'fetch_from_remote_count','fetch_from_storage_count','fetch_from_local_count',
            'evicted_pages_count'
        ]
        
        for k in names:
             # Fallback to index-based if key not found (legacy support)
             val = 0
             if k in summary_dict:
                 val = summary_dict[k][0]
             hf.write(f"{k}={val}\n")
        
        if bench_name in ('smallbank', 'smallbank_aff'):
            types = ['Amalgamate','Balance','DepositChecking','SendPayment','TransactSaving','WriteCheck']
            type_names = ['Amalgamate','Balance','DepositChecking','SendPayment','TransactSaving','WriteCheck']
        elif bench_name == 'ycsb':
            types = ['Tx1']
            type_names = ['Tx1']
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
            'tx_write_commit_log_time', 'tx_write_commit_log_time2',
            'tx_write_prepare_log_time', 'tx_write_backup_log_time',
            'tx_get_timestamp_time1','tx_get_timestamp_time2',
            'update_log_count',
            'single_txn_count', 'distribute_txn_count',
            'ownership_transfer_time_avg_ms',
            'lazy_getpage_dire', 'lazy_getpage_wait', 'lazy_2RTT_count', 'lazy_3RTT_count',
            'twopc_remote_fetch_time', 'twopc_remote_fetch_count'
        ]
        
        for k in stages:
            val = 0
            if k in summary_dict:
                val = summary_dict[k][0]
            hf.write(f"{k}={val}\n")
            
    logging.info(f"final summary in {result_dir}")

if __name__ == '__main__':
    main()
