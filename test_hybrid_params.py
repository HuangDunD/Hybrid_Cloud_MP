# 这个脚本用于测试 SYSTEM_MODE == 4 (Mix) 下两个核心参数对性能的影响：
#   1) hot_key_top_n        —— Zipfian 模式下定义为「头部热点 key 数量」
#   2) hybrid_skew_threshold —— 事务级热点 key 占比阈值，>= 该阈值走 2PC，否则走 Lazy
# 参考 test_multi_node.py 的多节点编排逻辑实现，但固定 mode=mix，并新增对这两个参数的笛卡尔遍历。

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
    except Exception:
        print('paramiko install failed; please ensure network/pip available or preinstall paramiko', file=sys.stderr)
        raise
    sys.path.insert(0, _tp)
    import paramiko

# ============== 实验变量 ==============
# 仅测 mix 模式（SYSTEM_MODE == 4）；其他模式下这两个参数不生效
mode = 'mix'
bench_names = ['ycsb']
thread_num = 10
write_txn_ratios = [0.9]
local_ratios = [0.5]
use_zipfian = True
zipfian_theta = [0.95]
tx_hot_list = [50]
repeats = 1

# 本脚本新增的两个待扫参数
hot_key_top_n_list = [10, 100, 1000, 5000]
hybrid_skew_threshold_list = [0.1, 0.3, 0.5, 0.7, 0.9]

attempt_num_by_bench = {
    "ycsb": 30000,
    "smallbank": 100000,
    "tpcc": 10000,
}
handshake_stagger_sec = 1
# ====================================

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logging.getLogger("paramiko").setLevel(logging.WARNING)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

workspace = os.getcwd()
remote_workspace = '/usr/local/exper/Hybrid_Cloud_MP'
remote_build_dir = '/usr/local/exper/Hybrid_Cloud_MP/build'

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


def resolve_attempt_num(bench_name: str) -> int:
    if bench_name in attempt_num_by_bench:
        return int(attempt_num_by_bench[bench_name])
    raise ValueError(f"unsupported bench_name for attempt_num: {bench_name}")


compute_server_hostnames, compute_server_ports, compute_server_usernames, compute_server_passwords = load_compute_server_ssh_config()


def detect_remote_server_host():
    config_path = os.path.join(workspace, 'config', 'compute_node_config.json')
    with open(config_path, 'r', encoding='utf-8') as f:
        config_data = json.load(f)
    remote_server_nodes = config_data.get('remote_server_nodes', {})
    remote_server_ips = remote_server_nodes.get('remote_server_node_ips', [])
    if isinstance(remote_server_ips, list) and remote_server_ips:
        return str(remote_server_ips[0])
    remote_storage_nodes = config_data.get('remote_storage_nodes', {})
    remote_storage_ips = remote_storage_nodes.get('remote_storage_node_ips', [])
    if isinstance(remote_storage_ips, list) and remote_storage_ips:
        return str(remote_storage_ips[0])
    raise ValueError("cannot detect remote server host from config/compute_node_config.json")


remote_server_host = os.environ.get('REMOTE_HOST') or detect_remote_server_host()
remote_server_port = int(os.environ.get('REMOTE_PORT', 22))
remote_server_user = os.environ.get('REMOTE_USER', 'root')
remote_server_passwd = os.environ.get('REMOTE_PASS', 'wljwlj123Wlj.')
remote_key_path = os.environ.get('REMOTE_KEY', None)


def ssh_client(host, port, user, passwd):
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    if remote_key_path and os.path.exists(remote_key_path):
        key = paramiko.RSAKey.from_private_key_file(remote_key_path)
        c.connect(hostname=host, port=port, username=user, pkey=key, timeout=10)
    else:
        c.connect(hostname=host, port=port, username=user, password=passwd, timeout=10)
    return c


def ssh_exec(client, cmds, verbose=True):
    outs = []
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
                stdin.close(); stdout.close(); stderr.close()
            except Exception:
                pass
    return outs


def sftp_get(client, remote_path, local_path):
    sftp = client.open_sftp()
    sftp.get(remote_path, local_path)
    sftp.close()


def distribute_config_to_node(client):
    configs = ['smallbank_config.json', 'ycsb_config.json', 'compute_node_config.json',
               'storage_node_config.json', 'remote_server_config.json']
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


def check_service_running(client, name):
    stdin, stdout, stderr = client.exec_command(f"pgrep {name}")
    try:
        out = stdout.read().decode().strip()
        return out != ''
    finally:
        try:
            stdin.close(); stdout.close(); stderr.close()
        except Exception:
            pass


def remote_file_contains(client, file_path, keyword):
    cmd = f"test -f {shlex.quote(file_path)} && grep -Fq -- {shlex.quote(keyword)} {shlex.quote(file_path)}"
    stdin, stdout, stderr = client.exec_command(cmd)
    try:
        return stdout.channel.recv_exit_status() == 0
    finally:
        try:
            stdin.close(); stdout.close(); stderr.close()
        except Exception:
            pass


def read_remote_file_tail(client, file_path, lines=80):
    cmd = f"if test -f {shlex.quote(file_path)}; then tail -n {int(lines)} {shlex.quote(file_path)}; fi"
    stdin, stdout, stderr = client.exec_command(cmd)
    try:
        return stdout.read().decode('utf-8', errors='ignore')
    finally:
        try:
            stdin.close(); stdout.close(); stderr.close()
        except Exception:
            pass


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
            logging.info(f"Waiting remote ready... storage={storage_ready} remote={remote_ready}")
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
                logging.error(f"launch {name} failed code={exit_code} err={err}")
                return False
        try:
            stdin.close(); stdout.close(); stderr.close()
        except Exception:
            pass
        return True
    finally:
        c.close()


def start_remote_services_checked(client, primary_build_dir, workload_name):
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

    logging.info("Launching storage_pool ...")
    if not launch_remote_background(cmd_storage, "storage_pool"):
        return False
    time.sleep(1)
    logging.info("Launching remote_node ...")
    if not launch_remote_background(cmd_remote, "remote_node"):
        return False

    storage_ready, remote_ready = wait_remote_services_ready(client, storage_log, remote_log, timeout_sec=1800)
    if not storage_ready:
        logging.warning(f"storage not ready:\n{read_remote_file_tail(client, storage_log, 80)}")
    if not remote_ready:
        logging.warning(f"remote not ready:\n{read_remote_file_tail(client, remote_log, 80)}")
    if not check_service_running(client, "storage_pool"):
        logging.error("storage_pool failed to start"); return False
    if not check_service_running(client, "remote_node"):
        logging.error("remote_node failed to start"); return False
    return True


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
    for fn in os.listdir(compute_dir):
        if fn.startswith("computeserver.log"):
            try:
                os.remove(os.path.join(compute_dir, fn))
            except Exception:
                pass


def cleanup_compute_logs_all_nodes(build_dir):
    for i, host in enumerate(compute_server_hostnames):
        try:
            client = ssh_client(host, compute_server_ports[i], compute_server_usernames[i], compute_server_passwords[i])
            remove_remote_compute_logs(client, build_dir)
            client.close()
        except Exception as e:
            logging.warning(f"cleanup remote compute logs failed: node={i} err={e}")
    remove_local_compute_logs()


def start_compute_blocking(client, build_dir, args, log_path):
    compute_dir = os.path.join(build_dir, "compute_server")
    cmd = f"bash -lc 'cd {compute_dir} && {compute_dir}/compute_server {args}'"
    stdin, stdout, stderr = client.exec_command(cmd)
    out_buf, err_buf = [], []
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
    if exit_code != 0 and (out or err):
        logging.error(f"STDOUT:\n{out}\nSTDERR:\n{err}")
    return exit_code, out, err


def fetch_node_results(client, node_idx, result_base_dir, build_dir):
    node_dir = os.path.join(result_base_dir, f"node{node_idx}")
    os.makedirs(node_dir, exist_ok=True)
    rp1 = f"{build_dir}/compute_server/result.txt"
    rp2 = f"{build_dir}/compute_server/delay_fetch_remote.txt"
    lp1 = os.path.join(node_dir, "result.txt")
    lp2 = os.path.join(node_dir, "delay_fetch_remote.txt")
    try:
        sftp_get(client, rp1, lp1)
    except Exception as e:
        raise FileNotFoundError(f"fetch node{node_idx} result failed: {rp1}") from e
    try:
        sftp_get(client, rp2, lp2)
    except Exception:
        pass


def update_access_pattern(client, bench_name, use_zipfian_mode, pattern_value):
    if bench_name == "smallbank":
        cfg_name = "smallbank_config.json"; section = "smallbank"; hot_key = "num_hot_rate"
    elif bench_name == "ycsb":
        cfg_name = "ycsb_config.json"; section = "ycsb"; hot_key = "TX_HOT"
    else:
        return
    remote_cfg = os.path.join(remote_workspace, 'config', cfg_name)
    sftp = client.open_sftp()
    try:
        rf = sftp.open(remote_cfg, 'r'); content = rf.read().decode('utf-8'); rf.close()
    except Exception:
        sftp.close(); return
    data = json.loads(content)
    if section in data:
        data[section]["use_zipfian"] = 1 if use_zipfian_mode else 0
        if use_zipfian_mode:
            data[section]["zipf_theta"] = float(pattern_value)
        else:
            data[section][hot_key] = int(pattern_value)
    tmp = os.path.join(remote_workspace, 'config', f'.{cfg_name}.tmp')
    wf = sftp.open(tmp, 'w'); wf.write(json.dumps(data, indent=2)); wf.flush(); wf.close()
    sftp.close()
    ssh_exec(client, [f"mv {tmp} {remote_cfg}"], verbose=False)


def update_attempt_num(client, bench_name, attempt_num):
    cfg_name = f"{bench_name}_config.json"
    section = bench_name
    remote_cfg = os.path.join(remote_workspace, 'config', cfg_name)
    sftp = client.open_sftp()
    try:
        rf = sftp.open(remote_cfg, 'r'); content = rf.read().decode('utf-8'); rf.close()
    except Exception:
        sftp.close(); return
    data = json.loads(content)
    if section in data:
        data[section]["attempted_num"] = int(attempt_num)
    tmp = os.path.join(remote_workspace, 'config', f'.{cfg_name}.tmp')
    wf = sftp.open(tmp, 'w'); wf.write(json.dumps(data, indent=2)); wf.flush(); wf.close()
    sftp.close()
    ssh_exec(client, [f"mv {tmp} {remote_cfg}"], verbose=False)


def update_compute_node_config(client, machine_id, hot_key_top_n, hybrid_skew_threshold):
    """同时设置 machine_id / hot_key_top_n / hybrid_skew_threshold。"""
    remote_cfg = os.path.join(remote_workspace, 'config', 'compute_node_config.json')
    sftp = client.open_sftp()
    rf = sftp.open(remote_cfg, 'r'); content = rf.read().decode('utf-8'); rf.close()
    data = json.loads(content)
    if 'local_compute_node' not in data:
        data['local_compute_node'] = {}
    data['local_compute_node']['machine_id'] = int(machine_id)
    data['hot_key_top_n'] = int(hot_key_top_n)
    data['hybrid_skew_threshold'] = float(hybrid_skew_threshold)
    tmp = os.path.join(remote_workspace, 'config', '.compute_node_config.json.tmp')
    wf = sftp.open(tmp, 'w'); wf.write(json.dumps(data, indent=2)); wf.flush(); wf.close()
    sftp.close()
    ssh_exec(client, [f"mv {tmp} {remote_cfg}"], verbose=False)


def update_local_compute_node_config(hot_key_top_n, hybrid_skew_threshold):
    """同步把本地 config 也改了（只改这两个参数），方便后面 distribute_config_to_node 把改动带到所有节点。"""
    cfg_path = os.path.join(workspace, 'config', 'compute_node_config.json')
    with open(cfg_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    data['hot_key_top_n'] = int(hot_key_top_n)
    data['hybrid_skew_threshold'] = float(hybrid_skew_threshold)
    with open(cfg_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)


def read_node_matrix_kv(path):
    if not os.path.exists(path):
        return {}, []
    data, keys_order = {}, []
    with open(path, 'r', encoding='utf-8') as f:
        for line_idx, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            key, val = None, None
            if '=' in line:
                parts = line.split('=')
                if len(parts) == 2:
                    key = parts[0].strip()
                    val_str = parts[1].strip()
                    try:
                        sub_parts = val_str.split()
                        val = float(sub_parts[0]) if len(sub_parts) == 1 else [float(x) for x in sub_parts]
                    except Exception:
                        pass
            else:
                try:
                    parts = line.split()
                    val = float(parts[0]) if len(parts) == 1 else [float(x) for x in parts]
                    key = f"line_{line_idx + 1}"
                except Exception:
                    pass
            if key and val is not None:
                data[key] = val
                keys_order.append(key)
    return data, keys_order


def aggregate_results(result_base_dir, node_count):
    known_sum_keys = {
        'throughput',
        'fetch_from_remote_count', 'fetch_from_storage_count', 'fetch_from_local_count', 'evicted_pages_count',
        'wait_log_flush_count', 'ownership_transfer_count', 'ownership_transfer_time_total',
        'notify_push_page_count', 'notify_push_page_time',
        'wait_log_flush_time', 'wait_log_flush_push_page_time', 'wait_log_flush_evict_page_time', 'wait_log_flush_tx_over_time',
        'log_flush_count', 'log_flush_time', 'log_flush_total_batch',
        'commit_log_count', 'prepare_log_count', 'backup_log_count', 'update_log_count',
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
    aggregated_data, all_keys_order = {}, []
    for i in range(node_count):
        p = os.path.join(result_base_dir, f"node{i}", "result.txt")
        node_data, node_keys = read_node_matrix_kv(p)
        if i == 0:
            all_keys_order = node_keys
        for k, v in node_data.items():
            aggregated_data.setdefault(k, []).append(v)
    if not aggregated_data:
        return [], []
    final_rows, final_keys = [], []
    for k in all_keys_order:
        vals = aggregated_data.get(k, [])
        if not vals:
            continue
        if isinstance(vals[0], list):
            val_len = len(vals[0])
            summed = [0.0] * val_len
            for v_list in vals:
                if isinstance(v_list, list) and len(v_list) == val_len:
                    for idx, v in enumerate(v_list):
                        summed[idx] += v
            final_rows.append(summed); final_keys.append(k)
        else:
            should_sum = (k in known_sum_keys) or (k in {'line_2', 'line_4', 'line_5', 'line_6', 'line_7'})
            v = sum(vals) if should_sum else sum(vals) / len(vals)
            final_rows.append([v]); final_keys.append(k)
    return final_rows, final_keys


def main():
    if not compute_server_hostnames:
        logging.error("no compute hostnames configured"); return
    node_count = len(compute_server_hostnames)
    try:
        import socket
        s = socket.socket(); s.settimeout(3); s.connect((remote_server_host, remote_server_port)); s.close()
    except Exception:
        logging.error(f"Cannot reach {remote_server_host}:{remote_server_port}"); return

    ts = time.strftime("%Y%m%d%H%M%S", time.localtime())
    result_dir = os.path.join(workspace, "result", f"hybrid_params_{ts}")
    os.makedirs(result_dir, exist_ok=True)
    build_dir = remote_build_dir

    # 顶层汇总 csv：每行一个 (bench, theta, top_n, threshold) 组合的关键指标
    summary_csv = os.path.join(result_dir, "hybrid_params_summary.csv")
    with open(summary_csv, 'w', encoding='utf-8') as f:
        f.write("bench,zipf_theta,hot_key_top_n,hybrid_skew_threshold,wr,lr,throughput,"
                "hybrid_2pc_commit_count,hybrid_lazy_commit_count\n")

    for r in range(repeats):
        round_dir = os.path.join(result_dir, f"round_{r:02d}")
        os.makedirs(round_dir, exist_ok=True)

        for bench_name in bench_names:
            current_attempt_num = resolve_attempt_num(bench_name)
            access_pattern_values = zipfian_theta if use_zipfian else tx_hot_list
            for pattern_value in access_pattern_values:
                for cr in local_ratios:
                    for write_txn_ratio in write_txn_ratios:
                        for hot_n in hot_key_top_n_list:
                            for skew_th in hybrid_skew_threshold_list:
                                # 先把本地 compute_node_config.json 中的两个参数改了，再 distribute 到所有节点
                                update_local_compute_node_config(hot_n, skew_th)

                                combo_name = (f"{bench_name}_theta{pattern_value}_lr{cr}_wr{write_txn_ratio}"
                                              f"_topn{hot_n}_skew{skew_th}")
                                combo_dir = os.path.join(round_dir, combo_name)
                                os.makedirs(combo_dir, exist_ok=True)
                                logging.info(f"=== running combo: {combo_name} ===")

                                # 分发 config 到所有计算节点 + remote 服务节点
                                try:
                                    cfg_clients = [ssh_client(h, compute_server_ports[i], compute_server_usernames[i],
                                                              compute_server_passwords[i])
                                                   for i, h in enumerate(compute_server_hostnames)]
                                    rs_client = ssh_client(remote_server_host, remote_server_port,
                                                           remote_server_user, remote_server_passwd)
                                except Exception as e:
                                    logging.error(f"SSH connect failed: {e}"); return
                                cfg_clients.append(rs_client)
                                for c in cfg_clients:
                                    distribute_config_to_node(c)
                                    c.close()
                                logging.info("Config Transfer Over")

                                # 启动 storage / remote 服务
                                try:
                                    rs_client = ssh_client(remote_server_host, remote_server_port,
                                                           remote_server_user, remote_server_passwd)
                                except Exception as e:
                                    logging.error(f"SSH connect failed: {e}"); return
                                ok = start_remote_services_checked(rs_client, remote_build_dir, bench_name)
                                rs_client.close()
                                if not ok:
                                    logging.error("remote services failed; abort"); exit(-1)
                                logging.info("Start Remote Over")

                                thread_errors = []
                                lock = threading.Lock()

                                def run_node(i, host, port, out_dir):
                                    try:
                                        rcli = ssh_client(remote_server_host, remote_server_port,
                                                          remote_server_user, remote_server_passwd)
                                        ok_s = check_service_running(rcli, "storage_pool")
                                        ok_r = check_service_running(rcli, "remote_node")
                                        rcli.close()
                                        if not ok_s or not ok_r:
                                            raise RuntimeError("remote services not running")

                                        client = ssh_client(host, port, compute_server_usernames[i],
                                                            compute_server_passwords[i])
                                        try:
                                            ensure_compute_killed(client)
                                            remove_remote_compute_outputs(client, build_dir)
                                            update_compute_node_config(client, i, hot_n, skew_th)
                                            update_access_pattern(client, bench_name, use_zipfian, pattern_value)
                                            update_attempt_num(client, bench_name, current_attempt_num)

                                            args = f"{bench_name} {mode} {thread_num} {write_txn_ratio} {cr} {i}"
                                            log_path = f"{build_dir}/compute_server/compute_server_{i}.out"
                                            time.sleep(handshake_stagger_sec * i)
                                            logging.info(f"Starting ComputeServer host={host} args={args}")
                                            exit_code, _, err = start_compute_blocking(client, build_dir, args, log_path)
                                            if exit_code != 0:
                                                raise RuntimeError(f"compute_server exit={exit_code} detail={err[-1200:]}")
                                            fetch_node_results(client, i, out_dir, build_dir)
                                        finally:
                                            client.close()
                                    except Exception as e:
                                        logging.error(f"node {i} failed: {e}")
                                        with lock:
                                            thread_errors.append(f"node{i}:{e}")

                                threads = []
                                for idx, host in enumerate(compute_server_hostnames):
                                    t = threading.Thread(target=run_node,
                                                         args=(idx, host, compute_server_ports[idx], combo_dir))
                                    threads.append(t); t.start()
                                for t in threads:
                                    t.join()
                                if thread_errors:
                                    raise RuntimeError(f"{combo_name} failed: {'; '.join(thread_errors)}")

                                combo_summary, combo_keys = aggregate_results(combo_dir, node_count)
                                summary_dict = {k: combo_summary[i] for i, k in enumerate(combo_keys)}

                                # 写人类可读 summary
                                human_path = os.path.join(combo_dir, "summary_human.txt")
                                header = {
                                    "round": r, "bench_name": bench_name, "system_name": mode,
                                    "local_ratio": cr, "use_zipfian": use_zipfian,
                                    "zipf_theta": pattern_value if use_zipfian else "",
                                    "tx_hot": pattern_value if not use_zipfian else "",
                                    "thread_num": thread_num, "write_txn_ratio": write_txn_ratio,
                                    "node_count": node_count,
                                    "hot_key_top_n": hot_n,
                                    "hybrid_skew_threshold": skew_th,
                                }
                                with open(human_path, 'w', encoding='utf-8') as hf:
                                    for k, v in header.items():
                                        hf.write(f"{k}={v}\n")
                                    for k in ['total_time_seconds', 'throughput', 'lock_ratio',
                                              'fetch_from_remote_count', 'fetch_from_storage_count',
                                              'fetch_from_local_count', 'evicted_pages_count',
                                              'single_txn_count', 'distribute_txn_count',
                                              'hybrid_2pc_commit_count', 'hybrid_lazy_commit_count',
                                              'tx_begin_time', 'tx_exe_time', 'tx_commit_time', 'tx_abort_time']:
                                        v = summary_dict.get(k, [0])
                                        hf.write(f"{k}={v[0] if isinstance(v, list) else v}\n")

                                tps = summary_dict.get('throughput', [0])
                                tps = tps[0] if isinstance(tps, list) else tps
                                c2pc = summary_dict.get('hybrid_2pc_commit_count', [0])
                                c2pc = c2pc[0] if isinstance(c2pc, list) else c2pc
                                clazy = summary_dict.get('hybrid_lazy_commit_count', [0])
                                clazy = clazy[0] if isinstance(clazy, list) else clazy
                                with open(summary_csv, 'a', encoding='utf-8') as f:
                                    f.write(f"{bench_name},{pattern_value},{hot_n},{skew_th},{write_txn_ratio},{cr},"
                                            f"{tps},{c2pc},{clazy}\n")
                                logging.info(f"=== combo done: {combo_name} tps={tps} 2pc={c2pc} lazy={clazy} ===")
                                cleanup_compute_logs_all_nodes(build_dir)

    logging.info(f"all done. summary csv: {summary_csv}")


if __name__ == '__main__':
    main()
