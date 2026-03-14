# 这个文件的目的是，测试多种条件下的性能对比，能够实现多物理机上自动化的脚本运行
# 需要先在预定的机器的指定目录下，安装本项目，然后配置好环境，如 brpc，boost 等，编译通过后，再跑这个脚本

import os
import sys
import io
import time
import json
import threading
import logging
import paramiko

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logging.getLogger("paramiko").setLevel(logging.WARNING)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

workspace = os.getcwd()
remote_workspace = '/usr/local/exper/Hybrid_Cloud_MP'
remote_build_dir = '/usr/local/exper/Hybrid_Cloud_MP/build'

compute_server_build_dir = '/usr/local/exper/Hybrid_Cloud_MP/build/compute_server'
compute_server_hostnames = ['10.10.2.31','10.10.2.32','10.10.2.33','10.10.2.34']
compute_server_ports = [22,22,22,22]           # ssh port
compute_server_usernames = ['root','root','root','root']            # username
compute_server_passwords = ['wljwlj123','wljwlj123','wljwlj123','wljwlj123']    # userpasswd

# remote_server 和 storage_server 在一个服务器上
remote_server_host = '10.10.2.38'
remote_server_port = 22
remote_server_user = 'root'
remote_server_passwd = 'wljwlj123'

modes = ['lazy', '2pc']
bench_names = ['ycsb', 'smallbank']
thread_num = 16
read_only_ratio = 0.2
attempt_num = 3000
repeats = 1
cross_ratios = [0.9 , 0.7 , 0.5, 0.3 , 0.1] #本地访问的比例
tx_hot_list = [90 ,70 , 50 , 30 , 10]  #热点访问比例
# 为了避免存储端一次性元信息发送的监听被并发连接挤爆，分节点顺序错峰启动
handshake_stagger_sec = 2


def ssh_client(host, port , user, passwd):
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(hostname=host, port=port, username=user, password=passwd)
    return c

def ssh_exec(client, cmds, verbose=True):
    outs = []       #存储每个命令的执行结果(输出)
    for cmd in cmds:
        stdin, stdout, stderr = client.exec_command(cmd)
        # 读取输出和错误的结果
        out = stdout.read().decode()
        err = stderr.read().decode()
        
        # 输出和错误
        if verbose:
            logging.info(out.strip())
            if err.strip():
                logging.info(err.strip())
        outs.append((out, err))
        time.sleep(1)
    return outs

def sftp_put(client, local_path, remote_path):
    sftp = client.open_sftp()
    sftp.put(local_path, remote_path)
    sftp.close()

def sftp_get(client, remote_path, local_path):
    sftp = client.open_sftp()
    sftp.get(remote_path, local_path)
    sftp.close()


def distribute_config_to_node(client):
    configs = ['smallbank_config.json', 'ycsb_config.json', 'compute_node_config.json' , 'storage_node_config.json' , 'remote_server_config.json']
    for cfg in configs:
        remote_cfg = os.path.join(remote_workspace, 'config', cfg)
        local_cfg = os.path.join(workspace, 'config', cfg)
        sftp_put(client, local_cfg, remote_cfg)

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
    out = stdout.read().decode().strip()
    return out != ''

# 启动 remote_sver 和 storage_server
def start_remote_services_checked(client, primary_build_dir, workload_name, fallback_build_dir=None):
    # 先把之前的 remote_server 和 storage_server 进程给关了
    ssh_exec(client, ["pkill -f remote_node"], verbose=True)
    ssh_exec(client, ["pkill -f storage_pool"], verbose=True)
    logging.info('Close Remote Service Success')
    time.sleep(2)
    
    def run_service(cmd):
        c = ssh_client(remote_server_host, remote_server_port, remote_server_user, remote_server_passwd)
        ssh_exec(c , [cmd])

    cmd_storage = f"cd {primary_build_dir}/storage_server && rm -f LOG_FILE && ./storage_pool {workload_name}"
    cmd_remote = f"cd {primary_build_dir}/remote_server && ./remote_node {workload_name}"
    
    logging.info(f"Storage Command: {cmd_storage}")
    logging.info(f"Remote Command: {cmd_remote}")
    
    t_storage = threading.Thread(target=run_service, args=(cmd_storage,))
    t_remote = threading.Thread(target=run_service, args=(cmd_remote,))
    
    logging.info('starting remote server and storage server (background threads)')
    t_storage.daemon = True
    t_remote.daemon = True
    
    # 依次启动
    t_storage.start()
    time.sleep(2)
    t_remote.start()
    
    # Give them a moment to start
    time.sleep(15)
    
    # 检查是否启动成功
    c = ssh_client(remote_server_host, remote_server_port, remote_server_user, remote_server_passwd)
    ok_storage = check_service_running(c, "storage_pool")
    ok_remote = check_service_running(c, "remote_node")
    c.close()
    
    if not ok_storage:
        logging.error("storage_pool failed to start")
        exit(-1)
    if not ok_remote:
        logging.error("remote_node failed to start")
        exit(-1)
        
    return True

def kill_compute(client):
    ssh_exec(client, ["pkill -f compute_server"], verbose=False)

def ensure_compute_killed(client):
    ssh_exec(client, ["pkill compute_server"], verbose=False)

def start_compute_blocking(client, build_dir, args, log_path):
    cmd = f"bash -lc 'cd {compute_server_build_dir} && {compute_server_build_dir}/compute_server {args}'"
    stdin, stdout, stderr = client.exec_command(cmd)
    
    # 必须持续读取输出直到命令结束，否则会直接返回或者因为 buffer 满而阻塞
    while not stdout.channel.exit_status_ready():
        if stdout.channel.recv_ready():
            out = stdout.channel.recv(1024)
        if stderr.channel.recv_ready():
            err = stderr.channel.recv(1024)
        time.sleep(1)
        
    # 确保读取完所有剩余输出
    out = stdout.read().decode().strip()
    err = stderr.read().decode().strip()
    
    logging.info(f'Compute Server {args} exit with {stdout.channel.recv_exit_status()}')

    if stdout.channel.recv_exit_status() != 0:
        if out:
            logging.error(f"STDOUT:\n{out}")
        if err:
            logging.error(f"STDERR:\n{err}")
    else:
        # 成功时也可以选择打印部分日志，或者保持静默
        if err:
            logging.warning(f"STDERR (warning):\n{err}")

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
    except Exception:
        pass
    try:
        sftp_get(client, rp2, lp2)
    except Exception:
        pass
    
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

def update_attempt_num(client, bench_name, attempt_num):
    if bench_name == "smallbank":
        cfg_name = "smallbank_config.json"
        section = "smallbank"
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

def update_remote_compute_config(client, machine_num, machine_id):
    remote_cfg = os.path.join(remote_workspace, 'config', 'compute_node_config.json')
    sftp = client.open_sftp()
    rf = sftp.open(remote_cfg, 'r')
    content = rf.read().decode('utf-8')
    rf.close()
    data = json.loads(content)
    if 'local_compute_node' not in data:
        data['local_compute_node'] = {}
    data['local_compute_node']['machine_num'] = int(machine_num)
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
        'log_flush_count', 'log_flush_time', 'log_flush_total_batch',
        'txn_participants_1', 'txn_participants_multi',
        'commit_log_count', 'prepare_log_count', 'backup_log_count',
        'update_log_count',
        'lazy_getpage_dire', 'lazy_getpage_wait'
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
    if not compute_server_hostnames or not compute_server_usernames or not compute_server_passwords:
        logging.info("not configure compute_server_hostnames, compute_server_ports, compute_server_usernames, compute_server_passwords, remote_server_host , break")
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
            for txh in tx_hot_list:
                for cr in cross_ratios:
                    for mode in modes:
                        mode_dir = os.path.join(round_dir, f"{bench_name}_{mode}")
                        os.makedirs(mode_dir, exist_ok=True)
                        local_ratio = cr

                        logging.info(f"Set Account Success for {bench_name}")
                        cfg_clients = [ssh_client(h, compute_server_ports[i], compute_server_usernames[i], compute_server_passwords[i]) for i, h in enumerate(compute_server_hostnames)]
                        
                        rs_client = ssh_client(remote_server_host, remote_server_port , remote_server_user, remote_server_passwd)
                        cfg_clients.append(rs_client)

                        for c in cfg_clients:
                            distribute_config_to_node(c)
                            # rebuild_compute_server(c, build_dir)
                            c.close()
                        logging.info("Config Transfer And Build Over")
                        
                        # 重新连接 rs_client 用于启动服务
                        rs_client = ssh_client(remote_server_host, remote_server_port , remote_server_user, remote_server_passwd)
                        # 启动 remote_server 和 storage_server
                        ok = start_remote_services_checked(rs_client, remote_build_dir, bench_name, fallback_build_dir=os.path.join("/usr/local/exper/Hybrid_Cloud_MP", "build"))
                        logging.info("Start Remote Over")
                        rs_client.close()
                        if not ok:
                            logging.error("remote services failed to start; check build_dir paths and binaries")
                            exit(-1)

                        # 构建一个字符串，表示各个参数的名字，例如 cr_0.9_tx_hot_39
                        combo_dir_name = f"cr_{cr}_txhot_{txh}"
                        # 在 round_dir 目录下再搞一个文件夹，表示当前参数
                        combo_dir = os.path.join(mode_dir, combo_dir_name)
                        os.makedirs(combo_dir, exist_ok=True)

                        logging.info(f"Creating Dir , {combo_dir}")
                        threads = []
                        def run_node(i, host, port, out_dir):
                            remote_client = ssh_client(remote_server_host, remote_server_port, remote_server_user, remote_server_passwd)
                            ok_storage = check_service_running(remote_client, "storage_pool")
                            ok_remote = check_service_running(remote_client, "remote_node")
                            remote_client.close()
                            if (not ok_remote or not ok_storage):
                                logging.error("try to starting computeserver , but remote not ok")
                                exit(-1)
                            client = ssh_client(host, port, compute_server_usernames[i], compute_server_passwords[i])
                            ensure_compute_killed(client)
                            update_remote_compute_config(client, len(compute_server_hostnames), i)
                            # Update hot rate config
                            update_hot_rate(client, bench_name, txh)
                            # Update attempt num config
                            update_attempt_num(client, bench_name, attempt_num)
                            
                            args = f"{bench_name} {mode} {thread_num} {read_only_ratio} {local_ratio} {i}"
                            log_path = f"{build_dir}/compute_server/compute_server_{i}.out"
                            time.sleep(20)
                            # 错峰等待：第 i 个节点等待 i*handshake_stagger_sec 秒，避免并发握手导致连接重置
                            time.sleep(handshake_stagger_sec * (i))
                            logging.info(f"Starting ComputeServer , hostname = {host} , args = {args}")
                            start_compute_blocking(client, build_dir, args, log_path)
                            logging.info("Running ComputeServer Over")
                            ok = True
                            header = {
                                "round": r,
                                "bench_name": bench_name,
                                "system_name": mode,
                                "cross_ratio": cr,
                                "local_txn_ratio": local_ratio,
                                "tx_hot": txh,
                                "thread_num": thread_num,
                                "read_only_ratio": read_only_ratio,
                                "node_count": len(compute_server_hostnames),
                                "combo_path": out_dir
                            }
                            # build_dir = .../build
                            fetch_node_results(client, i, out_dir, build_dir, header)
                            client.close()
                            if not ok:
                                logging.info(f"node {i} timeout")
                                exit(1)

                        # 让所有的计算节点，都去跑 computeserver
                        for idx, host in enumerate(compute_server_hostnames):
                            t = threading.Thread(target=run_node, args=(idx, host, compute_server_ports[idx], combo_dir))
                            threads.append(t)
                            t.start()

                        for t in threads:
                            t.join()

                        combo_summary, combo_keys = aggregate_results(combo_dir, len(compute_server_hostnames))
                        combo_header = {
                            "round": r,
                            "bench_name": bench_name,
                            "system_name": mode,
                            "cross_ratio": cr,
                            "local_txn_ratio": local_ratio,
                            "tx_hot": txh,
                            "thread_num": thread_num,
                            "read_only_ratio": read_only_ratio,
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
                                'wait_log_flush_push_page_time','wait_log_flush_tx_over_time',
                                'wait_log_flush_count','ownership_transfer_count','ownership_transfer_time_total','log_flush_count','log_flush_time','log_flush_avg_batch',
                                'log_flush_max_batch','log_flush_total_batch',
                                'tx_commit_time','tx_abort_time',
                                'tx_fetch_exe_time','tx_fetch_commit_time','tx_fetch_abort_time',
                                'tx_release_exe_time','tx_release_commit_time','tx_release_abort_time',
                                'txn_participants_1','txn_participants_multi',
                                'commit_log_count','prepare_log_count','backup_log_count',
                                'tx_write_commit_log_time','tx_write_commit_log_time2',
                                'tx_write_prepare_log_time','tx_write_backup_log_time',
                                'tx_get_timestamp_time1','tx_get_timestamp_time2',
                                'update_log_count',
                                'ownership_transfer_time_avg_ms',
                                'lazy_getpage_dire', 'lazy_getpage_wait'
                            ]
                            
                            for k in stages:
                                val = 0
                                if k in summary_dict:
                                    val = summary_dict[k][0]
                                hf.write(f"{k}={val}\n")
                                
                        logging.info(f"round {r} {combo_dir_name} done")

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
        "cross_ratios": ",".join(str(x) for x in cross_ratios),
        "tx_hot_list": ",".join(str(x) for x in tx_hot_list),
        "hot_accounts_list": ",".join(str(x) for x in hot_accounts_list),
        "thread_num": thread_num,
        "read_only_ratio": read_only_ratio,
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
        
        if bench_name == 'smallbank':
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
            'wait_log_flush_push_page_time','wait_log_flush_tx_over_time',
            'wait_log_flush_count','ownership_transfer_count','ownership_transfer_time_total','log_flush_count','log_flush_time','log_flush_avg_batch',
            'log_flush_max_batch','log_flush_total_batch',
            'tx_commit_time','tx_abort_time',
            'tx_fetch_exe_time','tx_fetch_commit_time','tx_fetch_abort_time',
            'tx_release_exe_time','tx_release_commit_time','tx_release_abort_time',
            'txn_participants_1','txn_participants_multi',
            'commit_log_count','prepare_log_count','backup_log_count',
            'tx_write_commit_log_time','tx_write_commit_log_time2',
            'tx_write_prepare_log_time','tx_write_backup_log_time',
            'tx_get_timestamp_time1','tx_get_timestamp_time2',
            'update_log_count',
            'ownership_transfer_time_avg_ms',
            'lazy_getpage_dire', 'lazy_getpage_wait'
        ]
        
        for k in stages:
            val = 0
            if k in summary_dict:
                val = summary_dict[k][0]
            hf.write(f"{k}={val}\n")
            
    logging.info(f"final summary in {result_dir}")

if __name__ == '__main__':
    main()
