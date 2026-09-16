# 这个文件的目的是，测试多种条件下的性能对比，能够实现多物理机上自动化的脚本运行
# 需要先在预定的机器的指定目录下，安装本项目，然后配置好环境，如 brpc，boost 等，编译通过后，再跑这个脚本

import io
import json
import logging
import os
from pathlib import Path
import sys
import threading
import time

import paramiko

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logging.getLogger("paramiko").setLevel(logging.WARNING)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

workspace = str(Path(__file__).resolve().parents[2])
remote_workspace = os.environ.get("HMP_REMOTE_WORKSPACE", "/opt/hybrid-cloud-mp")
remote_build_dir = os.path.join(remote_workspace, "build")

# 集群连接信息从环境变量读取，密码为空时 Paramiko 会尝试 SSH key/agent。
compute_server_hostnames = [host for host in os.environ.get("HMP_COMPUTE_HOSTS", "").split(",") if host]
compute_server_ports = [int(os.environ.get("HMP_SSH_PORT", "22"))] * len(compute_server_hostnames)
compute_server_usernames = [os.environ.get("HMP_SSH_USER", os.environ.get("USER", ""))] * len(compute_server_hostnames)
compute_server_passwords = [os.environ.get("HMP_SSH_PASSWORD", "")] * len(compute_server_hostnames)

# remote_server 和 storage_server 在同一台服务器上。
remote_server_host = os.environ.get("HMP_REMOTE_HOST", "")
remote_server_port = int(os.environ.get("HMP_REMOTE_PORT", os.environ.get("HMP_SSH_PORT", "22")))
remote_server_user = os.environ.get("HMP_REMOTE_USER", os.environ.get("HMP_SSH_USER", os.environ.get("USER", "")))
remote_server_passwd = os.environ.get("HMP_REMOTE_PASSWORD", os.environ.get("HMP_SSH_PASSWORD", ""))

modes = ['lazy', '2pc']
bench_names = ['ycsb', 'smallbank']
thread_num = 15
read_only_ratio = 0.0
attempt_num = 60000
repeats = 1
cross_ratios = [0.9 , 0.7 , 0.5, 0.3 , 0.1] #本地访问的比例
tx_hot_list = [10 ,30, 50 , 70 , 90]  #热点访问比例
# 为了避免存储端一次性元信息发送的监听被并发连接挤爆，分节点顺序错峰启动
handshake_stagger_sec = 2

COMMON_METRICS = [
    'total_time_seconds', 'throughput', 'lock_ratio',
    'fetch_from_remote_count', 'fetch_from_storage_count', 'fetch_from_local_count',
    'evicted_pages_count', 'fetch_three_count', 'fetch_four_count',
    'from_remote_ratio', 'from_storage_ratio', 'from_local_ratio',
]
TRANSACTION_NAMES = {
    'smallbank': ['Amalgamate', 'Balance', 'DepositChecking', 'SendPayment', 'TransactSaving', 'WriteCheck'],
    'tpcc': ['NewOrder', 'Payment', 'Delivery', 'OrderStatus', 'StockLevel'],
    'ycsb': ['Transaction'],
}
STAGE_METRICS = [
    'tx_begin_time', 'tx_exe_time', 'tx_commit_time', 'tx_abort_time',
    'tx_fetch_exe_time', 'tx_fetch_commit_time', 'tx_fetch_abort_time',
    'tx_release_exe_time', 'tx_release_commit_time', 'tx_release_abort_time',
]


def ssh_client(host, port, user, password=None):
    c = paramiko.SSHClient()
    c.load_system_host_keys()
    c.set_missing_host_key_policy(paramiko.RejectPolicy())
    c.connect(hostname=host, port=port, username=user, password=password or None)
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
    configs = ['smallbank_config.json', 'tpcc_config.json', 'ycsb_config.json', 'compute_node_config.json', 'storage_node_config.json', 'remote_server_config.json']
    for cfg in configs:
        remote_cfg = os.path.join(remote_workspace, 'config', cfg)
        local_cfg = os.path.join(workspace, 'config', cfg)
        sftp_put(client, local_cfg, remote_cfg)


def update_remote_benchmark_config(client, bench_name, attempted, tx_hot):
    if bench_name not in ('smallbank', 'tpcc', 'ycsb'):
        return

    remote_cfg = os.path.join(remote_workspace, 'config', f'{bench_name}_config.json')
    sftp = client.open_sftp()
    with sftp.open(remote_cfg, 'r') as config_file:
        data = json.loads(config_file.read().decode('utf-8'))

    bench_config = data[bench_name]
    bench_config['attempted_num'] = int(attempted)
    if bench_name == 'smallbank':
        bench_config['num_hot_rate'] = int(tx_hot)
    elif bench_name == 'ycsb':
        bench_config['TX_HOT'] = int(tx_hot)

    temp_cfg = f'{remote_cfg}.tmp'
    with sftp.open(temp_cfg, 'w') as config_file:
        config_file.write(json.dumps(data, indent=2))
        config_file.flush()
    sftp.close()
    ssh_exec(client, [f'mv {temp_cfg} {remote_cfg}'], verbose=False)


def rebuild_compute_server(client, build_dir):
    cmds = [
        f"cd {build_dir} && cmake ..",
        f"cd {build_dir} && make -j14"
    ]
    ssh_exec(client, cmds , verbose=True)

# 检查服务名为 name 的服务有没有真的跑起来
def check_service_running(client, name):
    stdin, stdout, stderr = client.exec_command(f"pgrep {name}")
    out = stdout.read().decode().strip()
    return out != ''

# 启动 remote_sver 和 storage_server
def start_remote_services_checked(client, primary_build_dir, workload_name):
    # 先把之前的 remote_server 和 storage_server 进程给关了
    ssh_exec(client, ["pkill -f remote_node"], verbose=True)
    ssh_exec(client, ["pkill -f storage_pool"], verbose=True)
    logging.info('Close Remote Service Success')
    time.sleep(2)
    
    def run_service(cmd):
        c = ssh_client(remote_server_host, remote_server_port, remote_server_user, remote_server_passwd)
        ssh_exec(c , [cmd])

    cmd_storage = f"cd {primary_build_dir}/storage_server && ./storage_pool {workload_name}"
    cmd_remote = f"cd {primary_build_dir}/remote_server && ./remote_node {workload_name}"
    
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

def ensure_compute_killed(client):
    ssh_exec(client, ["pkill compute_server"], verbose=False)

def start_compute_blocking(client, build_dir, args):
    compute_dir = os.path.join(build_dir, "compute_server")
    cmd = f"bash -lc 'cd {compute_dir} && ./compute_server {args}'"
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

    if out:
        # logging.info(out)
        pass
    if err:
        # logging.info(err)
        pass

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

def read_node_matrix(path):
    if not os.path.exists(path):
        return []
    with open(path, 'r', encoding='utf-8') as f:
        rows = []
        for line in f:
            parts = line.strip().split()
            try:
                nums = [float(x) for x in parts]
                rows.append(nums)
            except Exception:
                pass
        return rows


def write_human_summary(file_path, summary, header, include_transaction_details=True):
    with open(file_path, 'w', encoding='utf-8') as output:
        for key, value in header.items():
            output.write(f'{key}={value}\n')

        for row_index, metric_name in enumerate(COMMON_METRICS):
            value = summary[row_index][0] if row_index < len(summary) and summary[row_index] else 0
            output.write(f'{metric_name}={value}\n')

        if not include_transaction_details:
            return

        transaction_names = TRANSACTION_NAMES.get(header.get('bench_name'), [])
        transaction_base = len(COMMON_METRICS)
        for offset, transaction_name in enumerate(transaction_names):
            row_index = transaction_base + offset
            if row_index < len(summary) and len(summary[row_index]) >= 2:
                output.write(f'{transaction_name}_try={summary[row_index][0]}\n')
                output.write(f'{transaction_name}_commit={summary[row_index][1]}\n')

        rollback_base = transaction_base + len(transaction_names)
        for offset, transaction_name in enumerate(transaction_names):
            row_index = rollback_base + offset
            value = summary[row_index][0] if row_index < len(summary) and summary[row_index] else 0
            output.write(f'{transaction_name}_rollback_rate={value}\n')

        stage_base = rollback_base + len(transaction_names)
        for offset, metric_name in enumerate(STAGE_METRICS):
            row_index = stage_base + offset
            value = summary[row_index][0] if row_index < len(summary) and summary[row_index] else 0
            output.write(f'{metric_name}={value}\n')


def aggregate_results(result_base_dir, node_count, bench_name):
    data = []
    for i in range(node_count):
        p = os.path.join(result_base_dir, f"node{i}", "result.txt")
        rows = read_node_matrix(p)
        if rows:
            data.append(rows)
    if not data:
        return []
    max_rows = max(len(matrix) for matrix in data)
    transaction_base = len(COMMON_METRICS)
    transaction_count = len(TRANSACTION_NAMES.get(bench_name, []))
    sum_rows = {1, 3, 4, 5, 6, 7, 8}
    sum_rows.update(range(transaction_base, transaction_base + transaction_count))
    agg = []
    for r in range(max_rows):
        cols = []
        row_width = max(len(matrix[r]) if r < len(matrix) else 0 for matrix in data)
        for c in range(row_width):
            vals = []
            for m in data:
                if r < len(m) and c < len(m[r]):
                    vals.append(m[r][c])
            if vals:
                if r in sum_rows:
                    cols.append(sum(vals))
                else:
                    cols.append(sum(vals) / len(vals))
            else:
                cols.append(0.0)
        agg.append(cols)
    return agg

def aggregate_round_summaries(base_dir, repeats):
    data = []
    for r in range(repeats):
        p = os.path.join(base_dir, f"round_{r:02d}", "result.txt")
        rows = read_node_matrix(p)
        if rows:
            data.append(rows)
    if not data:
        return []
    max_rows = max(len(matrix) for matrix in data)
    agg = []
    for r in range(max_rows):
        cols = []
        row_width = max(len(matrix[r]) if r < len(matrix) else 0 for matrix in data)
        for c in range(row_width):
            vals = []
            for m in data:
                if r < len(m) and c < len(m[r]):
                    vals.append(m[r][c])
            cols.append(sum(vals) / len(vals) if vals else 0.0)
        agg.append(cols)
    return agg

def aggregate_round_from_combos(round_dir):
    data = []
    for name in os.listdir(round_dir):
        first = os.path.join(round_dir, name)
        if not os.path.isdir(first):
            continue
        p_direct = os.path.join(first, "summary_matrix.txt")
        rows = read_node_matrix(p_direct)
        if rows:
            data.append(rows)
            continue
        for subname in os.listdir(first):
            second = os.path.join(first, subname)
            if not os.path.isdir(second):
                continue
            p = os.path.join(second, "summary_matrix.txt")
            rows = read_node_matrix(p)
            if rows:
                data.append(rows)
    if not data:
        return []
    max_rows = max(len(matrix) for matrix in data)
    agg = []
    for r in range(max_rows):
        cols = []
        row_width = max(len(matrix[r]) if r < len(matrix) else 0 for matrix in data)
        for c in range(row_width):
            vals = []
            for m in data:
                if r < len(m) and c < len(m[r]):
                    vals.append(m[r][c])
            cols.append(sum(vals) / len(vals) if vals else 0.0)
        agg.append(cols)
    return agg

def main():
    if not compute_server_hostnames or not remote_server_host:
        logging.error("请设置 HMP_COMPUTE_HOSTS 和 HMP_REMOTE_HOST 环境变量")
        return 2
    ts = time.strftime("%Y%m%d%H%M%S", time.localtime())
    # workspace 就是当前运行这个脚本的目录，目前就是 workspace/result/时间戳
    result_dir = os.path.join(workspace, "result", ts)
    os.makedirs(result_dir, exist_ok=True)
    build_dir = remote_build_dir
    single_benchmark = len(set(bench_names)) == 1

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

                        logging.info(f"Apply {bench_name} config: attempted_num={attempt_num}, tx_hot={txh}")
                        cfg_clients = [ssh_client(h, compute_server_ports[i], compute_server_usernames[i], compute_server_passwords[i]) for i, h in enumerate(compute_server_hostnames)]

                        rs_client = ssh_client(remote_server_host, remote_server_port, remote_server_user, remote_server_passwd)
                        cfg_clients.append(rs_client)

                        for client in cfg_clients:
                            distribute_config_to_node(client)
                            update_remote_benchmark_config(client, bench_name, attempt_num, txh)
                            rebuild_compute_server(client, build_dir)
                            client.close()
                        logging.info("Config transfer, parameter update, and build complete")
                        
                        # 重新连接 rs_client 用于启动服务
                        rs_client = ssh_client(remote_server_host, remote_server_port , remote_server_user, remote_server_passwd)
                        # 启动 remote_server 和 storage_server
                        ok = start_remote_services_checked(rs_client, remote_build_dir, bench_name)
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
                            args = f"{bench_name} {mode} {thread_num} {read_only_ratio} {local_ratio} {i}"
                            time.sleep(20)
                            # 错峰等待：第 i 个节点等待 i*handshake_stagger_sec 秒，避免并发握手导致连接重置
                            time.sleep(handshake_stagger_sec * i)
                            logging.info(f"Starting ComputeServer , hostname = {host} , args = {args}")
                            start_compute_blocking(client, build_dir, args)
                            logging.info("Running ComputeServer Over")
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

                        # 让所有的计算节点，都去跑 computeserver
                        for idx, host in enumerate(compute_server_hostnames):
                            t = threading.Thread(target=run_node, args=(idx, host, compute_server_ports[idx], combo_dir))
                            threads.append(t)
                            t.start()

                        for t in threads:
                            t.join()

                        combo_summary = aggregate_results(combo_dir, len(compute_server_hostnames), bench_name)
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
                        human_path = os.path.join(combo_dir, "summary_human.txt")
                        write_human_summary(human_path, combo_summary, combo_header)
                        logging.info(f"round {r} {combo_dir_name} done")

        # 不同工作负载的事务明细布局不同，混合汇总时只保留公共指标。
        round_summary = aggregate_round_from_combos(round_dir)
        if not single_benchmark:
            round_summary = round_summary[:len(COMMON_METRICS)]
        round_result_path = os.path.join(round_dir, "result.txt")
        with open(round_result_path, 'w', encoding='utf-8') as rf:
            for row in round_summary:
                rf.write(" ".join(str(x) for x in row) + "\n")

    final_summary = aggregate_round_summaries(result_dir, repeats)
    final_header = {
        "type": "final_summary",
        "bench_names": ",".join(bench_names),
        "system_names": ",".join(modes),
        "repeats": repeats,
        "cross_ratios": ",".join(str(x) for x in cross_ratios),
        "tx_hot_list": ",".join(str(x) for x in tx_hot_list),
        "thread_num": thread_num,
        "read_only_ratio": read_only_ratio,
        "node_count": len(compute_server_hostnames)
    }
    if single_benchmark:
        final_header["bench_name"] = bench_names[0]
    # final matrix
    final_mat = os.path.join(result_dir, "final_matrix.txt")
    with open(final_mat, 'w', encoding='utf-8') as mf:
        for row in final_summary:
            mf.write(" ".join(str(x) for x in row) + "\n")
    final_human = os.path.join(result_dir, "final_human.txt")
    write_human_summary(
        final_human,
        final_summary,
        final_header,
        include_transaction_details=single_benchmark,
    )
    logging.info(f"final summary in {result_dir}")

if __name__ == '__main__':
    sys.exit(main() or 0)
