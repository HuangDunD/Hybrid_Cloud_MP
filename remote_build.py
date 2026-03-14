import paramiko
import threading
import logging
import sys

# 配置日志
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s')

# 机器列表配置
nodes = [
    {'host': '10.10.2.31', 'port': 22, 'user': 'root', 'pass': 'wljwlj123'},
    {'host': '10.10.2.32', 'port': 22, 'user': 'root', 'pass': 'wljwlj123'},
    {'host': '10.10.2.33', 'port': 22, 'user': 'root', 'pass': 'wljwlj123'},
    {'host': '10.10.2.34', 'port': 22, 'user': 'root', 'pass': 'wljwlj123'},
    {'host': '10.10.2.38', 'port': 22, 'user': 'root', 'pass': 'wljwlj123'},
]

# 编译命令
build_cmd = "cd /usr/local/exper/Hybrid_Cloud_MP/build/ && rm -rf compute_server && rm -rf CMakeCache.txt CMakeFiles && cmake .. && make -j30"

def ssh_exec(host, port, user, passwd, cmd):
    try:
        logging.info(f"Connecting to {host}...")
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=host, port=port, username=user, password=passwd)
        
        logging.info(f"[{host}] Executing build command...")
        stdin, stdout, stderr = client.exec_command(cmd)
        
        # 实时读取输出（可选），或者等待执行完毕
        # 这里为了简单，等待执行完毕后一次性打印，或者分块读取
        exit_status = stdout.channel.recv_exit_status()
        
        out = stdout.read().decode().strip()
        err = stderr.read().decode().strip()
        
        client.close()
        
        if exit_status == 0:
            logging.info(f"[{host}] Build Success!")
            # 如果需要看详细输出，可以取消下面注释
            # print(f"[{host}] STDOUT:\n{out}")
        else:
            logging.error(f"[{host}] Build Failed with exit code {exit_status}")
            logging.error(f"[{host}] STDERR:\n{err}")
            if out:
                logging.error(f"[{host}] STDOUT:\n{out}")
                
    except Exception as e:
        logging.error(f"[{host}] Exception: {e}")

def main():
    threads = []
    logging.info(f"Starting parallel build on {len(nodes)} nodes...")
    
    for node in nodes:
        t = threading.Thread(target=ssh_exec, args=(node['host'], node['port'], node['user'], node['pass'], build_cmd))
        t.start()
        threads.append(t)
        
    for t in threads:
        t.join()
        
    logging.info("All build tasks finished.")

if __name__ == "__main__":
    main()
