import paramiko
import threading
import subprocess
import sys

# 机器列表配置
nodes = [
    {'host': '172.16.0.37', 'port': 22, 'user': 'root', 'pass': 'wljwlj123Wlj.'},
    {'host': '172.16.0.38', 'port': 22, 'user': 'root', 'pass': 'wljwlj123Wlj.'},
    {'host': '172.16.0.39', 'port': 22, 'user': 'root', 'pass': 'wljwlj123Wlj.'},
    {'host': '172.16.0.40', 'port': 22, 'user': 'root', 'pass': 'wljwlj123Wlj.'},
]

# 编译命令
default_build_cmd = "cd /usr/local/exper/Hybrid_Cloud_MP/build/ && rm -rf * && cmake .. && make -j10"
storage_build_cmd = "cd /usr/local/exper/Hybrid_Cloud_MP/build/ && rm -rf * && cmake .. && make storage_pool remote_node -j10"

def build_node(node):
    host = node['host']
    port = node['port']
    user = node['user']
    passwd = node['pass']
    
    print(f"[{host}] 开始编译...")
    
    # 区分存储节点和计算节点的编译命令
    # 172.16.0.40 是存储层，只需编译 storage_pool (对应 storage_server) 和 remote_node
    cmd = storage_build_cmd if host == '172.16.0.40' else default_build_cmd
    
    try:
        # 1. 同步代码 (使用 -aq 替代 -avz，静默输出)
        if host == '172.16.0.37':
            sync_cmd = ["rsync", "-aq", "--exclude", "build", "--exclude", ".git", 
                        "/usr/local/workspace/Hybrid_Cloud_MP/", "/usr/local/exper/Hybrid_Cloud_MP/"]
            subprocess.run(sync_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            sync_cmd = ["sshpass", "-p", passwd, "rsync", "-aq", "-e", f"ssh -p {port} -o StrictHostKeyChecking=no",
                        "--exclude", "build", "--exclude", ".git",
                        "/usr/local/workspace/Hybrid_Cloud_MP/", f"{user}@{host}:/usr/local/exper/Hybrid_Cloud_MP/"]
            subprocess.run(sync_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
        # 2. 执行远端编译
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=host, port=port, username=user, password=passwd, timeout=30, banner_timeout=60)
        
        stdin, stdout, stderr = client.exec_command(cmd)
        exit_status = stdout.channel.recv_exit_status()
        client.close()
        
        if exit_status == 0:
            print(f"[{host}] 编译成功!")
        else:
            print(f"[{host}] 编译失败! (Exit Code: {exit_status})")
            
    except Exception as e:
        print(f"[{host}] 发生错误: {e}")

def main():
    threads = []
    for node in nodes:
        t = threading.Thread(target=build_node, args=(node,))
        t.start()
        threads.append(t)
        
    for t in threads:
        t.join()

if __name__ == "__main__":
    main()

