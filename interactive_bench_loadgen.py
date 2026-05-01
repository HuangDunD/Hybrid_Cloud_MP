#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
interactive_bench_loadgen.py — Interactive Bench 模式的 Python 压测脚本

针对计算节点的交互模式 (./compute_server interactive <bench> <node_id>)
建立指定数量的并发 TCP 连接，每个连接每行发送一个完整事务 (类似存储
过程)，事务里可以串多个 op，op 之间用 ';' 分隔，op 内部 ',' 分隔：

    <tid>,<key>,<is_write>[,<write_value>] ; <tid>,<key>,<is_write>[,<write_value>] ; ...

  - 写 (is_write=1) 必须带 write_value:
      ycsb     -> 字符串 (写入 file_0(100B); 自动右填充 '\0' / 截断)
      smallbank-> 浮点数 (写入 bal)
  - 读 (is_write=0) 不带 write_value

所有参数集中在下方 CONFIG 配置块里直接改，无需命令行。

按 Ctrl-C 可提前停止并打印统计。
"""
import random
import signal
import socket
import sys
import threading
import time
from collections import defaultdict


# =====================================================================
# CONFIG —— 所有可调参数全在这里改
# =====================================================================
class CONFIG:
    # --- 连接 ---
    HOST = "127.0.0.1"
    PORT = 9115                  # = 9115 + node_id
    BENCH = "ycsb"               # "ycsb" or "smallbank" — 决定 write_value 生成方式

    # --- 并发 / 时长 ---
    CONNECTIONS = 4              # 并发连接数
    DURATION = 60.0              # 持续时长(秒)；当 TOTAL_TXNS != None 时被忽略
    TOTAL_TXNS = None            # 总事务数；不为 None 时优先于 DURATION

    # --- 工作负载 ---
    TABLE_IDS = [0]              # 访问的 table_id 列表，例如 [0] 或 [0, 1]
    KEY_MIN = 0                  # key 范围下界 (含)
    KEY_MAX = 3000               # key 范围上界 (不含)
    WRITE_RATIO = 0.5            # 单个 op 是写的概率 [0.0, 1.0]
    OPS_PER_TXN_MIN = 1          # 每个事务里最少 op 数 (>=1)
    OPS_PER_TXN_MAX = 5          # 每个事务里最多 op 数
    YCSB_WRITE_VALUE_LEN = 16    # ycsb 写时随机字符串长度 (服务端会自动 pad/截断到 100B)

    # --- 杂项 ---
    TIMEOUT = 10.0               # 单次 RPC 超时秒数
    SEED = None                  # 随机种子；None 用 OS 熵
    REPORT_INTERVAL = 1.0        # 实时统计打印间隔；<=0 关闭
# =====================================================================



# ----------------------- 单连接 worker -----------------------
class Worker(threading.Thread):
    def __init__(self, idx, host, port, args, stop_event, stats):
        super().__init__(daemon=True)
        self.idx = idx
        self.host = host
        self.port = port
        self.args = args
        self.stop_event = stop_event
        self.stats = stats
        self.rng = random.Random(args.SEED + idx if args.SEED is not None
                                 else None)
        self.sock = None
        self.recv_buf = b""

    # 简易按行接收
    def recv_line(self, timeout=10.0):
        self.sock.settimeout(timeout)
        while b"\n" not in self.recv_buf:
            chunk = self.sock.recv(8192)
            if not chunk:
                return None
            self.recv_buf += chunk
        line, _, rest = self.recv_buf.partition(b"\n")
        self.recv_buf = rest
        return line.decode("utf-8", errors="replace").rstrip("\r")

    def connect(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.host, self.port))
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock = s

    def run(self):
        try:
            self.connect()
        except Exception as e:
            print(f"[worker-{self.idx}] connect failed: {e}", file=sys.stderr)
            self.stats["connect_failed"] += 1
            return

        # 字符集用于生成 ycsb write_value
        _alnum = ("abcdefghijklmnopqrstuvwxyz"
                  "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

        def gen_write_value(bench):
            if bench == "ycsb":
                n = self.args.YCSB_WRITE_VALUE_LEN
                return "".join(self.rng.choice(_alnum) for _ in range(n))
            # smallbank: 写 bal -> 浮点
            return f"{self.rng.uniform(0.0, 1000.0):.2f}"

        try:
            while not self.stop_event.is_set():
                # 取下一笔事务配额 (用于 TOTAL_TXNS 总数模式)
                if self.args.TOTAL_TXNS is not None:
                    with self.stats["lock"]:
                        if self.stats["txns_dispatched"] >= self.args.TOTAL_TXNS:
                            break
                        self.stats["txns_dispatched"] += 1

                # 构造一笔事务的 op 列表
                n_ops = self.rng.randint(self.args.OPS_PER_TXN_MIN,
                                         self.args.OPS_PER_TXN_MAX)
                op_strs = []
                w_in_txn = 0
                r_in_txn = 0
                for _ in range(n_ops):
                    table_id = self.rng.choice(self.args.TABLE_IDS)
                    key = self.rng.randint(self.args.KEY_MIN, self.args.KEY_MAX - 1)
                    is_write = 1 if self.rng.random() < self.args.WRITE_RATIO else 0
                    if is_write:
                        wv = gen_write_value(self.args.BENCH)
                        op_strs.append(f"{table_id},{key},1,{wv}")
                        w_in_txn += 1
                    else:
                        op_strs.append(f"{table_id},{key},0")
                        r_in_txn += 1

                cmd = ";".join(op_strs) + "\n"

                t0 = time.perf_counter()
                try:
                    self.sock.sendall(cmd.encode("utf-8"))
                    resp = self.recv_line(self.args.TIMEOUT)
                except (socket.timeout, ConnectionError, OSError) as e:
                    self.stats["errors"] += 1
                    self.stats["last_error"] = f"io: {e}"
                    break

                dt = time.perf_counter() - t0
                if resp is None:
                    self.stats["errors"] += 1
                    self.stats["last_error"] = "server closed conn"
                    break

                # 分类统计 (粒度：事务)
                if resp.startswith("OK:"):
                    self.stats["ok"] += 1
                    self.stats["ok_read"] += r_in_txn
                    self.stats["ok_write"] += w_in_txn
                elif resp.startswith("ABORT"):
                    self.stats["abort"] += 1
                    self.stats["last_error"] = resp
                elif resp.startswith("BUSY"):
                    self.stats["busy"] += 1
                    self.stats["last_error"] = resp
                    break
                else:
                    self.stats["err_resp"] += 1
                    self.stats["last_error"] = resp

                self.stats["latency_sum"] += dt
                self.stats["latency_cnt"] += 1
                if dt > self.stats["latency_max"]:
                    self.stats["latency_max"] = dt
        finally:
            try:
                if self.sock is not None:
                    try:
                        self.sock.sendall(b"quit\n")
                        try:
                            self.recv_line(timeout=1.0)
                        except Exception:
                            pass
                    except Exception:
                        pass
                    self.sock.close()
            except Exception:
                pass
            self.stats["worker_done"] += 1


# ----------------------- 主流程 -----------------------
def make_stats():
    s = defaultdict(int)
    s["lock"] = threading.Lock()
    s["last_error"] = ""
    s["latency_sum"] = 0.0
    s["latency_max"] = 0.0
    return s


def fmt_latency(s):
    n = s["latency_cnt"]
    if n == 0:
        return "n/a"
    avg_ms = s["latency_sum"] / n * 1000
    max_ms = s["latency_max"] * 1000
    return f"avg={avg_ms:.3f}ms max={max_ms:.3f}ms"


def validate_config():
    if CONFIG.KEY_MAX <= CONFIG.KEY_MIN:
        sys.exit("CONFIG.KEY_MAX must be > CONFIG.KEY_MIN")
    if not (0.0 <= CONFIG.WRITE_RATIO <= 1.0):
        sys.exit("CONFIG.WRITE_RATIO must be in [0.0, 1.0]")
    if not CONFIG.TABLE_IDS:
        sys.exit("CONFIG.TABLE_IDS must not be empty")
    if CONFIG.CONNECTIONS <= 0:
        sys.exit("CONFIG.CONNECTIONS must be > 0")
    if CONFIG.OPS_PER_TXN_MIN < 1 or CONFIG.OPS_PER_TXN_MAX < CONFIG.OPS_PER_TXN_MIN:
        sys.exit("CONFIG.OPS_PER_TXN_MIN/MAX invalid")
    if CONFIG.BENCH not in ("ycsb", "smallbank"):
        sys.exit("CONFIG.BENCH must be 'ycsb' or 'smallbank'")


def main():
    validate_config()
    stop_event = threading.Event()
    stats = make_stats()

    print(f">>> Connecting to {CONFIG.HOST}:{CONFIG.PORT}, "
          f"connections={CONFIG.CONNECTIONS}, "
          f"tables={CONFIG.TABLE_IDS}, "
          f"key=[{CONFIG.KEY_MIN},{CONFIG.KEY_MAX}), "
          f"write_ratio={CONFIG.WRITE_RATIO}", file=sys.stderr)
    if CONFIG.TOTAL_TXNS is not None:
        print(f">>> Mode: total_txns={CONFIG.TOTAL_TXNS}", file=sys.stderr)
    else:
        print(f">>> Mode: duration={CONFIG.DURATION}s", file=sys.stderr)

    workers = [Worker(i, CONFIG.HOST, CONFIG.PORT, CONFIG, stop_event, stats)
               for i in range(CONFIG.CONNECTIONS)]

    def handle_sigint(sig, frame):
        if not stop_event.is_set():
            print("\n>>> Caught SIGINT, stopping...", file=sys.stderr)
            stop_event.set()

    signal.signal(signal.SIGINT, handle_sigint)

    t_start = time.perf_counter()
    for w in workers:
        w.start()

    last_ok = 0
    last_t = t_start
    next_report = (t_start + CONFIG.REPORT_INTERVAL
                   if CONFIG.REPORT_INTERVAL > 0 else None)
    deadline = (t_start + CONFIG.DURATION if CONFIG.TOTAL_TXNS is None
                else None)

    try:
        while True:
            if stats["worker_done"] >= CONFIG.CONNECTIONS:
                break
            now = time.perf_counter()
            if deadline is not None and now >= deadline:
                stop_event.set()
            if CONFIG.TOTAL_TXNS is not None and stats["ok"] + stats["abort"] + \
                    stats["err_resp"] >= CONFIG.TOTAL_TXNS:
                stop_event.set()
            if next_report is not None and now >= next_report:
                ok = stats["ok"]
                d = now - last_t
                qps = (ok - last_ok) / d if d > 0 else 0
                print(f"[{int(now - t_start):>4}s] ok={ok} "
                      f"abort={stats['abort']} err={stats['err_resp']} "
                      f"busy={stats['busy']} qps={qps:.1f} "
                      f"{fmt_latency(stats)}",
                      file=sys.stderr)
                last_ok = ok
                last_t = now
                next_report = now + CONFIG.REPORT_INTERVAL
            time.sleep(0.05)
    except KeyboardInterrupt:
        stop_event.set()

    for w in workers:
        w.join(timeout=5)
    t_end = time.perf_counter()
    elapsed = t_end - t_start

    total_ok = stats["ok"]
    qps = total_ok / elapsed if elapsed > 0 else 0

    print("\n========== Summary ==========", file=sys.stderr)
    print(f"elapsed       : {elapsed:.2f} s", file=sys.stderr)
    print(f"connections   : {CONFIG.CONNECTIONS}", file=sys.stderr)
    print(f"ok            : {total_ok} txns ({stats['ok_read']} read ops / "
          f"{stats['ok_write']} write ops)", file=sys.stderr)
    print(f"abort         : {stats['abort']}", file=sys.stderr)
    print(f"err_resp      : {stats['err_resp']}", file=sys.stderr)
    print(f"busy_rejected : {stats['busy']}", file=sys.stderr)
    print(f"connect_fail  : {stats['connect_failed']}", file=sys.stderr)
    print(f"io_errors     : {stats['errors']}", file=sys.stderr)
    print(f"throughput    : {qps:.1f} txns/s (ok only)", file=sys.stderr)
    print(f"latency       : {fmt_latency(stats)}", file=sys.stderr)
    if stats["last_error"]:
        print(f"last_error    : {stats['last_error']}", file=sys.stderr)


if __name__ == "__main__":
    main()
