#!/usr/bin/env python3
import argparse
import os
import signal
import threading
import time


class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.interval_count = 0
        self.interval_ns = 0
        self.total_count = 0
        self.total_ns = 0

    def add(self, elapsed_ns: int):
        with self.lock:
            self.interval_count += 1
            self.interval_ns += elapsed_ns
            self.total_count += 1
            self.total_ns += elapsed_ns

    def snapshot_and_reset_interval(self):
        with self.lock:
            c = self.interval_count
            n = self.interval_ns
            self.interval_count = 0
            self.interval_ns = 0
            tc = self.total_count
            tn = self.total_ns
        return c, n, tc, tn


def worker(thread_id: int, directory: str, payload: bytes, stop_event: threading.Event, stats: Stats):
    file_path = os.path.join(directory, f"fdatasync_bench_{thread_id}.dat")
    fd = os.open(file_path, os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o644)
    try:
        while not stop_event.is_set():
            os.lseek(fd, 0, os.SEEK_SET)
            os.write(fd, payload)
            begin_ns = time.perf_counter_ns()
            os.fdatasync(fd)
            end_ns = time.perf_counter_ns()
            stats.add(end_ns - begin_ns)
    finally:
        os.close(fd)


def run(args):
    os.makedirs(args.dir, exist_ok=True)
    payload = os.urandom(args.block_size)
    stop_event = threading.Event()
    stats = Stats()
    threads = []

    def stop_handler(_sig, _frame):
        stop_event.set()

    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)

    for i in range(args.threads):
        t = threading.Thread(target=worker, args=(i, args.dir, payload, stop_event, stats), daemon=True)
        threads.append(t)
        t.start()

    start = time.time()
    next_report = start + 1.0
    deadline = start + args.duration if args.duration > 0 else None

    while not stop_event.is_set():
        now = time.time()
        if deadline is not None and now >= deadline:
            stop_event.set()
            break
        wait_s = max(0.0, next_report - now)
        if wait_s > 0:
            time.sleep(wait_s)
        count, interval_ns, total_count, total_ns = stats.snapshot_and_reset_interval()
        interval_avg_ms = (interval_ns / count / 1_000_000.0) if count > 0 else 0.0
        total_avg_ms = (total_ns / total_count / 1_000_000.0) if total_count > 0 else 0.0
        elapsed = int(time.time() - start)
        print(
            f"[{elapsed:>4}s] interval_count={count} interval_avg_fdatasync_ms={interval_avg_ms:.6f} "
            f"total_count={total_count} total_avg_fdatasync_ms={total_avg_ms:.6f}",
            flush=True,
        )
        next_report += 1.0

    for t in threads:
        t.join()

    _, _, total_count, total_ns = stats.snapshot_and_reset_interval()
    final_avg_ms = (total_ns / total_count / 1_000_000.0) if total_count > 0 else 0.0
    print(f"[final] total_count={total_count} total_avg_fdatasync_ms={final_avg_ms:.6f}", flush=True)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--block-size", type=int, default=4096)
    parser.add_argument("--duration", type=int, default=0)
    parser.add_argument("--dir", type=str, default="/tmp/fdatasync_bench")
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
