"""Closed-loop SQL point reads. Each client process holds one connection."""
import math
import multiprocessing
import socket
import statistics
import time


def _init_client(barrier):
    global start_gate
    start_gate = barrier


def _client(args):
    port, client_id, count, warmup = args
    key = str(client_id).encode()
    query = b'select id from ycsb_user_table where id = ' + key + b';'
    footer = b'Total record(s): 1\n'
    latencies = []
    with socket.create_connection(('127.0.0.1', port), timeout=20) as sock:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        for i in range(warmup + count):
            if i == warmup:
                start_gate.wait(timeout=30)
                start = time.perf_counter_ns()
            sent = time.perf_counter_ns()
            sock.sendall(query)
            reply = sock.recv(4096)
            while reply.startswith(b'+') and not reply.endswith(footer):
                part = sock.recv(4096)
                if not part:
                    raise IOError('incomplete SELECT reply')
                reply += part
            received = time.perf_counter_ns()
            if not reply.endswith(footer) or reply.splitlines()[3].strip(b'| ') != key:
                raise ValueError(f'unexpected result for {query!r}: {reply!r}')
            if i >= warmup:
                latencies.append(received - sent)
    return start, received, latencies


def measure(port, rounds=5, queries=2000, warmup=100):
    ctx = multiprocessing.get_context('fork')  # Linux integration runner
    samples = []
    for iteration in range(rounds):
        # Reverse the load order on alternate rounds to vary the cache/host history.
        for clients in ((1, 4, 8) if iteration % 2 == 0 else (8, 4, 1)):
            with ctx.Pool(clients, initializer=_init_client, initargs=(ctx.Barrier(clients),)) as pool:
                results = pool.map(_client, [(port, i, queries, warmup) for i in range(clients)])
            elapsed = max(end for _, end, _ in results) - min(start for start, _, _ in results)
            lat = sorted(ns for _, _, times in results for ns in times)
            sample = dict(round=iteration + 1, clients=clients, requests=len(lat),
                          qps=len(lat) * 1e9 / elapsed)
            for p in (50, 95, 99):
                sample[f'p{p}_us'] = lat[math.ceil(len(lat) * p / 100) - 1] / 1000
            samples.append(sample)
            print(sample, flush=True)
    medians = {c: {k: statistics.median(s[k] for s in samples if s['clients'] == c)
                   for k in ('qps', 'p50_us', 'p95_us', 'p99_us')} for c in (1, 4, 8)}
    return dict(samples=samples, median_of_rounds=medians, warmup_per_client=warmup)
