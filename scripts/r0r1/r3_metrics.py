#!/usr/bin/env python3
"""R3 22.8 成对对照指标提取（实测，无合成）。

输入：cluster.py 产出的 run 目录（ledger/requests.jsonl + logs/driver.out）。
指标：
  fired_ns            故障注入时刻（ledger anchor_fired）
  recovery_seconds    driver 实测恢复时长（driver.out）
  window_throughput   恢复窗口 [fired, fired+recovery] 内完成（terminal）
                      的在线请求数 / recovery_seconds
  first_blocking_ms   恢复窗口内发出（accepted>=fired）且终局晚于窗口内
                      正常分位的"被阻塞首请求"时延（terminal-accepted，
                      毫秒）。取窗口内 accepted 最早、且时延超过非窗口
                      p50×3 的第一个请求（=被恢复真实拖延的首请求）。
  normal_p50_ms       非窗口请求 terminal-accepted 中位时延（正常基线）
"""
import json
import re
import statistics
import sys


def load_ledger(run):
    planned = {}
    accepted = {}
    terminal = {}
    fired_ns = None
    with open(f"{run}/ledger/requests.jsonl") as f:
        for line in f:
            r = json.loads(line)
            ev = r.get("event")
            rid = r.get("request_id")
            if ev == "planned":
                planned[rid] = r
            elif ev == "accepted":
                accepted[rid] = r.get("time_ns")
            elif ev == "terminal":
                terminal[rid] = (r.get("time_ns"), r.get("outcome_class", "?"))
            elif ev == "anchor_fired":
                fired_ns = r.get("time_ns")
    return planned, accepted, terminal, fired_ns


def load_recovery_seconds(run):
    txt = open(f"{run}/logs/driver.out").read()
    m = re.search(r'"recovery_seconds": ([0-9.]+)', txt)
    return float(m.group(1)) if m else None


def metrics(run):
    planned, accepted, terminal, fired_ns = load_ledger(run)
    rec = load_recovery_seconds(run)
    done_ns = fired_ns + int(rec * 1e9)
    lat = {}
    for rid, t_ns in terminal.items():
        a_ns = accepted.get(rid)
        if a_ns is not None:
            lat[rid] = (a_ns, t_ns[0], t_ns[1])
    non_window = [ (t - a) / 1e6 for a, t, _ in lat.values()
                   if a < fired_ns or a > done_ns ]
    p50 = statistics.median(non_window) if non_window else 0.0
    in_window = sorted(((a, t, o, rid) for rid, (a, t, o) in lat.items()
                        if fired_ns <= a <= done_ns))
    done_in_window = [1 for a, t, o, rid in in_window if fired_ns <= t <= done_ns]
    thr = len(done_in_window) / rec if rec else 0.0
    first = None
    for a, t, o, rid in in_window:
        latency_ms = (t - a) / 1e6
        if latency_ms >= 3 * p50 and p50 > 0:
            first = (rid, o, latency_ms)
            break
    iface = {}
    import glob
    import os
    for logf in glob.glob(f"{run}/compute_*/build/compute_*/computeserver.log*"):
        for line in open(logf, errors="ignore"):
            if "[R3-IFACE] policy=" in line:
                m = re.search(r"policy=(\S+) pages=(\d+) groups_accepted=(\d+)"
                              r".*materialize_us=(\d+) propose_us=(\d+) schedule_us=(\d+)"
                              r" total_iface_us=(\d+)", line)
                if m:
                    iface.setdefault(m.group(1), []).append(
                        tuple(int(x) for x in m.groups()[1:]))
    return {
        "run": run,
        "recovery_seconds": rec,
        "window_throughput_rps": round(thr, 1),
        "first_blocking": first,
        "normal_p50_ms": round(p50, 2),
        "iface": {k: v for k, v in iface.items()},
    }


if __name__ == "__main__":
    for run in sys.argv[1:]:
        m = metrics(run)
        fb = m["first_blocking"]
        fb_s = f"{fb[0]} {fb[1]} {fb[2]:.0f}ms" if fb else "none"
        iface = m["iface"]
        iface_s = " ".join(
            f"{k}:propose={sum(x[3] for x in v)}us,total={sum(x[5] for x in v)}us"
            for k, v in iface.items()) or "-"
        print(f"{m['run'].split('/')[-1]:36s} rec={m['recovery_seconds']:6.3f}s "
              f"thr={m['window_throughput_rps']:7.1f}rps "
              f"first=[{fb_s}] p50={m['normal_p50_ms']}ms iface[{iface_s}]")
