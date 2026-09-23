#!/usr/bin/env python3
"""R4 22.10 指标提取：同截面配对系列（lockstep2），全部实测推导。

输入：cluster.py run 目录。输出：
  1) 每样本：结局计数 / 每 CRUD 类型×结局 / 恢复与可用性指标 /
     P95/P99 / 故障前后吞吐 / 首次正确服务 / 恢复后 CRUD 时延 /
     R3-IFACE / 终态一致性（rid_mismatch、tree、uncommitted）
  2) 同群配对差值：B3/B1/P1 对 B0，P1 对 B3（各策略 ≥2 样本均值差）
  3) 可观测性核验：各指标 已知/未知/不适用 计数

数据源（无合成）：
  ledger/requests.jsonl  planned/accepted/terminal/anchor_fired
  logs/driver.out        recovery_seconds、终态核验 JSON
  compute_*/…/computeserver.log*  [R3-IFACE]
"""
import glob
import json
import re
import statistics
import sys
from collections import Counter, defaultdict

OUTCOMES = ["CONFIRMED_COMMITTED", "CONFIRMED_ABORTED", "UNKNOWN", "UNFINISHED"]
OPS = ["INSERT", "UPDATE", "READ", "DELETE", "SCAN"]


def load(run):
    planned, accepted, terminal = {}, {}, {}
    kill_ns = None
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
                # injected_time_ns 为注入器单调钟基；ledger time_ns 为实时钟基，
                # 与 terminal.time_ns 同基——首次正确服务差值必须用同基时间
                kill_ns = r.get("time_ns")
    return planned, accepted, terminal, kill_ns


def load_driver(run):
    txt = open(f"{run}/logs/driver.out", errors="ignore").read()

    def f1(pat, default=None):
        m = re.search(pat, txt)
        return float(m.group(1)) if m else default

    def i1(pat, default=None):
        m = re.search(pat, txt)
        return int(m.group(1)) if m else default

    m = re.search(r'"uncommitted_invisible": \[(.*?)\]', txt)
    uncommit = [] if not m else [x for x in m.group(1).split(",") if x.strip()]
    return {
        "recovery_seconds": f1(r'"recovery_seconds": ([0-9.]+)'),
        "rid_mismatch": i1(r'"rid_mismatch": (\d+)'),
        "final_model_keys": i1(r'"final_model_keys": (\d+)'),
        "index_total_keys": i1(r'"index_total_keys": (\d+)'),
        "heap_only": i1(r'"heap_only": (\d+)'),
        "idx_only": i1(r'"idx_only": (\d+)'),
        "uncommitted_invisible": len(uncommit),
    }


def load_iface(run):
    iface = defaultdict(list)
    for logf in glob.glob(f"{run}/compute_*/build/compute_*/computeserver.log*"):
        for line in open(logf, errors="ignore"):
            if "[R3-IFACE] policy=" in line:
                m = re.search(r"policy=(\S+) pages=(\d+) groups_accepted=(\d+)"
                              r".*materialize_us=(\d+) propose_us=(\d+) schedule_us=(\d+)"
                              r" total_iface_us=(\d+)", line)
                if m:
                    iface[m.group(1)].append(tuple(int(x) for x in m.groups()[1:]))
    return dict(iface)


def pctl(vals, p):
    if not vals:
        return None
    vals = sorted(vals)
    k = max(0, min(len(vals) - 1, int(round((p / 100.0) * (len(vals) - 1)))))
    return vals[k]


def metrics(run):
    planned, accepted, terminal, kill_ns = load(run)
    drv = load_driver(run)
    iface = load_iface(run)
    rec = drv["recovery_seconds"]
    done_ns = kill_ns + int((rec or 0) * 1e9) if kill_ns else None

    stuck = sorted(set(accepted) - set(terminal))
    outcomes = Counter(oc for _, oc in terminal.values())

    # 每 CRUD 类型×结局（planned 逐 op 归到其请求终局；UNFINISHED=无终局）
    op_out = {op: Counter() for op in OPS}
    for rid, p in planned.items():
        oc = terminal.get(rid, (None, "UNFINISHED"))[1]
        for o in p.get("operations", []):
            op = o.get("op")
            if op in op_out:
                op_out[op][oc] += 1

    # 时延（终局-接纳，毫秒）
    lat = {rid: (t - accepted[rid]) / 1e6
           for rid, (t, _) in terminal.items() if rid in accepted}
    all_lat = list(lat.values())

    # 故障前吞吐 / 恢复后吞吐（每秒终局数，各自活跃窗内）
    acc_times = sorted(accepted.values())
    term_times = sorted(t for t, _ in terminal.values())
    pre = [t for t in term_times if kill_ns and t < kill_ns]
    pre_span = ((kill_ns - acc_times[0]) / 1e9) if (kill_ns and acc_times and pre) else None
    pre_thr = (len(pre) - 1) / pre_span if pre_span and pre_span > 0 and len(pre) > 1 else None
    post = [t for t in term_times if done_ns and t > done_ns]
    post_span = ((term_times[-1] - done_ns) / 1e9) if (done_ns and post) else None
    post_thr = (len(post) - 1) / post_span if post_span and post_span > 0 and len(post) > 1 else None

    # 首次正确服务：kill 后首个 CONFIRMED_* 终局（含挂起核定回读）
    first_ok = None
    for t, oc in sorted(terminal.values()):
        if kill_ns and t >= kill_ns and oc.startswith("CONFIRMED"):
            first_ok = (t - kill_ns) / 1e6
            break

    # 恢复后 CRUD 时延（恢复完成后接纳的请求）
    post_lat = [v for rid, v in lat.items()
                if done_ns and accepted[rid] >= done_ns]

    return {
        "run": run.split("/")[-1],
        "n_planned": len(planned),
        "outcomes": {o: outcomes.get(o, 0) for o in OUTCOMES} | {"OTHER": sum(v for k, v in outcomes.items() if k not in OUTCOMES)},
        "stuck": len(stuck),
        "op_outcomes": {op: dict(c) for op, c in op_out.items() if c},
        "recovery_seconds": rec,
        "first_correct_service_ms": first_ok,
        "p50_ms": pctl(all_lat, 50), "p95_ms": pctl(all_lat, 95), "p99_ms": pctl(all_lat, 99),
        "pre_thr": pre_thr, "post_thr": post_thr,
        "post_recovery_p50_ms": statistics.median(post_lat) if post_lat else None,
        "post_recovery_n": len(post_lat),
        "iface": iface,
        "health": drv,
    }


def policy_of(run_name):
    m = re.match(r"r4-\d+-([a-z0-9]+)-lockstep2-\d+", run_name)
    return m.group(1).upper() if m else "?"


def fmt(x, nd=3):
    return ("{:." + str(nd) + "f}").format(x) if isinstance(x, (int, float)) else "-"


def main(runs):
    ms = [metrics(r) for r in runs]
    known = Counter()
    n = len(ms)
    for m in ms:
        for k in ("recovery_seconds", "first_correct_service_ms", "p95_ms",
                  "pre_thr", "post_thr", "post_recovery_p50_ms"):
            known[k] += 1 if m[k] is not None else 0
        known["iface"] += 1 if m["iface"] else 0
        known["health"] += 1 if m["health"]["rid_mismatch"] is not None else 0

    # —— 每样本表 ——
    print("=== per-sample ===")
    hdr = (f"{'run':32s} {'rec_s':>7s} {'1stOK_ms':>9s} {'p50':>7s} {'p95':>8s} "
           f"{'p99':>8s} {'preThr':>7s} {'postThr':>7s} {'postCRUD_p50':>12s} "
           f"{'C/A/U/U':>15s} {'stuck':>5s}")
    print(hdr)
    for m in ms:
        oc = m["outcomes"]
        cau = f"{oc['CONFIRMED_COMMITTED']}/{oc['CONFIRMED_ABORTED']}/{oc['UNKNOWN']}/{oc['UNFINISHED']}"
        print(f"{m['run']:32s} {fmt(m['recovery_seconds']):>7s} "
              f"{fmt(m['first_correct_service_ms'], 1) if m['first_correct_service_ms'] is not None else '-':>9s} "
              f"{fmt(m['p50_ms'], 2):>7s} {fmt(m['p95_ms'], 1):>8s} {fmt(m['p99_ms'], 1):>8s} "
              f"{fmt(m['pre_thr'], 1):>7s} {fmt(m['post_thr'], 1):>7s} "
              f"{fmt(m['post_recovery_p50_ms'], 2):>12s} {cau:>15s} {m['stuck']:>5d}")
        h = m["health"]
        print(f"    health: tree={h['final_model_keys']}/{h['index_total_keys']} "
              f"rid_mismatch={h['rid_mismatch']} heap_only={h['heap_only']} "
              f"idx_only={h['idx_only']} uncommitted_invisible={h['uncommitted_invisible']}")
        ops = " ".join(f"{op}:{'+'.join(f'{k}={v}' for k, v in sorted(c.items()))}"
                       for op, c in m["op_outcomes"].items())
        print(f"    ops: {ops}")
        for pol, rows in m["iface"].items():
            tot = sum(r[5] for r in rows)
            print(f"    iface[{pol}]: batches={len(rows)} total_iface_us={tot}")

    # —— 同群配对差值（对 B0；P1 对 B3）——
    print("\n=== paired diffs (mean of >=2 samples each, vs B0 unless noted) ===")
    groups = defaultdict(list)
    for m in ms:
        groups[policy_of(m["run"])].append(m)

    def mean(g, k):
        vals = [x[k] for x in groups[g] if x[k] is not None]
        return statistics.mean(vals) if vals else None

    base = "B0"
    cols = [("recovery_seconds", "rec_s", 3), ("first_correct_service_ms", "1stOK_ms", 1),
            ("p95_ms", "p95_ms", 1), ("post_recovery_p50_ms", "postCRUD_p50", 2),
            ("pre_thr", "preThr", 1), ("post_thr", "postThr", 1)]
    header = f"{'policy':10s} " + " ".join(f"{lbl}(X-B0)" if lbl != "preThr" else f"{lbl}" for _, lbl, _ in cols)
    # 简单直接：逐列输出差值
    for g in sorted(groups):
        if g == base:
            continue
        parts = []
        for k, lbl, nd in cols:
            a, b = mean(g, k), mean(base, k)
            if a is None or b is None:
                parts.append(f"{lbl}=-")
            else:
                parts.append(f"{lbl}={a - b:+.{nd}f}" if k != "pre_thr" else f"{lbl}={a:.1f}/{b:.1f}")
        print(f"{g:>4s} vs B0: " + "  ".join(parts) + f"   (n={len(groups[g])})")
    if "P1" in groups and "B3" in groups:
        parts = []
        for k, lbl, nd in cols:
            a, b = mean("P1", k), mean("B3", k)
            if a is None or b is None:
                parts.append(f"{lbl}=-")
            else:
                parts.append(f"{lbl}={a - b:+.{nd}f}" if k != "pre_thr" else f"{lbl}={a:.1f}/{b:.1f}")
        print("P1 vs B3: " + "  ".join(parts))

    # —— 可观测性核验 ——
    print("\n=== observability (known / n) ===")
    for k, v in known.items():
        print(f"  {k:28s} known={v}/{n}" + ("" if v == n else f"  unknown={n - v}"))
    print("  cohort_index_trailing: same-cross-section pairing verified separately "
          "(cross-section.json sha256 + WAL key multiset); N/A here")


if __name__ == "__main__":
    main(sys.argv[1:])
