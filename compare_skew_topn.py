#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
对比同一 (lr, theta, wr) 组合下，不同 (skew, topn) 的 TPS。

用法：
    python3 compare_skew_topn.py <result_dir> [-o out.txt]
        result_dir 可以是 round_00 目录，也可以是它下面的 ycsb_mix 目录。
        默认输出文件: <result_dir>/tps_compare.txt
"""

import argparse
import os
import re
import sys
from collections import defaultdict

COMBO_RE = re.compile(
    r"^lr(?P<lr>[\d.]+)_theta_(?P<theta>[\d.]+)_wr_(?P<wr>[\d.]+)"
    r"_skew_(?P<skew>[\d.]+)_topn_(?P<topn>\d+)$"
)


def parse_throughput(path):
    with open(path, "r") as f:
        for line in f:
            k, _, v = line.strip().partition("=")
            if k.strip() == "throughput":
                try:
                    return float(v.strip())
                except ValueError:
                    return None
    return None


def discover_combos(root):
    items = []
    for name in sorted(os.listdir(root)):
        sub = os.path.join(root, name)
        if not os.path.isdir(sub):
            continue
        if COMBO_RE.match(name):
            items.append((os.path.basename(root), name, sub))
        else:
            for combo in sorted(os.listdir(sub)):
                cp = os.path.join(sub, combo)
                if os.path.isdir(cp) and COMBO_RE.match(combo):
                    items.append((name, combo, cp))
    return items


def render(rows, gk):
    skews = sorted({r["skew"] for r in rows}, key=float)
    topns = sorted({r["topn"] for r in rows}, key=int)
    cell = {(r["skew"], r["topn"]): r["tps"] for r in rows}
    title = (f"[{gk['bench_mode']}] lr={gk['lr']} theta={gk['theta']} "
             f"wr={gk['wr']} | TPS")
    col_w = 10
    head = ["skew\\topn".ljust(10)] + [str(t).rjust(col_w) for t in topns]
    lines = [title, "-" * len(title), " ".join(head)]
    for s in skews:
        cells = []
        for t in topns:
            v = cell.get((s, t))
            cells.append(("-" if v is None else f"{v:.2f}").rjust(col_w))
        lines.append(str(s).ljust(10) + " " + " ".join(cells))
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("result_dir")
    ap.add_argument("-o", "--output", default=None,
                    help="输出文件路径（默认 <result_dir>/tps_compare.txt）")
    args = ap.parse_args()

    items = discover_combos(args.result_dir)
    if not items:
        print(f"[!] 在 {args.result_dir} 下没找到任何 combo 目录", file=sys.stderr)
        sys.exit(1)

    groups = defaultdict(list)
    for bench_mode, combo, cp in items:
        m = COMBO_RE.match(combo)
        summary = os.path.join(cp, "summary_human.txt")
        if not os.path.isfile(summary):
            continue
        groups[(bench_mode, m.group("lr"), m.group("theta"), m.group("wr"))].append({
            "skew": m.group("skew"),
            "topn": m.group("topn"),
            "tps": parse_throughput(summary),
        })

    blocks = []
    for key in sorted(groups.keys()):
        bench_mode, lr, theta, wr = key
        gk = {"bench_mode": bench_mode, "lr": lr, "theta": theta, "wr": wr}
        blocks.append(render(groups[key], gk))
        blocks.append("")

    out_path = args.output or os.path.join(args.result_dir, "tps_compare.txt")
    with open(out_path, "w") as f:
        f.write("\n".join(blocks))
    print(f"[ok] written to {out_path}")


if __name__ == "__main__":
    main()
