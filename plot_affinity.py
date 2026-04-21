#!/usr/bin/env python3
"""Phase 6 — minimal plotter for affinity_timeseries.csv.

Reads one or more affinity_timeseries.csv.<node_id> files and produces:
  - <out>_edgecut.png          edgecut over time
  - <out>_remote_ratio.png     from_remote_ratio over time (per node + sum)
  - <out>_remote_ratio_inst.png instantaneous from_remote_ratio per tick
  - <out>_migrations.png       migrations_done_delta cumulative + rate

Usage:
    python3 plot_affinity.py affinity_timeseries.csv.0 affinity_timeseries.csv.1 \\
        --out affinity_run1
"""
import argparse
import csv
import os
import sys
from collections import defaultdict


def read_csv(path):
    rows = []
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("inputs", nargs="+", help="affinity_timeseries.csv.<node_id> files")
    ap.add_argument("--out", default="affinity_plot", help="output prefix")
    args = ap.parse_args()

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed; pip install matplotlib", file=sys.stderr)
        sys.exit(1)

    per_node = defaultdict(list)
    for path in args.inputs:
        # Filename convention: affinity_timeseries.csv.<node_id>
        node_id = os.path.basename(path).rsplit('.', 1)[-1]
        try:
            int(node_id)
        except ValueError:
            node_id = os.path.basename(path)
        per_node[node_id] = read_csv(path)

    # Edgecut (per-node; should converge across nodes since it's global)
    fig, ax = plt.subplots(figsize=(10, 5))
    for nid, rows in per_node.items():
        xs = [int(r["elapsed_ms"]) / 1000.0 for r in rows]
        ys = [int(r["edgecut"]) for r in rows]
        ax.plot(xs, ys, label=f"node {nid}")
    ax.set_xlabel("elapsed (s)")
    ax.set_ylabel("edgecut")
    ax.set_title("ParMETIS edgecut over time (lower = better affinity)")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(f"{args.out}_edgecut.png", dpi=120)

    # from_remote_ratio per node (key paper metric)
    fig, ax = plt.subplots(figsize=(10, 5))
    for nid, rows in per_node.items():
        xs = [int(r["elapsed_ms"]) / 1000.0 for r in rows]
        ys = [float(r["from_remote_ratio"]) for r in rows]
        ax.plot(xs, ys, label=f"node {nid}")
    ax.set_xlabel("elapsed (s)")
    ax.set_ylabel("from_remote_ratio")
    ax.set_title("Page fetches from remote compute / total fetches (lower = better)")
    ax.legend()
    ax.set_ylim(0, 1)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(f"{args.out}_remote_ratio.png", dpi=120)

    # Instantaneous from_remote_ratio per node (per-tick delta rather than
    # cumulative counters). This is the better view for short-term oscillation.
    fig, ax = plt.subplots(figsize=(10, 5))
    for nid, rows in per_node.items():
        xs = [int(r["elapsed_ms"]) / 1000.0 for r in rows]
        ys = []
        for r in rows:
            d_remote = int(r["from_remote_delta"])
            d_storage = int(r["from_storage_delta"])
            d_local = int(r["from_local_delta"])
            total = d_remote + d_storage + d_local
            ys.append((d_remote / total) if total > 0 else 0.0)
        ax.plot(xs, ys, label=f"node {nid}")
    ax.set_xlabel("elapsed (s)")
    ax.set_ylabel("instant from_remote_ratio")
    ax.set_title("Per-tick remote compute fetch ratio (instantaneous)")
    ax.legend()
    ax.set_ylim(0, 1)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(f"{args.out}_remote_ratio_inst.png", dpi=120)

    # Migrations: cumulative done across nodes
    fig, ax = plt.subplots(figsize=(10, 5))
    for nid, rows in per_node.items():
        xs = [int(r["elapsed_ms"]) / 1000.0 for r in rows]
        cum = []
        s = 0
        for r in rows:
            s += int(r["migrations_done_delta"])
            cum.append(s)
        ax.plot(xs, cum, label=f"node {nid} cumulative")
    ax.set_xlabel("elapsed (s)")
    ax.set_ylabel("cumulative migrations_done")
    ax.set_title("Tuple migrations performed by this node over time")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(f"{args.out}_migrations.png", dpi=120)

    print(
        f"wrote {args.out}_edgecut.png, {args.out}_remote_ratio.png, "
        f"{args.out}_remote_ratio_inst.png, {args.out}_migrations.png"
    )


if __name__ == "__main__":
    main()
