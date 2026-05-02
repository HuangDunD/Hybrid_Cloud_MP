#!/usr/bin/env python3
"""Generate thesis figures from a multinode_parmetis_smoke result directory."""
import argparse
import csv
import os
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")
Path(os.environ["MPLCONFIGDIR"]).mkdir(parents=True, exist_ok=True)

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.ticker import FuncFormatter, PercentFormatter


COLORS = {
    "edge": "#0072B2",
    "local": "#009E73",
    "remote": "#CC79A7",
    "storage": "#56B4E9",
    "planned": "#0072B2",
    "done": "#009E73",
    "backlog": "#D55E00",
    "band": "#D9D9D9",
}


def setup_style():
    for font_path in (
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSerifCJK-Regular.ttc",
    ):
        if Path(font_path).exists():
            font_manager.fontManager.addfont(font_path)
    plt.rcParams.update(
        {
            "font.family": "sans-serif",
            "font.sans-serif": [
                "Noto Sans CJK JP",
                "Noto Serif CJK JP",
                "SimSun",
                "Microsoft YaHei",
                "DejaVu Sans",
            ],
            "axes.unicode_minus": False,
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "axes.edgecolor": "#333333",
            "axes.linewidth": 0.8,
            "axes.labelsize": 9,
            "xtick.labelsize": 8,
            "ytick.labelsize": 8,
            "legend.fontsize": 8,
            "font.size": 8.5,
            "grid.color": "#E6E6E6",
            "grid.linewidth": 0.55,
            "lines.linewidth": 1.65,
            "savefig.facecolor": "white",
        }
    )


def polish_axis(ax, grid_axis="y"):
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_linewidth(0.8)
    ax.spines["bottom"].set_linewidth(0.8)
    ax.tick_params(axis="both", width=0.8, length=3.2, pad=2)
    if grid_axis:
        ax.grid(axis=grid_axis, alpha=0.75, zorder=0)
    ax.set_axisbelow(True)


def k_formatter(x, _pos):
    if abs(x) >= 1000:
        return f"{x / 1000:.0f}k"
    return f"{x:.0f}"


def latest_affinity_dir(root: Path) -> Path:
    candidates = sorted(root.glob("*/affinity_on/summary.txt"))
    if not candidates:
        raise FileNotFoundError(f"no affinity_on/summary.txt found under {root}")
    return candidates[-1].parent


def read_series(path: Path):
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def find_series_files(result_dir: Path):
    files = sorted(result_dir.glob("node*/affinity_timeseries.*.csv"))
    if not files:
        files = sorted(result_dir.glob("**/affinity_timeseries.*.csv"))
    if not files:
        raise FileNotFoundError(f"no affinity_timeseries.*.csv found under {result_dir}")
    return files


def node_label(path: Path) -> str:
    for part in path.parts:
        if part.startswith("node") and "_" in part:
            return part.split("_", 1)[1]
    return path.stem.rsplit(".", 1)[-1]


def save(fig, out_dir: Path, name: str):
    path = out_dir / name
    fig.savefig(path, dpi=420, bbox_inches="tight", pad_inches=0.03)
    plt.close(fig)
    return path


def mean_at_index(series_by_node, field, index):
    vals = []
    for rows in series_by_node.values():
        if index < len(rows):
            vals.append(float(rows[index][field]))
    return sum(vals) / len(vals) if vals else 0.0


def stats_at_index(series_by_node, field, index, scale=1.0):
    vals = []
    for rows in series_by_node.values():
        if index < len(rows):
            vals.append(float(rows[index][field]) * scale)
    if not vals:
        return 0.0, 0.0, 0.0
    return min(vals), sum(vals) / len(vals), max(vals)


def common_x(series_by_node):
    max_len = max(len(rows) for rows in series_by_node.values())
    base_rows = next(iter(series_by_node.values()))
    xs = []
    for i in range(max_len):
        if i < len(base_rows):
            xs.append(int(base_rows[i]["elapsed_ms"]) / 1000)
        else:
            xs.append(xs[-1] + 1)
    return xs


def plot_edgecut(series_by_node, out_dir: Path):
    xs = common_x(series_by_node)
    lo, mid, hi = [], [], []
    for i in range(len(xs)):
        a, b, c = stats_at_index(series_by_node, "edgecut", i)
        lo.append(a)
        mid.append(b)
        hi.append(c)

    fig, ax = plt.subplots(figsize=(5.8, 3.0))
    ax.fill_between(xs, lo, hi, color=COLORS["band"], alpha=0.7, linewidth=0, label="min-max")
    ax.plot(xs, mid, color=COLORS["edge"], marker="o", markersize=2.1, markevery=max(1, len(xs) // 12),
            label="mean")
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Edgecut")
    polish_axis(ax)
    ax.legend(frameon=False, loc="upper right", handlelength=1.6)
    return save(fig, out_dir, "fig5_5_parmetis_edgecut_timeseries.png")


def plot_access_timeseries(series_by_node, out_dir: Path):
    xs = common_x(series_by_node)
    local = []
    remote = []
    storage = []
    for i in range(len(xs)):
        local.append(mean_at_index(series_by_node, "from_local_ratio", i) * 100)
        remote.append(mean_at_index(series_by_node, "from_remote_ratio", i) * 100)
        storage.append(mean_at_index(series_by_node, "from_storage_ratio", i) * 100)

    fig, ax = plt.subplots(figsize=(5.8, 3.0))
    ax.plot(xs, local, label="Local", color=COLORS["local"], marker="o", markersize=2.0,
            markevery=max(1, len(xs) // 12))
    ax.plot(xs, remote, label="Remote", color=COLORS["remote"], marker="s", markersize=2.0,
            markevery=max(1, len(xs) // 12))
    ax.plot(xs, storage, label="Storage", color=COLORS["storage"], marker="^", markersize=2.0,
            markevery=max(1, len(xs) // 12))
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Access ratio")
    ax.yaxis.set_major_formatter(PercentFormatter(xmax=100))
    ax.set_ylim(0, 78)
    polish_axis(ax)
    ax.legend(ncols=3, loc="upper center", bbox_to_anchor=(0.5, 1.13), frameon=False,
              handlelength=1.5, columnspacing=1.4)
    return save(fig, out_dir, "fig5_6_access_ratio_timeseries.png")


def plot_migration_timeseries(series_by_node, out_dir: Path):
    xs = common_x(series_by_node)
    planned = []
    done = []
    failed = []
    cum_planned = 0
    cum_done = 0
    cum_failed = 0
    for i in range(len(xs)):
        for rows in series_by_node.values():
            if i >= len(rows):
                continue
            cum_planned += int(float(rows[i]["migrations_planned_delta"]))
            cum_done += int(float(rows[i]["migrations_done_delta"]))
            cum_failed += int(float(rows[i]["migrations_failed_delta"]))
        planned.append(cum_planned)
        done.append(cum_done)
        failed.append(cum_failed)

    backlog = [p - d - f for p, d, f in zip(planned, done, failed)]
    fig, ax = plt.subplots(figsize=(5.8, 3.1))
    ax.plot(xs, planned, label="Planned", color=COLORS["planned"], linestyle="-")
    ax.plot(xs, done, label="Done", color=COLORS["done"], linestyle="--")
    ax.plot(xs, backlog, label="Backlog", color=COLORS["backlog"], linestyle=":")
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Tuples")
    ax.yaxis.set_major_formatter(FuncFormatter(k_formatter))
    polish_axis(ax)
    ax.legend(ncols=3, loc="upper center", bbox_to_anchor=(0.5, 1.13), frameon=False,
              handlelength=1.7, columnspacing=1.4)
    return save(fig, out_dir, "fig5_7_migration_timeseries.png")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--result-dir",
        default=None,
        help="affinity_on directory; default selects the latest under result/multinode_parmetis_smoke",
    )
    parser.add_argument("--out-dir", default="docs/photos", help="directory for generated PNG files")
    args = parser.parse_args()

    setup_style()
    result_dir = Path(args.result_dir) if args.result_dir else latest_affinity_dir(Path("result/multinode_parmetis_smoke"))
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    series_by_node = {node_label(path): read_series(path) for path in find_series_files(result_dir)}
    paths = [
        plot_edgecut(series_by_node, out_dir),
        plot_access_timeseries(series_by_node, out_dir),
        plot_migration_timeseries(series_by_node, out_dir),
    ]
    print(f"result_dir={result_dir}")
    for path in paths:
        print(path)


if __name__ == "__main__":
    main()
