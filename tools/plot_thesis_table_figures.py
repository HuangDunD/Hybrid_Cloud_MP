#!/usr/bin/env python3
"""Generate thesis figures from the summarized experiment tables."""
import argparse
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
    "baseline": "#9A9A9A",
    "affinity": "#0072B2",
    "batched": "#D55E00",
    "local": "#009E73",
    "remote": "#CC79A7",
    "storage": "#56B4E9",
    "wait": "#D55E00",
    "gray": "#6F6F6F",
    "light_gray": "#D9D9D9",
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
            "axes.labelcolor": "#222222",
            "xtick.color": "#222222",
            "ytick.color": "#222222",
            "axes.linewidth": 0.8,
            "axes.labelsize": 9,
            "xtick.labelsize": 8,
            "ytick.labelsize": 8,
            "legend.fontsize": 8,
            "font.size": 8.5,
            "grid.color": "#E6E6E6",
            "grid.linewidth": 0.55,
            "lines.linewidth": 1.6,
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


def annotate_bars(ax, bars, suffix="", fmt="{:.2f}", pad=3):
    for bar in bars:
        height = bar.get_height()
        ax.annotate(
            fmt.format(height) + suffix,
            xy=(bar.get_x() + bar.get_width() / 2, height),
            xytext=(0, pad),
            textcoords="offset points",
            ha="center",
            va="bottom",
            fontsize=8,
        )


def save(fig, out_dir: Path, name: str):
    path = out_dir / name
    fig.savefig(path, dpi=420, bbox_inches="tight", pad_inches=0.03)
    plt.close(fig)
    return path


def plot_throughput(out_dir: Path):
    labels = ["Baseline", "Affinity", "Batched\nAffinity"]
    values = [144.10, 397.40, 575.312]
    colors = [COLORS["baseline"], COLORS["affinity"], COLORS["batched"]]

    fig, ax = plt.subplots(figsize=(4.8, 2.9))
    bars = ax.bar(labels, values, color=colors, width=0.54, edgecolor="black", linewidth=0.45, zorder=3)
    ax.set_ylabel("Throughput (txn/s)")
    ax.set_ylim(0, max(values) * 1.18)
    polish_axis(ax)
    annotate_bars(ax, bars, "", "{:.1f}")

    y = max(values) * 1.08
    ax.plot([0, 0, 1, 1], [y - 18, y, y, y - 18], color="#333333", lw=0.75, clip_on=False)
    ax.text(0.5, y + 10, "+175.8%", ha="center", va="bottom", fontsize=8.2, color="#222222")
    return save(fig, out_dir, "fig5_1_throughput_comparison.png")


def plot_access_ratios(out_dir: Path):
    labels = ["Local", "Compute\nlocal", "Remote", "Storage"]
    baseline = [53.75, 58.18, 38.64, 7.61]
    affinity = [66.09, 70.60, 27.52, 6.40]
    batched = [67.26, 68.75, 30.57, 2.17]

    x = range(len(labels))
    width = 0.25
    fig, ax = plt.subplots(figsize=(5.8, 3.15))
    b1 = ax.bar([i - width for i in x], baseline, width, label="Baseline", color=COLORS["baseline"],
                edgecolor="black", linewidth=0.35, zorder=3)
    b2 = ax.bar(list(x), affinity, width, label="Affinity", color=COLORS["affinity"],
                edgecolor="black", linewidth=0.35, zorder=3)
    b3 = ax.bar([i + width for i in x], batched, width, label="Batched affinity", color=COLORS["batched"],
                edgecolor="black", linewidth=0.35, zorder=3)
    ax.set_ylabel("Ratio")
    ax.set_xticks(list(x), labels)
    ax.set_ylim(0, 80)
    ax.yaxis.set_major_formatter(PercentFormatter(xmax=100))
    polish_axis(ax)
    ax.legend(ncols=3, loc="upper center", bbox_to_anchor=(0.5, 1.13), frameon=False,
              handlelength=1.2, columnspacing=1.2)
    return save(fig, out_dir, "fig5_2_access_ratio_comparison.png")


def plot_migration_status(out_dir: Path):
    planned = 109768
    done = 52913
    backlog = 56855
    failed = 6
    values = [done, backlog, failed]
    labels = ["Done", "Backlog", "Failed"]
    colors = [COLORS["affinity"], COLORS["light_gray"], COLORS["batched"]]

    fig, ax = plt.subplots(figsize=(5.25, 1.25))
    left = 0
    for value, label, color in zip(values, labels, colors):
        ax.barh([0], [value], left=left, height=0.36, label=label, color=color,
                edgecolor="black", linewidth=0.35, zorder=3)
        if value / planned > 0.04:
            ax.text(left + value / 2, 0, f"{label}\n{value / planned * 100:.1f}%",
                    ha="center", va="center", fontsize=8, color="#111111")
        left += value
    ax.set_xlim(0, planned)
    ax.set_ylim(-0.28, 0.28)
    ax.set_yticks([])
    ax.set_xlabel("Planned migrations")
    ax.xaxis.set_major_formatter(FuncFormatter(k_formatter))
    polish_axis(ax, grid_axis="x")
    ax.spines["left"].set_visible(False)
    ax.text(planned, 0.22, f"n={planned:,}", ha="right", va="bottom", fontsize=8)
    return save(fig, out_dir, "fig5_3_migration_status.png")


def plot_ownership_breakdown(out_dir: Path):
    labels = ["wait_lock_success", "wait_push_page", "lock_request", "storage_fetch", "other"]
    values = [524.481, 217.5411, 114.7705, 16.56603, 3.991213]
    total = 877.35
    colors = [COLORS["wait"], COLORS["baseline"], COLORS["baseline"], COLORS["baseline"], COLORS["baseline"]]

    fig, ax = plt.subplots(figsize=(5.8, 3.1))
    y = range(len(labels))
    bars = ax.barh(list(y), values, color=colors, height=0.52, edgecolor="black", linewidth=0.35, zorder=3)
    ax.set_xlabel("Cumulative time (s)")
    ax.set_yticks(list(y), labels)
    ax.invert_yaxis()
    ax.set_xlim(0, max(values) * 1.23)
    polish_axis(ax, grid_axis="x")
    for bar, value in zip(bars, values):
        ax.annotate(
            f"{value / total * 100:.1f}%",
            xy=(value, bar.get_y() + bar.get_height() / 2),
            xytext=(6, 0),
            textcoords="offset points",
            ha="left",
            va="center",
            fontsize=8,
        )
    return save(fig, out_dir, "fig5_4_ownership_transfer_breakdown.png")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="docs/photos", help="directory for generated PNG files")
    args = parser.parse_args()

    setup_style()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    paths = [
        plot_throughput(out_dir),
        plot_access_ratios(out_dir),
        plot_migration_status(out_dir),
        plot_ownership_breakdown(out_dir),
    ]
    for path in paths:
        print(path)


if __name__ == "__main__":
    main()
