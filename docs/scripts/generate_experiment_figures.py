from __future__ import annotations

import csv
import os
import re
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib-wookongdb-mp")

import matplotlib

matplotlib.use("Agg")

from matplotlib import font_manager
from matplotlib import pyplot as plt
import numpy as np


ROOT = Path(__file__).resolve().parents[2]
FIG_DIR = ROOT / "docs" / "photos" / "experiment_rewrite"

ONLINE = (
    ROOT
    / "result/multinode_mprouter_mode13_30k_10min_batch_on_ablation/"
    / "20260517_164338/default/summary.txt"
)
NO_AFFINITY = (
    ROOT
    / "result/multinode_mprouter_mode13_30k_10min_noaffinity_timeseries/"
    / "20260517_161413/default/summary.txt"
)
BATCH_OFF = (
    ROOT
    / "result/multinode_mprouter_mode13_30k_10min_batch_off_ablation/"
    / "20260517_162946/default/summary.txt"
)
SCHISM = (
    ROOT
    / "result/multinode_mprouter_mode13_30k_10min_schism/"
    / "20260516_224407/schism_static/summary.txt"
)
LOG_6H = (
    ROOT
    / "result/multinode_mprouter_mode13_30k_6h_blinkfix/"
    / "20260516_032208/default/_local_logs/mprouter.log"
)

MODE_SUMMARIES = {
    "Mode 13": ROOT
    / "result/multinode_mprouter_mode_compare_32w/mode13/"
    / "20260513_162157/default/summary.txt",
    "Mode 23": ROOT
    / "result/multinode_mprouter_mode_compare_32w/mode23/"
    / "20260513_162405/default/summary.txt",
    "Mode 24": ROOT
    / "result/multinode_mprouter_mode_compare_32w/mode24/"
    / "20260513_162612/default/summary.txt",
}

DECAY_SUMMARIES = {
    0.70: ROOT
    / "result/multinode_mprouter_random_generate_mode13_decay07/"
    / "20260511_175036/default/summary.txt",
    0.73: ROOT
    / "result/multinode_mprouter_random_generate_mode13_decay073/"
    / "20260511_180235/default/summary.txt",
    0.75: ROOT
    / "result/multinode_mprouter_random_generate_mode13_decay075/"
    / "20260511_173847/default/summary.txt",
    0.77: ROOT
    / "result/multinode_mprouter_random_generate_mode13_decay077/"
    / "20260511_181438/default/summary.txt",
    0.78: ROOT
    / "result/multinode_mprouter_random_generate_mode13_decay078/"
    / "20260511_182641/default/summary.txt",
    0.90: ROOT
    / "result/multinode_mprouter_random_generate_mode13_decay09/"
    / "20260511_172228/default/summary.txt",
}

PARTITION_SUMMARIES = {
    "10 s": ROOT
    / "result/multinode_mprouter_random_generate_mode13_partcycle10000/"
    / "20260511_164055/default/summary.txt",
    "20 s": ROOT
    / "result/multinode_mprouter_random_generate_mode13_partcycle20000/"
    / "20260511_165240/default/summary.txt",
}

COLORS = {
    "online": "#2A6FBB",
    "schism": "#D55E00",
    "no_affinity": "#6B7280",
    "gray": "#6B7280",
    "green": "#009E73",
    "purple": "#7E57C2",
    "orange": "#E69F00",
    "light": "#D9DEE7",
}


def require(path: Path) -> Path:
    if not path.exists():
        raise FileNotFoundError(path)
    return path


def setup_plot_style() -> None:
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    cjk_font = Path("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc")
    if cjk_font.exists():
        font_manager.fontManager.addfont(str(cjk_font))
        font_name = font_manager.FontProperties(fname=str(cjk_font)).get_name()
    else:
        font_name = "DejaVu Sans"

    plt.rcParams.update(
        {
            "font.family": font_name,
            "axes.unicode_minus": False,
            "figure.dpi": 140,
            "savefig.dpi": 240,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.grid": True,
            "grid.alpha": 0.22,
            "grid.linewidth": 0.8,
            "axes.titleweight": "bold",
            "axes.titlesize": 12,
            "axes.labelsize": 10,
            "xtick.labelsize": 9,
            "ytick.labelsize": 9,
            "legend.frameon": False,
        }
    )


def savefig(fig: plt.Figure, name: str) -> Path:
    path = FIG_DIR / name
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    return path


def read_summary(path: Path) -> dict[str, str]:
    data: dict[str, str] = {}
    with require(path).open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or "=" not in line:
                continue
            key, value = line.split("=", 1)
            data[key] = value
    return data


def fnum(data: dict[str, str], key: str, default: float = 0.0) -> float:
    try:
        return float(data.get(key, default))
    except (TypeError, ValueError):
        return default


def pct(value: float) -> float:
    return value * 100.0


def float_list(raw: str) -> list[float]:
    if not raw:
        return []
    return [float(item) for item in raw.split(",") if item.strip()]


def parse_6h_tps_series(path: Path) -> list[tuple[float, float]]:
    text = require(path).read_text(encoding="utf-8", errors="replace")
    values = [
        float(match.group(1))
        for match in re.finditer(r"\[Routed TPS\]\s+([0-9.]+)\s+txn/sec", text)
    ]
    values = [value for value in values if value > 0]
    return [(idx * 2.0 / 3600.0, value) for idx, value in enumerate(values)]


def read_affinity_timeseries() -> list[dict[str, float]]:
    base = ONLINE.parent
    paths = sorted(base.glob("node*/affinity_timeseries.*.csv"))
    if not paths:
        return []

    node_rows: list[list[dict[str, str]]] = []
    for path in paths:
        with path.open("r", encoding="utf-8") as f:
            node_rows.append(list(csv.DictReader(f)))

    limit = min(len(rows) for rows in node_rows)
    merged: list[dict[str, float]] = []
    for idx in range(limit):
        rows = [node_rows[node][idx] for node in range(len(node_rows))]
        local = np.mean([float(row.get("from_local_ratio") or 0) for row in rows])
        remote = np.mean([float(row.get("from_remote_ratio") or 0) for row in rows])
        storage = np.mean([float(row.get("from_storage_ratio") or 0) for row in rows])
        if local + remote + storage <= 0:
            continue
        merged.append(
            {
                "minute": float(rows[0].get("elapsed_ms") or 0) / 60000.0,
                "local": local,
                "remote": remote,
                "storage": storage,
                "edgecut": np.mean([float(row.get("edgecut") or 0) for row in rows]),
                "migrations_planned_delta": sum(
                    float(row.get("migrations_planned_delta") or 0) for row in rows
                ),
                "migrations_done_delta": sum(
                    float(row.get("migrations_done_delta") or 0) for row in rows
                ),
            }
        )
    return merged


def label_bars(ax: plt.Axes, bars, fmt: str = "{:.0f}", dy: float = 2.0) -> None:
    for bar in bars:
        height = bar.get_height()
        ax.annotate(
            fmt.format(height),
            xy=(bar.get_x() + bar.get_width() / 2, height),
            xytext=(0, dy),
            textcoords="offset points",
            ha="center",
            va="bottom",
            fontsize=8,
        )


def build_metrics() -> dict[str, object]:
    return {
        "online": read_summary(ONLINE),
        "no_affinity": read_summary(NO_AFFINITY),
        "batch_off": read_summary(BATCH_OFF),
        "schism": read_summary(SCHISM),
        "mode": {name: read_summary(path) for name, path in MODE_SUMMARIES.items()},
        "decay": {value: read_summary(path) for value, path in DECAY_SUMMARIES.items()},
        "partition": {
            name: read_summary(path) for name, path in PARTITION_SUMMARIES.items()
        },
        "series_6h": parse_6h_tps_series(LOG_6H),
        "affinity_timeseries": read_affinity_timeseries(),
    }


def make_figures(metrics: dict[str, object]) -> list[Path]:
    setup_plot_style()
    online = metrics["online"]
    no_affinity = metrics["no_affinity"]
    batch_off = metrics["batch_off"]
    schism = metrics["schism"]
    mode = metrics["mode"]
    decay = metrics["decay"]
    partition = metrics["partition"]
    series_6h = metrics["series_6h"]
    affinity_timeseries = metrics["affinity_timeseries"]

    outputs: list[Path] = []

    # 图5-1：主对照吞吐。
    labels = ["无亲和", "Schism", "在线亲和"]
    values = [
        fnum(no_affinity, "mprouter_throughput_tps"),
        fnum(schism, "mprouter_throughput_tps"),
        fnum(online, "mprouter_throughput_tps"),
    ]
    fig, ax = plt.subplots(figsize=(7.2, 4.2))
    bars = ax.bar(
        labels,
        values,
        color=[COLORS["no_affinity"], COLORS["schism"], COLORS["online"]],
        width=0.58,
    )
    ax.set_ylabel("吞吐量（txn/s）")
    ax.set_title("三组主实验吞吐量对比")
    ax.set_ylim(0, max(values) * 1.18)
    label_bars(ax, bars, "{:.0f}")
    outputs.append(savefig(fig, "fig5_1_schism_throughput.png"))

    # 图5-2：主对照尾延迟。
    exec_p99 = [
        fnum(no_affinity, "mprouter_exec_p99_ms"),
        fnum(schism, "mprouter_exec_p99_ms"),
        fnum(online, "mprouter_exec_p99_ms"),
    ]
    fetch_p99 = [
        fnum(no_affinity, "mprouter_fetch_p99_ms"),
        fnum(schism, "mprouter_fetch_p99_ms"),
        fnum(online, "mprouter_fetch_p99_ms"),
    ]
    x = np.arange(len(labels))
    fig, axes = plt.subplots(1, 2, figsize=(9.6, 4.0))
    axes[0].bar(x, exec_p99, color=[COLORS["no_affinity"], COLORS["schism"], COLORS["online"]])
    axes[0].set_xticks(x, labels)
    axes[0].set_ylabel("执行 P99（ms）")
    axes[0].set_title("事务执行尾延迟")
    axes[1].bar(x, fetch_p99, color=[COLORS["no_affinity"], COLORS["schism"], COLORS["online"]])
    axes[1].set_xticks(x, labels)
    axes[1].set_ylabel("端到端 P99（ms）")
    axes[1].set_title("端到端尾延迟")
    for axis, vals in zip(axes, [exec_p99, fetch_p99]):
        axis.set_ylim(0, max(vals) * 1.18)
        label_bars(axis, axis.patches, "{:.1f}" if max(vals) < 100 else "{:.0f}")
    outputs.append(savefig(fig, "fig5_2_schism_latency.png"))

    # 图5-3：6 小时长时间吞吐。
    hours = np.array([point[0] for point in series_6h])
    tps = np.array([point[1] for point in series_6h])
    fig, ax = plt.subplots(figsize=(8.8, 4.2))
    ax.plot(hours, tps, color=COLORS["light"], linewidth=0.8, label="2 秒采样")
    if len(tps) >= 60:
        window = 60
        smooth = np.convolve(tps, np.ones(window) / window, mode="valid")
        ax.plot(hours[window - 1 :], smooth, color=COLORS["online"], linewidth=1.8, label="2 分钟滑动平均")
    ax.axhline(tps.mean(), color=COLORS["orange"], linestyle="--", linewidth=1.3, label=f"平均 {tps.mean():.0f}")
    ax.set_xlabel("运行时间（小时）")
    ax.set_ylabel("吞吐量（txn/s）")
    ax.set_title("在线亲和 6 小时长时间吞吐变化")
    ax.legend(loc="lower right")
    outputs.append(savefig(fig, "fig5_3_long_run_tps.png"))

    # 图5-4：迁移计划、完成和积压。
    mig_labels = ["Schism", "批量关闭", "批量开启"]
    planned = [
        fnum(schism, "affinity_migrations_planned_total"),
        fnum(batch_off, "affinity_migrations_planned_total"),
        fnum(online, "affinity_migrations_planned_total"),
    ]
    done = [
        fnum(schism, "affinity_migrations_done_total"),
        fnum(batch_off, "affinity_migrations_done_total"),
        fnum(online, "affinity_migrations_done_total"),
    ]
    backlog = [
        fnum(schism, "affinity_migration_backlog"),
        fnum(batch_off, "affinity_migration_backlog"),
        fnum(online, "affinity_migration_backlog"),
    ]
    y = np.arange(len(mig_labels))
    fig, ax = plt.subplots(figsize=(8.8, 4.5))
    ax.barh(y + 0.24, planned, height=0.22, color=COLORS["light"], label="计划迁移")
    ax.barh(y, done, height=0.22, color=COLORS["green"], label="完成迁移")
    ax.barh(y - 0.24, backlog, height=0.22, color=COLORS["orange"], label="最终积压")
    ax.set_yticks(y, mig_labels)
    ax.set_xlabel("元组数")
    ax.set_title("迁移任务完成情况对比")
    ax.legend(loc="lower right")
    outputs.append(savefig(fig, "fig5_4_migration_comparison.png"))

    # 图5-5：在线亲和运行期本地/远程访问比例。
    minutes = np.array([row["minute"] for row in affinity_timeseries])
    local = np.array([pct(row["local"]) for row in affinity_timeseries])
    remote = np.array([pct(row["remote"]) for row in affinity_timeseries])
    fig, ax = plt.subplots(figsize=(8.8, 4.2))
    ax.plot(minutes, local, color=COLORS["green"], linewidth=1.8, label="本地访问")
    ax.plot(minutes, remote, color=COLORS["schism"], linewidth=1.8, label="远程访问")
    ax.set_xlabel("运行时间（分钟）")
    ax.set_ylabel("访问比例（%）")
    ax.set_title("在线亲和运行期访问来源比例")
    ax.set_ylim(0, 100)
    ax.legend(loc="center right")
    outputs.append(savefig(fig, "fig5_5_local_ratio_timeseries.png"))

    # 图5-6：各节点远程访问比例。
    node_labels = ["节点0", "节点1", "节点2", "节点3"]
    noaff_node = [pct(v) for v in float_list(no_affinity.get("per_node_from_remote_ratio_final", ""))]
    sch_node = [pct(v) for v in float_list(schism.get("per_node_from_remote_ratio_final", ""))]
    online_node = [pct(v) for v in float_list(online.get("per_node_from_remote_ratio_final", ""))]
    x = np.arange(len(node_labels))
    width = 0.25
    fig, ax = plt.subplots(figsize=(8.8, 4.2))
    ax.bar(x - width, noaff_node, width, color=COLORS["no_affinity"], label="无亲和")
    ax.bar(x, sch_node, width, color=COLORS["schism"], label="Schism")
    ax.bar(x + width, online_node, width, color=COLORS["online"], label="在线亲和")
    ax.set_xticks(x, node_labels)
    ax.set_ylabel("远程访问比例（%）")
    ax.set_title("各计算节点远程访问比例对比")
    ax.legend(loc="upper right")
    outputs.append(savefig(fig, "fig5_6_per_node_remote_ratio.png"))

    # 图5-7：批量迁移开关消融，用两个直观子图替代复杂中间图。
    batch_labels = ["关闭批量迁移", "开启批量迁移"]
    batch_tps = [
        fnum(batch_off, "mprouter_throughput_tps"),
        fnum(online, "mprouter_throughput_tps"),
    ]
    batch_done = [
        fnum(batch_off, "affinity_migrations_done_total"),
        fnum(online, "affinity_migrations_done_total"),
    ]
    batch_backlog = [
        fnum(batch_off, "affinity_migration_backlog"),
        fnum(online, "affinity_migration_backlog"),
    ]
    fig, axes = plt.subplots(1, 2, figsize=(10.0, 4.2))
    tps_bars = axes[0].bar(batch_labels, batch_tps, color=[COLORS["gray"], COLORS["online"]], width=0.56)
    axes[0].set_ylabel("吞吐量（txn/s）")
    axes[0].set_title("吞吐量")
    axes[0].set_ylim(0, max(batch_tps) * 1.18)
    label_bars(axes[0], tps_bars, "{:.0f}")
    xpos = np.arange(len(batch_labels))
    axes[1].bar(xpos - 0.16, batch_done, width=0.32, color=COLORS["green"], label="完成迁移")
    axes[1].bar(xpos + 0.16, batch_backlog, width=0.32, color=COLORS["orange"], label="最终积压")
    axes[1].set_xticks(xpos, batch_labels)
    axes[1].set_ylabel("元组数")
    axes[1].set_title("迁移完成与积压")
    axes[1].legend(loc="upper left")
    outputs.append(savefig(fig, "fig5_7_batch_migration_ablation.png"))

    # 图5-7：路由模式消融。
    mode_labels = list(mode.keys())
    mode_tps = [fnum(mode[name], "mprouter_throughput_tps") for name in mode_labels]
    mode_remote = [pct(fnum(mode[name], "cluster_from_remote_ratio")) for name in mode_labels]
    x = np.arange(len(mode_labels))
    fig, ax1 = plt.subplots(figsize=(8.4, 4.2))
    ax2 = ax1.twinx()
    ax1.bar(x, mode_tps, color=COLORS["online"], width=0.54, label="吞吐量")
    ax2.plot(x, mode_remote, color=COLORS["schism"], marker="o", linewidth=1.8, label="远程访问")
    ax1.set_xticks(x, mode_labels)
    ax1.set_ylabel("吞吐量（txn/s）")
    ax2.set_ylabel("远程访问比例（%）")
    ax1.set_title("路由模式消融")
    h1, l1 = ax1.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax1.legend(h1 + h2, l1 + l2, loc="upper right")
    outputs.append(savefig(fig, "fig5_7_mode_ablation.png"))

    # 图5-8：衰减系数消融。
    decay_keys = sorted(decay.keys())
    decay_tps = [fnum(decay[key], "mprouter_throughput_tps") for key in decay_keys]
    decay_p99 = [fnum(decay[key], "mprouter_exec_p99_ms") for key in decay_keys]
    fig, ax1 = plt.subplots(figsize=(8.4, 4.2))
    ax2 = ax1.twinx()
    ax1.plot(decay_keys, decay_tps, color=COLORS["online"], marker="o", linewidth=1.9, label="吞吐量")
    ax2.plot(decay_keys, decay_p99, color=COLORS["schism"], marker="s", linewidth=1.6, label="执行 P99")
    ax1.set_xlabel("亲和边衰减系数")
    ax1.set_ylabel("吞吐量（txn/s）")
    ax2.set_ylabel("执行 P99（ms）")
    ax1.set_title("亲和边衰减系数消融")
    h1, l1 = ax1.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax1.legend(h1 + h2, l1 + l2, loc="best")
    outputs.append(savefig(fig, "fig5_8_decay_ablation.png"))

    # 图5-9：重分区周期消融。
    part_labels = list(partition.keys())
    part_tps = [fnum(partition[name], "mprouter_throughput_tps") for name in part_labels]
    part_edgecut = [fnum(partition[name], "affinity_edgecut_final_avg") for name in part_labels]
    x = np.arange(len(part_labels))
    fig, ax1 = plt.subplots(figsize=(7.6, 4.2))
    ax2 = ax1.twinx()
    bars = ax1.bar(x, part_tps, color=COLORS["online"], width=0.5, label="吞吐量")
    ax2.plot(x, part_edgecut, color=COLORS["orange"], marker="o", linewidth=1.8, label="最终 edgecut")
    ax1.set_xticks(x, part_labels)
    ax1.set_xlabel("重分区周期")
    ax1.set_ylabel("吞吐量（txn/s）")
    ax2.set_ylabel("最终 edgecut")
    ax1.set_title("重分区周期消融")
    label_bars(ax1, bars, "{:.0f}")
    h1, l1 = ax1.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax1.legend(h1 + h2, l1 + l2, loc="best")
    outputs.append(savefig(fig, "fig5_9_partition_cycle_ablation.png"))

    return outputs


def main() -> None:
    metrics = build_metrics()
    outputs = make_figures(metrics)
    for path in outputs:
        print(path.relative_to(ROOT))


if __name__ == "__main__":
    main()
