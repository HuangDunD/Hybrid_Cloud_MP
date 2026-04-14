import csv
import os
from collections import defaultdict

try:
    import matplotlib.pyplot as plt
except ImportError as exc:
    raise SystemExit(
        "未找到 matplotlib。请先安装，或使用:\n"
        "PYTHONPATH=/usr/local/workspace/Hybrid_Cloud_MP/.pydeps python3 generate_time_breakdown.py"
    ) from exc


CSV_PATH = "/usr/local/workspace/Hybrid_Cloud_MP/throughput_comparison.csv"
OUTPUT_DIR = "/usr/local/workspace/Hybrid_Cloud_MP/time_breakdown_charts"


def parse_float(v):
    try:
        return float(v)
    except Exception:
        return 0.0


def load_rows(csv_path):
    with open(csv_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)


def build_case_map(rows):
    # key: (local_ratio, theta, wr) -> {"Lazy": row, "2PC": row}
    case_map = defaultdict(dict)
    for row in rows:
        mode = row["Mode"].strip()
        lr = row["Local Ratio"].strip()
        theta = row["zipfian theta"].strip()
        wr = row["WR Ratio"].strip()
        case_map[(lr, theta, wr)][mode] = row
    return case_map


def global_sort_key(case_key):
    # 固定全局排序优先级: wr(升序) > theta(升序) > lr(降序)
    lr, theta, wr = case_key
    return float(wr), float(theta), -float(lr)


def get_label(dim):
    if dim == "lr":
        return "Local Ratio"
    if dim == "theta":
        return "zipfian theta"
    if dim == "wr":
        return "WR Ratio"
    raise ValueError(f"unknown dim: {dim}")


def matches_fixed(case_key, fixed_dim, fixed_value):
    lr, theta, wr = case_key
    if fixed_dim == "lr":
        return lr == fixed_value
    if fixed_dim == "theta":
        return theta == fixed_value
    if fixed_dim == "wr":
        return wr == fixed_value
    raise ValueError(f"unknown fixed_dim: {fixed_dim}")


def x_label_for_case(case_key, fixed_dim):
    lr, theta, wr = case_key
    if fixed_dim == "lr":
        return f"{theta}|wr{wr}"
    if fixed_dim == "theta":
        return f"lr{lr}|wr{wr}"
    if fixed_dim == "wr":
        return f"lr{lr}|theta{theta}"
    raise ValueError(f"unknown fixed_dim: {fixed_dim}")


def x_axis_title(fixed_dim):
    if fixed_dim == "lr":
        return "zipfian theta | WR Ratio"
    if fixed_dim == "theta":
        return "Local Ratio | WR Ratio"
    if fixed_dim == "wr":
        return "Local Ratio | zipfian theta"
    raise ValueError(f"unknown fixed_dim: {fixed_dim}")


def plot_for_fixed_dim(fixed_dim, fixed_value, cases):
    components = [
        ("Tx fetch exe Time", "#4C78A8"),
        ("Tx commit Time", "#F58518"),
        ("Tx fetch commit Time", "#54A24B"),
        ("Tx abort Time", "#E45756"),
        ("wait push page time", "#B279A2"),
    ]

    x_labels = []
    lazy_stack = {name: [] for name, _ in components}
    twopc_stack = {name: [] for name, _ in components}

    for case_key, mode_rows in sorted(cases.items(), key=lambda x: global_sort_key(x[0])):
        if not matches_fixed(case_key, fixed_dim, fixed_value):
            continue
        if "Lazy" not in mode_rows or "2PC" not in mode_rows:
            continue

        x_labels.append(x_label_for_case(case_key, fixed_dim))
        lazy_row = mode_rows["Lazy"]
        twopc_row = mode_rows["2PC"]
        for comp_name, _ in components:
            lazy_stack[comp_name].append(parse_float(lazy_row.get(comp_name, "0")))
            twopc_stack[comp_name].append(parse_float(twopc_row.get(comp_name, "0")))

    if not x_labels:
        return None

    x = list(range(len(x_labels)))
    width = 0.38

    plt.figure(figsize=(max(12, len(x_labels) * 0.9), 7))
    lazy_bottom = [0.0] * len(x_labels)
    twopc_bottom = [0.0] * len(x_labels)

    for comp_name, color in components:
        plt.bar(
            [i - width / 2 for i in x],
            lazy_stack[comp_name],
            width=width,
            bottom=lazy_bottom,
            color=color,
            label=comp_name,
        )
        plt.bar(
            [i + width / 2 for i in x],
            twopc_stack[comp_name],
            width=width,
            bottom=twopc_bottom,
            color=color,
            alpha=0.55,
        )
        lazy_bottom = [b + v for b, v in zip(lazy_bottom, lazy_stack[comp_name])]
        twopc_bottom = [b + v for b, v in zip(twopc_bottom, twopc_stack[comp_name])]

    fixed_label = get_label(fixed_dim)
    plt.title(f"Time Breakdown (Fixed {fixed_label}={fixed_value})")
    plt.xlabel(x_axis_title(fixed_dim))
    plt.ylabel("Time")
    plt.xticks(x, x_labels, rotation=45, ha="right")
    plt.bar([], [], color="gray", label="Lazy (left, solid)")
    plt.bar([], [], color="gray", alpha=0.55, label="2PC (right, transparent)")
    plt.legend()
    plt.tight_layout()

    out_dir = os.path.join(OUTPUT_DIR, fixed_dim)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"time_breakdown_{fixed_dim}_{fixed_value}.png")
    plt.savefig(out_path, dpi=200)
    plt.close()
    return out_path


def plot_throughput_for_fixed_dim(fixed_dim, fixed_value, cases):
    x_labels = []
    lazy_tps = []
    twopc_tps = []

    for case_key, mode_rows in sorted(cases.items(), key=lambda x: global_sort_key(x[0])):
        if not matches_fixed(case_key, fixed_dim, fixed_value):
            continue
        if "Lazy" not in mode_rows or "2PC" not in mode_rows:
            continue

        x_labels.append(x_label_for_case(case_key, fixed_dim))
        lazy_tps.append(parse_float(mode_rows["Lazy"].get("TPS", "0")))
        twopc_tps.append(parse_float(mode_rows["2PC"].get("TPS", "0")))

    if not x_labels:
        return None

    x = list(range(len(x_labels)))
    width = 0.38

    plt.figure(figsize=(max(12, len(x_labels) * 0.9), 7))
    plt.bar([i - width / 2 for i in x], lazy_tps, width=width, label="Lazy", color="#4C78A8")
    plt.bar([i + width / 2 for i in x], twopc_tps, width=width, label="2PC", color="#F58518")
    fixed_label = get_label(fixed_dim)
    plt.title(f"Throughput Comparison (Fixed {fixed_label}={fixed_value})")
    plt.xlabel(x_axis_title(fixed_dim))
    plt.ylabel("TPS")
    plt.xticks(x, x_labels, rotation=45, ha="right")
    plt.legend()
    plt.tight_layout()

    out_dir = os.path.join(OUTPUT_DIR, "throughput", fixed_dim)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"throughput_{fixed_dim}_{fixed_value}.png")
    plt.savefig(out_path, dpi=200)
    plt.close()
    return out_path


def plot_lazy_only_for_fixed_dim(fixed_dim, fixed_value, cases):
    components = [
        ("Tx fetch exe Time", "#4C78A8"),
        ("Tx commit Time", "#F58518"),
        ("Tx fetch commit Time", "#54A24B"),
        ("Tx abort Time", "#E45756"),
        ("wait push page time", "#B279A2"),
    ]

    x_labels = []
    lazy_stack = {name: [] for name, _ in components}

    for case_key, mode_rows in sorted(cases.items(), key=lambda x: global_sort_key(x[0])):
        if not matches_fixed(case_key, fixed_dim, fixed_value):
            continue
        if "Lazy" not in mode_rows:
            continue

        x_labels.append(x_label_for_case(case_key, fixed_dim))
        lazy_row = mode_rows["Lazy"]
        for comp_name, _ in components:
            lazy_stack[comp_name].append(parse_float(lazy_row.get(comp_name, "0")))

    if not x_labels:
        return None

    x = list(range(len(x_labels)))
    width = 0.65
    plt.figure(figsize=(max(12, len(x_labels) * 0.8), 7))
    lazy_bottom = [0.0] * len(x_labels)

    for comp_name, color in components:
        plt.bar(
            x,
            lazy_stack[comp_name],
            width=width,
            bottom=lazy_bottom,
            color=color,
            label=comp_name,
        )
        lazy_bottom = [b + v for b, v in zip(lazy_bottom, lazy_stack[comp_name])]

    fixed_label = get_label(fixed_dim)
    plt.title(f"Lazy Time Breakdown (Fixed {fixed_label}={fixed_value})")
    plt.xlabel(x_axis_title(fixed_dim))
    plt.ylabel("Time")
    plt.xticks(x, x_labels, rotation=45, ha="right")
    plt.legend()
    plt.tight_layout()

    out_dir = os.path.join(OUTPUT_DIR, "lazy_only", fixed_dim)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"lazy_time_breakdown_{fixed_dim}_{fixed_value}.png")
    plt.savefig(out_path, dpi=200)
    plt.close()
    return out_path


def plot_lazy_only_total(cases):
    components = [
        ("ownership_transfer_time", "OwnershipTrans Time", "#4C78A8"),
        ("Tx Commit Time", "Tx commit Time", "#F58518"),
        ("Tx Abort Time", "Tx abort Time", "#E45756"),
        ("wait push page time", "wait push page time", "#B279A2"),
    ]

    x_labels = []
    lazy_stack = {display_name: [] for display_name, _, _ in components}

    for case_key, mode_rows in sorted(cases.items(), key=lambda x: global_sort_key(x[0])):
        if "Lazy" not in mode_rows:
            continue

        lr, theta, wr = case_key
        x_labels.append(f"LR={lr}|theta={theta}|WR={wr}")
        lazy_row = mode_rows["Lazy"]
        for display_name, csv_col, _ in components:
            lazy_stack[display_name].append(parse_float(lazy_row.get(csv_col, "0")))

    if not x_labels:
        return None

    x = list(range(len(x_labels)))
    width = 0.65
    plt.figure(figsize=(max(14, len(x_labels) * 0.45), 7))
    lazy_bottom = [0.0] * len(x_labels)

    for display_name, _, color in components:
        plt.bar(
            x,
            lazy_stack[display_name],
            width=width,
            bottom=lazy_bottom,
            color=color,
            label=display_name,
        )
        lazy_bottom = [b + v for b, v in zip(lazy_bottom, lazy_stack[display_name])]

    plt.title("Lazy Time Breakdown (All Cases)")
    plt.xlabel("Case")
    plt.ylabel("Time")
    plt.xticks(x, x_labels, rotation=70, ha="right")
    plt.legend()
    plt.tight_layout()

    out_dir = os.path.join(OUTPUT_DIR, "lazy_only")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "lazy_time_breakdown_total.png")
    plt.savefig(out_path, dpi=200)
    plt.close()
    return out_path


def main():
    rows = load_rows(CSV_PATH)
    case_map = build_case_map(rows)
    all_lrs = sorted({k[0] for k in case_map.keys()}, key=float)
    all_thetas = sorted({k[1] for k in case_map.keys()}, key=float)
    all_wrs = sorted({k[2] for k in case_map.keys()}, key=float)

    if not all_lrs or not all_thetas or not all_wrs:
        print("没有可绘制的数据。")
        return

    print(f"读取 CSV: {CSV_PATH}")
    generated = []
    generated_tps = []
    generated_lazy_only = []
    for lr in all_lrs:
        out = plot_for_fixed_dim("lr", lr, case_map)
        if out:
            generated.append(out)
            print(f"时间分解图已生成: {out}")
        out_tps = plot_throughput_for_fixed_dim("lr", lr, case_map)
        if out_tps:
            generated_tps.append(out_tps)
            print(f"吞吐量对比图已生成: {out_tps}")
        out_lazy_only = plot_lazy_only_for_fixed_dim("lr", lr, case_map)
        if out_lazy_only:
            generated_lazy_only.append(out_lazy_only)
            print(f"Lazy时间分解图已生成: {out_lazy_only}")

    for theta in all_thetas:
        out = plot_for_fixed_dim("theta", theta, case_map)
        if out:
            generated.append(out)
            print(f"时间分解图已生成: {out}")
        out_tps = plot_throughput_for_fixed_dim("theta", theta, case_map)
        if out_tps:
            generated_tps.append(out_tps)
            print(f"吞吐量对比图已生成: {out_tps}")
        out_lazy_only = plot_lazy_only_for_fixed_dim("theta", theta, case_map)
        if out_lazy_only:
            generated_lazy_only.append(out_lazy_only)
            print(f"Lazy时间分解图已生成: {out_lazy_only}")

    for wr in all_wrs:
        out = plot_for_fixed_dim("wr", wr, case_map)
        if out:
            generated.append(out)
            print(f"时间分解图已生成: {out}")
        out_tps = plot_throughput_for_fixed_dim("wr", wr, case_map)
        if out_tps:
            generated_tps.append(out_tps)
            print(f"吞吐量对比图已生成: {out_tps}")
        out_lazy_only = plot_lazy_only_for_fixed_dim("wr", wr, case_map)
        if out_lazy_only:
            generated_lazy_only.append(out_lazy_only)
            print(f"Lazy时间分解图已生成: {out_lazy_only}")

    out_lazy_total = plot_lazy_only_total(case_map)
    if out_lazy_total:
        generated_lazy_only.append(out_lazy_total)
        print(f"Lazy总时间分解图已生成: {out_lazy_total}")

    if not generated:
        print("没有生成图片，请检查 CSV 列是否齐全。")
    if not generated_tps:
        print("没有生成吞吐量图片，请检查 CSV 列是否齐全。")
    if not generated_lazy_only:
        print("没有生成Lazy时间分解图，请检查 CSV 列是否齐全。")


if __name__ == "__main__":
    main()
