import argparse
import csv
import os


DEFAULT_LOCAL_RATIOS = [0.2, 0.5, 0.8]
DEFAULT_TX_HOT_LIST = [20, 50, 80]
DEFAULT_WR_RATIOS = [0.3, 0.6, 0.9]

METRIC_SPECS = [
    ("TPS", "throughput=", "int"),
    ("WaitLog (s)", "wait_log_flush_time=", "float"),
    ("Logs (count)", "log_flush_total_batch=", "int"),
    ("WaitLogFlush (count)", "wait_log_flush_count=", "int"),
    ("OwnerShip Trans (count)", "ownership_transfer_count=", "int"),
    ("OwnerTran TimeAvg (ms)", "ownership_transfer_time_avg_ms=", "float"),
]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("result_dir_1")
    parser.add_argument("result_dir_2")
    parser.add_argument("--mode", default="smallbank_lazy")
    parser.add_argument("--round-dir", default="round_00")
    parser.add_argument("--output", default=None)
    return parser.parse_args()


def parse_summary(file_path):
    metrics = {name: 0.0 for name, _, _ in METRIC_SPECS}
    try:
        with open(file_path, "r") as file:
            for line in file:
                for metric_name, prefix, value_type in METRIC_SPECS:
                    if not line.startswith(prefix):
                        continue
                    value = float(line.strip().split("=", 1)[1])
                    metrics[metric_name] = value
                    break
    except FileNotFoundError:
        pass
    return metrics


def pct_change(old_value, new_value):
    # 现在计算 pre (old_value) 比 next (new_value) 好多少
    if new_value == 0:
        return 0.0 if old_value == 0 else 100.0
    return ((old_value - new_value) / new_value) * 100.0


def format_metric_value(metric_name, value):
    if any(token in metric_name for token in ["TPS", "count"]):
        return int(value)
    return f"{value:.2f}"


def collect_mode_results(base_dir, round_dir, mode):
    mode_dir = os.path.join(base_dir, round_dir, mode)
    results = {}
    for cr in DEFAULT_LOCAL_RATIOS:
        for hot in DEFAULT_TX_HOT_LIST:
            for wr in DEFAULT_WR_RATIOS:
                dir_name = f"cr_{cr}_txhot_{hot}_wr_{wr}"
                summary_path = os.path.join(mode_dir, dir_name, "summary_human.txt")
                results[(cr, hot, wr)] = parse_summary(summary_path)
    return results


def build_output_path(result_dir_1, result_dir_2, mode, output_path):
    if output_path:
        return output_path
    name_1 = os.path.basename(os.path.normpath(result_dir_1))
    name_2 = os.path.basename(os.path.normpath(result_dir_2))
    return os.path.abspath(f"{mode}_diff_{name_1}_vs_{name_2}.csv")


def generate_report():
    args = parse_args()
    results_1 = collect_mode_results(args.result_dir_1, args.round_dir, args.mode)
    results_2 = collect_mode_results(args.result_dir_2, args.round_dir, args.mode)
    output_path = build_output_path(args.result_dir_1, args.result_dir_2, args.mode, args.output)

    header = ["Version", "Local Ratio", "Tx Hot (%)", "WR Ratio"]
    for metric_name, _, _ in METRIC_SPECS:
        header.append(metric_name)
        if metric_name == "TPS":
            header.append("pre vs next TPS (%)")

    with open(output_path, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(header)

        for cr in DEFAULT_LOCAL_RATIOS:
            for hot in DEFAULT_TX_HOT_LIST:
                for wr in DEFAULT_WR_RATIOS:
                    metrics_1 = results_1[(cr, hot, wr)]
                    metrics_2 = results_2[(cr, hot, wr)]
                    comparison_values = {
                        metric_name: f"{pct_change(metrics_1[metric_name], metrics_2[metric_name]):.2f}"
                        for metric_name, _, _ in METRIC_SPECS
                    }

                    for row_index, (version_name, metrics) in enumerate([
                        ("pre", metrics_1),
                        ("next", metrics_2),
                    ]):
                        row = [
                            version_name,
                            cr if row_index == 0 else "",
                            hot if row_index == 0 else "",
                            wr if row_index == 0 else "",
                        ]
                        for metric_name, _, _ in METRIC_SPECS:
                            row.append(format_metric_value(metric_name, metrics[metric_name]))
                            if metric_name == "TPS":
                                row.append(f"{pct_change(metrics_1['TPS'], metrics_2['TPS']):.2f}")
                        writer.writerow(row)
                    writer.writerow([])

    print(output_path)


if __name__ == "__main__":
    generate_report()
