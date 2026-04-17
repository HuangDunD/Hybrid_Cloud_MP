import os
import glob
import csv
import re
try:
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

all_results = glob.glob('/usr/local/workspace/Hybrid_Cloud_MP/result/*/round_00')

# 要对比的模式和参数
modes = ['ycsb_lazy', 'ycsb_2pc']
# modes = ['smallbank_lazy' , 'smallbank_2pc']
local_ratios = [0.2, 0.5, 0.8]
use_zipfian = True
zipfian_theta = [0.99, 0.8, 0.6, 0.4]
tx_hot_list = [20, 50, 80]
wr_ratios = [0.3, 0.6, 0.9]


def parse_case_tuple_from_dir_name(dir_name):
    patterns = [
        (r"^lr([0-9.]+)_txhot_([0-9]+)_wr_([0-9.]+)$", "txhot"),
        (r"^local_txn_([0-9.]+)_txhot_([0-9]+)_wr_([0-9.]+)$", "txhot"),
        (r"^cr_([0-9.]+)_txhot_([0-9]+)_wr_([0-9.]+)$", "txhot"),
        (r"^lr([0-9.]+)_theta_([0-9.]+)_wr_([0-9.]+)$", "theta"),
        (r"^local_txn_([0-9.]+)_theta_([0-9.]+)_wr_([0-9.]+)$", "theta"),
        (r"^cr_([0-9.]+)_theta_([0-9.]+)_wr_([0-9.]+)$", "theta"),
    ]
    for pattern, pattern_type in patterns:
        match = re.match(pattern, dir_name)
        if match:
            return float(match.group(1)), pattern_type, float(match.group(2)), float(match.group(3))
    return None

def format_pattern_value(pattern_type, pattern_value):
    if pattern_type == "txhot":
        return str(int(pattern_value))
    return str(pattern_value)


def discover_cases(base_dir, mode):
    mode_dir = os.path.join(base_dir, mode)
    if not os.path.isdir(mode_dir):
        return set()
    cases = set()
    for entry in os.listdir(mode_dir):
        full_path = os.path.join(mode_dir, entry)
        if not os.path.isdir(full_path):
            continue
        case = parse_case_tuple_from_dir_name(entry)
        if case is None:
            continue
        summary_path = os.path.join(full_path, 'summary_human.txt')
        if os.path.isfile(summary_path):
            cases.add(case)
    return cases


def select_result_base_dir(results_dirs, selected_modes):
    if not results_dirs:
        return '/usr/local/workspace/Hybrid_Cloud_MP/result/20260331214228/round_00'
    candidates = sorted(results_dirs, key=os.path.getmtime, reverse=True)
    for base_dir in candidates:
        if all(discover_cases(base_dir, mode) for mode in selected_modes):
            return base_dir
    return candidates[0]

def parse_throughput(file_path):
    """从 summary_human.txt 中解析 throughput 参数"""
    tps = 0.0
    wait_log_time = 0.0
    log_count = 0.0
    wait_log_flush_count = 0.0
    prepare_log_count = 0.0
    backup_log_count = 0.0
    ownership_transfer_count = 0.0
    ownership_transfer_time_avg_ms = 0.0
    ownership_transfer_time_total = 0.0

    wait_prepare_log_time = 0.0
    wait_backup_log_time = 0.0
    wait_commit_log_time = 0.0
    tx_wait_abort_log_time = 0.0
    tx_write_prepare_log_time = 0.0
    tx_write_backup_log_time = 0.0
    tx_write_commit_log_time = 0.0
    wait_log_flush_push_page_time = 0
    wait_log_flush_tx_over_time = 0.0
    tx_exe_time = 0
    tx_fetch_exe_time = 0.0
    tx_commit_time = 0
    tx_abort_time = 0
    twopc_remote_fetch_time = 0.0
    twopc_remote_fetch_count = 0.0
    fetch_storage_page_time = 0.0
    single_txn_count = 0.0
    distribute_txn_count = 0.0

    tx_commit_fetch_page_time = 0.0
    total_time_seconds = 0.0

    try:
        with open(file_path, 'r') as f:
            for line in f:
                if line.startswith('throughput='):
                    tps = float(line.strip().split('=')[1])
                elif line.startswith('wait_log_flush_time='):
                    wait_log_time = float(line.strip().split('=')[1])
                elif line.startswith('log_flush_total_batch='):
                    log_count = float(line.strip().split('=')[1])
                elif line.startswith('wait_log_flush_count='):
                    wait_log_flush_count = float(line.strip().split('=')[1])
                elif line.startswith('prepare_log_count='):
                    prepare_log_count = float(line.strip().split('=')[1])
                elif line.startswith('backup_log_count='):
                    backup_log_count = float(line.strip().split('=')[1])
                elif line.startswith('ownership_transfer_count='):
                    ownership_transfer_count = float(line.strip().split('=')[1])
                elif line.startswith('ownership_transfer_time_avg_ms='):
                    ownership_transfer_time_avg_ms = float(line.strip().split('=')[1])
                elif line.startswith('ownership_transfer_time_total='):
                    ownership_transfer_time_total = float(line.strip().split('=')[1])
                elif line.startswith('wait_commit_log_time='):
                    wait_commit_log_time = float(line.strip().split('=')[1])
                elif line.startswith('TxWaitAbortLogTime='):
                    tx_wait_abort_log_time = float(line.strip().split('=')[1])
                elif line.startswith('wait_prepare_log_time='):
                    wait_prepare_log_time = float(line.strip().split('=')[1])
                elif line.startswith('wait_backup_log_time='):
                    wait_backup_log_time = float(line.strip().split('=')[1])
                elif line.startswith('tx_write_commit_log_time='):
                    tx_write_commit_log_time += float(line.strip().split('=')[1])
                elif line.startswith('tx_write_commit_log_time2='):
                    tx_write_commit_log_time += float(line.strip().split('=')[1])
                elif line.startswith('tx_write_prepare_log_time='):
                    tx_write_prepare_log_time = float(line.strip().split('=')[1])
                elif line.startswith('tx_write_backup_log_time='):
                    tx_write_backup_log_time = float(line.strip().split('=')[1])
                elif line.startswith('wait_log_flush_push_page_time='):
                    wait_log_flush_push_page_time = float(line.strip().split('=')[1])
                elif line.startswith('wait_log_flush_tx_over_time='):
                    wait_log_flush_tx_over_time = float(line.strip().split('=')[1])
                elif line.startswith('tx_exe_time='):
                    tx_exe_time = float(line.strip().split('=')[1])
                elif line.startswith('tx_fetch_exe_time='):
                    tx_fetch_exe_time = float(line.strip().split('=')[1])
                elif line.startswith('tx_commit_time='):
                    tx_commit_time = float(line.strip().split('=')[1])
                elif line.startswith('tx_commit_fetch_page_time='):
                    tx_commit_fetch_page_time = float(line.strip().split('=')[1])
                elif line.startswith('total_time_seconds='):
                    total_time_seconds = float(line.strip().split('=')[1])
                elif line.startswith('tx_abort_time='):
                    tx_abort_time = float(line.strip().split('=')[1])
                elif line.startswith('twopc_remote_fetch_time='):
                    twopc_remote_fetch_time = float(line.strip().split('=')[1])
                elif line.startswith('twopc_remote_fetch_count='):
                    twopc_remote_fetch_count = float(line.strip().split('=')[1])
                elif line.startswith('fetch_storage_page_time='):
                    fetch_storage_page_time = float(line.strip().split('=')[1])
                elif line.startswith('single_txn_count='):
                    single_txn_count = float(line.strip().split('=')[1])
                elif line.startswith('distribute_txn_count='):
                    distribute_txn_count = float(line.strip().split('=')[1])
    except FileNotFoundError:
        pass
    return tps, wait_log_time, wait_log_flush_tx_over_time, log_count, wait_log_flush_count, prepare_log_count, backup_log_count, ownership_transfer_count, ownership_transfer_time_avg_ms, ownership_transfer_time_total, wait_prepare_log_time, wait_backup_log_time, wait_commit_log_time, tx_wait_abort_log_time, tx_write_prepare_log_time, tx_write_backup_log_time, tx_write_commit_log_time, wait_log_flush_push_page_time, tx_exe_time, tx_commit_time, tx_abort_time, twopc_remote_fetch_time, fetch_storage_page_time, twopc_remote_fetch_count, single_txn_count, distribute_txn_count, tx_fetch_exe_time, tx_commit_fetch_page_time, total_time_seconds

def generate_report():
    result_base_dir = select_result_base_dir(all_results, modes)
    print(f"Using result directory: {result_base_dir}")

    results = {}  # key: (mode, cr, hot, wr), value: (throughput, wait_log_time, log_count, wait_log_flush_count, prepare_log_count, backup_log_count, ownership_transfer_count, ownership_transfer_time_avg_ms)
    mode_cases = {mode: discover_cases(result_base_dir, mode) for mode in modes}

    common_cases = set()
    if len(modes) >= 2 and mode_cases[modes[0]] and mode_cases[modes[1]]:
        common_cases = mode_cases[modes[0]] & mode_cases[modes[1]]
    all_cases = set().union(*mode_cases.values()) if mode_cases else set()
    # 固定排序优先级: theta > wr > lr
    target_cases = sorted(
        common_cases if common_cases else all_cases,
        key=lambda x: (x[2], x[3], x[0], x[1]),
    )
    if not target_cases:
        fallback_pattern_type = "theta" if use_zipfian else "txhot"
        fallback_pattern_values = zipfian_theta if use_zipfian else tx_hot_list
        target_cases = [
            (cr, fallback_pattern_type, float(pattern_value), wr)
            for cr in local_ratios
            for pattern_value in fallback_pattern_values
            for wr in wr_ratios
        ]

    for mode in modes:
        for cr, pattern_type, pattern_value, wr in target_cases:
            pattern_value_str = format_pattern_value(pattern_type, pattern_value)
            dir_candidates = [
                f"lr{cr}_{pattern_type}_{pattern_value_str}_wr_{wr}",
                f"local_txn_{cr}_{pattern_type}_{pattern_value_str}_wr_{wr}",
                f"cr_{cr}_{pattern_type}_{pattern_value_str}_wr_{wr}",
            ]
            summary_path = ''
            for dir_name in dir_candidates:
                candidate_path = os.path.join(result_base_dir, mode, dir_name, 'summary_human.txt')
                if os.path.exists(candidate_path):
                    summary_path = candidate_path
                    break
            if not summary_path:
                summary_path = os.path.join(result_base_dir, mode, dir_candidates[0], 'summary_human.txt')
            tps, wait_log_time, wait_log_flush_tx_over_time, log_count, wait_log_flush_count, prepare_log_count, backup_log_count, ownership_transfer_count, ownership_transfer_time_avg_ms, ownership_transfer_time_total, wait_prepare_log_time, wait_backup_log_time, wait_commit_log_time, tx_wait_abort_log_time, tx_write_prepare_log_time, tx_write_backup_log_time, tx_write_commit_log_time, wait_log_flush_push_page_time, tx_exe_time, tx_commit_time, tx_abort_time, twopc_remote_fetch_time, fetch_storage_page_time, twopc_remote_fetch_count, single_txn_count, distribute_txn_count, tx_fetch_exe_time, tx_commit_fetch_page_time, total_time_seconds = parse_throughput(summary_path)

            if '2pc' in mode.lower() or '2PC' in mode:
                ownership_transfer_count = twopc_remote_fetch_count
                ownership_transfer_time_total = twopc_remote_fetch_time
                if ownership_transfer_count > 0:
                    ownership_transfer_time_avg_ms = (ownership_transfer_time_total / ownership_transfer_count) * 1000.0
                else:
                    ownership_transfer_time_avg_ms = 0.0

            results[(mode, cr, pattern_type, pattern_value, wr)] = (tps, wait_log_time, wait_log_flush_tx_over_time, log_count, wait_log_flush_count, prepare_log_count, backup_log_count, ownership_transfer_count, ownership_transfer_time_avg_ms, ownership_transfer_time_total, wait_prepare_log_time, wait_backup_log_time, wait_commit_log_time, tx_wait_abort_log_time, tx_write_prepare_log_time, tx_write_backup_log_time, tx_write_commit_log_time, wait_log_flush_push_page_time, tx_exe_time, tx_commit_time, tx_abort_time, twopc_remote_fetch_time, fetch_storage_page_time, single_txn_count, distribute_txn_count, tx_fetch_exe_time, tx_commit_fetch_page_time, total_time_seconds)

    # 2. 生成 CSV 报表 (适合导入 Excel 做进一步分析)
    csv_file = 'throughput_comparison.csv'
    if len(modes) < 2:
        print("需要至少两个模式进行对比")
        return

    mode1 = modes[0]
    mode2 = modes[1]

    # 简单的字符串处理，去掉前缀，只保留 2PC 或 Lazy
    name1 = 'Lazy' if 'lazy' in mode1.lower() else '2PC'
    name2 = 'Lazy' if 'lazy' in mode2.lower() else '2PC'
    mode_names = {
        mode1: name1,
        mode2: name2,
    }
    comparison_label = f'{name1} vs {name2} (%)'
    pattern_value_label = 'zipfian theta' if use_zipfian else 'Tx Hot'

    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Mode', 'Local Ratio', pattern_value_label, 'WR Ratio', 'TPS', 'Total Time(s)', comparison_label, 'Tx exe Time', 'Tx fetch exe Time', 'Tx commit Time', 'Tx fetch commit Time', 'Tx abort Time', 'Tx commit phase time', 'TxWaitAbortLogTime', 'Tx backup phase time', 'Tx prepare phase time', 'DistTxn (%)', 'wait push page time', 'OwnershipTrans Count', 'OwnershipTrans Time', 'OwnershipTrans Avg Time(ms)'])
        
        print(f"{'Case':<36} | {'Mode':<8} | {'TPS':<10} | {comparison_label:<18} | {'Tx exe':<10} | {'Tx fet exe':<10} | {'Tx commit':<10} | {'Tx fet com':<10} | {'Tx abort':<10} | {'TxComPh':<10} | {'WAbortLog':<10} | {'TxBackPh':<10} | {'TxPrepPh':<10} | {'DistTxn%':<10} | {'WPush':<10} | {'OwnTransCt':<10} | {'OwnTransTm':<10} | {'OwnTransAvg':<10}")
        print("-" * 235)

        for cr, pattern_type, pattern_value, wr in target_cases:
            pattern_value_str = format_pattern_value(pattern_type, pattern_value)
            tps1, wait_log_tot1, wait_tx_over1, logs1, wait_ct1, prep1, back1, owner_trans1, owner_time_avg1, owner_time_tot1, wprep1, wback1, wcom1, wabortlog1, txwprep1, txwback1, txwcom1, wpush1, txexe1, txcom1, txabt1, twopc1, stor1, single_txn1, dist_txn1, txfetchexe1, txcommitfet1, total_time_sec1 = results.get((mode1, cr, pattern_type, pattern_value, wr), (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
            tps2, wait_log_tot2, wait_tx_over2, logs2, wait_ct2, prep2, back2, owner_trans2, owner_time_avg2, owner_time_tot2, wprep2, wback2, wcom2, wabortlog2, txwprep2, txwback2, txwcom2, wpush2, txexe2, txcom2, txabt2, twopc2, stor2, single_txn2, dist_txn2, txfetchexe2, txcommitfet2, total_time_sec2 = results.get((mode2, cr, pattern_type, pattern_value, wr), (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))

            comparison_value = 0.0
            if tps1 > 0 and tps2 > 0:
                base = min(tps1, tps2)
                comparison_value = (tps1 - tps2) / base * 100

            mode_rows = [
                (mode1, tps1, total_time_sec1, wait_log_tot1, wait_tx_over1, logs1, wait_ct1, prep1, back1, owner_trans1, owner_time_avg1, owner_time_tot1, wprep1, wback1, wcom1, wabortlog1, txwprep1, txwback1, txwcom1, wpush1, txexe1, txfetchexe1, txcom1, txcommitfet1, txabt1, stor1, single_txn1, dist_txn1),
                (mode2, tps2, total_time_sec2, wait_log_tot2, wait_tx_over2, logs2, wait_ct2, prep2, back2, owner_trans2, owner_time_avg2, owner_time_tot2, wprep2, wback2, wcom2, wabortlog2, txwprep2, txwback2, txwcom2, wpush2, txexe2, txfetchexe2, txcom2, txcommitfet2, txabt2, stor2, single_txn2, dist_txn2),
            ]

            case_label = f"LR={cr} | {pattern_type}={pattern_value_str} | WR={wr}"
            for row_idx, (mode, tps, total_time_sec, wait_log_tot, wait_tx_over, log_count, wait_log_flush_count, prepare_log_count, backup_log_count, ownership_transfer_count, ownership_transfer_time_avg_ms, ownership_transfer_time_total, wprep, wback, wcom, wabortlog, txwprep, txwback, txwcom, wpush, txexe, txfetchexe, txcom, txcommitfet, txabt, stor, single_txn, dist_txn) in enumerate(mode_rows):
                dist_ratio = 0.0
                if single_txn + dist_txn > 0:
                    dist_ratio = dist_txn / (single_txn + dist_txn) * 100.0

                writer.writerow([
                    mode_names[mode], cr, pattern_value_str, wr,
                    int(tps),
                    f"{total_time_sec:.4f}",
                    f"{comparison_value:.2f}%" if row_idx == 0 else "",
                    f"{txexe:.2f}",
                    f"{txfetchexe:.2f}",
                    f"{txcom:.2f}",
                    f"{txcommitfet:.2f}",
                    f"{txabt:.2f}",
                    f"{txwcom:.2f}",
                    f"{wabortlog:.2f}",
                    f"{txwback:.2f}",
                    f"{txwprep:.2f}",
                    f"{dist_ratio:.2f}",
                    f"{wpush:.2f}",
                    int(ownership_transfer_count),
                    f"{ownership_transfer_time_total:.2f}",
                    f"{ownership_transfer_time_avg_ms:.2f}"
                ])

                display_case = case_label if row_idx == 0 else ""
                comp_str = f"{comparison_value:.2f}%" if row_idx == 0 else ""
                print(f"{display_case:<36} | {mode_names[mode]:<8} | {int(tps):<10} | {comp_str:<18} | {txexe:<10.2f} | {txfetchexe:<10.2f} | {txcom:<10.2f} | {txcommitfet:<10.2f} | {txabt:<10.2f} | {txwcom:<10.2f} | {wabortlog:<10.2f} | {txwback:<10.2f} | {txwprep:<10.2f} | {dist_ratio:<10.2f} | {wpush:<10.2f} | {int(ownership_transfer_count):<10} | {ownership_transfer_time_total:<10.2f} | {ownership_transfer_time_avg_ms:<10.2f}")
            print("-" * 235)
    
    print(f"\nCSV 报表已生成: {csv_file}")

    # 3. 生成总执行时间对比图（单位：秒）
    if not HAS_MATPLOTLIB:
        print("未安装 matplotlib，跳过 Total Time(s) 对比图生成。")
        return

    labels = []
    lazy_total_times = []
    twopc_total_times = []
    for cr, pattern_type, pattern_value, wr in target_cases:
        pattern_value_str = format_pattern_value(pattern_type, pattern_value)
        case_label = f"LR={cr}|{pattern_value_label}={pattern_value_str}|WR={wr}"
        labels.append(case_label)

        mode1_vals = results.get((mode1, cr, pattern_type, pattern_value, wr), None)
        mode2_vals = results.get((mode2, cr, pattern_type, pattern_value, wr), None)
        lazy_total_times.append(float(mode1_vals[27]) if mode1_vals else 0.0)
        twopc_total_times.append(float(mode2_vals[27]) if mode2_vals else 0.0)

    if labels:
        x = list(range(len(labels)))
        width = 0.38
        plt.figure(figsize=(max(14, len(labels) * 0.45), 7))
        plt.bar([i - width / 2 for i in x], lazy_total_times, width=width, label=mode_names[mode1], color="#4C78A8")
        plt.bar([i + width / 2 for i in x], twopc_total_times, width=width, label=mode_names[mode2], color="#F58518")
        plt.ylabel("Total Time (seconds)")
        plt.title("Total Execution Time Comparison")
        plt.xticks(x, labels, rotation=70, ha='right')
        plt.legend()
        plt.tight_layout()

        out_dir = "time_breakdown_charts"
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, "total_time_seconds_comparison.png")
        plt.savefig(out_path, dpi=200)
        plt.close()
        print(f"总执行时间对比图已生成: {out_path}")

    # 4. 生成按负载 case 对齐的总时间分解图（样式与固定 theta 图一致）
    # 每个 case 下放置两根堆叠柱（左: Lazy 实心，右: 2PC 半透明）。
    time_components = [
        ("Tx fetch exe Time", 25, "#4C78A8"),
        ("Tx commit Time", 19, "#F58518"),
        ("Tx fetch commit Time", 26, "#54A24B"),
        ("Tx abort Time", 20, "#E45756"),
        ("wait push page time", 17, "#B279A2"),
    ]

    breakdown_labels = []
    mode1_series = {name: [] for name, _, _ in time_components}
    mode2_series = {name: [] for name, _, _ in time_components}
    case_records = []

    for cr, pattern_type, pattern_value, wr in target_cases:
        pattern_value_str = format_pattern_value(pattern_type, pattern_value)
        case_label = f"LR={cr}|{pattern_value_label}={pattern_value_str}|WR={wr}"
        breakdown_labels.append(case_label)

        vals1 = results.get((mode1, cr, pattern_type, pattern_value, wr), (0.0,) * 28)
        vals2 = results.get((mode2, cr, pattern_type, pattern_value, wr), (0.0,) * 28)
        case_records.append({
            "case_label": case_label,
            "vals1": vals1,
            "vals2": vals2,
        })
        for component_name, idx, _ in time_components:
            v1 = float(vals1[idx])
            v2 = float(vals2[idx])
            mode1_series[component_name].append(v1)
            mode2_series[component_name].append(v2)

    has_breakdown_data = False
    for component_data in list(mode1_series.values()) + list(mode2_series.values()):
        if any(v > 0 for v in component_data):
            has_breakdown_data = True
            break

    if has_breakdown_data and breakdown_labels:
        x = list(range(len(breakdown_labels)))
        width = 0.38
        plt.figure(figsize=(max(12, len(breakdown_labels) * 0.9), 7))
        ax = plt.gca()

        mode1_bottom = [0.0] * len(breakdown_labels)
        mode2_bottom = [0.0] * len(breakdown_labels)
        for component_name, _, color in time_components:
            vals_mode1 = mode1_series[component_name]
            vals_mode2 = mode2_series[component_name]
            if any(v > 0 for v in vals_mode1) or any(v > 0 for v in vals_mode2):
                plt.bar(
                    [i - width / 2 for i in x], vals_mode1, width=width, bottom=mode1_bottom,
                    color=color, label=component_name
                )
                plt.bar(
                    [i + width / 2 for i in x], vals_mode2, width=width, bottom=mode2_bottom,
                    color=color, alpha=0.55
                )
                mode1_bottom = [mode1_bottom[i] + vals_mode1[i] for i in range(len(x))]
                mode2_bottom = [mode2_bottom[i] + vals_mode2[i] for i in range(len(x))]

        bar_positions = []
        bar_labels = []
        for i in x:
            bar_positions.extend([i - width / 2, i + width / 2])
            bar_labels.extend([mode_names[mode1], mode_names[mode2]])
        plt.xticks(bar_positions, bar_labels, rotation=0, ha='center')

        for i, case_label in enumerate(breakdown_labels):
            ax.text(
                i, -0.25, case_label, transform=ax.get_xaxis_transform(),
                ha='center', va='top', rotation=45, fontsize=8
            )

        plt.xlabel("Mode (per bar)")
        plt.ylabel("Time")
        plt.title("Time Breakdown (All Cases)")
        plt.bar([], [], color="gray", label=f"{mode_names[mode1]} (left, solid)")
        plt.bar([], [], color="gray", alpha=0.55, label=f"{mode_names[mode2]} (right, transparent)")
        plt.legend(loc="upper right")
        plt.subplots_adjust(bottom=0.35)
        plt.tight_layout()

        out_dir = "time_breakdown_charts"
        os.makedirs(out_dir, exist_ok=True)
        breakdown_out_path = os.path.join(out_dir, "total_time_breakdown_comparison.png")
        plt.savefig(breakdown_out_path, dpi=200)
        plt.close()
        print(f"按负载分组的总时间分解图已生成: {breakdown_out_path}")
    else:
        print("未发现可用时间分解数据，跳过总时间分解图生成。")

    # 5. 生成差异最大的 Top4 负载（2 个 Lazy 更大，2 个 2PC 更大）
    def select_top4_extremes(items, diff_getter):
        pos = [item for item in items if diff_getter(item) > 0]
        neg = [item for item in items if diff_getter(item) < 0]
        pos = sorted(pos, key=diff_getter, reverse=True)
        neg = sorted(neg, key=diff_getter)
        selected = pos[:2] + neg[:2]
        if len(selected) < 4:
            remaining = [item for item in sorted(items, key=lambda it: abs(diff_getter(it)), reverse=True) if item not in selected]
            selected.extend(remaining[: max(0, 4 - len(selected))])
        return selected[:4]

    top4_time_cases = select_top4_extremes(
        case_records,
        lambda r: float(r["vals1"][27]) - float(r["vals2"][27])
    )
    top4_tps_cases = select_top4_extremes(
        case_records,
        lambda r: float(r["vals1"][0]) - float(r["vals2"][0])
    )

    # 两张 Top4 图需要下标一一对应：构造统一 case 顺序供两图共用。
    # 优先保留时间分解 Top4 的顺序，再补充 TPS Top4 中缺失的 case。
    aligned_top4_cases = []
    seen_case_labels = set()
    for case in top4_time_cases + top4_tps_cases:
        case_label = case["case_label"]
        if case_label in seen_case_labels:
            continue
        aligned_top4_cases.append(case)
        seen_case_labels.add(case_label)
        if len(aligned_top4_cases) == 4:
            break

    # 若仍不足 4 个，按“时间差或TPS差的最大绝对值”补齐。
    if len(aligned_top4_cases) < 4:
        remaining_cases = [
            r for r in case_records if r["case_label"] not in seen_case_labels
        ]
        remaining_cases.sort(
            key=lambda r: max(
                abs(float(r["vals1"][27]) - float(r["vals2"][27])),
                abs(float(r["vals1"][0]) - float(r["vals2"][0])),
            ),
            reverse=True,
        )
        need = 4 - len(aligned_top4_cases)
        aligned_top4_cases.extend(remaining_cases[:need])

    aligned_top4_cases = aligned_top4_cases[:4]

    def plot_breakdown_for_selected_cases(selected_cases, out_path, title):
        if not selected_cases:
            return False
        x_labels = [r["case_label"] for r in selected_cases]
        x = list(range(len(x_labels)))
        width = 0.38
        plt.figure(figsize=(max(10, len(x_labels) * 2.2), 7.6))
        ax = plt.gca()

        mode1_bottom = [0.0] * len(x_labels)
        mode2_bottom = [0.0] * len(x_labels)

        for component_name, idx, color in time_components:
            vals_mode1 = [float(r["vals1"][idx]) for r in selected_cases]
            vals_mode2 = [float(r["vals2"][idx]) for r in selected_cases]
            plt.bar(
                [i - width / 2 for i in x], vals_mode1, width=width, bottom=mode1_bottom,
                color=color, label=component_name
            )
            plt.bar(
                [i + width / 2 for i in x], vals_mode2, width=width, bottom=mode2_bottom,
                color=color, alpha=0.55
            )
            mode1_bottom = [mode1_bottom[i] + vals_mode1[i] for i in range(len(x))]
            mode2_bottom = [mode2_bottom[i] + vals_mode2[i] for i in range(len(x))]

        bar_positions = []
        for i in x:
            bar_positions.extend([i - width / 2, i + width / 2])
        plt.xticks(bar_positions, [""] * len(bar_positions))

        ymax = max(mode1_bottom + mode2_bottom) if (mode1_bottom or mode2_bottom) else 0.0
        y_offset = max(ymax * 0.02, 80.0)
        # 预留更充足的顶部空间，避免柱顶模式文本与图框/标题拥挤
        plt.ylim(0, ymax + y_offset * 3.8)
        for i in x:
            lazy_x = i - width / 2
            twopc_x = i + width / 2
            plt.text(lazy_x, mode1_bottom[i] + y_offset, mode_names[mode1], ha='center', va='bottom', fontsize=10)
            plt.text(twopc_x, mode2_bottom[i] + y_offset, mode_names[mode2], ha='center', va='bottom', fontsize=10)

        for i, case_label in enumerate(x_labels):
            case_label_pretty = case_label.replace("|", "\n")
            ax.text(
                i, -0.10, case_label_pretty, transform=ax.get_xaxis_transform(),
                ha='center', va='top', rotation=0, fontsize=9
            )

        plt.xlabel("")
        plt.ylabel("Time")
        plt.title(title)
        plt.legend(loc="upper right")
        plt.tight_layout()
        plt.subplots_adjust(bottom=0.16, top=0.90)
        plt.savefig(out_path, dpi=200)
        plt.close()
        return True

    def plot_tps_for_selected_cases(selected_cases, out_path, title):
        if not selected_cases:
            return False
        x_labels = [r["case_label"] for r in selected_cases]
        x = list(range(len(x_labels)))
        width = 0.38
        plt.figure(figsize=(max(10, len(x_labels) * 2.2), 6.6))
        ax = plt.gca()

        lazy_tps = [float(r["vals1"][0]) for r in selected_cases]
        twopc_tps = [float(r["vals2"][0]) for r in selected_cases]
        plt.bar([i - width / 2 for i in x], lazy_tps, width=width, label=mode_names[mode1], color="#4C78A8")
        plt.bar([i + width / 2 for i in x], twopc_tps, width=width, label=mode_names[mode2], color="#F58518")

        bar_positions = []
        for i in x:
            bar_positions.extend([i - width / 2, i + width / 2])
        plt.xticks(bar_positions, [""] * len(bar_positions))

        ymax = max(lazy_tps + twopc_tps) if (lazy_tps or twopc_tps) else 0.0
        y_offset = max(ymax * 0.02, 60.0)
        # 预留更充足的顶部空间，避免柱顶模式文本与图框/标题拥挤
        plt.ylim(0, ymax + y_offset * 3.8)
        for i in x:
            lazy_x = i - width / 2
            twopc_x = i + width / 2
            plt.text(lazy_x, lazy_tps[i] + y_offset, mode_names[mode1], ha='center', va='bottom', fontsize=10)
            plt.text(twopc_x, twopc_tps[i] + y_offset, mode_names[mode2], ha='center', va='bottom', fontsize=10)

        for i, case_label in enumerate(x_labels):
            case_label_pretty = case_label.replace("|", "\n")
            ax.text(
                i, -0.10, case_label_pretty, transform=ax.get_xaxis_transform(),
                ha='center', va='top', rotation=0, fontsize=9
            )

        plt.xlabel("")
        plt.ylabel("TPS")
        plt.title(title)
        plt.tight_layout()
        plt.subplots_adjust(bottom=0.16, top=0.90)
        plt.savefig(out_path, dpi=200)
        plt.close()
        return True

    out_dir = "time_breakdown_charts"
    os.makedirs(out_dir, exist_ok=True)
    top4_time_out = os.path.join(out_dir, "time_breakdown_top4_gap_cases.png")
    top4_tps_out = os.path.join(out_dir, "tps_top4_gap_cases.png")
    if plot_breakdown_for_selected_cases(
        aligned_top4_cases,
        top4_time_out,
        "Time Breakdown Top4 Gap Cases"
    ):
        print(f"时间分解 Top4 差异图已生成: {top4_time_out}")
    else:
        print("未找到可用于 Top4 时间分解图的数据。")

    if plot_tps_for_selected_cases(
        aligned_top4_cases,
        top4_tps_out,
        "TPS Top4 Gap Cases"
    ):
        print(f"TPS Top4 差异图已生成: {top4_tps_out}")
    else:
        print("未找到可用于 Top4 TPS 图的数据。")

    # 3. (可选) 生成简单的对比图表
    # 这里我们按 Local Ratio 分组，画出不同 Tx Hot 下的 TPS 变化
    # 或者固定 Tx Hot，画不同 Local Ratio 的变化
    # 为了清晰，我们生成 5 张图，每张图对应一个 Local Ratio
    
    # output_img_dir = 'throughput_charts'
    # os.makedirs(output_img_dir, exist_ok=True)

    # for cr in local_ratios:
    #     lazy_data = []
    #     two_pc_data = []
    #     x_labels = []
        
    #     for hot in tx_hot_list:
    #         lazy_data.append(results.get(('smallbank_lazy', cr, hot), 0))
    #         two_pc_data.append(results.get(('smallbank_2pc', cr, hot), 0))
    #         x_labels.append(str(hot))
            
    #     plt.figure(figsize=(10, 6))
    #     plt.plot(x_labels, lazy_data, marker='o', label='Lazy')
    #     plt.plot(x_labels, two_pc_data, marker='s', label='2PC')
        
    #     plt.title(f'Throughput Comparison (Cross Ratio = {cr})')
    #     plt.xlabel('Tx Hot Rate (%)')
    #     plt.ylabel('Throughput (TPS)')
    #     plt.legend()
    #     plt.grid(True)
        
    #     img_path = os.path.join(output_img_dir, f'throughput_cr_{cr}.png')
    #     plt.savefig(img_path)
    #     plt.close()
    #     print(f"图表已生成: {img_path}")

if __name__ == '__main__':
    generate_report()
