import os
import glob
# import matplotlib.pyplot as plt
import csv
import re

all_results = glob.glob('/usr/local/workspace/Hybrid_Cloud_MP/result/*/round_00')

# 要对比的模式和参数
modes = ['ycsb_lazy', 'ycsb_2pc']
# modes = ['smallbank_lazy' , 'smallbank_2pc']
local_ratios = [0.2, 0.5, 0.8]
tx_hot_list = [20, 50, 80]
wr_ratios = [0.3, 0.6, 0.9]


def parse_case_tuple_from_dir_name(dir_name):
    patterns = [
        r"^lr([0-9.]+)_txhot_([0-9]+)_wr_([0-9.]+)$",
        r"^local_txn_([0-9.]+)_txhot_([0-9]+)_wr_([0-9.]+)$",
        r"^cr_([0-9.]+)_txhot_([0-9]+)_wr_([0-9.]+)$",
    ]
    for pattern in patterns:
        match = re.match(pattern, dir_name)
        if match:
            return float(match.group(1)), int(match.group(2)), float(match.group(3))
    return None


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
    return tps, wait_log_time, wait_log_flush_tx_over_time, log_count, wait_log_flush_count, prepare_log_count, backup_log_count, ownership_transfer_count, ownership_transfer_time_avg_ms, ownership_transfer_time_total, wait_prepare_log_time, wait_backup_log_time, wait_commit_log_time, tx_wait_abort_log_time, tx_write_prepare_log_time, tx_write_backup_log_time, tx_write_commit_log_time, wait_log_flush_push_page_time, tx_exe_time, tx_commit_time, tx_abort_time, twopc_remote_fetch_time, fetch_storage_page_time, twopc_remote_fetch_count, single_txn_count, distribute_txn_count, tx_fetch_exe_time, tx_commit_fetch_page_time

def generate_report():
    result_base_dir = select_result_base_dir(all_results, modes)
    print(f"Using result directory: {result_base_dir}")

    results = {}  # key: (mode, cr, hot, wr), value: (throughput, wait_log_time, log_count, wait_log_flush_count, prepare_log_count, backup_log_count, ownership_transfer_count, ownership_transfer_time_avg_ms)
    mode_cases = {mode: discover_cases(result_base_dir, mode) for mode in modes}

    common_cases = set()
    if len(modes) >= 2 and mode_cases[modes[0]] and mode_cases[modes[1]]:
        common_cases = mode_cases[modes[0]] & mode_cases[modes[1]]
    all_cases = set().union(*mode_cases.values()) if mode_cases else set()
    target_cases = sorted(common_cases if common_cases else all_cases, key=lambda x: (x[0], x[1], x[2]))
    if not target_cases:
        target_cases = [
            (cr, hot, wr)
            for cr in local_ratios
            for hot in tx_hot_list
            for wr in wr_ratios
        ]

    for mode in modes:
        for cr, hot, wr in target_cases:
            dir_candidates = [
                f"lr{cr}_txhot_{hot}_wr_{wr}",
                f"local_txn_{cr}_txhot_{hot}_wr_{wr}",
                f"cr_{cr}_txhot_{hot}_wr_{wr}",
            ]
            summary_path = ''
            for dir_name in dir_candidates:
                candidate_path = os.path.join(result_base_dir, mode, dir_name, 'summary_human.txt')
                if os.path.exists(candidate_path):
                    summary_path = candidate_path
                    break
            if not summary_path:
                summary_path = os.path.join(result_base_dir, mode, dir_candidates[0], 'summary_human.txt')
            tps, wait_log_time, wait_log_flush_tx_over_time, log_count, wait_log_flush_count, prepare_log_count, backup_log_count, ownership_transfer_count, ownership_transfer_time_avg_ms, ownership_transfer_time_total, wait_prepare_log_time, wait_backup_log_time, wait_commit_log_time, tx_wait_abort_log_time, tx_write_prepare_log_time, tx_write_backup_log_time, tx_write_commit_log_time, wait_log_flush_push_page_time, tx_exe_time, tx_commit_time, tx_abort_time, twopc_remote_fetch_time, fetch_storage_page_time, twopc_remote_fetch_count, single_txn_count, distribute_txn_count, tx_fetch_exe_time, tx_commit_fetch_page_time = parse_throughput(summary_path)

            if '2pc' in mode.lower() or '2PC' in mode:
                ownership_transfer_count = twopc_remote_fetch_count
                ownership_transfer_time_total = twopc_remote_fetch_time
                if ownership_transfer_count > 0:
                    ownership_transfer_time_avg_ms = (ownership_transfer_time_total / ownership_transfer_count) * 1000.0
                else:
                    ownership_transfer_time_avg_ms = 0.0

            results[(mode, cr, hot, wr)] = (tps, wait_log_time, wait_log_flush_tx_over_time, log_count, wait_log_flush_count, prepare_log_count, backup_log_count, ownership_transfer_count, ownership_transfer_time_avg_ms, ownership_transfer_time_total, wait_prepare_log_time, wait_backup_log_time, wait_commit_log_time, tx_wait_abort_log_time, tx_write_prepare_log_time, tx_write_backup_log_time, tx_write_commit_log_time, wait_log_flush_push_page_time, tx_exe_time, tx_commit_time, tx_abort_time, twopc_remote_fetch_time, fetch_storage_page_time, single_txn_count, distribute_txn_count, tx_fetch_exe_time, tx_commit_fetch_page_time)

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

    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Mode', 'Local Ratio', 'Tx Hot', 'WR Ratio', 'TPS', comparison_label, 'Tx exe Time', 'Tx fetch exe Time', 'Tx commit Time', 'Tx fetch commit Time', 'Tx abort Time', 'Tx commit phase time', 'TxWaitAbortLogTime', 'Tx backup phase time', 'Tx prepare phase time', 'DistTxn (%)', 'wait push page time', 'OwnershipTrans Count', 'OwnershipTrans Time', 'OwnershipTrans Avg Time(ms)'])
        
        print(f"{'Case':<36} | {'Mode':<8} | {'TPS':<10} | {comparison_label:<18} | {'Tx exe':<10} | {'Tx fet exe':<10} | {'Tx commit':<10} | {'Tx fet com':<10} | {'Tx abort':<10} | {'TxComPh':<10} | {'WAbortLog':<10} | {'TxBackPh':<10} | {'TxPrepPh':<10} | {'DistTxn%':<10} | {'WPush':<10} | {'OwnTransCt':<10} | {'OwnTransTm':<10} | {'OwnTransAvg':<10}")
        print("-" * 235)

        for cr, hot, wr in target_cases:
            tps1, wait_log_tot1, wait_tx_over1, logs1, wait_ct1, prep1, back1, owner_trans1, owner_time_avg1, owner_time_tot1, wprep1, wback1, wcom1, wabortlog1, txwprep1, txwback1, txwcom1, wpush1, txexe1, txcom1, txabt1, twopc1, stor1, single_txn1, dist_txn1, txfetchexe1, txcommitfet1 = results.get((mode1, cr, hot, wr), (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
            tps2, wait_log_tot2, wait_tx_over2, logs2, wait_ct2, prep2, back2, owner_trans2, owner_time_avg2, owner_time_tot2, wprep2, wback2, wcom2, wabortlog2, txwprep2, txwback2, txwcom2, wpush2, txexe2, txcom2, txabt2, twopc2, stor2, single_txn2, dist_txn2, txfetchexe2, txcommitfet2 = results.get((mode2, cr, hot, wr), (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))

            comparison_value = 0.0
            if tps2 > 0:
                comparison_value = (tps1 - tps2) / tps2 * 100

            mode_rows = [
                (mode1, tps1, wait_log_tot1, wait_tx_over1, logs1, wait_ct1, prep1, back1, owner_trans1, owner_time_avg1, owner_time_tot1, wprep1, wback1, wcom1, wabortlog1, txwprep1, txwback1, txwcom1, wpush1, txexe1, txfetchexe1, txcom1, txcommitfet1, txabt1, stor1, single_txn1, dist_txn1),
                (mode2, tps2, wait_log_tot2, wait_tx_over2, logs2, wait_ct2, prep2, back2, owner_trans2, owner_time_avg2, owner_time_tot2, wprep2, wback2, wcom2, wabortlog2, txwprep2, txwback2, txwcom2, wpush2, txexe2, txfetchexe2, txcom2, txcommitfet2, txabt2, stor2, single_txn2, dist_txn2),
            ]

            case_label = f"CR={cr} | Hot={hot} | WR={wr}"
            for row_idx, (mode, tps, wait_log_tot, wait_tx_over, log_count, wait_log_flush_count, prepare_log_count, backup_log_count, ownership_transfer_count, ownership_transfer_time_avg_ms, ownership_transfer_time_total, wprep, wback, wcom, wabortlog, txwprep, txwback, txwcom, wpush, txexe, txfetchexe, txcom, txcommitfet, txabt, stor, single_txn, dist_txn) in enumerate(mode_rows):
                dist_ratio = 0.0
                if single_txn + dist_txn > 0:
                    dist_ratio = dist_txn / (single_txn + dist_txn) * 100.0

                writer.writerow([
                    mode_names[mode], cr, hot, wr,
                    int(tps),
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
