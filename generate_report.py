import os
# import matplotlib.pyplot as plt
import csv

# 结果目录路径
result_base_dir = '/usr/local/workspace/Hybrid_Cloud_MP/result/20260328152035/round_00'

# 要对比的模式和参数
# modes = ['ycsb_lazy', 'ycsb_2pc']
modes = ['smallbank_lazy' , 'smallbank_2pc']
cross_ratios = [0.2, 0.5, 0.8]
tx_hot_list = [20, 50, 80]
wr_ratios = [0.3, 0.6, 0.9]

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
    try:
        with open(file_path, 'r') as f:
            for line in f:
                if line.startswith('throughput='):
                    tps = float(line.strip().split('=')[1])
                elif line.startswith('wait_log_flush_time='):
                    wait_log_time = float(line.strip().split('=')[1]) / 16.0
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
    except FileNotFoundError:
        return 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
    return tps, wait_log_time, log_count, wait_log_flush_count, prepare_log_count, backup_log_count, ownership_transfer_count, ownership_transfer_time_avg_ms

def generate_report():
    results = {}  # key: (mode, cr, hot, wr), value: (throughput, wait_log_time, log_count, wait_log_flush_count, prepare_log_count, backup_log_count, ownership_transfer_count, ownership_transfer_time_avg_ms)

    # 1. 遍历目录收集数据
    for mode in modes:
        for cr in cross_ratios:
            for hot in tx_hot_list:
                for wr in wr_ratios:
                    dir_name = f"cr_{cr}_txhot_{hot}_wr_{wr}"
                    summary_path = os.path.join(result_base_dir, mode, dir_name, 'summary_human.txt')
                    tps, wait_log_time, log_count, wait_log_flush_count, prepare_log_count, backup_log_count, ownership_transfer_count, ownership_transfer_time_avg_ms = parse_throughput(summary_path)
                    results[(mode, cr, hot, wr)] = (tps, wait_log_time, log_count, wait_log_flush_count, prepare_log_count, backup_log_count, ownership_transfer_count, ownership_transfer_time_avg_ms)

    # 2. 生成 CSV 报表 (适合导入 Excel 做进一步分析)
    csv_file = 'throughput_comparison.csv'
    if len(modes) < 2:
        print("需要至少两个模式进行对比")
        return

    mode1 = modes[0]
    mode2 = modes[1]

    # 简单的字符串处理，去掉 ycsb_ 前缀
    name1 = mode1.replace('ycsb_', '')
    name2 = mode2.replace('ycsb_', '')
    mode_names = {
        mode1: name1,
        mode2: name2,
    }
    comparison_label = f'{name1} vs {name2} (%)'

    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Mode', 'Cross Ratio', 'Tx Hot (%)', 'WR Ratio', 'TPS', comparison_label, 'WaitLog (s)', 'Logs (count)', 'WaitLogFlush (count)', 'Prepare (count)', 'Backup (count)', 'OwnerShip Trans (count)', 'OwnerTran TimeAvg (ms)'])
        
        print(f"{'Case':<36} | {'Mode':<8} | {'TPS':<10} | {comparison_label:<18} | {'Wait':<10} | {'Logs':<10} | {'WaitCt':<10} | {'Prep':<10} | {'Back':<10} | {'Trans':<10} | {'AvgTime':<10}")
        print("-" * 170)

        for cr in cross_ratios:
            for hot in tx_hot_list:
                for wr in wr_ratios:
                    tps1, wait1, logs1, wait_ct1, prep1, back1, owner_trans1, owner_time1 = results.get((mode1, cr, hot, wr), (0, 0, 0, 0, 0, 0, 0, 0))
                    tps2, wait2, logs2, wait_ct2, prep2, back2, owner_trans2, owner_time2 = results.get((mode2, cr, hot, wr), (0, 0, 0, 0, 0, 0, 0, 0))
                    comparison_value = 0.0
                    if tps2 > 0:
                        comparison_value = ((tps1 - tps2) / tps2) * 100
                    mode_rows = [
                        (mode1, tps1, wait1, logs1, wait_ct1, prep1, back1, owner_trans1, owner_time1),
                        (mode2, tps2, wait2, logs2, wait_ct2, prep2, back2, owner_trans2, owner_time2),
                    ]

                    case_label = f"CR={cr} | Hot={hot} | WR={wr}"

                    for row_idx, (mode, tps, wait_log_time, log_count, wait_log_flush_count, prepare_log_count, backup_log_count, ownership_transfer_count, ownership_transfer_time_avg_ms) in enumerate(mode_rows):
                        display_cr = cr if row_idx == 0 else ''
                        display_hot = hot if row_idx == 0 else ''
                        display_wr = wr if row_idx == 0 else ''
                        writer.writerow([
                            mode_names[mode],
                            display_cr,
                            display_hot,
                            display_wr,
                            int(tps),
                            f"{comparison_value:.2f}",
                            f"{wait_log_time:.2f}",
                            int(log_count),
                            int(wait_log_flush_count),
                            int(prepare_log_count),
                            int(backup_log_count),
                            int(ownership_transfer_count),
                            f"{ownership_transfer_time_avg_ms:.2f}"
                        ])
                        display_case = case_label if row_idx == 0 else ''
                        print(f"{display_case:<36} | {mode_names[mode]:<8} | {int(tps):<10} | {comparison_value:<18.2f}% | {wait_log_time:<10.2f} | {int(log_count):<10} | {int(wait_log_flush_count):<10} | {int(prepare_log_count):<10} | {int(backup_log_count):<10} | {int(ownership_transfer_count):<10} | {ownership_transfer_time_avg_ms:<10.2f}")

                    writer.writerow([])
                    print("-" * 170)
    
    print(f"\nCSV 报表已生成: {csv_file}")

    # 3. (可选) 生成简单的对比图表
    # 这里我们按 Cross Ratio 分组，画出不同 Tx Hot 下的 TPS 变化
    # 或者固定 Tx Hot，画不同 Cross Ratio 的变化
    # 为了清晰，我们生成 5 张图，每张图对应一个 Cross Ratio
    
    # output_img_dir = 'throughput_charts'
    # os.makedirs(output_img_dir, exist_ok=True)

    # for cr in cross_ratios:
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
