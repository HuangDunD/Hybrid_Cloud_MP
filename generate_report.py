import os
# import matplotlib.pyplot as plt
import csv

# 结果目录路径
result_base_dir = '/usr/local/workspace/Hybrid_Cloud_MP/result/20260328134114/round_00'

# 要对比的模式和参数
modes = ['ycsb_lazy', 'ycsb_2pc']
# modes = ['smallbank_lazy' , 'smallbank_2pc']
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

    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Cross Ratio', 'Tx Hot (%)', 'WR Ratio', f'{name1} TPS', f'{name2} TPS', 'Comparation (%)', f'{name1} WaitLog (s)', f'{name2} WaitLog (s)', f'{name1} Logs (count)', f'{name2} Logs (count)', f'{name1} WaitLogFlush (count)', f'{name2} WaitLogFlush (count)', f'{name2} Prepare (count)', f'{name2} Backup (count)', f'{name1} OwnerShip Trans (count)', f'{name1} OwnerTran TimeAvg (ms)'])
        
        print(f"{'Cross Ratio':<12} | {'Tx Hot':<8} | {'WR Ratio':<8} | {f'{name1} TPS':<15} | {f'{name2} TPS':<15} | {'Comparation':<15} | {f'{name1} Wait':<15} | {f'{name2} Wait':<15} | {f'{name1} Logs':<15} | {f'{name2} Logs':<15} | {f'{name1} WaitCt':<15} | {f'{name2} WaitCt':<15} | {f'{name2} Prep':<15} | {f'{name2} Back':<15} | {f'{name1} Trans':<15} | {f'{name1} AvgTime':<15}")
        print("-" * 270)

        for cr in cross_ratios:
            for hot in tx_hot_list:
                for wr in wr_ratios:
                    tps1, wait1, logs1, wait_ct1, prep1, back1, owner_trans1, owner_time1 = results.get((mode1, cr, hot, wr), (0, 0, 0, 0, 0, 0, 0, 0))
                    tps2, wait2, logs2, wait_ct2, prep2, back2, owner_trans2, owner_time2 = results.get((mode2, cr, hot, wr), (0, 0, 0, 0, 0, 0, 0, 0))
                    
                    improvement = 0.0
                    if tps2 > 0:
                        improvement = ((tps1 - tps2) / tps2) * 100
                    
                    writer.writerow([cr, hot, wr, int(tps1), int(tps2), f"{improvement:.2f}", f"{wait1:.2f}", f"{wait2:.2f}", int(logs1), int(logs2), int(wait_ct1), int(wait_ct2), int(prep2), int(back2), int(owner_trans1), f"{owner_time1:.2f}"])
                    print(f"{cr:<12} | {hot:<8} | {wr:<8} | {int(tps1):<15} | {int(tps2):<15} | {improvement:<15.2f}% | {wait1:<15.2f} | {wait2:<15.2f} | {int(logs1):<15} | {int(logs2):<15} | {int(wait_ct1):<15} | {int(wait_ct2):<15} | {int(prep2):<15} | {int(back2):<15} | {int(owner_trans1):<15} | {owner_time1:<15.2f}")
    
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
