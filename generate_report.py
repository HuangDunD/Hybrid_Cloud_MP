import os
# import matplotlib.pyplot as plt
import csv

# 结果目录路径
result_base_dir = '/usr/local/workspace/Hybrid_Cloud_MP/result/20260306144309/round_00'

# 要对比的模式和参数
modes = ['smallbank_lazy', 'smallbank_2pc']
cross_ratios = [0.1, 0.3, 0.5, 0.7, 0.9]
tx_hot_list = [10, 30, 50, 70, 90]

def parse_throughput(file_path):
    """从 summary_human.txt 中解析 throughput 参数"""
    try:
        with open(file_path, 'r') as f:
            for line in f:
                if line.startswith('throughput='):
                    return float(line.strip().split('=')[1])
    except FileNotFoundError:
        return 0.0
    return 0.0

def generate_report():
    results = {}  # key: (mode, cr, hot), value: throughput

    # 1. 遍历目录收集数据
    for mode in modes:
        for cr in cross_ratios:
            for hot in tx_hot_list:
                dir_name = f"cr_{cr}_txhot_{hot}"
                summary_path = os.path.join(result_base_dir, mode, dir_name, 'summary_human.txt')
                throughput = parse_throughput(summary_path)
                results[(mode, cr, hot)] = throughput

    # 2. 生成 CSV 报表 (适合导入 Excel 做进一步分析)
    csv_file = 'throughput_comparison.csv'
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Cross Ratio', 'Tx Hot', 'Lazy Throughput', '2PC Throughput', 'Improvement (%)'])
        
        print(f"{'Cross Ratio':<12} | {'Tx Hot':<8} | {'Lazy TPS':<15} | {'2PC TPS':<15} | {'Improvement':<15}")
        print("-" * 75)

        for cr in cross_ratios:
            for hot in tx_hot_list:
                lazy_tps = results.get(('smallbank_lazy', cr, hot), 0)
                two_pc_tps = results.get(('smallbank_2pc', cr, hot), 0)
                
                improvement = 0.0
                if two_pc_tps > 0:
                    improvement = ((lazy_tps - two_pc_tps) / two_pc_tps) * 100
                
                writer.writerow([cr, hot, lazy_tps, two_pc_tps, f"{improvement:.2f}"])
                print(f"{cr:<12} | {hot:<8} | {lazy_tps:<15.2f} | {two_pc_tps:<15.2f} | {improvement:<15.2f}%")
    
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
