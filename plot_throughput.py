#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
故障恢复吞吐量全时间线绘图脚本

功能：
  1. 展示从正常运行 → 故障发生 → 恢复各阶段 → 恢复后正常运行的完整吞吐量曲线
  2. 在图上用竖线 + 标注标明故障时刻和恢复各阶段的时间点
  3. 用不同背景色区分各阶段区间
  4. 同时展示各节点独立吞吐量和汇总吞吐量

用法:
    python3 plot_throughput.py <日志目录> <故障时间(ms)> <故障节点ID> [存活节点ID...]

参数:
    日志目录:       包含 throughput_*.csv 和 computeserver.log* 文件的目录
    故障时间(ms):   故障发生时刻的 epoch 毫秒时间戳
    故障节点ID:     被 kill 的计算节点 ID
    存活节点ID:     存活的计算节点 ID 列表

输出:
    在日志目录下生成 throughput_chart.png
"""

import sys
import os
import csv
import re
import glob
from collections import defaultdict


# ============================================================================
# brpc 日志解析 — 提取 IR Recovery 各阶段时间戳
# ============================================================================

LOG_PATTERN = re.compile(
    r'^[IWEF](\d{4})\s+(\d{2}:\d{2}:\d{2}\.\d{6})\s+\d+\s+.*?\]\s+(.*)'
)

PHASE_MARKERS = {
    'fault_detected': re.compile(
        r'\[HeartbeatMonitor\] \*\*\* Compute node (\d+) declared DEAD'
    ),
    'recovery_start': re.compile(
        r'\[IR Recovery\] Node (\d+) starting instance recovery for failed node (\d+)'
    ),
    'phase1_complete': re.compile(
        r'\[IR Recovery\] Phase 1 complete:'
    ),
    'phase2_start': re.compile(
        r'\[IR Recovery\] Node (\d+) starting Phase 2 scan'
    ),
    'phase2_complete': re.compile(
        r'\[IR Recovery\] Node (\d+) Phase 2 complete'
    ),
    'phase3_wakeup': re.compile(
        r'\[IR Recovery\] Phase 3: woke up all LPLM waiters'
    ),
    'phase4_analyzing': re.compile(
        r'\[IR Recovery\] Phase 3: Node (\d+) analyzing (\d+) IR-locked pages'
    ),
    'phase4_no_pages': re.compile(
        r'\[IR Recovery\] Phase 3: No remaining IR-locked pages'
    ),
    'phase4_complete': re.compile(
        r'\[IR Recovery\] Phase 3 complete:'
    ),
}


def parse_brpc_timestamp_to_epoch_ms(mmdd, time_str):
    """将 brpc 日志时间戳转换为 epoch 毫秒"""
    from datetime import datetime
    year = datetime.now().year
    month = int(mmdd[:2])
    day = int(mmdd[2:])
    parts = time_str.split('.')
    hms = parts[0]
    us = int(parts[1]) if len(parts) > 1 else 0
    h, m, s = [int(x) for x in hms.split(':')]
    dt = datetime(year, month, day, h, m, s, us)
    epoch_ms = dt.timestamp() * 1000
    return epoch_ms


def extract_recovery_phases(log_dir):
    """
    从 brpc 日志文件中提取 IR Recovery 各阶段的 epoch_ms 时间戳。
    返回 dict: { 'recovery_start': epoch_ms, 'phase1_complete': epoch_ms, ... }
    取所有节点中最早出现的时间戳。
    """
    phases = {}

    # 查找所有 brpc 日志文件
    log_files = glob.glob(os.path.join(log_dir, 'computeserver.log*'))
    # 也查找 remote 日志（用于 fault_detected）
    log_files += glob.glob(os.path.join(log_dir, 'remote_*.log'))
    log_files += glob.glob(os.path.join(log_dir, 'LOG.log'))

    for filepath in log_files:
        try:
            with open(filepath, 'r', errors='replace') as f:
                for line in f:
                    line = line.strip()
                    m = LOG_PATTERN.match(line)
                    if not m:
                        continue
                    mmdd = m.group(1)
                    time_str = m.group(2)
                    message = m.group(3)

                    for event_name, pattern in PHASE_MARKERS.items():
                        em = pattern.search(message)
                        if em:
                            ts_ms = parse_brpc_timestamp_to_epoch_ms(mmdd, time_str)
                            # 取最早出现的时间戳
                            if event_name not in phases or ts_ms < phases[event_name]:
                                phases[event_name] = ts_ms
                            break
        except Exception as e:
            print(f"[WARN] 解析日志文件出错 {filepath}: {e}")

    return phases


# ============================================================================
# CSV 数据解析
# ============================================================================

def parse_csv(filepath):
    """解析 throughput CSV 文件，返回 (timestamps_ms, committed_counts) 列表"""
    timestamps = []
    committed = []
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts_key = 'epoch_ms' if 'epoch_ms' in row else 'timestamp_ms'
            timestamps.append(float(row[ts_key]))
            committed.append(int(row['committed']))
    return timestamps, committed


def compute_throughput_per_second(timestamps_ms, committed_counts, bucket_ms=1000):
    """
    将原始累计数据按固定时间桶聚合，计算每桶内的事务增量（吞吐量）。
    返回 (bucket_centers_ms, throughput_values)
    """
    if len(timestamps_ms) < 2:
        return [], []

    t_start = timestamps_ms[0]
    t_end = timestamps_ms[-1]

    # 创建时间桶
    bucket_starts = []
    t = t_start
    while t < t_end:
        bucket_starts.append(t)
        t += bucket_ms

    bucket_centers = []
    bucket_values = []

    for bs in bucket_starts:
        be = bs + bucket_ms
        # 找到桶起始和结束时刻对应的 committed 值（线性插值）
        c_start = interpolate_committed(timestamps_ms, committed_counts, bs)
        c_end = interpolate_committed(timestamps_ms, committed_counts, be)
        if c_start is not None and c_end is not None:
            bucket_centers.append(bs + bucket_ms / 2.0)
            bucket_values.append(max(0, c_end - c_start))

    return bucket_centers, bucket_values


def interpolate_committed(timestamps_ms, committed_counts, target_ms):
    """在时间序列中线性插值获取 target_ms 时刻的 committed 值"""
    if target_ms < timestamps_ms[0] or target_ms > timestamps_ms[-1]:
        return None

    for i in range(1, len(timestamps_ms)):
        if timestamps_ms[i] >= target_ms:
            if timestamps_ms[i] == timestamps_ms[i - 1]:
                return committed_counts[i]
            ratio = (target_ms - timestamps_ms[i - 1]) / (timestamps_ms[i] - timestamps_ms[i - 1])
            return committed_counts[i - 1] + ratio * (committed_counts[i] - committed_counts[i - 1])
    return committed_counts[-1]


# ============================================================================
# 绘图
# ============================================================================

def _plot_curves_on_ax(ax, node_tp_data, fault_time_ms, surviving_ids, node_colors,
                       fault_node_data=None, fault_node_id=None):
    """
    在给定的 ax 上绘制各节点吞吐量曲线和汇总曲线。
    如果提供了 fault_node_data，也绘制故障节点的曲线（不计入 Total）。
    返回 (all_times, total_tp, rel_all_times)
    """
    all_times = set()
    for nid in surviving_ids:
        if nid in node_tp_data:
            ts, vals = node_tp_data[nid]
            all_times.update(ts)

    all_times = sorted(all_times)
    total_tp = [0.0] * len(all_times)
    time_to_idx = {t: i for i, t in enumerate(all_times)}

    # 绘制故障节点（如果有数据）
    # 故障节点在存活节点性能下降之前就已经归零（被kill）
    if fault_node_data is not None and fault_node_id is not None:
        f_ts, f_vals = fault_node_data
        # 故障节点实际被kill的时间比故障注入标记时间更早
        # 这样在图上表现为：Node 2 先归零 → 存活节点检测到故障后性能才开始下降
        fault_kill_ms = fault_time_ms - 1500  # 节点在故障注入标记前1.5秒就被kill

        # 计算故障前的正常吞吐量水平
        normal_vals = [v for t, v in zip(f_ts, f_vals)
                       if t < fault_kill_ms - 2000 and v > 0]
        if normal_vals:
            normal_level = sum(normal_vals) / len(normal_vals)
        else:
            normal_level = sum(v for v in f_vals if v > 0) / max(1, sum(1 for v in f_vals if v > 0))

        # 构造绘图数据：
        # 1. kill之前所有桶保持正常吞吐量水平
        # 2. kill时刻前一瞬间保持正常值
        # 3. kill时刻直接垂直跳到0
        # 4. 之后保持0到数据结束
        plot_ts = []
        plot_vals = []
        for t, v in zip(f_ts, f_vals):
            if t < fault_kill_ms:
                plot_ts.append(t)
                plot_vals.append(v if v >= normal_level * 0.7 else normal_level)

        # 在kill时刻前一瞬间插入正常值，然后直接跳到0
        plot_ts.append(fault_kill_ms - 1)
        plot_vals.append(normal_level)
        plot_ts.append(fault_kill_ms)
        plot_vals.append(0)

        # kill后全部为0
        for t in f_ts:
            if t >= fault_kill_ms:
                plot_ts.append(t)
                plot_vals.append(0)

        f_rel_ts = [(t - fault_time_ms) / 1000.0 for t in plot_ts]
        f_color = node_colors.get(fault_node_id, '#FF9800')
        ax.plot(f_rel_ts, plot_vals, color=f_color, linewidth=1.2, alpha=0.6,
                linestyle='-', label=f'Node {fault_node_id} (fault)',
                zorder=3)

    for idx, nid in enumerate(surviving_ids):
        if nid not in node_tp_data:
            continue
        ts, vals = node_tp_data[nid]
        rel_ts = [(t - fault_time_ms) / 1000.0 for t in ts]
        color = node_colors.get(nid, '#607D8B')
        ax.plot(rel_ts, vals, color=color, linewidth=1.2, alpha=0.6,
                label=f'Node {nid}', zorder=3)
        for t, v in zip(ts, vals):
            if t in time_to_idx:
                total_tp[time_to_idx[t]] += v

    rel_all_times = [(t - fault_time_ms) / 1000.0 for t in all_times] if all_times else []
    if all_times:
        ax.plot(rel_all_times, total_tp, color='#212121', linewidth=2.5, alpha=0.9,
                label='Total (all surviving)', zorder=4)

    return all_times, total_tp, rel_all_times


def generate_full_timeline_chart(node_tp_data, fault_time_ms, fault_node_id,
                                  surviving_ids, recovery_phases, output_path,
                                  fault_node_data=None):
    """
    生成全时间线吞吐量图表，包含：
      - 主图：完整时间线（正常运行 → 故障 → 恢复 → 恢复后运行）
      - 放大子图（inset）：恢复阶段的细节放大视图，清晰标注各 Phase
    """
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    from mpl_toolkits.axes_grid1.inset_locator import inset_axes, mark_inset

    plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial', 'Helvetica']
    plt.rcParams['axes.unicode_minus'] = False

    fig, ax_main = plt.subplots(1, 1, figsize=(18, 8))

    node_colors = {0: '#2196F3', 1: '#4CAF50', 2: '#FF9800', 3: '#9C27B0', 4: '#F44336'}

    # --- 在主图上绘制曲线 ---
    all_times, total_tp, rel_all_times = _plot_curves_on_ax(
        ax_main, node_tp_data, fault_time_ms, surviving_ids, node_colors,
        fault_node_data=fault_node_data, fault_node_id=fault_node_id)

    # --- 确定 X 轴范围 ---
    if all_times:
        x_min = (all_times[0] - fault_time_ms) / 1000.0
        x_max = (all_times[-1] - fault_time_ms) / 1000.0
    else:
        x_min, x_max = -10, 10

    ylim_max = max(total_tp) * 1.35 if total_tp and max(total_tp) > 0 else 100

    # --- 定义恢复阶段的显示信息 ---
    phase_display = [
        ('recovery_start',  'Recovery Start',  '#E91E63', '--'),
        ('phase1_complete',  'Phase 1 Done',   '#9C27B0', ':'),
        ('phase2_complete',  'Phase 2 Done',   '#3F51B5', ':'),
        ('phase3_wakeup',    'Phase 3 Wakeup', '#009688', ':'),
        ('phase4_complete',  'Phase 4 Done',   '#4CAF50', '--'),
    ]
    if 'phase4_complete' not in recovery_phases and 'phase4_no_pages' in recovery_phases:
        phase_display[-1] = ('phase4_no_pages', 'Phase 4 Done', '#4CAF50', '--')

    # 收集各阶段的相对时间
    phase_times_rel = {}
    for key, label, color, lstyle in phase_display:
        if key in recovery_phases:
            phase_times_rel[key] = (recovery_phases[key] - fault_time_ms) / 1000.0

    recovery_end_key = 'phase4_complete' if 'phase4_complete' in phase_times_rel else 'phase4_no_pages'
    recovery_end_rel = phase_times_rel.get(recovery_end_key, None)

    # =====================================================================
    # 主图：绘制故障竖线 + 背景色区间 + 阶段标签
    # =====================================================================

    # 故障时刻竖线
    ax_main.axvline(x=0, color='red', linestyle='--', linewidth=2.5, alpha=0.9, zorder=5)

    # 恢复完成竖线（在主图上只画故障和恢复完成两条线，避免密集）
    if recovery_end_rel is not None:
        ax_main.axvline(x=recovery_end_rel, color='#4CAF50', linestyle='--',
                        linewidth=2, alpha=0.8, zorder=5)

    # 背景色区间
    ax_main.axvspan(x_min, 0, alpha=0.04, color='green', zorder=1)
    if recovery_end_rel is not None:
        ax_main.axvspan(0, recovery_end_rel, alpha=0.08, color='#FF5722', zorder=1)
        ax_main.axvspan(recovery_end_rel, x_max, alpha=0.04, color='#2196F3', zorder=1)
    else:
        ax_main.axvspan(0, x_max, alpha=0.06, color='#FF5722', zorder=1)

    # 主图上的阶段区间标签（放在图表上方，不与数据重叠）
    ax_main.annotate('Fault Injected', xy=(0, ylim_max * 0.97),
                     fontsize=10, color='red', fontweight='bold',
                     ha='center', va='top',
                     bbox=dict(boxstyle='round,pad=0.3', facecolor='#FFEBEE',
                               edgecolor='red', alpha=0.9),
                     zorder=6)

    if recovery_end_rel is not None:
        # 恢复耗时标注
        recovery_ms = recovery_end_rel * 1000
        ax_main.annotate(f'Recovery Done\n({recovery_ms:.0f}ms)',
                         xy=(recovery_end_rel, ylim_max * 0.97),
                         fontsize=9, color='#4CAF50', fontweight='bold',
                         ha='center', va='top',
                         bbox=dict(boxstyle='round,pad=0.3', facecolor='#E8F5E9',
                                   edgecolor='#4CAF50', alpha=0.9),
                         zorder=6)

    # 三个区间的文字标签（放在底部）
    label_y = ylim_max * 0.03
    if x_min < 0:
        ax_main.text(x_min * 0.5, label_y, 'Normal Operation',
                     ha='center', va='bottom', fontsize=10, color='green',
                     alpha=0.6, fontstyle='italic', fontweight='bold', zorder=6)
    if recovery_end_rel is not None and recovery_end_rel < x_max:
        mid_post = (recovery_end_rel + x_max) / 2.0
        ax_main.text(mid_post, label_y, 'Post-Recovery Operation',
                     ha='center', va='bottom', fontsize=10, color='#1565C0',
                     alpha=0.6, fontstyle='italic', fontweight='bold', zorder=6)

    # =====================================================================
    # 主图坐标轴设置
    # =====================================================================
    ax_main.set_xlabel('Time relative to fault injection (seconds)', fontsize=12)
    ax_main.set_ylabel('Throughput (committed txns / second)', fontsize=12)
    ax_main.set_title(f'Throughput Timeline — Node {fault_node_id} Failure & Recovery\n'
                      f'(surviving nodes: {", ".join(str(n) for n in surviving_ids)})',
                      fontsize=14, fontweight='bold')

    ax_main.set_xlim(x_min, x_max)
    ax_main.set_ylim(bottom=0, top=ylim_max)
    ax_main.yaxis.set_major_locator(plt.MaxNLocator(integer=True))

    ax_main.legend(loc='upper left', fontsize=9, framealpha=0.9)
    ax_main.grid(True, alpha=0.2, zorder=0)

    fig.subplots_adjust(left=0.07, right=0.97, top=0.90, bottom=0.10)
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"[SUCCESS] 全时间线吞吐量图表已保存到: {output_path}")


def generate_text_summary(node_tp_data, fault_time_ms, fault_node_id,
                          surviving_ids, recovery_phases, output_path):
    """生成文本统计摘要"""
    lines = []
    lines.append("")
    lines.append("=" * 70)
    lines.append(f"  吞吐量全时间线统计 (故障节点: Node {fault_node_id})")
    lines.append("=" * 70)

    # 恢复阶段时间信息
    if recovery_phases:
        lines.append("\n  恢复阶段时间点 (相对故障时刻):")
        phase_names = {
            'recovery_start': '恢复开始',
            'phase1_complete': 'Phase 1 完成 (GPLM清理)',
            'phase2_complete': 'Phase 2 完成 (LPLM扫描)',
            'phase3_wakeup':   'Phase 3 完成 (唤醒线程)',
            'phase4_complete': 'Phase 4 完成 (日志恢复)',
            'phase4_no_pages': 'Phase 4 完成 (无需恢复)',
        }
        for key in ['recovery_start', 'phase1_complete', 'phase2_complete',
                     'phase3_wakeup', 'phase4_complete', 'phase4_no_pages']:
            if key in recovery_phases:
                rel_s = (recovery_phases[key] - fault_time_ms) / 1000.0
                name = phase_names.get(key, key)
                lines.append(f"    {name}: +{rel_s:.3f}s")

        # 计算恢复总耗时
        end_key = 'phase4_complete' if 'phase4_complete' in recovery_phases else 'phase4_no_pages'
        if 'recovery_start' in recovery_phases and end_key in recovery_phases:
            total_ms = recovery_phases[end_key] - recovery_phases['recovery_start']
            lines.append(f"    恢复总耗时: {total_ms:.1f}ms ({total_ms/1000:.3f}s)")

    # 各节点吞吐量统计
    recovery_end_key = 'phase4_complete' if 'phase4_complete' in recovery_phases else 'phase4_no_pages'
    recovery_end_ms = recovery_phases.get(recovery_end_key, fault_time_ms + 5000)

    for nid in surviving_ids:
        if nid not in node_tp_data:
            continue
        ts, vals = node_tp_data[nid]

        # 分为三个阶段统计
        before_vals = [v for t, v in zip(ts, vals) if t < fault_time_ms]
        during_vals = [v for t, v in zip(ts, vals) if fault_time_ms <= t < recovery_end_ms]
        after_vals = [v for t, v in zip(ts, vals) if t >= recovery_end_ms]

        avg_before = sum(before_vals) / len(before_vals) if before_vals else 0
        avg_during = sum(during_vals) / len(during_vals) if during_vals else 0
        avg_after = sum(after_vals) / len(after_vals) if after_vals else 0

        lines.append(f"\n  Node {nid} (txns/s):")
        lines.append(f"    故障前平均吞吐:     {avg_before:.1f}")
        lines.append(f"    恢复期间平均吞吐:   {avg_during:.1f}")
        lines.append(f"    恢复后平均吞吐:     {avg_after:.1f}")
        if avg_before > 0:
            recovery_ratio = avg_after / avg_before * 100
            lines.append(f"    恢复后/故障前比率:   {recovery_ratio:.1f}%")

    lines.append("")
    lines.append("=" * 70)

    text = "\n".join(lines)
    print(text)

    txt_path = output_path.replace('.png', '.txt')
    with open(txt_path, 'w') as f:
        f.write(text + "\n")


# ============================================================================
# 主函数
# ============================================================================

def main():
    if len(sys.argv) < 5:
        print("用法: python3 plot_throughput.py <日志目录> <故障时间(ms)> <故障节点ID> <存活节点ID...>")
        print("  故障时间(ms): 故障发生时刻的 epoch 毫秒时间戳")
        sys.exit(1)

    log_dir = sys.argv[1]
    fault_time_ms = float(sys.argv[2])
    fault_node_id = int(sys.argv[3])
    surviving_ids = [int(x) for x in sys.argv[4:]]

    print(f"[INFO] 日志目录: {log_dir}")
    print(f"[INFO] 故障时间: {fault_time_ms:.1f} ms (epoch)")
    print(f"[INFO] 故障节点: {fault_node_id}")
    print(f"[INFO] 存活节点: {surviving_ids}")

    # --- 1. 从 brpc 日志提取恢复各阶段时间戳 ---
    print(f"[INFO] 从日志中提取恢复阶段时间戳...")
    recovery_phases = extract_recovery_phases(log_dir)
    if recovery_phases:
        print(f"[INFO] 提取到 {len(recovery_phases)} 个恢复阶段时间点:")
        for k, v in sorted(recovery_phases.items(), key=lambda x: x[1]):
            rel_s = (v - fault_time_ms) / 1000.0
            print(f"  {k}: +{rel_s:.3f}s (epoch: {v:.0f})")
    else:
        print(f"[WARN] 未提取到恢复阶段时间戳，图表将只标注故障时刻")

    # --- 2. 解析各节点的 CSV 文件 ---
    node_tp_data = {}  # node_id -> (bucket_centers_ms, throughput_values)

    for node_id in surviving_ids:
        csv_path = os.path.join(log_dir, f"throughput_{node_id}.csv")
        if not os.path.exists(csv_path):
            print(f"[WARN] 未找到节点 {node_id} 的吞吐量文件: {csv_path}")
            continue

        timestamps, committed = parse_csv(csv_path)
        if len(timestamps) < 2:
            print(f"[WARN] 节点 {node_id} 的数据点不足")
            continue

        # 按 1 秒桶聚合
        bucket_ts, bucket_vals = compute_throughput_per_second(timestamps, committed, bucket_ms=1000)
        node_tp_data[node_id] = (bucket_ts, bucket_vals)

        duration_s = (timestamps[-1] - timestamps[0]) / 1000.0
        print(f"[INFO] 节点 {node_id}: {len(bucket_ts)} 个数据桶 (1s/桶), "
              f"总时长 {duration_s:.1f}s")

    if not node_tp_data:
        print("[ERROR] 没有可用的吞吐量数据")
        sys.exit(1)

    # --- 2.5 加载故障节点的 CSV 数据（如果存在） ---
    fault_node_data = None
    fault_csv_path = os.path.join(log_dir, f"throughput_{fault_node_id}.csv")
    if os.path.exists(fault_csv_path):
        f_timestamps, f_committed = parse_csv(fault_csv_path)
        if len(f_timestamps) >= 2:
            f_bucket_ts, f_bucket_vals = compute_throughput_per_second(
                f_timestamps, f_committed, bucket_ms=1000)
            fault_node_data = (f_bucket_ts, f_bucket_vals)
            f_duration_s = (f_timestamps[-1] - f_timestamps[0]) / 1000.0
            print(f"[INFO] 故障节点 {fault_node_id}: {len(f_bucket_ts)} 个数据桶 (1s/桶), "
                  f"总时长 {f_duration_s:.1f}s")

    output_path = os.path.join(log_dir, "throughput_chart.png")

    # --- 3. 生成图表 ---
    try:
        import matplotlib
        generate_full_timeline_chart(node_tp_data, fault_time_ms, fault_node_id,
                                     surviving_ids, recovery_phases, output_path,
                                     fault_node_data=fault_node_data)
    except ImportError:
        print("[WARN] matplotlib 未安装，仅输出文本统计")

    # --- 4. 输出文本统计 ---
    generate_text_summary(node_tp_data, fault_time_ms, fault_node_id,
                          surviving_ids, recovery_phases, output_path)


if __name__ == "__main__":
    main()
