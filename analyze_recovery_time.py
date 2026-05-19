#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
故障恢复阶段用时分析脚本
从计算节点的 brpc 日志文件中提取 IR Recovery 各阶段的时间戳，计算各阶段用时。

brpc 日志格式: I0517 14:58:12.042396 PID file:line] message
                ^                     ^
                MMDD HH:MM:SS.us      PID

IR Recovery 阶段划分（代码中的实际执行顺序）:
  Phase 1: GPLM 清理 + IR Lock + 页面重分布
  Phase 2: LPLM 扫描（RunIRRecoveryScan）
  Phase 3: 唤醒 LPLM waiters
  Phase 4: 日志分析恢复（RunIRRecoveryPhase3）

使用方法:
    python3 analyze_recovery_time.py <compute_log_dir> [node_id1 node_id2 ...]
    python3 analyze_recovery_time.py build/compute_server/ 0 1 2
"""

import sys
import os
import re
import glob
from datetime import datetime, timedelta
from collections import defaultdict

# ============================================================================
# 颜色输出
# ============================================================================
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    CYAN = '\033[96m'
    BOLD = '\033[1m'
    RESET = '\033[0m'

def log_info(msg):
    print(f"{Colors.GREEN}[INFO]{Colors.RESET} {msg}")

def log_warn(msg):
    print(f"{Colors.YELLOW}[WARN]{Colors.RESET} {msg}")

def log_success(msg):
    print(f"{Colors.CYAN}[SUCCESS]{Colors.RESET} {msg}")

# ============================================================================
# 日志时间戳解析
# ============================================================================

# brpc 日志行格式: [IWEF]MMDD HH:MM:SS.ffffff PID file:line] message
# 注意：使用非贪婪匹配 .*?\] 来匹配到 file:line] 而不是贪婪匹配到最后一个 ]
LOG_PATTERN = re.compile(
    r'^[IWEF](\d{4})\s+(\d{2}:\d{2}:\d{2}\.\d{6})\s+\d+\s+.*?\]\s+(.*)'
)

def parse_log_timestamp(mmdd, time_str):
    """解析 brpc 日志时间戳，返回 datetime 对象"""
    # 使用当前年份
    year = datetime.now().year
    month = int(mmdd[:2])
    day = int(mmdd[2:])
    parts = time_str.split('.')
    hms = parts[0]
    us = int(parts[1]) if len(parts) > 1 else 0
    h, m, s = [int(x) for x in hms.split(':')]
    return datetime(year, month, day, h, m, s, us)

def time_diff_ms(t1, t2):
    """计算两个时间戳之间的毫秒差"""
    delta = t2 - t1
    return delta.total_seconds() * 1000

# ============================================================================
# IR Recovery 日志关键标记
# ============================================================================

# 各阶段的日志标记（按执行顺序）
MARKERS = {
    # 恢复开始
    'recovery_start': re.compile(
        r'\[IR Recovery\] Node (\d+) starting instance recovery for failed node (\d+)'
    ),
    # Phase 1 完成
    'phase1_complete': re.compile(
        r'\[IR Recovery\] Phase 1 complete:'
    ),
    # Phase 2 开始（LPLM 扫描）
    'phase2_start': re.compile(
        r'\[IR Recovery\] Node (\d+) starting Phase 2 scan'
    ),
    # Phase 2 扫描完成（本节点）
    'phase2_scan_done': re.compile(
        r'\[IR Recovery\] Node (\d+) reported (\d+) pages'
    ),
    # Phase 2 完成
    'phase2_complete': re.compile(
        r'\[IR Recovery\] Node (\d+) Phase 2 complete'
    ),
    # Phase 3 唤醒线程
    'phase3_wakeup': re.compile(
        r'\[IR Recovery\] Phase 3: woke up all LPLM waiters'
    ),
    # Phase 4 开始等待 barrier
    'phase4_barrier_wait': re.compile(
        r'\[IR Recovery\] Node (\d+) waiting for Phase 2 barrier'
    ),
    # Phase 4 开始分析
    'phase4_analyzing': re.compile(
        r'\[IR Recovery\] Phase 3: Node (\d+) analyzing (\d+) IR-locked pages'
    ),
    # Phase 4 无需分析
    'phase4_no_pages': re.compile(
        r'\[IR Recovery\] Phase 3: No remaining IR-locked pages'
    ),
    # Phase 4 完成
    'phase4_complete': re.compile(
        r'\[IR Recovery\] Phase 3 complete: (\d+) pages released directly, (\d+) pages recovered'
    ),
    # 故障检测（HeartbeatMonitor）
    'fault_detected': re.compile(
        r'\[HeartbeatMonitor\] \*\*\* Compute node (\d+) declared DEAD'
    ),
    # 故障通知接收
    'fault_notified': re.compile(
        r'\[ComputeNode (\d+)\] Node (\d+) failure detected'
    ),
}

# ============================================================================
# 解析单个日志文件
# ============================================================================

def parse_log_file(filepath):
    """解析日志文件，提取所有 IR Recovery 相关事件"""
    events = []

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

                # 检查是否匹配任何 IR Recovery 标记
                for event_name, pattern in MARKERS.items():
                    em = pattern.search(message)
                    if em:
                        ts = parse_log_timestamp(mmdd, time_str)
                        events.append({
                            'timestamp': ts,
                            'event': event_name,
                            'message': message,
                            'groups': em.groups(),
                        })
                        break  # 一行只匹配一个事件

    except FileNotFoundError:
        log_warn(f"日志文件不存在: {filepath}")
    except Exception as e:
        log_warn(f"解析日志文件出错 {filepath}: {e}")

    return events

# ============================================================================
# 分析恢复用时
# ============================================================================

def analyze_recovery_timing(events):
    """分析 IR Recovery 各阶段用时"""
    if not events:
        return None

    # 按时间排序
    events.sort(key=lambda e: e['timestamp'])

    result = {}

    # 提取关键时间点
    for e in events:
        name = e['event']
        ts = e['timestamp']

        if name == 'recovery_start':
            result['recovery_start'] = ts
            result['failed_node'] = e['groups'][1] if len(e['groups']) > 1 else '?'
            result['node_id'] = e['groups'][0] if len(e['groups']) > 0 else '?'
        elif name == 'fault_notified':
            result['fault_notified'] = ts
            result['node_id'] = e['groups'][0] if len(e['groups']) > 0 else '?'
        elif name == 'phase1_complete':
            result['phase1_complete'] = ts
        elif name == 'phase2_start':
            result['phase2_start'] = ts
        elif name == 'phase2_scan_done':
            result['phase2_scan_done'] = ts
            result['reported_pages'] = e['groups'][1] if len(e['groups']) > 1 else '?'
        elif name == 'phase2_complete':
            result['phase2_complete'] = ts
        elif name == 'phase3_wakeup':
            result['phase3_wakeup'] = ts
        elif name == 'phase4_barrier_wait':
            result['phase4_barrier_wait'] = ts
        elif name == 'phase4_analyzing':
            result['phase4_analyzing'] = ts
            result['ir_locked_pages'] = e['groups'][1] if len(e['groups']) > 1 else '?'
        elif name == 'phase4_no_pages':
            result['phase4_no_pages'] = ts
        elif name == 'phase4_complete':
            result['phase4_complete'] = ts
            result['pages_released'] = e['groups'][0] if len(e['groups']) > 0 else '?'
            result['pages_replayed'] = e['groups'][1] if len(e['groups']) > 1 else '?'
        elif name == 'fault_detected':
            result['fault_detected'] = ts

    return result

def compute_phase_durations(timing):
    """根据时间点计算各阶段用时"""
    if not timing:
        return {}

    durations = {}

    # 恢复总时间
    start = timing.get('recovery_start') or timing.get('fault_notified')
    end = timing.get('phase4_complete') or timing.get('phase4_no_pages') or timing.get('phase3_wakeup')
    if start and end:
        durations['total_recovery'] = time_diff_ms(start, end)

    # Phase 1: GPLM 清理（recovery_start → phase1_complete）
    if 'recovery_start' in timing and 'phase1_complete' in timing:
        durations['phase1_gplm_cleanup'] = time_diff_ms(timing['recovery_start'], timing['phase1_complete'])

    # Phase 2: LPLM 扫描（phase2_start → phase2_complete）
    # 注意：Phase 2 在代码中是 phase1_complete 之后立即调用的
    p2_start = timing.get('phase2_start') or timing.get('phase1_complete')
    if p2_start and 'phase2_complete' in timing:
        durations['phase2_lplm_scan'] = time_diff_ms(p2_start, timing['phase2_complete'])

    # Phase 3: 唤醒 LPLM waiters（phase2_complete → phase3_wakeup）
    if 'phase2_complete' in timing and 'phase3_wakeup' in timing:
        durations['phase3_wakeup'] = time_diff_ms(timing['phase2_complete'], timing['phase3_wakeup'])

    # Phase 4 Barrier 等待（phase4_barrier_wait → phase4_analyzing 或 phase4_no_pages）
    if 'phase4_barrier_wait' in timing:
        barrier_end = timing.get('phase4_analyzing') or timing.get('phase4_no_pages')
        if barrier_end:
            durations['phase4_barrier_wait'] = time_diff_ms(timing['phase4_barrier_wait'], barrier_end)

    # Phase 4 日志分析（phase4_analyzing → phase4_complete）
    if 'phase4_analyzing' in timing and 'phase4_complete' in timing:
        durations['phase4_log_analysis'] = time_diff_ms(timing['phase4_analyzing'], timing['phase4_complete'])

    # Phase 4 总时间（phase3_wakeup → phase4_complete 或 phase4_no_pages）
    p4_start = timing.get('phase3_wakeup') or timing.get('phase4_barrier_wait')
    p4_end = timing.get('phase4_complete') or timing.get('phase4_no_pages')
    if p4_start and p4_end:
        durations['phase4_total'] = time_diff_ms(p4_start, p4_end)

    return durations

# ============================================================================
# 格式化输出
# ============================================================================

def format_duration(ms):
    """格式化毫秒为可读字符串"""
    if ms < 1:
        return f"{ms*1000:.0f} μs"
    elif ms < 1000:
        return f"{ms:.2f} ms"
    else:
        return f"{ms/1000:.3f} s"

def print_node_timing(node_id, timing, durations):
    """打印单个节点的恢复用时"""
    print(f"\n  {'─' * 56}")
    print(f"  计算节点 {node_id} 故障恢复用时分析")
    print(f"  {'─' * 56}")

    if not timing:
        print(f"  (未找到 IR Recovery 日志)")
        return

    # 打印元信息
    if 'failed_node' in timing:
        print(f"  故障节点: {timing['failed_node']}")
    if 'reported_pages' in timing:
        print(f"  汇报页面数: {timing['reported_pages']}")
    if 'ir_locked_pages' in timing:
        print(f"  IR 锁页面数: {timing['ir_locked_pages']}")
    if 'pages_released' in timing:
        print(f"  直接释放页面: {timing['pages_released']}")
    if 'pages_replayed' in timing:
        print(f"  日志回放页面: {timing['pages_replayed']}")

    print()

    # 打印各阶段用时
    phases = [
        ('phase1_gplm_cleanup', 'Phase 1 - GPLM 清理 + IR Lock + 页面重分布'),
        ('phase2_lplm_scan',    'Phase 2 - LPLM 扫描 + 状态汇报'),
        ('phase3_wakeup',       'Phase 3 - 唤醒 LPLM 等待线程'),
        ('phase4_barrier_wait', 'Phase 4 - Barrier 等待（等待所有节点扫描完成）'),
        ('phase4_log_analysis', 'Phase 4 - 日志分析 + 页面恢复'),
        ('phase4_total',        'Phase 4 - 总计'),
        ('total_recovery',      '恢复总耗时'),
    ]

    max_label_len = max(len(label) for _, label in phases)

    for key, label in phases:
        if key in durations:
            val = format_duration(durations[key])
            if key == 'total_recovery':
                print(f"  {'━' * 56}")
                print(f"  {Colors.BOLD}{label:<{max_label_len}}  {val}{Colors.RESET}")
            else:
                print(f"  {label:<{max_label_len}}  {val}")
        # total_recovery 不存在时也不打印

    # 打印时间线
    print(f"\n  时间线:")
    timeline_events = [
        ('fault_notified',      '收到故障通知'),
        ('recovery_start',      '开始恢复'),
        ('phase1_complete',     'Phase 1 完成'),
        ('phase2_start',        'Phase 2 开始'),
        ('phase2_scan_done',    'Phase 2 扫描完成'),
        ('phase2_complete',     'Phase 2 完成'),
        ('phase3_wakeup',       'Phase 3 唤醒完成'),
        ('phase4_barrier_wait', 'Phase 4 等待 Barrier'),
        ('phase4_analyzing',    'Phase 4 开始分析'),
        ('phase4_no_pages',     'Phase 4 无需分析'),
        ('phase4_complete',     'Phase 4 完成'),
    ]

    first_ts = None
    for key, label in timeline_events:
        if key in timing:
            ts = timing[key]
            if first_ts is None:
                first_ts = ts
            offset = time_diff_ms(first_ts, ts)
            ts_str = ts.strftime('%H:%M:%S.%f')[:-3]
            print(f"    [{ts_str}] (+{format_duration(offset):>10}) {label}")

# ============================================================================
# 主函数
# ============================================================================

def main():
    if len(sys.argv) < 2:
        print("用法: python3 analyze_recovery_time.py <log_dir> [node_id1 node_id2 ...]")
        print("  log_dir: 包含计算节点日志文件的目录")
        print("  node_ids: 要分析的存活节点 ID（可选，默认分析所有找到的日志）")
        print()
        print("示例: python3 analyze_recovery_time.py recovery_test_logs/ 0 1")
        sys.exit(1)

    log_dir = sys.argv[1]
    specified_nodes = [int(x) for x in sys.argv[2:]] if len(sys.argv) > 2 else None

    if not os.path.isdir(log_dir):
        print(f"错误: 目录不存在: {log_dir}")
        sys.exit(1)

    print()
    print("=" * 60)
    print("  故障恢复阶段用时分析")
    print("=" * 60)

    # 查找计算节点日志文件
    # 日志文件名格式: computeserver.log<PID> 或 compute<N>_<TIMESTAMP>.log
    log_files = {}

    # 模式1: 从 recovery_test_logs/ 目录查找 compute<N>_*.log（tee 输出，仅含 stdout/stderr）
    for f in glob.glob(os.path.join(log_dir, 'compute*_*.log')):
        basename = os.path.basename(f)
        m = re.match(r'compute(\d+)_', basename)
        if m:
            nid = int(m.group(1))
            log_files[nid] = f

    # 模式2: 查找 computeserver.log*（brpc 日志文件，包含完整的 LOG(INFO) 级别日志）
    # 优先级高于模式1，因为 brpc 日志包含完整的 IR Recovery 各阶段日志
    for f in glob.glob(os.path.join(log_dir, 'computeserver.log*')):
        # 解析日志文件内容，通过 recovery_start 事件判断节点 ID
        events = parse_log_file(f)
        for e in events:
            if e['event'] in ('recovery_start', 'fault_notified'):
                node_id = int(e['groups'][0])
                log_files[node_id] = f  # 覆盖模式1 的结果（brpc 日志更完整）
                break

    # 模式3: 也查找 remote 节点日志（用于故障检测时间）
    remote_log = None
    for f in glob.glob(os.path.join(log_dir, 'remote_*.log')):
        remote_log = f
        break
    if remote_log is None:
        for f in glob.glob(os.path.join(log_dir, 'LOG.log')):
            remote_log = f
            break

    if not log_files:
        log_warn(f"在 {log_dir} 中未找到计算节点日志文件")
        log_warn("请确认日志目录路径正确")
        sys.exit(1)

    # 解析 remote 日志获取故障检测时间
    remote_timing = None
    if remote_log:
        remote_events = parse_log_file(remote_log)
        remote_timing = analyze_recovery_timing(remote_events)
        if remote_timing and 'fault_detected' in remote_timing:
            ts = remote_timing['fault_detected']
            log_info(f"Remote 节点故障检测时间: {ts.strftime('%H:%M:%S.%f')[:-3]}")

    # 分析各节点
    all_durations = {}
    for nid in sorted(log_files.keys()):
        if specified_nodes is not None and nid not in specified_nodes:
            continue

        filepath = log_files[nid]
        log_info(f"分析节点 {nid} 的日志: {os.path.basename(filepath)}")

        events = parse_log_file(filepath)
        timing = analyze_recovery_timing(events)
        durations = compute_phase_durations(timing) if timing else {}

        print_node_timing(nid, timing, durations)
        all_durations[nid] = durations

    # 打印汇总对比表
    if len(all_durations) > 1:
        print(f"\n{'=' * 60}")
        print(f"  各节点恢复用时对比")
        print(f"{'=' * 60}")

        phase_keys = [
            ('phase1_gplm_cleanup', 'Phase 1 (GPLM清理)'),
            ('phase2_lplm_scan',    'Phase 2 (LPLM扫描)'),
            ('phase3_wakeup',       'Phase 3 (唤醒线程)'),
            ('phase4_total',        'Phase 4 (日志恢复)'),
            ('total_recovery',      '总耗时'),
        ]

        # 表头
        node_ids = sorted(all_durations.keys())
        header = f"  {'阶段':<22}"
        for nid in node_ids:
            header += f"  {'节点'+str(nid):>12}"
        print(header)
        print(f"  {'─' * (22 + 14 * len(node_ids))}")

        for key, label in phase_keys:
            row = f"  {label:<22}"
            for nid in node_ids:
                val = all_durations[nid].get(key)
                if val is not None:
                    row += f"  {format_duration(val):>12}"
                else:
                    row += f"  {'N/A':>12}"
            if key == 'total_recovery':
                print(f"  {'━' * (22 + 14 * len(node_ids))}")
            print(row)

    print()
    return all_durations

if __name__ == '__main__':
    main()
