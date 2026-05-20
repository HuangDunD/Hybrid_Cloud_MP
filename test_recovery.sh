#!/bin/bash
# ============================================================================
# 故障恢复测试脚本
# ============================================================================
# 功能：
#   1. 启动所有节点（存储节点、remote节点、3个计算节点）
#   2. 启动 30s 后随机 kill 一个计算节点
#   3. 故障注入后 ${SHUTDOWN_DELAY}s 关闭所有节点
#   4. 收集日志并验证数据一致性
#
# 使用方法：
#   ./test_recovery.sh              # 运行故障恢复测试
#   ./test_recovery.sh stop         # 停止所有节点
# ============================================================================

WORKSPACE=$(cd "$(dirname "$0")" && pwd)
BUILD_DIR="${WORKSPACE}/build"
SESSION_NAME="recovery_test"
LOG_DIR="${WORKSPACE}/recovery_test_logs"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
TEST_LOG="${LOG_DIR}/test_${TIMESTAMP}.log"

# 计算节点列表（ID 0, 1, 2）
COMPUTE_NODES=(0 1 2)
# 等待多少秒后 kill 计算节点
KILL_DELAY=35
# 故障注入后等待多少秒关闭所有节点
SHUTDOWN_DELAY=40

# ============================================================================
# 工具函数
# ============================================================================

log_info() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] $1"
    echo -e "\033[32m${msg}\033[0m"
    echo "${msg}" >> "${TEST_LOG}"
}

log_warn() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] [WARN] $1"
    echo -e "\033[33m${msg}\033[0m"
    echo "${msg}" >> "${TEST_LOG}"
}

log_error() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] $1"
    echo -e "\033[31m${msg}\033[0m"
    echo "${msg}" >> "${TEST_LOG}"
}

log_success() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] [SUCCESS] $1"
    echo -e "\033[36m${msg}\033[0m"
    echo "${msg}" >> "${TEST_LOG}"
}

# 检查 tmux 是否安装
check_tmux() {
    if ! command -v tmux &> /dev/null; then
        log_error "未安装 tmux，请先安装: yum install -y tmux"
        exit 1
    fi
}

# 检查可执行文件是否存在
check_binaries() {
    local bins=(
        "${BUILD_DIR}/storage_server/storage_pool"
        "${BUILD_DIR}/remote_server/remote_node"
        "${BUILD_DIR}/compute_server/compute_server"
    )
    for bin in "${bins[@]}"; do
        if [ ! -x "$bin" ]; then
            log_error "可执行文件不存在: $bin"
            log_error "请先编译项目: cd build && make -j\$(nproc)"
            exit 1
        fi
    done
}

# 停止所有节点
stop_all() {
    log_info "正在停止所有节点..."
    tmux kill-session -t ${SESSION_NAME} 2>/dev/null
    # 确保进程被清理
    pkill -f "storage_pool smallbank" 2>/dev/null
    pkill -f "remote_node smallbank" 2>/dev/null
    pkill -f "compute_server smallbank" 2>/dev/null
    sleep 1
    log_info "所有节点已停止。"
}

# 获取 tmux 窗口中进程的 PID
get_pane_pid() {
    local window_name=$1
    tmux list-panes -t "${SESSION_NAME}:${window_name}" -F '#{pane_pid}' 2>/dev/null | head -1
}

# 获取窗口中实际运行的子进程 PID（compute_server 等）
get_child_pid() {
    local pane_pid=$1
    # 获取 pane 的子进程（实际的 compute_server 进程）
    local child_pid=$(pgrep -P "$pane_pid" 2>/dev/null | head -1)
    if [ -z "$child_pid" ]; then
        # 如果没有直接子进程，尝试查找 bash 的子进程
        for mid_pid in $(pgrep -P "$pane_pid" 2>/dev/null); do
            child_pid=$(pgrep -P "$mid_pid" 2>/dev/null | head -1)
            if [ -n "$child_pid" ]; then
                break
            fi
        done
    fi
    echo "$child_pid"
}

# 检查 tmux 窗口中的进程是否还在运行
is_window_process_running() {
    local window_name=$1
    local pane_pid=$(get_pane_pid "$window_name")
    if [ -z "$pane_pid" ]; then
        return 1
    fi
    # 检查 pane 中是否有子进程在运行
    local children=$(pgrep -P "$pane_pid" 2>/dev/null)
    if [ -n "$children" ]; then
        return 0
    fi
    return 1
}

# 捕获 tmux 窗口的输出
capture_window_output() {
    local window_name=$1
    local output_file=$2
    tmux capture-pane -t "${SESSION_NAME}:${window_name}" -p -S -1000 > "$output_file" 2>/dev/null
}

# ============================================================================
# 主流程
# ============================================================================

# 停止功能
if [ "$1" == "stop" ]; then
    stop_all
    exit 0
fi

# 前置检查
check_tmux
check_binaries

# 清理之前的会话
tmux kill-session -t ${SESSION_NAME} 2>/dev/null
pkill -f "storage_pool smallbank" 2>/dev/null
pkill -f "remote_node smallbank" 2>/dev/null
pkill -f "compute_server smallbank" 2>/dev/null
sleep 2

# 清除所有之前的日志记录
echo "正在清除历史日志..."
# 1. 测试脚本自身的日志目录
rm -rf "${LOG_DIR}"
# 2. 存储节点日志
rm -f "${BUILD_DIR}"/storage_server/storageserver.log*
rm -f "${BUILD_DIR}"/storage_server/LOG_FILE
rm -rf "${BUILD_DIR}"/storage_server/Raft_Log*
# 3. Remote 节点日志
rm -f "${BUILD_DIR}"/remote_server/LOG.log
rm -f "${BUILD_DIR}"/remote_server/remote_server.txt
# 4. 计算节点日志
rm -f "${BUILD_DIR}"/compute_server/computeserver.log*
rm -f "${BUILD_DIR}"/compute_server/result.txt
rm -f "${BUILD_DIR}"/compute_server/delay_fetch_remote.txt
# 5. 吞吐量监控 CSV
rm -f "${BUILD_DIR}"/compute_server/throughput_*.csv
# 6. 数据库数据文件（不清除，避免存储节点重新加载耗时过长）
# 如需从干净状态开始，取消下面的注释
# rm -f "${BUILD_DIR}"/storage_server/smallbank_*
# rm -f "${BUILD_DIR}"/storage_server/tpcc_*
# rm -f "${BUILD_DIR}"/storage_server/ycsb_*
echo "历史日志已清除"

# 重新创建日志目录
mkdir -p "${LOG_DIR}"

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║              故障恢复测试 - Hybrid Cloud MP                 ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  工作负载: smallbank                                        ║"
echo "║  计算节点: 3 个 (ID: 0, 1, 2)                              ║"
echo "║  故障注入: 启动 ${KILL_DELAY}s 后随机 kill 1 个计算节点            ║"
echo "║  关闭时间: 故障注入 ${SHUTDOWN_DELAY}s 后关闭所有节点              ║"
echo "║  日志目录: ${LOG_DIR}                                       ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# ============================================================================
# 阶段 1: 启动所有节点
# ============================================================================
log_info "========== 阶段 1: 启动所有节点 =========="

# 1. 启动存储节点
log_info "[1/5] 启动存储节点..."
tmux new-session -d -s ${SESSION_NAME} -n "storage" \
    "cd ${BUILD_DIR}/storage_server/ && ./storage_pool smallbank 2>&1 | tee ${LOG_DIR}/storage_${TIMESTAMP}.log; echo '--- 存储节点已退出 ---'; read"
sleep 1

# 2. 启动 remote 节点
log_info "[2/5] 启动 remote 节点..."
tmux new-window -t ${SESSION_NAME} -n "remote" \
    "cd ${BUILD_DIR}/remote_server/ && ./remote_node smallbank 2>&1 | tee ${LOG_DIR}/remote_${TIMESTAMP}.log; echo '--- Remote 节点已退出 ---'; read"
sleep 1

# 3. 启动计算节点 0
log_info "[3/5] 启动计算节点 0..."
tmux new-window -t ${SESSION_NAME} -n "compute0" \
    "cd ${BUILD_DIR}/compute_server/ && ./compute_server smallbank lazy 5 0.6 0.9 0 2>&1 | tee ${LOG_DIR}/compute0_${TIMESTAMP}.log; echo '--- 计算节点0已退出 ---'; read"
sleep 1

# 4. 启动计算节点 1
log_info "[4/5] 启动计算节点 1..."
tmux new-window -t ${SESSION_NAME} -n "compute1" \
    "cd ${BUILD_DIR}/compute_server/ && ./compute_server smallbank lazy 5 0.6 0.9 1 2>&1 | tee ${LOG_DIR}/compute1_${TIMESTAMP}.log; echo '--- 计算节点1已退出 ---'; read"
sleep 1

# 5. 启动计算节点 2
log_info "[5/5] 启动计算节点 2..."
tmux new-window -t ${SESSION_NAME} -n "compute2" \
    "cd ${BUILD_DIR}/compute_server/ && ./compute_server smallbank lazy 5 0.6 0.9 2 2>&1 | tee ${LOG_DIR}/compute2_${TIMESTAMP}.log; echo '--- 计算节点2已退出 ---'; read"

log_info "所有节点已启动，各节点会自动等待依赖服务就绪..."
sleep 5

# ============================================================================
# 阶段 2: 故障注入 - 随机 kill 一个计算节点
# ============================================================================
log_info "========== 阶段 2: 故障注入 =========="
log_info "等待 ${KILL_DELAY}s 后注入故障..."
sleep ${KILL_DELAY}

# 随机选择一个计算节点
VICTIM_INDEX=$((RANDOM % ${#COMPUTE_NODES[@]}))
VICTIM_ID=${COMPUTE_NODES[$VICTIM_INDEX]}
VICTIM_WINDOW="compute${VICTIM_ID}"

log_warn ">>> 随机选中计算节点 ${VICTIM_ID} 作为故障节点 <<<"

# 记录故障注入的精确时间（毫秒）
FAULT_TIME_MS=$(date +%s%3N)
log_info "故障注入时间: ${FAULT_TIME_MS} (epoch ms)"

# 获取该计算节点的进程 PID
VICTIM_PANE_PID=$(get_pane_pid "${VICTIM_WINDOW}")
if [ -n "$VICTIM_PANE_PID" ]; then
    # 查找实际的 compute_server 进程
    VICTIM_PID=$(pgrep -f "compute_server smallbank lazy 5 0.6 0.9 ${VICTIM_ID}" | head -1)
    if [ -n "$VICTIM_PID" ]; then
        log_warn "正在 kill 计算节点 ${VICTIM_ID} (PID: ${VICTIM_PID})..."
        kill -9 ${VICTIM_PID}
        log_warn "计算节点 ${VICTIM_ID} 已被 kill (SIGKILL)"
    else
        log_error "未找到计算节点 ${VICTIM_ID} 的进程，尝试通过 tmux 发送信号..."
        tmux send-keys -t "${SESSION_NAME}:${VICTIM_WINDOW}" C-c 2>/dev/null
        sleep 1
        # 强制关闭窗口中的进程
        tmux send-keys -t "${SESSION_NAME}:${VICTIM_WINDOW}" "kill -9 \$\$" Enter 2>/dev/null
    fi
else
    log_error "未找到计算节点 ${VICTIM_ID} 的 tmux 窗口"
fi

# 构建存活节点列表
SURVIVING_NODES=()
for nid in "${COMPUTE_NODES[@]}"; do
    if [ "$nid" != "$VICTIM_ID" ]; then
        SURVIVING_NODES+=("$nid")
    fi
done

log_info "存活计算节点: ${SURVIVING_NODES[*]}"
log_info "等待 Remote 节点检测到故障并触发恢复流程..."

# ============================================================================
# 阶段 3: 等待一段时间后关闭所有节点
# ============================================================================
log_info "========== 阶段 3: 等待 ${SHUTDOWN_DELAY}s 后关闭所有节点 =========="
log_info "故障注入后等待 ${SHUTDOWN_DELAY}s 关闭所有节点..."
sleep ${SHUTDOWN_DELAY}

ALL_FINISHED=true
log_info "等待时间已到，准备关闭所有节点..."

# ============================================================================
# 阶段 4: 收集故障恢复日志
# ============================================================================
log_info "========== 阶段 4: 收集运行日志 =========="

# 捕获各窗口输出
for window in storage remote compute0 compute1 compute2; do
    capture_window_output "$window" "${LOG_DIR}/${window}_output_${TIMESTAMP}.txt"
done

# 检查 remote 节点日志中是否检测到故障
REMOTE_OUTPUT="${LOG_DIR}/remote_output_${TIMESTAMP}.txt"
if grep -q "declared DEAD" "$REMOTE_OUTPUT" 2>/dev/null; then
    DEAD_NODE=$(grep "declared DEAD" "$REMOTE_OUTPUT" | head -1)
    log_success "Remote 节点已检测到故障: ${DEAD_NODE}"
else
    log_warn "未在 Remote 节点输出中找到故障检测信息"
fi

# 检查存活节点是否收到故障通知
for nid in "${SURVIVING_NODES[@]}"; do
    NODE_OUTPUT="${LOG_DIR}/compute${nid}_output_${TIMESTAMP}.txt"
    if grep -q "failure detected" "$NODE_OUTPUT" 2>/dev/null; then
        log_success "计算节点 ${nid} 已收到故障通知并开始恢复"
    else
        log_warn "计算节点 ${nid} 的输出中未找到故障恢复信息"
    fi
done

# 打印存活节点的事务统计
log_info "--- 存活节点事务统计 ---"
for nid in "${SURVIVING_NODES[@]}"; do
    NODE_OUTPUT="${LOG_DIR}/compute${nid}_output_${TIMESTAMP}.txt"
    log_info "计算节点 ${nid}:"
    grep -E "(Total Transactions|Commit Transactions|Abort Ratio|tx_)" "$NODE_OUTPUT" 2>/dev/null | while read line; do
        log_info "  $line"
    done
done

# --- 4.1 分析故障恢复各阶段用时 ---
log_info "--- 故障恢复各阶段用时分析 ---"
RECOVERY_TIME_LOG="${LOG_DIR}/recovery_timing_${TIMESTAMP}.log"

# 将 brpc 日志文件（包含完整的 LOG(INFO) 级别 IR Recovery 日志）复制到日志目录
# brpc 日志文件名格式: computeserver.log<PID>，包含完整的各阶段时间戳
log_info "收集 brpc 日志文件..."
for brpc_log in ${BUILD_DIR}/compute_server/computeserver.log*; do
    if [ -f "$brpc_log" ]; then
        cp "$brpc_log" "${LOG_DIR}/" 2>/dev/null
    fi
done
# 同时收集 remote 节点的 brpc 日志（包含故障检测时间戳）
if [ -f "${BUILD_DIR}/remote_server/LOG.log" ]; then
    cp "${BUILD_DIR}/remote_server/LOG.log" "${LOG_DIR}/" 2>/dev/null
    log_info "  已收集 Remote 节点 brpc 日志"
fi

# 将存活节点 ID 列表传给分析脚本
SURVIVING_ARGS=""
for nid in "${SURVIVING_NODES[@]}"; do
    SURVIVING_ARGS="${SURVIVING_ARGS} ${nid}"
done

# 运行用时分析脚本（日志目录中同时包含 tee 日志和 brpc 日志，脚本会优先使用 brpc 日志）
python3 "${WORKSPACE}/analyze_recovery_time.py" "${LOG_DIR}" ${SURVIVING_ARGS} 2>&1 | tee "${RECOVERY_TIME_LOG}"

# --- 4.2 收集吞吐量监控 CSV 文件 ---
log_info "收集吞吐量监控数据..."
for nid in "${COMPUTE_NODES[@]}"; do
    TP_CSV="${BUILD_DIR}/compute_server/throughput_${nid}.csv"
    if [ -f "$TP_CSV" ]; then
        cp "$TP_CSV" "${LOG_DIR}/" 2>/dev/null
        log_info "  已收集节点 ${nid} 的吞吐量数据"
    fi
done

# ============================================================================
# 阶段 5: 依次关闭节点并进行数据一致性验证
# ============================================================================
log_info "========== 阶段 5: 关闭节点并验证数据一致性 =========="

# --- 5.1 依次关闭存活的计算节点 ---
log_info "依次关闭存活的计算节点..."
for nid in "${SURVIVING_NODES[@]}"; do
    COMPUTE_PID=$(pgrep -f "compute_server smallbank lazy 5 0.6 0.9 ${nid}" | head -1)
    if [ -n "$COMPUTE_PID" ]; then
        log_info "  关闭计算节点 ${nid} (PID: ${COMPUTE_PID})..."
        kill ${COMPUTE_PID} 2>/dev/null
        wait ${COMPUTE_PID} 2>/dev/null
    else
        log_info "  计算节点 ${nid} 已退出"
    fi
    sleep 1
done
log_info "所有计算节点已关闭"

# --- 5.2 关闭 Remote 节点 ---
log_info "关闭 Remote 节点..."
REMOTE_PID=$(pgrep -f "remote_node smallbank" | head -1)
if [ -n "$REMOTE_PID" ]; then
    kill ${REMOTE_PID} 2>/dev/null
    wait ${REMOTE_PID} 2>/dev/null
fi
sleep 1
log_info "Remote 节点已关闭"

# --- 5.3 关闭存储节点 ---
log_info "关闭存储节点..."
STORAGE_PID=$(pgrep -f "storage_pool smallbank" | head -1)
if [ -n "$STORAGE_PID" ]; then
    kill ${STORAGE_PID} 2>/dev/null
    wait ${STORAGE_PID} 2>/dev/null
fi
sleep 1
log_info "存储节点已关闭"

# 清理 tmux 会话
tmux kill-session -t ${SESSION_NAME} 2>/dev/null
sleep 2

# --- 5.4 直接读取数据库文件进行一致性验证 ---
log_info "直接读取数据库文件进行一致性验证..."
VERIFY_LOG="${LOG_DIR}/verify_${TIMESTAMP}.log"
STORAGE_DATA_DIR="${BUILD_DIR}/storage_server"

# 使用 Python 脚本直接读取数据库文件验证
python3 "${WORKSPACE}/verify_consistency.py" "${STORAGE_DATA_DIR}" 2>&1 | tee "${VERIFY_LOG}"
VERIFY_EXIT_CODE=${PIPESTATUS[0]}

if [ $VERIFY_EXIT_CODE -eq 0 ]; then
    VERIFY_RESULT="PASS"
    log_success "✅ 数据一致性验证通过！"
else
    VERIFY_RESULT="FAIL"
    log_error "❌ 数据一致性验证失败！"
    log_error "详细信息请查看: ${VERIFY_LOG}"
fi

# --- 5.5 生成吞吐量图表 ---
log_info "生成吞吐量图表..."
TP_CHART_LOG="${LOG_DIR}/throughput_analysis_${TIMESTAMP}.log"

# CSV 中使用绝对时间戳（epoch ms），直接传入故障时刻的 epoch ms
TP_SURVIVING_ARGS=""
for nid in "${SURVIVING_NODES[@]}"; do
    TP_SURVIVING_ARGS="${TP_SURVIVING_ARGS} ${nid}"
done

log_info "故障时刻 (epoch ms): ${FAULT_TIME_MS}"

python3 "${WORKSPACE}/plot_throughput.py" "${LOG_DIR}" "${FAULT_TIME_MS}" "${VICTIM_ID}" ${TP_SURVIVING_ARGS} 2>&1 | tee "${TP_CHART_LOG}"

if [ -f "${LOG_DIR}/throughput_chart.png" ]; then
    log_success "吞吐量图表已生成: ${LOG_DIR}/throughput_chart.png"
else
    log_warn "吞吐量图表生成失败，请查看: ${TP_CHART_LOG}"
fi

# ============================================================================
# 阶段 6: 测试报告
# ============================================================================
echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║                    故障恢复测试报告                         ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  测试时间: ${TIMESTAMP}                                     "
echo "║  工作负载: smallbank (lazy 模式)                            "
echo "║  故障节点: 计算节点 ${VICTIM_ID} (启动 ${KILL_DELAY}s 后被 kill)    "
echo "║  存活节点: 计算节点 ${SURVIVING_NODES[*]}                          "
echo "║  工作负载完成: $([ "$ALL_FINISHED" = true ] && echo '✅ 是' || echo '❌ 否')                                       "
echo "║  数据一致性: $([ "$VERIFY_RESULT" = "PASS" ] && echo '✅ 通过' || echo "❌ ${VERIFY_RESULT}")                       "
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  日志文件:                                                  "
echo "║    测试日志:   ${TEST_LOG}"
echo "║    存储节点:   ${LOG_DIR}/storage_${TIMESTAMP}.log"
echo "║    Remote节点: ${LOG_DIR}/remote_${TIMESTAMP}.log"
echo "║    计算节点0:  ${LOG_DIR}/compute0_${TIMESTAMP}.log"
echo "║    计算节点1:  ${LOG_DIR}/compute1_${TIMESTAMP}.log"
echo "║    计算节点2:  ${LOG_DIR}/compute2_${TIMESTAMP}.log"
echo "║    验证日志:   ${VERIFY_LOG}"
echo "║    恢复用时:   ${RECOVERY_TIME_LOG}"
echo "║    吞吐量图:   ${LOG_DIR}/throughput_chart.png"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# 写入测试结果摘要
SUMMARY_FILE="${LOG_DIR}/summary_${TIMESTAMP}.txt"
cat > "${SUMMARY_FILE}" << EOF
========== 故障恢复测试摘要 ==========
测试时间: ${TIMESTAMP}
工作负载: smallbank (lazy 模式)
故障节点: 计算节点 ${VICTIM_ID}
存活节点: 计算节点 ${SURVIVING_NODES[*]}
Kill 延迟: ${KILL_DELAY}s
关闭延迟: 故障注入后 ${SHUTDOWN_DELAY}s
工作负载完成: ${ALL_FINISHED}
数据一致性验证: ${VERIFY_RESULT}
恢复用时分析: ${RECOVERY_TIME_LOG}
EOF

log_info "测试摘要已保存到: ${SUMMARY_FILE}"

# 返回测试结果
if [ "$VERIFY_RESULT" = "PASS" ] && [ "$ALL_FINISHED" = true ]; then
    log_success "🎉 故障恢复测试通过！"
    exit 0
else
    log_error "故障恢复测试未完全通过，请检查日志"
    exit 1
fi
