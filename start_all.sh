#!/bin/bash
# 启动脚本：使用 tmux 在不同终端窗口中依次启动存储节点、remote节点、计算节点0、计算节点1、计算节点2
# 参考 .vscode/launch.json 配置
#
# 使用方法：
#   ./start_all.sh          # 启动所有节点
#   ./start_all.sh stop     # 停止所有节点（关闭 tmux 会话）
#
# tmux 操作提示：
#   Ctrl+B 然后按数字键 (0-4) 切换窗口
#   Ctrl+B 然后按 n/p 切换到下一个/上一个窗口
#   Ctrl+B 然后按 d 脱离会话（节点继续运行）
#   重新连接会话：tmux attach -t hybrid_cloud

WORKSPACE=$(cd "$(dirname "$0")" && pwd)
BUILD_DIR="${WORKSPACE}/build"
SESSION_NAME="hybrid_cloud"

# 检查是否安装了 tmux
if ! command -v tmux &> /dev/null; then
    echo "错误: 未安装 tmux，请先安装 tmux"
    echo "  Ubuntu/Debian: sudo apt install tmux"
    echo "  CentOS/RHEL:   sudo yum install tmux"
    exit 1
fi

# 停止功能
if [ "$1" == "stop" ]; then
    echo "正在停止所有节点..."
    tmux kill-session -t ${SESSION_NAME} 2>/dev/null
    if [ $? -eq 0 ]; then
        echo "所有节点已停止，tmux 会话已关闭。"
    else
        echo "未找到运行中的会话 '${SESSION_NAME}'。"
    fi
    exit 0
fi

# 检查会话是否已存在
if tmux has-session -t ${SESSION_NAME} 2>/dev/null; then
    echo "会话 '${SESSION_NAME}' 已存在。"
    echo "  连接会话: tmux attach -t ${SESSION_NAME}"
    echo "  停止节点: $0 stop"
    exit 1
fi

echo "=========================================="
echo "  使用 tmux 启动所有节点"
echo "=========================================="

START_TIME=$(date +%s%3N)
echo "[INFO] 脚本启动时间: $(date '+%Y-%m-%d %H:%M:%S.%3N')"

# 清理旧的吞吐量 CSV 文件
rm -f ${BUILD_DIR}/compute_server/throughput_*.csv

# PID 文件目录
PID_DIR="${BUILD_DIR}/compute_server"

# 1. 创建 tmux 会话，第一个窗口运行存储节点
echo "[1/5] 启动存储节点..."
tmux new-session -d -s ${SESSION_NAME} -n "storage" \
    "cd ${BUILD_DIR}/storage_server/ && echo '=== 存储节点 ===' && ./storage_pool smallbank; echo '--- 存储节点已退出，按回车关闭 ---'; read"

# 2. 新建窗口运行 remote 节点
echo "[2/5] 启动 remote 节点..."
tmux new-window -t ${SESSION_NAME} -n "remote" \
    "cd ${BUILD_DIR}/remote_server/ && echo '=== Remote 节点 ===' && ./remote_node smallbank; echo '--- Remote 节点已退出，按回车关闭 ---'; read"

# 3. 新建窗口运行计算节点0
echo "[3/5] 启动计算节点0..."
tmux new-window -t ${SESSION_NAME} -n "compute0" \
    "cd ${BUILD_DIR}/compute_server/ && echo '=== 计算节点0 ===' && ./compute_server smallbank lazy 5 0.6 0.9 0; echo '--- 计算节点0已退出，按回车关闭 ---'; read"
sleep 1

# 4. 新建窗口运行计算节点1
echo "[4/5] 启动计算节点1..."
tmux new-window -t ${SESSION_NAME} -n "compute1" \
    "cd ${BUILD_DIR}/compute_server/ && echo '=== 计算节点1 ===' && ./compute_server smallbank lazy 5 0.6 0.9 1; echo '--- 计算节点1已退出，按回车关闭 ---'; read"
sleep 1

# 5. 新建窗口运行计算节点2
echo "[5/5] 启动计算节点2..."
tmux new-window -t ${SESSION_NAME} -n "compute2" \
    "cd ${BUILD_DIR}/compute_server/ && echo '=== 计算节点2 ===' && ./compute_server smallbank lazy 5 0.6 0.9 2; echo '--- 计算节点2已退出，按回车关闭 ---'; read"

LAUNCH_TIME=$(date +%s%3N)
LAUNCH_ELAPSED=$(( LAUNCH_TIME - START_TIME ))
echo "[INFO] 所有节点已同时启动 (启动耗时: ${LAUNCH_ELAPSED}ms)"
echo "[INFO] 各节点会自动等待依赖服务就绪后开始运行..."
echo ""

# 等待计算节点进程出现
sleep 1
COMPUTE_PIDS=""
for i in $(seq 1 30); do
    COMPUTE_PIDS=$(pgrep -x compute_server 2>/dev/null | tr '\n' ' ')
    if [ $(echo "$COMPUTE_PIDS" | wc -w) -ge 3 ]; then
        break
    fi
    sleep 0.5
done
echo "[INFO] 计算节点 PIDs: ${COMPUTE_PIDS}"

# 等待计算节点开始运行（检测 "Server Start" 日志）
echo "[INFO] 等待计算节点开始运行..."
RUN_DETECTED=0
RUN_START_TIME=0
for i in $(seq 1 300); do
    # 检查计算节点的输出中是否有开始运行的标志
    if tmux capture-pane -t ${SESSION_NAME}:compute0 -p -S -100 2>/dev/null | grep -q "Server Start"; then
        RUN_START_TIME=$(date +%s%3N)
        RUN_ELAPSED=$(( RUN_START_TIME - START_TIME ))
        echo "[INFO] 计算节点开始运行时间: $(date '+%Y-%m-%d %H:%M:%S.%3N') (距启动 ${RUN_ELAPSED}ms)"
        RUN_DETECTED=1
        break
    fi
    sleep 0.5
done

if [ $RUN_DETECTED -eq 0 ]; then
    echo "[WARN] 等待超时，未检测到计算节点开始运行"
fi

# 等待所有计算节点工作负载完成（通过检测吞吐量CSV是否停止增长判断）
echo "[INFO] 等待所有计算节点运行结束..."
END_DETECTED=0
HAS_STARTED=0     # 标记是否已经检测到事务开始执行（committed>0）
PREV_LINES_0=0
PREV_LINES_1=0
PREV_LINES_2=0
STABLE_COUNT=0
REQUIRED_STABLE=6  # 连续6次（3秒）CSV行数不变则认为运行结束
TX_START_TIME=0

for i in $(seq 1 1200); do
    # 获取各节点CSV行数
    CSV_0="${BUILD_DIR}/compute_server/throughput_0.csv"
    CSV_1="${BUILD_DIR}/compute_server/throughput_1.csv"
    CSV_2="${BUILD_DIR}/compute_server/throughput_2.csv"

    LINES_0=0; LINES_1=0; LINES_2=0
    [ -f "$CSV_0" ] && LINES_0=$(wc -l < "$CSV_0" 2>/dev/null)
    [ -f "$CSV_1" ] && LINES_1=$(wc -l < "$CSV_1" 2>/dev/null)
    [ -f "$CSV_2" ] && LINES_2=$(wc -l < "$CSV_2" 2>/dev/null)

    # 阶段1：等待事务开始执行（CSV中committed值大于0）
    if [ $HAS_STARTED -eq 0 ]; then
        for node_id in 0 1 2; do
            CSV_FILE="${BUILD_DIR}/compute_server/throughput_${node_id}.csv"
            if [ -f "$CSV_FILE" ]; then
                LAST_VAL=$(tail -1 "$CSV_FILE" 2>/dev/null | cut -d',' -f2)
                if [ -n "$LAST_VAL" ] && [ "$LAST_VAL" != "0" ] && [ "$LAST_VAL" != "committed" ]; then
                    HAS_STARTED=1
                    TX_START_TIME=$(date +%s%3N)
                    TX_START_ELAPSED=$(( TX_START_TIME - START_TIME ))
                    echo "[INFO] 检测到事务开始执行: $(date '+%Y-%m-%d %H:%M:%S.%3N') (距启动 ${TX_START_ELAPSED}ms)"
                    break
                fi
            fi
        done
        PREV_LINES_0=$LINES_0; PREV_LINES_1=$LINES_1; PREV_LINES_2=$LINES_2
        sleep 0.5
        continue
    fi

    # 阶段2：等待CSV停止增长（所有节点的CSV行数不再变化）
    # 至少要有10行数据才开始检测
    if [ $LINES_0 -lt 10 ] || [ $LINES_1 -lt 10 ] || [ $LINES_2 -lt 10 ]; then
        PREV_LINES_0=$LINES_0; PREV_LINES_1=$LINES_1; PREV_LINES_2=$LINES_2
        sleep 0.5
        continue
    fi

    if [ $LINES_0 -eq $PREV_LINES_0 ] && [ $LINES_1 -eq $PREV_LINES_1 ] && [ $LINES_2 -eq $PREV_LINES_2 ]; then
        STABLE_COUNT=$((STABLE_COUNT + 1))
        if [ $STABLE_COUNT -ge $REQUIRED_STABLE ]; then
            END_TIME=$(date +%s%3N)
            END_ELAPSED=$(( END_TIME - START_TIME ))
            if [ $RUN_START_TIME -gt 0 ]; then
                RUN_DURATION=$(( END_TIME - RUN_START_TIME ))
            else
                RUN_DURATION=0
            fi
            if [ $TX_START_TIME -gt 0 ]; then
                TX_DURATION=$(( END_TIME - TX_START_TIME ))
            else
                TX_DURATION=0
            fi
            echo "[INFO] 所有计算节点运行结束时间: $(date '+%Y-%m-%d %H:%M:%S.%3N') (距启动 ${END_ELAPSED}ms)"
            echo ""
            echo "=========================================="
            echo "  时间统计"
            echo "=========================================="
            echo "  脚本启动时间:       $(date -d @$((START_TIME/1000)) '+%H:%M:%S').$(printf '%03d' $((START_TIME%1000)))"
            echo "  节点启动耗时:       ${LAUNCH_ELAPSED}ms"
            if [ $RUN_START_TIME -gt 0 ]; then
                echo "  初始化完成时间:     $(date -d @$((RUN_START_TIME/1000)) '+%H:%M:%S').$(printf '%03d' $((RUN_START_TIME%1000)))  (距启动 ${RUN_ELAPSED}ms)"
            fi
            if [ $TX_START_TIME -gt 0 ]; then
                echo "  事务开始执行时间:   $(date -d @$((TX_START_TIME/1000)) '+%H:%M:%S').$(printf '%03d' $((TX_START_TIME%1000)))  (距启动 ${TX_START_ELAPSED}ms)"
            fi
            echo "  运行结束时间:       $(date -d @$((END_TIME/1000)) '+%H:%M:%S').$(printf '%03d' $((END_TIME%1000)))  (距启动 ${END_ELAPSED}ms)"
            if [ $TX_DURATION -gt 0 ]; then
                echo "  事务执行时长:       ${TX_DURATION}ms ($(echo "scale=1; ${TX_DURATION}/1000" | bc)s)"
            fi
            if [ $RUN_DURATION -gt 0 ]; then
                echo "  运行总时长(含预热): ${RUN_DURATION}ms ($(echo "scale=1; ${RUN_DURATION}/1000" | bc)s)"
            fi
            echo "  总耗时(启动到结束): ${END_ELAPSED}ms ($(echo "scale=1; ${END_ELAPSED}/1000" | bc)s)"
            echo "=========================================="
            END_DETECTED=1
            break
        fi
    else
        STABLE_COUNT=0
    fi

    PREV_LINES_0=$LINES_0; PREV_LINES_1=$LINES_1; PREV_LINES_2=$LINES_2
    sleep 0.5
done

if [ $END_DETECTED -eq 0 ]; then
    echo "[WARN] 等待超时(600s)，计算节点未全部结束"
    echo "[INFO] 仍在运行的计算节点进程:"
    for pid in $COMPUTE_PIDS; do
        if kill -0 "$pid" 2>/dev/null; then
            echo "  PID $pid 仍在运行"
        fi
    done
fi

# 切换到第一个窗口
tmux select-window -t ${SESSION_NAME}:0

echo ""
echo "=========================================="
echo "  所有节点已在 tmux 会话中启动"
echo "=========================================="
echo ""
echo "  会话名称: ${SESSION_NAME}"
echo "  窗口列表:"
echo "    0: storage  - 存储节点"
echo "    1: remote   - Remote 节点"
echo "    2: compute0 - 计算节点0"
echo "    3: compute1 - 计算节点1"
echo "    4: compute2 - 计算节点2"
echo ""
echo "  连接会话: tmux attach -t ${SESSION_NAME}"
echo "  停止所有: $0 stop"
echo "=========================================="
