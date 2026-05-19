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

# 1. 创建 tmux 会话，第一个窗口运行存储节点
echo "[1/5] 启动存储节点..."
tmux new-session -d -s ${SESSION_NAME} -n "storage" \
    "cd ${BUILD_DIR}/storage_server/ && echo '=== 存储节点 ===' && ./storage_pool smallbank; echo '--- 存储节点已退出，按回车关闭 ---'; read"
sleep 10

# 2. 新建窗口运行 remote 节点
echo "[2/5] 启动 remote 节点..."
tmux new-window -t ${SESSION_NAME} -n "remote" \
    "cd ${BUILD_DIR}/remote_server/ && echo '=== Remote 节点 ===' && ./remote_node smallbank; echo '--- Remote 节点已退出，按回车关闭 ---'; read"
sleep 10

# 3. 新建窗口运行计算节点0
echo "[3/5] 启动计算节点0..."
tmux new-window -t ${SESSION_NAME} -n "compute0" \
    "cd ${BUILD_DIR}/compute_server/ && echo '=== 计算节点0 ===' && ./compute_server smallbank lazy 5 0.6 0.9 0; echo '--- 计算节点0已退出，按回车关闭 ---'; read"
sleep 5

# 4. 新建窗口运行计算节点1
echo "[4/5] 启动计算节点1..."
tmux new-window -t ${SESSION_NAME} -n "compute1" \
    "cd ${BUILD_DIR}/compute_server/ && echo '=== 计算节点1 ===' && ./compute_server smallbank lazy 5 0.6 0.9 1; echo '--- 计算节点1已退出，按回车关闭 ---'; read"
sleep 5

# 5. 新建窗口运行计算节点2
echo "[5/5] 启动计算节点2..."
tmux new-window -t ${SESSION_NAME} -n "compute2" \
    "cd ${BUILD_DIR}/compute_server/ && echo '=== 计算节点2 ===' && ./compute_server smallbank lazy 5 0.6 0.9 2; echo '--- 计算节点2已退出，按回车关闭 ---'; read"

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

# 自动连接到 tmux 会话
tmux attach -t ${SESSION_NAME}
