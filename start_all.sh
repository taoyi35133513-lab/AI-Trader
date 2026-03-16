#!/usr/bin/env zsh
#
# AI-Trader 一键启动脚本
# 启动: 后端(8888) + 前端(8080) + 日频/小时频模拟交易调度器
#
# 用法:
#   ./start_all.sh          # 启动所有服务
#   ./start_all.sh --debug  # 调试模式(后端热重载)
#
# 停止: Ctrl+C 即可终止所有服务

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# 颜色
GREEN='\033[92m'
BLUE='\033[94m'
YELLOW='\033[93m'
RED='\033[91m'
BOLD='\033[1m'
RESET='\033[0m'

log()  { echo "${BLUE}[INFO]${RESET} $1"; }
ok()   { echo "${GREEN}[OK]${RESET} $1"; }
warn() { echo "${YELLOW}[WARN]${RESET} $1"; }
err()  { echo "${RED}[ERROR]${RESET} $1"; }

# 虚拟环境
VENV="$SCRIPT_DIR/.venv"
if [ ! -d "$VENV" ]; then
    err "虚拟环境 .venv 不存在，请先运行: poetry install"
    exit 1
fi
PYTHON="$VENV/bin/python"
ok "虚拟环境: $VENV"

# 参数
DEBUG_FLAG=""
if [ "$1" = "--debug" ]; then
    DEBUG_FLAG="--reload"
    log "调试模式已开启"
fi

# 清理旧进程
for port in 8888 8080; do
    old_pids=$(lsof -ti:$port 2>/dev/null)
    if [ -n "$old_pids" ]; then
        warn "端口 $port 被占用，正在停止旧进程..."
        echo "$old_pids" | xargs kill 2>/dev/null
        sleep 2
    fi
done

# 日志目录
mkdir -p "$SCRIPT_DIR/logs"

# 记录子进程 PID，退出时统一清理
BACKEND_PID=""
FRONTEND_PID=""

cleanup() {
    echo ""
    log "正在停止所有服务..."
    for pid in $BACKEND_PID $FRONTEND_PID; do
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null
        fi
    done
    ok "所有服务已停止"
    exit 0
}
trap cleanup INT TERM

# ========== 1. 启动后端 (端口 8888) ==========
log "启动后端服务 (port 8888)..."
export UNIFIED_MCP_MODE=true
export AI_TRADER_MODE=live
export AI_TRADER_FREQUENCY="daily+hourly"

"$PYTHON" -m uvicorn api.main:app \
    --host 0.0.0.0 --port 8888 $DEBUG_FLAG \
    >> "$SCRIPT_DIR/logs/backend.log" 2>&1 &
BACKEND_PID=$!
ok "后端 PID: $BACKEND_PID (日志: logs/backend.log)"

# 等待后端就绪
log "等待后端启动..."
ready=0
for i in {1..30}; do
    # 先检查进程是否还活着
    if ! kill -0 "$BACKEND_PID" 2>/dev/null; then
        err "后端进程已退出，请检查 logs/backend.log"
        tail -5 "$SCRIPT_DIR/logs/backend.log"
        exit 1
    fi
    if "$PYTHON" -c "import urllib.request; urllib.request.urlopen('http://localhost:8888/health', timeout=2)" 2>/dev/null; then
        ok "后端已就绪"
        ready=1
        break
    fi
    sleep 1
done
if [ "$ready" -eq 0 ]; then
    err "后端启动超时(30s)，最近日志："
    tail -10 "$SCRIPT_DIR/logs/backend.log"
    cleanup
fi

# ========== 2. 启动前端 (端口 8080) ==========
log "启动前端服务 (port 8080)..."
"$PYTHON" -m http.server 8080 \
    --directory "$SCRIPT_DIR/docs" \
    >> "$SCRIPT_DIR/logs/frontend.log" 2>&1 &
FRONTEND_PID=$!
ok "前端 PID: $FRONTEND_PID (日志: logs/frontend.log)"

# ========== 3. 打印状态 ==========
echo ""
echo "${BOLD}========================================${RESET}"
echo "${BOLD}  AI-Trader 所有服务已启动${RESET}"
echo "${BOLD}========================================${RESET}"
echo "  后端 API:  ${GREEN}http://localhost:8888${RESET}"
echo "  API 文档:  ${GREEN}http://localhost:8888/docs${RESET}"
echo "  前端页面:  ${GREEN}http://localhost:8080${RESET}"
echo "  模拟交易:  ${GREEN}日频 + 小时频 调度器已自动启动${RESET}"
echo ""
echo "  日频调度:  每个交易日 09:35"
echo "  时频调度:  每个交易日 10:35, 11:35, 14:05, 15:05"
echo ""
echo "  日志文件:  logs/backend.log, logs/frontend.log"
echo "  ${YELLOW}按 Ctrl+C 停止所有服务${RESET}"
echo "${BOLD}========================================${RESET}"
echo ""

# 保持前台运行，定期检查子进程
while true; do
    if ! kill -0 "$BACKEND_PID" 2>/dev/null; then
        err "后端服务异常退出！最近日志："
        tail -10 "$SCRIPT_DIR/logs/backend.log"
        cleanup
    fi
    if ! kill -0 "$FRONTEND_PID" 2>/dev/null; then
        err "前端服务异常退出！"
        cleanup
    fi
    sleep 5
done
