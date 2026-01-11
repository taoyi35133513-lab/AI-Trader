#!/bin/bash

# 获取项目根目录（scripts/ 的父目录）
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

cd "$PROJECT_ROOT"

# Check and activate virtual environment
if [ -d ".venv" ]; then
    echo "🔌 Activating virtual environment..."
    source .venv/bin/activate
else
    echo "❌ Error: Virtual environment .venv not found in project root."
    exit 1
fi

echo "🤖 正在启动主交易智能体（A股模式）..."

python3 main.py configs/astock_config.json  # 运行A股配置
python3 main.py configs/astock_hour_config.json  # 运行A股配置（小时级）
echo "✅ AI-Trader 已停止"
