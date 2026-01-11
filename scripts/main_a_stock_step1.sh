#!/bin/bash

# A股数据准备

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

cd data/A_stock

# for alphavantage
# python3 get_daily_price_alphavantage.py
# python3 merge_jsonl_alphavantage.py
# # for tushare
python3 get_daily_price_tushare.py
python3 merge_jsonl_tushare.py
python3 get_interdaily_price_astock.py
python3 merge_jsonl_hourly.py

cd ..
