#!/usr/bin/env bash
# -----------------------------------------------------------
# backtest.sh — Run historical backtest from last data date
#               to the last A-share trading day.
#
# Usage:
#   ./backtest.sh              # daily backtest
#   ./backtest.sh hourly       # hourly backtest
#   ./backtest.sh --dry-run    # show date range without running
# -----------------------------------------------------------
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

FREQ="daily"
DRY_RUN=false

for arg in "$@"; do
    case "$arg" in
        hourly)  FREQ="hourly" ;;
        daily)   FREQ="daily" ;;
        --dry-run) DRY_RUN=true ;;
        -h|--help)
            echo "Usage: $0 [daily|hourly] [--dry-run]"
            echo "  daily    — daily frequency backtest (default)"
            echo "  hourly   — hourly frequency backtest"
            echo "  --dry-run — show calculated date range without executing"
            exit 0
            ;;
        *) echo "Unknown argument: $arg"; exit 1 ;;
    esac
done

# Activate venv
if [[ -f "$PROJECT_DIR/.venv/bin/activate" ]]; then
    source "$PROJECT_DIR/.venv/bin/activate"
else
    echo "[ERROR] Virtual environment not found at $PROJECT_DIR/.venv"
    exit 1
fi

echo "=========================================="
echo "  AI-Trader Historical Backtest"
echo "  Frequency: $FREQ"
echo "=========================================="

# Step 1: Update price data
echo ""
echo "[STEP 1] Updating price data..."
python start.py --only-data -f "$FREQ"

# Step 2: Dry-run — show date range only
if $DRY_RUN; then
    echo ""
    echo "[DRY RUN] Calculating date ranges for enabled models..."
    python -c "
import json, sys
sys.path.insert(0, '.')
from main import load_config, derive_signature, calculate_date_range, get_latest_trading_day

config = load_config()
freq = '$FREQ'
latest = get_latest_trading_day(freq)
print(f'Latest trading day in data: {latest}')
print()

enabled = [m for m in config['models'] if m.get('enabled')]
for m in enabled:
    name = m.get('name', 'unknown')
    sig = derive_signature(name, freq)
    start, end = calculate_date_range(sig, freq)
    print(f'  {name:30s}  {start} → {end}')
"
    echo ""
    echo "[DRY RUN] No backtest executed."
    exit 0
fi

# Step 3: Run agent backtest (skip backend — it's already running)
echo ""
echo "[STEP 2] Running backtest agents..."
python start.py --skip-backend -f "$FREQ"

echo ""
echo "[DONE] Backtest complete."
