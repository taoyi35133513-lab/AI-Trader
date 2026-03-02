#!/usr/bin/env bash
# -----------------------------------------------------------
# backtest.sh — Run historical backtest from last position date
#               to the last A-share trading day.
#
# Supports daily, hourly, or both frequencies.
# Automatically detects each model's last position date and
# backtests from there to the latest available trading day.
#
# Usage:
#   ./backtest.sh              # both daily + hourly
#   ./backtest.sh daily        # daily only
#   ./backtest.sh hourly       # hourly only
#   ./backtest.sh --dry-run    # show date ranges without running
#   ./backtest.sh --all-models # run ALL models, not just enabled
# -----------------------------------------------------------
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

# ── defaults ────────────────────────────────────────────────
RUN_DAILY=false
RUN_HOURLY=false
DRY_RUN=false
ALL_MODELS=false
EXPLICIT_FREQ=false

# ── parse args ──────────────────────────────────────────────
for arg in "$@"; do
    case "$arg" in
        daily)   RUN_DAILY=true;  EXPLICIT_FREQ=true ;;
        hourly)  RUN_HOURLY=true; EXPLICIT_FREQ=true ;;
        --dry-run)    DRY_RUN=true ;;
        --all-models) ALL_MODELS=true ;;
        -h|--help)
            echo "Usage: $0 [daily|hourly] [--dry-run] [--all-models]"
            echo ""
            echo "  (no freq)    — run both daily and hourly backtests (default)"
            echo "  daily        — daily frequency only"
            echo "  hourly       — hourly frequency only"
            echo "  --dry-run    — show calculated date ranges without executing"
            echo "  --all-models — include disabled models (default: enabled only)"
            exit 0
            ;;
        *) echo "[ERROR] Unknown argument: $arg"; exit 1 ;;
    esac
done

# If no explicit frequency, run both
if ! $EXPLICIT_FREQ; then
    RUN_DAILY=true
    RUN_HOURLY=true
fi

# ── activate venv ───────────────────────────────────────────
if [[ -f "$PROJECT_DIR/.venv/bin/activate" ]]; then
    source "$PROJECT_DIR/.venv/bin/activate"
else
    echo "[ERROR] Virtual environment not found at $PROJECT_DIR/.venv"
    exit 1
fi

# ── banner ──────────────────────────────────────────────────
FREQ_LABEL=""
$RUN_DAILY  && FREQ_LABEL="daily"
$RUN_HOURLY && FREQ_LABEL="${FREQ_LABEL:+$FREQ_LABEL + }hourly"

echo ""
echo "══════════════════════════════════════════"
echo "  AI-Trader Historical Backtest"
echo "  Frequency : $FREQ_LABEL"
$ALL_MODELS && echo "  Models    : ALL (including disabled)"
$DRY_RUN    && echo "  Mode      : DRY RUN (no execution)"
echo "══════════════════════════════════════════"

# ── helper: show date ranges ────────────────────────────────
show_date_ranges() {
    local freq="$1"
    local all_flag="$2"
    python3 -c "
import sys, json
sys.path.insert(0, '.')
from main import (load_config, derive_signature, calculate_date_range,
                  get_latest_trading_day, get_latest_position_date)

config = load_config()
freq = '$freq'
all_models = $all_flag

latest = get_latest_trading_day(freq)
print(f'  Latest trading day in price data : {latest or \"N/A\"}')
print()
print(f'  {\"Model\":<28s}  {\"Last Position\":<22s}  {\"Backtest Range\":<s}')
print(f'  {\"─\" * 28}  {\"─\" * 22}  {\"─\" * 30}')

models = config['models']
if not all_models:
    models = [m for m in models if m.get('enabled')]

if not models:
    print('  (no models selected)')
else:
    for m in models:
        name = m.get('name', 'unknown')
        sig = derive_signature(name, freq)
        last_pos = get_latest_position_date(sig, freq)
        start, end = calculate_date_range(sig, freq)
        status = ''
        if start > end:
            status = '  (up to date)'
        print(f'  {name:<28s}  {last_pos or \"(new)\":<22s}  {start} → {end}{status}')
"
}

# ── helper: run one frequency ───────────────────────────────
run_backtest() {
    local freq="$1"
    local step_num="$2"

    echo ""
    echo "┌──────────────────────────────────────┐"
    echo "│  [$freq] Step ${step_num}a: Updating price data"
    echo "└──────────────────────────────────────┘"
    python3 start.py --only-data -f "$freq"

    echo ""
    echo "┌──────────────────────────────────────┐"
    echo "│  [$freq] Date ranges"
    echo "└──────────────────────────────────────┘"
    local all_py="True"
    $ALL_MODELS || all_py="False"
    show_date_ranges "$freq" "$all_py"

    if $DRY_RUN; then
        echo ""
        echo "  [DRY RUN] Skipping execution."
        return 0
    fi

    echo ""
    echo "┌──────────────────────────────────────┐"
    echo "│  [$freq] Step ${step_num}b: Running backtest"
    echo "└──────────────────────────────────────┘"

    if $ALL_MODELS; then
        # Run ALL models (including disabled) via inline Python
        python3 -c "
import sys, asyncio
sys.path.insert(0, '.')
from main import (load_config, derive_signature, calculate_date_range,
                  derive_agent_type, derive_log_path, get_agent_class,
                  DEFAULT_MAX_STEPS, DEFAULT_MAX_RETRIES,
                  DEFAULT_BASE_DELAY, DEFAULT_INITIAL_CASH)
from tools.general_tools import write_config_value
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('backtest')

async def run_all():
    config = load_config()
    freq = '$freq'
    agent_type = derive_agent_type(freq, config)
    log_path = derive_log_path(freq)
    AgentClass = get_agent_class(agent_type, config)
    market = config.get('market', 'cn')

    for model_config in config['models']:
        name = model_config.get('name', 'unknown')
        basemodel = model_config.get('basemodel')
        if not basemodel:
            continue
        sig = derive_signature(name, freq)
        init_date, end_date = calculate_date_range(sig, freq)
        if init_date > end_date:
            logger.info('Model %s (%s): already up to date, skipping', name, sig)
            continue

        logger.info('=' * 60)
        logger.info('Model: %s | Sig: %s | Range: %s -> %s', name, sig, init_date, end_date)

        write_config_value('SIGNATURE', sig)
        write_config_value('IF_TRADE', False)
        write_config_value('MARKET', market)
        write_config_value('LOG_PATH', log_path)

        agent = AgentClass(
            signature=sig, basemodel=basemodel, stock_symbols=None,
            log_path=log_path, max_steps=DEFAULT_MAX_STEPS,
            max_retries=DEFAULT_MAX_RETRIES, base_delay=DEFAULT_BASE_DELAY,
            initial_cash=DEFAULT_INITIAL_CASH, init_date=init_date,
            openai_base_url=model_config.get('openai_base_url'),
            openai_api_key=model_config.get('openai_api_key'),
        )
        await agent.initialize()
        await agent.run_date_range(init_date, end_date)
        logger.info('Model %s completed', name)

    logger.info('All models done!')

asyncio.run(run_all())
"
    else
        python3 start.py --skip-backend -f "$freq"
    fi
}

# ── main flow ───────────────────────────────────────────────
STEP=1

if $RUN_DAILY; then
    run_backtest "daily" "$STEP"
    STEP=$((STEP + 1))
fi

if $RUN_HOURLY; then
    run_backtest "hourly" "$STEP"
fi

echo ""
echo "══════════════════════════════════════════"
if $DRY_RUN; then
    echo "  DRY RUN complete — no backtests executed"
else
    echo "  All backtests complete!"
fi
echo "══════════════════════════════════════════"
