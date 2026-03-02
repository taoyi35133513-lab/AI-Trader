#!/usr/bin/env bash
#
# AI-Trader Linux Server Deployment & Verification Script
#
# Usage:
#   chmod +x deploy.sh
#   ./deploy.sh              # Full deploy: install + start + nginx + verify
#   ./deploy.sh --install    # Only install dependencies
#   ./deploy.sh --start      # Only start backend service
#   ./deploy.sh --nginx      # Install and configure Nginx reverse proxy
#   ./deploy.sh --verify     # Only verify running services
#   ./deploy.sh --stop       # Stop all services
#   ./deploy.sh --live       # Deploy in live trading mode
#   ./deploy.sh -f hourly    # Deploy with hourly frequency
#
set -euo pipefail

# ─── Configuration ─────────────────────────────────────────────────────────────
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${PROJECT_DIR}/.venv"
LOG_DIR="${PROJECT_DIR}/logs"
PID_DIR="${PROJECT_DIR}/.pids"

BACKEND_PORT=8888
NGINX_CONF="${PROJECT_DIR}/nginx/ai-trader.conf"

PYTHON="${VENV_DIR}/bin/python"
UVICORN="${VENV_DIR}/bin/uvicorn"

# Defaults
FREQUENCY="daily"
MODE="backtest"
ACTION="deploy"      # deploy | install | start | verify | stop

# ─── Colors ────────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

info()    { echo -e "${BLUE}[INFO]${NC}  $*"; }
ok()      { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()     { echo -e "${RED}[ERROR]${NC} $*"; }
step()    { echo -e "\n${BOLD}${CYAN}>>> $*${NC}"; }

# ─── Parse Arguments ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --install)  ACTION="install";  shift ;;
        --start)    ACTION="start";    shift ;;
        --nginx)    ACTION="nginx";    shift ;;
        --verify)   ACTION="verify";   shift ;;
        --stop)     ACTION="stop";     shift ;;
        --live)     MODE="live";       shift ;;
        -f|--freq)  FREQUENCY="$2";    shift 2 ;;
        -h|--help)
            echo "Usage: $0 [--install|--start|--nginx|--verify|--stop] [--live] [-f daily|hourly]"
            exit 0
            ;;
        *)
            err "Unknown argument: $1"
            exit 1
            ;;
    esac
done

# ─── Helper Functions ──────────────────────────────────────────────────────────

check_python() {
    if command -v python3 &>/dev/null; then
        local ver
        ver=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
        local major minor
        major=$(echo "$ver" | cut -d. -f1)
        minor=$(echo "$ver" | cut -d. -f2)
        if [[ "$major" -ge 3 && "$minor" -ge 10 ]]; then
            ok "Python $ver found"
            return 0
        fi
    fi
    err "Python 3.10+ is required"
    return 1
}

save_pid() {
    # $1 = service name, $2 = pid
    mkdir -p "$PID_DIR"
    echo "$2" > "${PID_DIR}/$1.pid"
}

read_pid() {
    local pidfile="${PID_DIR}/$1.pid"
    if [[ -f "$pidfile" ]]; then
        cat "$pidfile"
    fi
}

is_running() {
    local pid
    pid=$(read_pid "$1")
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        return 0
    fi
    return 1
}

wait_for_port() {
    # $1 = port, $2 = timeout_seconds (default 30), $3 = service name
    local port="$1"
    local timeout="${2:-30}"
    local name="${3:-service}"
    local elapsed=0

    info "Waiting for ${name} on port ${port}..."
    while ! ss -tlnp 2>/dev/null | grep -q ":${port} " && \
          ! netstat -tlnp 2>/dev/null | grep -q ":${port} "; do
        sleep 1
        elapsed=$((elapsed + 1))
        if [[ $elapsed -ge $timeout ]]; then
            err "${name} did not start within ${timeout}s"
            return 1
        fi
    done
    ok "${name} is listening on port ${port} (${elapsed}s)"
    return 0
}

http_check() {
    # $1 = url, $2 = description
    local url="$1"
    local desc="${2:-$url}"
    local status

    if ! command -v curl &>/dev/null; then
        warn "curl not installed, skipping HTTP check for ${desc}"
        return 1
    fi

    status=$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 5 "$url" 2>/dev/null || echo "000")
    if [[ "$status" -ge 200 && "$status" -lt 400 ]]; then
        ok "${desc} -> HTTP ${status}"
        return 0
    else
        err "${desc} -> HTTP ${status}"
        return 1
    fi
}

# ─── Install ───────────────────────────────────────────────────────────────────

do_install() {
    step "1. Checking prerequisites"
    check_python

    if ! command -v git &>/dev/null; then
        err "git is required"
        exit 1
    fi
    ok "git found"

    step "2. Setting up Poetry"
    if ! command -v poetry &>/dev/null; then
        info "Installing Poetry..."
        python3 -m pip install --user poetry -q 2>/dev/null || pip3 install poetry -q
    fi
    if ! command -v poetry &>/dev/null; then
        err "Poetry installation failed. Install manually: https://python-poetry.org/docs/#installation"
        exit 1
    fi
    ok "Poetry $(poetry --version | awk '{print $NF}') found"

    step "3. Installing Python dependencies"
    # Ensure venv is created inside the project (.venv/)
    export POETRY_VIRTUALENVS_IN_PROJECT=true
    poetry install
    ok "Dependencies installed"

    step "4. Verifying .env file"
    if [[ ! -f "${PROJECT_DIR}/.env" ]]; then
        if [[ -f "${PROJECT_DIR}/.env.example" ]]; then
            warn ".env not found — copying from .env.example"
            cp "${PROJECT_DIR}/.env.example" "${PROJECT_DIR}/.env"
            warn "Please edit .env and fill in your API keys!"
        else
            warn "No .env or .env.example found — some features may not work"
        fi
    else
        ok ".env file exists"
    fi

    step "5. Creating required directories"
    mkdir -p "$LOG_DIR" "$PID_DIR"
    mkdir -p "${PROJECT_DIR}/data/agent_data_astock"
    mkdir -p "${PROJECT_DIR}/data/agent_data_astock_hour"
    mkdir -p "${PROJECT_DIR}/data/A_stock/A_stock_data"
    mkdir -p "${PROJECT_DIR}/data/database"
    ok "Directories ready"

    step "6. Running tests"
    if "$PYTHON" -m pytest "${PROJECT_DIR}/tests/" -q --tb=short 2>&1; then
        ok "All tests passed"
    else
        warn "Some tests failed — deployment will continue but check test output above"
    fi
}

# ─── Stop ──────────────────────────────────────────────────────────────────────

do_stop() {
    step "Stopping AI-Trader services"

    local stopped=0

    for service in backend frontend live_scheduler; do
        if is_running "$service"; then
            local pid
            pid=$(read_pid "$service")
            info "Stopping ${service} (PID: ${pid})..."
            kill "$pid" 2>/dev/null || true
            # Wait for graceful shutdown
            for _ in $(seq 1 10); do
                if ! kill -0 "$pid" 2>/dev/null; then
                    break
                fi
                sleep 0.5
            done
            # Force kill if still running
            if kill -0 "$pid" 2>/dev/null; then
                warn "Force killing ${service}..."
                kill -9 "$pid" 2>/dev/null || true
            fi
            rm -f "${PID_DIR}/${service}.pid"
            ok "${service} stopped"
            stopped=$((stopped + 1))
        fi
    done

    if [[ $stopped -eq 0 ]]; then
        info "No running services found"
    fi
}

# ─── Start ─────────────────────────────────────────────────────────────────────

do_start() {
    # Ensure prerequisites
    if [[ ! -x "$PYTHON" ]]; then
        err "Virtual environment not found at ${VENV_DIR}. Run: $0 --install"
        exit 1
    fi

    # Stop existing services first
    do_stop

    mkdir -p "$LOG_DIR" "$PID_DIR"

    # ── Backend ──────────────────────────────────────────────────────────────
    step "Starting backend (port ${BACKEND_PORT})"

    local backend_env=()
    backend_env+=(UNIFIED_MCP_MODE=true)
    backend_env+=(PYTHONPATH="${PROJECT_DIR}")

    if [[ "$MODE" == "live" ]]; then
        backend_env+=(AI_TRADER_MODE=live)
        backend_env+=(AI_TRADER_FREQUENCY="${FREQUENCY}")
        info "Live trading mode — scheduler will auto-start"
    fi

    env "${backend_env[@]}" \
        nohup "$UVICORN" api.main:app \
            --host 0.0.0.0 \
            --port "$BACKEND_PORT" \
            --workers 1 \
        > "${LOG_DIR}/backend.log" 2>&1 &
    save_pid "backend" $!
    info "Backend PID: $!"

    if ! wait_for_port "$BACKEND_PORT" 30 "Backend"; then
        err "Backend failed to start. Check ${LOG_DIR}/backend.log"
        tail -20 "${LOG_DIR}/backend.log" 2>/dev/null || true
        exit 1
    fi

    ok "Backend started"
    echo ""
    info "Backend API:  http://localhost:${BACKEND_PORT}"
    info "API Docs:     http://localhost:${BACKEND_PORT}/docs"
    info "Health Check: http://localhost:${BACKEND_PORT}/api/health"
    info "Frontend:     Configure Nginx with '$0 --nginx' to serve at http://localhost/"
    echo ""
    info "Logs: ${LOG_DIR}/"
    info "PIDs: ${PID_DIR}/"
    info "Stop: $0 --stop"
}

# ─── Nginx ─────────────────────────────────────────────────────────────────────

do_nginx() {
    step "Configuring Nginx reverse proxy"

    if ! command -v nginx &>/dev/null; then
        info "Installing Nginx..."
        if command -v apt-get &>/dev/null; then
            sudo apt-get update -qq && sudo apt-get install -y -qq nginx
        elif command -v yum &>/dev/null; then
            sudo yum install -y nginx
        else
            err "Cannot auto-install Nginx. Please install manually and re-run."
            exit 1
        fi
        ok "Nginx installed"
    else
        ok "Nginx already installed"
    fi

    if [[ ! -f "$NGINX_CONF" ]]; then
        err "Nginx config template not found: ${NGINX_CONF}"
        exit 1
    fi

    # Generate config with actual project path
    local target="/etc/nginx/sites-available/ai-trader"
    info "Writing Nginx config to ${target}"
    sed "s|__PROJECT_DIR__|${PROJECT_DIR}|g" "$NGINX_CONF" | sudo tee "$target" > /dev/null

    # Enable site
    sudo mkdir -p /etc/nginx/sites-enabled
    sudo ln -sf "$target" /etc/nginx/sites-enabled/ai-trader

    # Remove default site if it exists
    if [[ -f /etc/nginx/sites-enabled/default ]]; then
        sudo rm -f /etc/nginx/sites-enabled/default
        info "Removed default Nginx site"
    fi

    # Validate and reload
    if sudo nginx -t 2>&1; then
        ok "Nginx config syntax valid"
    else
        err "Nginx config syntax error — check ${target}"
        exit 1
    fi

    sudo systemctl reload nginx 2>/dev/null || sudo nginx -s reload 2>/dev/null || true
    ok "Nginx reloaded"

    echo ""
    info "Frontend:     http://<server-ip>/"
    info "Backend API:  http://<server-ip>/api/"
    info "API Docs:     http://<server-ip>/docs"
    info "Health Check: http://<server-ip>/health"
}

# ─── Verify ────────────────────────────────────────────────────────────────────

do_verify() {
    step "Verifying AI-Trader services"

    local pass=0
    local fail=0

    # ── Process checks ───────────────────────────────────────────────────────
    echo ""
    info "--- Process Status ---"

    if is_running "backend"; then
        ok "Backend process running (PID: $(read_pid backend))"
        ((pass++))
    else
        err "Backend process NOT running"
        ((fail++))
    fi

    if command -v nginx &>/dev/null && (systemctl is-active nginx &>/dev/null || pgrep nginx &>/dev/null); then
        ok "Nginx is running"
        ((pass++))
    else
        warn "Nginx not running (run: $0 --nginx)"
    fi

    # ── Port checks ──────────────────────────────────────────────────────────
    echo ""
    info "--- Port Status ---"

    if ss -tlnp 2>/dev/null | grep -q ":${BACKEND_PORT} " || \
       netstat -tlnp 2>/dev/null | grep -q ":${BACKEND_PORT} "; then
        ok "Port ${BACKEND_PORT} (backend) is open"
        ((pass++))
    else
        err "Port ${BACKEND_PORT} (backend) is NOT open"
        ((fail++))
    fi

    if ss -tlnp 2>/dev/null | grep -q ":80 " || \
       netstat -tlnp 2>/dev/null | grep -q ":80 "; then
        ok "Port 80 (Nginx) is open"
        ((pass++))
    else
        warn "Port 80 (Nginx) is not open (run: $0 --nginx)"
    fi

    # ── HTTP endpoint checks ─────────────────────────────────────────────────
    echo ""
    info "--- HTTP Endpoint Checks ---"

    if http_check "http://localhost:${BACKEND_PORT}/api/health" "Backend /api/health"; then
        ((pass++))

        # Parse health response for service details
        local health_json
        health_json=$(curl -s "http://localhost:${BACKEND_PORT}/api/health" 2>/dev/null)
        if [[ -n "$health_json" ]]; then
            echo "  $health_json" | "$PYTHON" -m json.tool 2>/dev/null || echo "  $health_json"
        fi
    else
        ((fail++))
    fi

    if http_check "http://localhost:${BACKEND_PORT}/docs" "Backend /docs (Swagger UI)"; then
        ((pass++))
    else
        ((fail++))
    fi

    if http_check "http://localhost:${BACKEND_PORT}/api/config/full" "Config API /api/config/full"; then
        ((pass++))
    else
        ((fail++))
    fi

    if http_check "http://localhost/" "Nginx frontend (port 80)"; then
        ((pass++))
    else
        warn "Nginx frontend not reachable (run: $0 --nginx)"
    fi

    # ── MCP service checks ───────────────────────────────────────────────────
    echo ""
    info "--- MCP Service Endpoints ---"

    for svc in math trade search price; do
        if http_check "http://localhost:${BACKEND_PORT}/mcp/${svc}/mcp" "MCP /${svc}"; then
            ((pass++))
        else
            ((fail++))
        fi
    done

    # ── Live scheduler check ─────────────────────────────────────────────────
    echo ""
    info "--- Live Trading Scheduler ---"

    local sched_status
    sched_status=$(curl -s "http://localhost:${BACKEND_PORT}/api/live-trading/status" 2>/dev/null || echo "{}")
    if [[ -n "$sched_status" ]]; then
        local is_running_val
        is_running_val=$(echo "$sched_status" | "$PYTHON" -c "import sys,json; print(json.load(sys.stdin).get('is_running', 'unknown'))" 2>/dev/null || echo "unknown")

        if [[ "$MODE" == "live" ]]; then
            if [[ "$is_running_val" == "True" ]]; then
                ok "Live scheduler is running"
                ((pass++))
            else
                err "Live scheduler should be running but is: ${is_running_val}"
                ((fail++))
            fi
        else
            info "Live scheduler status: ${is_running_val} (backtest mode — not required)"
        fi
    fi

    # ── Data files check ─────────────────────────────────────────────────────
    echo ""
    info "--- Data Files ---"

    local data_dir="${PROJECT_DIR}/data/A_stock"
    for f in A_stock_data/daily_prices_sse_50.csv A_stock_data/sse_50_weight.csv merged.jsonl; do
        if [[ -f "${data_dir}/${f}" ]]; then
            local size
            size=$(du -h "${data_dir}/${f}" | cut -f1)
            ok "${f} (${size})"
        else
            warn "${f} not found (run data preparation first)"
        fi
    done

    if [[ "$FREQUENCY" == "hourly" ]]; then
        for f in A_stock_data/A_stock_hourly.csv merged_hourly.jsonl; do
            if [[ -f "${data_dir}/${f}" ]]; then
                local size
                size=$(du -h "${data_dir}/${f}" | cut -f1)
                ok "${f} (${size})"
            else
                warn "${f} not found (run: python start.py -f hourly --only-data)"
            fi
        done
    fi

    # ── DuckDB check ─────────────────────────────────────────────────────────
    local db_file="${PROJECT_DIR}/data/database/ai_trader.duckdb"
    if [[ -f "$db_file" ]]; then
        local db_size
        db_size=$(du -h "$db_file" | cut -f1)
        ok "DuckDB database (${db_size})"
    else
        warn "DuckDB database not found at ${db_file}"
    fi

    # ── Summary ──────────────────────────────────────────────────────────────
    echo ""
    echo -e "${BOLD}==========================================${NC}"
    if [[ $fail -eq 0 ]]; then
        echo -e "${BOLD}${GREEN}  RESULT: ALL CHECKS PASSED (${pass} passed)${NC}"
    else
        echo -e "${BOLD}${RED}  RESULT: ${fail} FAILED, ${pass} passed${NC}"
    fi
    echo -e "${BOLD}==========================================${NC}"
    echo ""

    return "$fail"
}

# ─── Main ──────────────────────────────────────────────────────────────────────

echo -e "\n${BOLD}${CYAN}=================================================="
echo "  AI-Trader Deployment Script"
echo "  Mode: ${MODE} | Frequency: ${FREQUENCY} | Action: ${ACTION}"
echo -e "==================================================${NC}\n"

cd "$PROJECT_DIR"

case "$ACTION" in
    install)
        do_install
        ;;
    start)
        do_start
        ;;
    nginx)
        do_nginx
        ;;
    verify)
        do_verify
        ;;
    stop)
        do_stop
        ;;
    deploy)
        do_install
        do_start
        do_nginx
        echo ""
        sleep 2
        do_verify
        ;;
esac
