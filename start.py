#!/usr/bin/env python3
"""
AI-Trader Unified Startup Script (A-Stock Only)
Consolidates data preparation, backend services, and trading agent into one script.

Architecture (v2.1):
- Single FastAPI backend hosts REST API + MCP services + Scheduler on port 8888
- Supports both backtest mode (date range) and live trading mode (scheduled)
- Agent connects to unified backend at http://localhost:8888

Usage:
    python start.py                           # Default: A-stock daily backtest
    python start.py -f hourly                 # A-stock hourly backtest
    python start.py --skip-data               # Skip data preparation
    python start.py --only-backend            # Only start unified backend
    python start.py --only-agent              # Only start agent (backend must be running)
    python start.py --ui                      # Also start static web UI
    python start.py --debug                   # Verbose debug output
    python start.py --legacy-mcp              # Use legacy separate MCP services

    # New: Live Trading Mode
    python start.py --live                    # Start backend + live trading scheduler
    python start.py --live -f hourly          # Live trading with hourly frequency

Live Trading Mode (--live):
    Starts the unified backend with live trading scheduler.
    - Scheduler automatically executes trading sessions at configured times
    - Daily: 09:35 (5 min after market open)
    - Hourly: 10:35, 11:35, 14:05, 15:05
    - Uses "-live" suffix in signatures to separate from backtest data

Legacy mode (--legacy-mcp):
    Uses the old architecture with separate MCP service processes on ports 8000-8003.
    Set UNIFIED_MCP_MODE=false to make agents use legacy mode by default.
"""

import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

# Project root directory
PROJECT_ROOT = Path(__file__).parent.absolute()


class Colors:
    """ANSI color codes for terminal output"""
    RESET = "\033[0m"
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"


def log(msg: str, level: str = "info"):
    """Print colored log message"""
    icons = {
        "info": f"{Colors.BLUE}[INFO]{Colors.RESET}",
        "success": f"{Colors.GREEN}[OK]{Colors.RESET}",
        "warning": f"{Colors.YELLOW}[WARN]{Colors.RESET}",
        "error": f"{Colors.RED}[ERROR]{Colors.RESET}",
        "step": f"{Colors.MAGENTA}[STEP]{Colors.RESET}",
        "debug": f"{Colors.CYAN}[DEBUG]{Colors.RESET}",
    }
    print(f"{icons.get(level, icons['info'])} {msg}")


def run_command(cmd: list, cwd: Optional[Path] = None, capture: bool = False, debug: bool = False):
    """Run a command and handle errors"""
    if debug:
        log(f"Running: {' '.join(cmd)}", "debug")

    try:
        if capture:
            result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
            return result.returncode == 0, result.stdout, result.stderr
        else:
            result = subprocess.run(cmd, cwd=cwd)
            return result.returncode == 0, "", ""
    except Exception as e:
        return False, "", str(e)


def activate_venv():
    """Check and activate virtual environment"""
    venv_path = PROJECT_ROOT / ".venv"
    if not venv_path.exists():
        log("Virtual environment .venv not found!", "error")
        log("Please create it with: python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt", "info")
        return False

    # Update PATH for subprocess calls
    venv_bin = venv_path / "bin"
    os.environ["PATH"] = str(venv_bin) + os.pathsep + os.environ.get("PATH", "")
    os.environ["VIRTUAL_ENV"] = str(venv_path)

    log(f"Using virtual environment: {venv_path}", "success")
    return True


def get_config_path(freq: str) -> Path:
    """Get config file path (unified config for all frequencies)"""
    return PROJECT_ROOT / "configs" / "config.json"


def validate_data(freq: str, fix_missing: bool = False, debug: bool = False) -> bool:
    """Validate data completeness after preparation

    Args:
        freq: Trading frequency ("daily" or "hourly")
        fix_missing: Whether to automatically fix missing data
        debug: Enable debug output

    Returns:
        True if data is valid or was fixed successfully
    """
    log("Validating data completeness...", "step")

    data_dir = PROJECT_ROOT / "data" / "A_stock"
    validate_script = data_dir / "validate_data.py"

    if not validate_script.exists():
        log("Validation script not found, skipping validation", "warning")
        return True

    cmd = [sys.executable, str(validate_script), "-f", freq]
    success, stdout, stderr = run_command(cmd, cwd=data_dir, capture=True, debug=debug)

    if debug:
        print(stdout)

    # Check for missing stocks
    if "缺失股票" in stdout or "MISSING" in stdout.upper():
        log("Found missing stock data!", "warning")

        if fix_missing:
            log("Attempting to fix missing data...", "info")
            fix_cmd = [
                sys.executable,
                str(data_dir / "get_daily_price_akshare.py"),
                "--fix-missing"
            ]
            fix_success, fix_stdout, fix_stderr = run_command(
                fix_cmd, cwd=data_dir, capture=True, debug=debug
            )

            if debug:
                print(fix_stdout)

            if fix_success:
                log("Missing data fixed successfully", "success")
                # Re-run merge to update JSONL
                log("Re-running merge to update JSONL...", "info")
                merge_cmd = [sys.executable, str(data_dir / "merge_jsonl.py")]
                run_command(merge_cmd, cwd=data_dir, debug=debug)
                return True
            else:
                log("Failed to fix missing data", "error")
                return False
        else:
            log("Run with --fix-missing to automatically fetch missing data", "info")
            return False

    log("Data validation passed", "success")
    return True


def prepare_data(freq: str, debug: bool = False, force: bool = False, fix_missing: bool = False):
    """Prepare A-stock data (supports incremental updates)

    Args:
        freq: Trading frequency ("daily" or "hourly")
        debug: Enable debug output
        force: Force full data refresh, ignore existing data
        fix_missing: Automatically fix missing stock data
    """
    if force:
        log("Preparing A-stock data (FORCE full refresh)...", "step")
    else:
        log("Preparing A-stock data (incremental update)...", "step")

    data_dir = PROJECT_ROOT / "data" / "A_stock"

    # Build script commands with appropriate flags
    if freq == "daily":
        scripts = [
            (["get_daily_price_akshare.py"] + (["--force"] if force else []), "Fetching daily prices"),
            (["merge_jsonl.py"], "Converting to JSONL"),
        ]
    else:
        scripts = [
            (["get_daily_price_akshare.py"] + (["--force"] if force else []), "Fetching daily prices"),
            (["merge_jsonl.py"], "Converting daily to JSONL"),
            (["get_interdaily_price_astock.py"], "Fetching hourly prices"),
            (["merge_jsonl_hourly.py"], "Converting hourly to JSONL"),
        ]

    for script_args, description in scripts:
        script_name = script_args[0]
        script_path = data_dir / script_name
        if not script_path.exists():
            log(f"Script not found: {script_path}", "error")
            return False

        log(f"{description} ({script_name})...", "info")
        cmd = [sys.executable, str(script_path)] + script_args[1:]
        success, stdout, stderr = run_command(cmd, cwd=data_dir, capture=True, debug=debug)

        # Check output for "already up to date" message
        if stdout and "数据已是最新" in stdout:
            log(f"  ✅ Data already up to date, skipped", "info")
        elif not success:
            log(f"Failed to run {script_name}: {stderr}", "error")
            return False

    # Validate data completeness after preparation
    if not validate_data(freq, fix_missing=fix_missing, debug=debug):
        log("Data validation found issues", "warning")
        if not fix_missing:
            log("Use --fix-missing to automatically fix missing data", "info")

    log("A-stock data prepared", "success")
    return True


def start_unified_backend(background: bool = True, debug: bool = False):
    """Start unified FastAPI backend (REST API + MCP services)"""
    log("Starting unified backend (FastAPI + MCP services)...", "step")

    try:
        cmd = [
            sys.executable, "-m", "uvicorn",
            "api.main:app",
            "--host", "0.0.0.0",
            "--port", "8888",
        ]

        if debug:
            cmd.append("--reload")

        if background:
            # Start in background
            log_dir = PROJECT_ROOT / "logs"
            log_dir.mkdir(exist_ok=True)

            with open(log_dir / "backend.log", "w") as log_file:
                process = subprocess.Popen(
                    cmd,
                    cwd=PROJECT_ROOT,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                )

            log(f"Unified backend started in background (PID: {process.pid})", "success")
            log(f"Backend URL: http://localhost:8888", "info")
            log(f"API Docs: http://localhost:8888/docs", "info")
            log(f"Log file: {log_dir / 'backend.log'}", "info")

            # Wait for backend to be ready
            log("Waiting for backend to initialize...", "info")
            time.sleep(3)

            return process
        else:
            # Start in foreground (blocking)
            subprocess.run(cmd, cwd=PROJECT_ROOT)
            return None
    except Exception as e:
        log(f"Failed to start unified backend: {e}", "error")
        return None


def start_legacy_mcp_services(background: bool = True, debug: bool = False):
    """Start legacy MCP services (separate processes on different ports)"""
    log("Starting legacy MCP services...", "step")
    log("Note: Consider using unified backend instead (--no-legacy-mcp)", "warning")

    # Set environment to use legacy mode
    os.environ["UNIFIED_MCP_MODE"] = "false"

    mcp_script = PROJECT_ROOT / "agent_tools" / "start_mcp_services.py"
    if not mcp_script.exists():
        log(f"MCP script not found: {mcp_script}", "error")
        return None

    try:
        if background:
            # Start in background
            log_dir = PROJECT_ROOT / "logs"
            log_dir.mkdir(exist_ok=True)

            with open(log_dir / "mcp_services.log", "w") as log_file:
                process = subprocess.Popen(
                    [sys.executable, str(mcp_script)],
                    cwd=PROJECT_ROOT / "agent_tools",
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                )

            log(f"Legacy MCP services started in background (PID: {process.pid})", "success")
            log(f"Log file: {log_dir / 'mcp_services.log'}", "info")

            # Wait for services to be ready
            log("Waiting for MCP services to initialize...", "info")
            time.sleep(3)

            return process
        else:
            # Start in foreground (blocking)
            subprocess.run(
                [sys.executable, str(mcp_script)],
                cwd=PROJECT_ROOT / "agent_tools",
            )
            return None
    except Exception as e:
        log(f"Failed to start legacy MCP services: {e}", "error")
        return None


def start_agent(config_path: Path, freq: str = "daily", debug: bool = False):
    """Start trading agent"""
    log(f"Starting trading agent with config: {config_path.name} (frequency: {freq})", "step")

    main_script = PROJECT_ROOT / "main.py"
    if not main_script.exists():
        log(f"Main script not found: {main_script}", "error")
        return False

    if not config_path.exists():
        log(f"Config file not found: {config_path}", "error")
        return False

    try:
        subprocess.run(
            [sys.executable, str(main_script), str(config_path), "-f", freq],
            cwd=PROJECT_ROOT,
        )
        return True
    except KeyboardInterrupt:
        log("Agent stopped by user", "warning")
        return True
    except Exception as e:
        log(f"Agent error: {e}", "error")
        return False


def start_ui(debug: bool = False):
    """Start static web UI server (for frontend files in docs/)"""
    log("Starting static web UI server...", "step")

    docs_dir = PROJECT_ROOT / "docs"
    if not docs_dir.exists():
        log(f"Docs directory not found: {docs_dir}", "error")
        return None

    try:
        # Use port 8080 for static files (8888 is for backend)
        process = subprocess.Popen(
            [sys.executable, "-m", "http.server", "8080"],
            cwd=docs_dir,
            stdout=subprocess.DEVNULL if not debug else None,
            stderr=subprocess.DEVNULL if not debug else None,
        )
        log(f"Static web UI started at http://localhost:8080 (PID: {process.pid})", "success")
        log("Note: Frontend should connect to API at http://localhost:8888", "info")
        return process
    except Exception as e:
        log(f"Failed to start web UI: {e}", "error")
        return None


def stop_process(process: subprocess.Popen, name: str):
    """Stop a subprocess gracefully"""
    if process and process.poll() is None:
        log(f"Stopping {name}...", "info")
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
        log(f"{name} stopped", "success")


def main():
    parser = argparse.ArgumentParser(
        description="AI-Trader Unified Startup Script (A-Stock Only)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python start.py                       # A-stock daily trading (unified backend)
  python start.py -f hourly             # A-stock hourly trading
  python start.py --skip-data           # Skip data preparation
  python start.py --only-backend        # Only start unified backend
  python start.py --only-agent          # Only start agent
  python start.py -c configs/my.json    # Use custom config file
  python start.py --ui                  # Also start static web UI
  python start.py --debug               # Enable debug output
  python start.py --legacy-mcp          # Use legacy separate MCP services
  python start.py --validate-only       # Only validate data completeness
  python start.py --fix-missing         # Auto-fix missing stock data
        """,
    )

    parser.add_argument(
        "-f", "--freq",
        choices=["daily", "hourly"],
        default="daily",
        help="Trading frequency: daily or hourly (default: daily)",
    )

    parser.add_argument(
        "-c", "--config",
        type=str,
        help="Path to custom config file (overrides -m and -f)",
    )

    parser.add_argument(
        "--skip-data",
        action="store_true",
        help="Skip data preparation step",
    )

    parser.add_argument(
        "--skip-backend",
        action="store_true",
        help="Skip backend startup (assume already running)",
    )

    parser.add_argument(
        "--skip-agent",
        action="store_true",
        help="Skip agent startup",
    )

    parser.add_argument(
        "--only-backend",
        action="store_true",
        help="Only start unified backend (foreground, blocking)",
    )

    parser.add_argument(
        "--only-agent",
        action="store_true",
        help="Only start agent (skip data and backend)",
    )

    parser.add_argument(
        "--only-data",
        action="store_true",
        help="Only prepare data (skip backend and agent)",
    )

    parser.add_argument(
        "--force-data",
        action="store_true",
        help="Force full data refresh (ignore existing data)",
    )

    parser.add_argument(
        "--fix-missing",
        action="store_true",
        help="Automatically fix missing stock data during validation",
    )

    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only validate data completeness, don't prepare or run agent",
    )

    parser.add_argument(
        "--ui",
        action="store_true",
        help="Start static web UI server",
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output",
    )

    parser.add_argument(
        "--legacy-mcp",
        action="store_true",
        help="Use legacy separate MCP services instead of unified backend",
    )

    # Mode selection (mutually exclusive)
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--backtest",
        action="store_true",
        help="Backtest mode: run agents over date range (default behavior)",
    )
    mode_group.add_argument(
        "--live",
        action="store_true",
        help="Live trading mode: start backend with scheduler for real-time trading",
    )

    # Keep old argument names for backwards compatibility
    parser.add_argument("--skip-mcp", action="store_true", dest="skip_backend", help=argparse.SUPPRESS)
    parser.add_argument("--only-mcp", action="store_true", help=argparse.SUPPRESS)

    args = parser.parse_args()

    # Handle deprecated --only-mcp
    if args.only_mcp:
        log("--only-mcp is deprecated, use --only-backend or --legacy-mcp --only-backend", "warning")
        args.only_backend = True
        args.legacy_mcp = True

    # Print banner
    mode_str = "Live Trading" if args.live else "Backtest"
    print(f"\n{Colors.BOLD}{Colors.CYAN}=" * 50)
    print(f"     AI-Trader Unified Startup Script v2.1")
    print(f"              Mode: {mode_str}")
    print("=" * 50 + f"{Colors.RESET}\n")

    # Validate arguments
    if args.only_backend and (args.only_agent or args.only_data):
        log("Cannot use --only-backend with --only-agent or --only-data", "error")
        return 1

    # Activate virtual environment
    if not activate_venv():
        return 1

    # Set unified mode environment variable
    if args.legacy_mcp:
        os.environ["UNIFIED_MCP_MODE"] = "false"
        log("Using legacy MCP mode (separate services)", "info")
    else:
        os.environ["UNIFIED_MCP_MODE"] = "true"
        log("Using unified backend mode", "info")

    # Determine config path
    if args.config:
        config_path = Path(args.config)
        if not config_path.is_absolute():
            config_path = PROJECT_ROOT / config_path
    else:
        config_path = get_config_path(args.freq)
        if not config_path:
            return 1

    log(f"Market: A-Stock (CN), Frequency: {args.freq}", "info")
    log(f"Config: {config_path}", "info")

    backend_process = None
    ui_process = None

    def cleanup(signum=None, frame=None):
        """Cleanup on exit"""
        print()  # New line after ^C
        stop_process(backend_process, "Backend services")
        stop_process(ui_process, "Web UI")
        log("Cleanup complete", "success")
        sys.exit(0)

    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    try:
        # Handle --only-* options
        if args.only_backend:
            log("Starting backend in foreground...", "step")
            if args.legacy_mcp:
                start_legacy_mcp_services(background=False, debug=args.debug)
            else:
                start_unified_backend(background=False, debug=args.debug)
            return 0

        if args.validate_only:
            log("Running data validation only...", "step")
            is_valid = validate_data(args.freq, fix_missing=args.fix_missing, debug=args.debug)
            if is_valid:
                log("Data validation passed", "success")
                return 0
            else:
                log("Data validation failed", "error")
                return 1

        if args.only_data:
            if not prepare_data(args.freq, args.debug, force=args.force_data, fix_missing=args.fix_missing):
                return 1
            log("Data preparation complete", "success")
            return 0

        if args.only_agent:
            if not start_agent(config_path, args.freq, args.debug):
                return 1
            return 0

        # Live trading mode
        if args.live:
            log("Starting Live Trading Mode...", "step")
            log(f"Frequency: {args.freq}", "info")
            log("Scheduler will auto-start when backend is ready", "info")
            log("", "info")
            log("API endpoints for scheduler control:", "info")
            log("  GET  /api/live-trading/status  - Get scheduler status", "info")
            log("  POST /api/live-trading/stop    - Stop scheduler", "info")
            log("  POST /api/live-trading/start   - Restart scheduler", "info")
            log("  POST /api/live-trading/trigger - Trigger immediate execution", "info")
            log("", "info")

            # Set environment variables to signal live mode (read by api/main.py)
            os.environ["AI_TRADER_MODE"] = "live"
            os.environ["AI_TRADER_FREQUENCY"] = args.freq

            # Start backend in foreground (blocking) - scheduler auto-starts
            start_unified_backend(background=False, debug=args.debug)
            return 0

        # Backtest mode (default)

        # Step 1: Data preparation
        if not args.skip_data:
            if not prepare_data(args.freq, args.debug, force=args.force_data, fix_missing=args.fix_missing):
                log("Data preparation failed, but continuing...", "warning")
        else:
            log("Skipping data preparation", "info")

        # Step 2: Backend services
        if not args.skip_backend:
            if args.legacy_mcp:
                backend_process = start_legacy_mcp_services(background=True, debug=args.debug)
            else:
                backend_process = start_unified_backend(background=True, debug=args.debug)

            if not backend_process:
                log("Failed to start backend services", "error")
                return 1
        else:
            log("Skipping backend startup", "info")

        # Step 3: Agent (backtest)
        if not args.skip_agent:
            if not start_agent(config_path, args.freq, args.debug):
                log("Agent execution failed", "error")
                cleanup()
                return 1
        else:
            log("Skipping agent startup", "info")

        # Step 4: Web UI (optional)
        if args.ui:
            ui_process = start_ui(args.debug)
            if ui_process:
                log("Press Ctrl+C to stop...", "info")
                ui_process.wait()

        # Cleanup
        cleanup()
        return 0

    except Exception as e:
        log(f"Unexpected error: {e}", "error")
        cleanup()
        return 1


if __name__ == "__main__":
    sys.exit(main())
