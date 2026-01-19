#!/usr/bin/env python3
"""
AI-Trader Unified Startup Script
Consolidates data preparation, MCP services, and trading agent into one script.

Usage:
    python start.py                           # Default: US stock daily
    python start.py -m cn -f hourly           # A-stock hourly
    python start.py -m crypto                 # Crypto
    python start.py --skip-data               # Skip data preparation
    python start.py --only-mcp                # Only start MCP services
    python start.py --only-agent              # Only start agent (MCP must be running)
    python start.py --ui                      # Start web UI after agent
    python start.py --debug                   # Verbose debug output
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


def get_config_path(market: str, freq: str) -> Path:
    """Get config file path based on market and frequency"""
    config_map = {
        ("us", "daily"): "configs/default_config.json",
        ("us", "hourly"): "configs/default_hour_config.json",
        ("cn", "daily"): "configs/astock_config.json",
        ("cn", "hourly"): "configs/astock_hour_config.json",
        ("crypto", "daily"): "configs/default_crypto_config.json",
        ("crypto", "hourly"): "configs/default_crypto_config.json",  # Crypto doesn't have hourly
    }

    config_file = config_map.get((market, freq))
    if not config_file:
        log(f"Unknown market/freq combination: {market}/{freq}", "error")
        return None

    return PROJECT_ROOT / config_file


def prepare_data_us(freq: str, debug: bool = False):
    """Prepare US stock data"""
    log("Preparing US stock data...", "step")
    data_dir = PROJECT_ROOT / "data"

    if freq == "daily":
        scripts = ["get_daily_price.py", "merge_jsonl.py"]
    else:
        scripts = ["get_interdaily_price.py", "merge_jsonl.py"]

    for script in scripts:
        script_path = data_dir / script
        if not script_path.exists():
            log(f"Script not found: {script_path}", "error")
            return False

        log(f"Running {script}...", "info")
        success, _, stderr = run_command(
            [sys.executable, str(script_path)],
            cwd=data_dir,
            debug=debug
        )
        if not success:
            log(f"Failed to run {script}: {stderr}", "error")
            return False

    log("US stock data prepared", "success")
    return True


def prepare_data_cn(freq: str, debug: bool = False):
    """Prepare A-stock data"""
    log("Preparing A-stock data...", "step")
    data_dir = PROJECT_ROOT / "data" / "A_stock"

    if freq == "daily":
        scripts = [
            "get_daily_price_tushare.py",
            "merge_jsonl_tushare.py",
        ]
    else:
        scripts = [
            "get_daily_price_tushare.py",
            "merge_jsonl_tushare.py",
            "get_interdaily_price_astock.py",
            "merge_jsonl_hourly.py",
        ]

    for script in scripts:
        script_path = data_dir / script
        if not script_path.exists():
            log(f"Script not found: {script_path}", "error")
            return False

        log(f"Running {script}...", "info")
        success, _, stderr = run_command(
            [sys.executable, str(script_path)],
            cwd=data_dir,
            debug=debug
        )
        if not success:
            log(f"Failed to run {script}: {stderr}", "error")
            return False

    log("A-stock data prepared", "success")
    return True


def prepare_data_crypto(debug: bool = False):
    """Prepare crypto data"""
    log("Preparing crypto data...", "step")
    data_dir = PROJECT_ROOT / "data" / "crypto"

    scripts = ["get_daily_price_crypto.py", "merge_crypto_jsonl.py"]

    for script in scripts:
        script_path = data_dir / script
        if not script_path.exists():
            log(f"Script not found: {script_path}", "error")
            return False

        log(f"Running {script}...", "info")
        success, _, stderr = run_command(
            [sys.executable, str(script_path)],
            cwd=data_dir,
            debug=debug
        )
        if not success:
            log(f"Failed to run {script}: {stderr}", "error")
            return False

    log("Crypto data prepared", "success")
    return True


def prepare_data(market: str, freq: str, debug: bool = False):
    """Prepare data based on market type"""
    if market == "us":
        return prepare_data_us(freq, debug)
    elif market == "cn":
        return prepare_data_cn(freq, debug)
    elif market == "crypto":
        return prepare_data_crypto(debug)
    else:
        log(f"Unknown market: {market}", "error")
        return False


def start_mcp_services(background: bool = True, debug: bool = False):
    """Start MCP services"""
    log("Starting MCP services...", "step")

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

            log(f"MCP services started in background (PID: {process.pid})", "success")
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
        log(f"Failed to start MCP services: {e}", "error")
        return None


def start_agent(config_path: Path, debug: bool = False):
    """Start trading agent"""
    log(f"Starting trading agent with config: {config_path.name}", "step")

    main_script = PROJECT_ROOT / "main.py"
    if not main_script.exists():
        log(f"Main script not found: {main_script}", "error")
        return False

    if not config_path.exists():
        log(f"Config file not found: {config_path}", "error")
        return False

    try:
        subprocess.run(
            [sys.executable, str(main_script), str(config_path)],
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
    """Start web UI server"""
    log("Starting web UI server...", "step")

    docs_dir = PROJECT_ROOT / "docs"
    if not docs_dir.exists():
        log(f"Docs directory not found: {docs_dir}", "error")
        return None

    # Sync frontend config first
    sync_script = PROJECT_ROOT / "scripts" / "sync_frontend_config.py"
    if sync_script.exists():
        log("Syncing frontend config...", "info")
        run_command([sys.executable, str(sync_script)], cwd=PROJECT_ROOT, debug=debug)

    try:
        process = subprocess.Popen(
            [sys.executable, "-m", "http.server", "8888"],
            cwd=docs_dir,
            stdout=subprocess.DEVNULL if not debug else None,
            stderr=subprocess.DEVNULL if not debug else None,
        )
        log(f"Web UI started at http://localhost:8888 (PID: {process.pid})", "success")
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
        description="AI-Trader Unified Startup Script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python start.py                       # US stock daily trading
  python start.py -m cn                 # A-stock daily trading
  python start.py -m cn -f hourly       # A-stock hourly trading
  python start.py -m crypto             # Crypto trading
  python start.py --skip-data           # Skip data preparation
  python start.py --only-mcp            # Only start MCP services
  python start.py --only-agent          # Only start agent
  python start.py -c configs/my.json    # Use custom config file
  python start.py --ui                  # Also start web UI
  python start.py --debug               # Enable debug output
        """,
    )

    parser.add_argument(
        "-m", "--market",
        choices=["us", "cn", "crypto"],
        default="us",
        help="Market type: us (US stock), cn (A-stock), crypto (default: us)",
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
        "--skip-mcp",
        action="store_true",
        help="Skip MCP services startup (assume already running)",
    )

    parser.add_argument(
        "--skip-agent",
        action="store_true",
        help="Skip agent startup",
    )

    parser.add_argument(
        "--only-mcp",
        action="store_true",
        help="Only start MCP services (foreground, blocking)",
    )

    parser.add_argument(
        "--only-agent",
        action="store_true",
        help="Only start agent (skip data and MCP)",
    )

    parser.add_argument(
        "--only-data",
        action="store_true",
        help="Only prepare data (skip MCP and agent)",
    )

    parser.add_argument(
        "--ui",
        action="store_true",
        help="Start web UI after agent finishes",
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output",
    )

    args = parser.parse_args()

    # Print banner
    print(f"\n{Colors.BOLD}{Colors.CYAN}=" * 50)
    print("     AI-Trader Unified Startup Script")
    print("=" * 50 + f"{Colors.RESET}\n")

    # Validate arguments
    if args.only_mcp and (args.only_agent or args.only_data):
        log("Cannot use --only-mcp with --only-agent or --only-data", "error")
        return 1

    # Activate virtual environment
    if not activate_venv():
        return 1

    # Determine config path
    if args.config:
        config_path = Path(args.config)
        if not config_path.is_absolute():
            config_path = PROJECT_ROOT / config_path
    else:
        config_path = get_config_path(args.market, args.freq)
        if not config_path:
            return 1

    log(f"Market: {args.market.upper()}, Frequency: {args.freq}", "info")
    log(f"Config: {config_path}", "info")

    mcp_process = None
    ui_process = None

    def cleanup(signum=None, frame=None):
        """Cleanup on exit"""
        print()  # New line after ^C
        stop_process(mcp_process, "MCP services")
        stop_process(ui_process, "Web UI")
        log("Cleanup complete", "success")
        sys.exit(0)

    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    try:
        # Handle --only-* options
        if args.only_mcp:
            log("Starting MCP services in foreground...", "step")
            start_mcp_services(background=False, debug=args.debug)
            return 0

        if args.only_data:
            if not prepare_data(args.market, args.freq, args.debug):
                return 1
            log("Data preparation complete", "success")
            return 0

        if args.only_agent:
            if not start_agent(config_path, args.debug):
                return 1
            return 0

        # Full startup flow

        # Step 1: Data preparation
        if not args.skip_data:
            if not prepare_data(args.market, args.freq, args.debug):
                log("Data preparation failed, but continuing...", "warning")
        else:
            log("Skipping data preparation", "info")

        # Step 2: MCP services
        if not args.skip_mcp:
            mcp_process = start_mcp_services(background=True, debug=args.debug)
            if not mcp_process:
                log("Failed to start MCP services", "error")
                return 1
        else:
            log("Skipping MCP services startup", "info")

        # Step 3: Agent
        if not args.skip_agent:
            if not start_agent(config_path, args.debug):
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
