"""
Scheduler Service

Manages APScheduler lifecycle within FastAPI backend for live trading.
Integrates with AgentRunnerService for execution and handles price data updates.
"""

import asyncio
import importlib
import json
import os
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from api.services.trading_mode import (
    TradingMode,
    derive_agent_type,
    derive_log_path,
    generate_signature,
)

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent.parent


@dataclass
class SchedulerStatus:
    """Scheduler status information"""
    running: bool = False
    frequency: Optional[str] = None
    market: str = "cn"
    started_at: Optional[datetime] = None
    jobs: List[Dict[str, Any]] = field(default_factory=list)
    next_runs: List[Dict[str, Any]] = field(default_factory=list)
    last_execution: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None


class SchedulerService:
    """
    Manages live trading scheduler lifecycle.

    Features:
    - APScheduler integration for cron-based scheduling
    - Price data updates before trading sessions
    - Agent execution via dynamic import
    - Status tracking and reporting
    """

    # A-Stock trading schedules
    ASTOCK_DAILY_TIME = (9, 35)  # 5 minutes after market open
    ASTOCK_HOURLY_TIMES = [
        (10, 35),  # After 10:30 candle
        (11, 35),  # After 11:30 candle
        (14, 5),   # After 14:00 candle
        (15, 5),   # After 15:00 candle (market close)
    ]

    def __init__(self):
        self._scheduler: Optional[AsyncIOScheduler] = None
        self._status = SchedulerStatus()
        self._config: Optional[Dict[str, Any]] = None
        self._tz = pytz.timezone("Asia/Shanghai")
        self._lock = asyncio.Lock()

    @property
    def is_running(self) -> bool:
        """Check if scheduler is running"""
        return self._scheduler is not None and self._scheduler.running

    async def start_scheduler(
        self,
        config: Dict[str, Any],
        frequency: str,
        market: str = "cn"
    ) -> SchedulerStatus:
        """
        Start the live trading scheduler.

        Args:
            config: Configuration dictionary with models and settings
            frequency: Trading frequency ("daily" or "hourly")
            market: Market type (default: "cn")

        Returns:
            Current scheduler status
        """
        async with self._lock:
            if self.is_running:
                self._status.error_message = "Scheduler is already running"
                return self._status

            try:
                self._config = config
                self._status.frequency = frequency
                self._status.market = market

                # Create scheduler
                self._scheduler = AsyncIOScheduler(timezone=self._tz)

                # Add jobs based on frequency
                if frequency == "daily":
                    self._add_daily_job()
                elif frequency == "hourly":
                    self._add_hourly_jobs()
                else:
                    raise ValueError(f"Invalid frequency: {frequency}")

                # Start scheduler
                self._scheduler.start()

                # Update status
                self._status.running = True
                self._status.started_at = datetime.now(self._tz)
                self._status.error_message = None
                self._update_job_info()

                print(f"[SchedulerService] Started for {market} market, {frequency} frequency")

            except Exception as e:
                self._status.error_message = str(e)
                self._status.running = False
                if self._scheduler:
                    self._scheduler.shutdown(wait=False)
                    self._scheduler = None

            return self._status

    async def stop_scheduler(self) -> SchedulerStatus:
        """
        Stop the live trading scheduler.

        Returns:
            Current scheduler status
        """
        async with self._lock:
            if not self.is_running:
                self._status.error_message = "Scheduler is not running"
                return self._status

            try:
                self._scheduler.shutdown(wait=True)
                self._scheduler = None

                self._status.running = False
                self._status.jobs = []
                self._status.next_runs = []
                self._status.error_message = None

                print("[SchedulerService] Stopped")

            except Exception as e:
                self._status.error_message = str(e)

            return self._status

    async def get_status(self) -> SchedulerStatus:
        """
        Get current scheduler status.

        Returns:
            Current scheduler status
        """
        if self.is_running:
            self._update_job_info()
        return self._status

    async def trigger_now(self) -> Dict[str, Any]:
        """
        Manually trigger a trading session immediately.
        Useful for testing.

        Returns:
            Execution result
        """
        if not self._config:
            return {"success": False, "error": "No configuration loaded. Start scheduler first."}

        try:
            await self._run_live_trading_session()
            return {"success": True, "message": "Trading session triggered"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _add_daily_job(self):
        """Add daily trading job"""
        hour, minute = self.ASTOCK_DAILY_TIME

        self._scheduler.add_job(
            self._run_live_trading_session,
            CronTrigger(
                hour=hour,
                minute=minute,
                day_of_week="mon-fri",
                timezone=self._tz,
            ),
            id="live_trading_daily",
            name="Live Trading (Daily)",
            replace_existing=True,
        )
        print(f"[SchedulerService] Added daily job: {hour:02d}:{minute:02d} (Mon-Fri)")

    def _add_hourly_jobs(self):
        """Add hourly trading jobs"""
        for hour, minute in self.ASTOCK_HOURLY_TIMES:
            job_id = f"live_trading_hourly_{hour:02d}{minute:02d}"
            self._scheduler.add_job(
                self._run_live_trading_session,
                CronTrigger(
                    hour=hour,
                    minute=minute,
                    day_of_week="mon-fri",
                    timezone=self._tz,
                ),
                id=job_id,
                name=f"Live Trading ({hour:02d}:{minute:02d})",
                replace_existing=True,
            )

        times_str = ", ".join([f"{h:02d}:{m:02d}" for h, m in self.ASTOCK_HOURLY_TIMES])
        print(f"[SchedulerService] Added hourly jobs: {times_str} (Mon-Fri)")

    def _update_job_info(self):
        """Update job information in status"""
        if not self._scheduler:
            return

        jobs = self._scheduler.get_jobs()
        self._status.jobs = [
            {
                "id": job.id,
                "name": job.name,
                "trigger": str(job.trigger),
            }
            for job in jobs
        ]

        self._status.next_runs = [
            {
                "id": job.id,
                "name": job.name,
                "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
            }
            for job in jobs
            if job.next_run_time
        ]

    async def _run_live_trading_session(self):
        """Execute a live trading session"""
        now = datetime.now(self._tz)
        frequency = self._status.frequency or "daily"
        market = self._status.market

        print(f"\n{'=' * 60}")
        print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] Live Trading Session Started")
        print(f"{'=' * 60}")

        execution_result = {
            "started_at": now.isoformat(),
            "completed_at": None,
            "models_executed": [],
            "errors": [],
        }

        try:
            # Step 1: Update price data
            print("\n[Step 1] Updating price data...")
            price_update_success = await self._update_prices(market, frequency)
            if not price_update_success:
                print("[Warning] Price update failed, continuing with existing data...")

            # Step 2: Get trading date/time
            if frequency == "daily":
                today_date = now.strftime("%Y-%m-%d")
            else:
                # Align to trading hours for hourly
                hour = now.hour
                if hour == 10:
                    aligned_time = "10:30:00"
                elif hour == 11:
                    aligned_time = "11:30:00"
                elif hour == 14:
                    aligned_time = "14:00:00"
                elif hour == 15:
                    aligned_time = "15:00:00"
                else:
                    aligned_time = f"{hour:02d}:00:00"
                today_date = now.strftime(f"%Y-%m-%d {aligned_time}")

            print(f"[Step 2] Trading date/time: {today_date}")

            # Step 3: Execute trading for each enabled model
            print("\n[Step 3] Executing trading sessions...")
            enabled_models = [
                m for m in self._config.get("models", [])
                if m.get("enabled", False)
            ]

            if not enabled_models:
                print("[Warning] No enabled models found")
            else:
                for model_config in enabled_models:
                    model_name = model_config.get("name", "unknown")
                    try:
                        print(f"\n  [Model] {model_name}")
                        await self._execute_single_model(model_config, today_date, frequency, market)
                        execution_result["models_executed"].append(model_name)
                        print(f"  [Model] {model_name} - Completed")
                    except Exception as e:
                        error_msg = f"{model_name}: {str(e)}"
                        execution_result["errors"].append(error_msg)
                        print(f"  [Model] {model_name} - Failed: {e}")

            execution_result["completed_at"] = datetime.now(self._tz).isoformat()
            self._status.last_execution = execution_result

            print(f"\n{'=' * 60}")
            print(f"[Live Trading Session Completed]")
            print(f"  Models: {len(execution_result['models_executed'])}")
            print(f"  Errors: {len(execution_result['errors'])}")
            if self.is_running:
                self._update_job_info()
                if self._status.next_runs:
                    print(f"  Next run: {self._status.next_runs[0]['next_run']}")
            print(f"{'=' * 60}\n")

        except Exception as e:
            execution_result["errors"].append(str(e))
            execution_result["completed_at"] = datetime.now(self._tz).isoformat()
            self._status.last_execution = execution_result
            print(f"\n[ERROR] Trading session failed: {e}")
            import traceback
            traceback.print_exc()

    async def _update_prices(self, market: str, frequency: str) -> bool:
        """
        Update price data before trading.

        Args:
            market: Market type
            frequency: Trading frequency

        Returns:
            True if successful, False otherwise
        """
        try:
            # Import and call the price update function
            from data.fetch_realtime import update_realtime_prices
            return await update_realtime_prices(market, frequency)
        except ImportError:
            # Fallback: Run data scripts directly
            print("[Warning] fetch_realtime module not found, using script fallback")
            return await self._run_data_scripts(frequency)
        except Exception as e:
            print(f"[Error] Price update failed: {e}")
            return False

    async def _run_data_scripts(self, frequency: str) -> bool:
        """
        Fallback: Run data preparation scripts directly.

        Args:
            frequency: Trading frequency

        Returns:
            True if successful, False otherwise
        """
        data_dir = PROJECT_ROOT / "data" / "A_stock"

        scripts = ["get_daily_price_akshare.py", "merge_jsonl.py"]
        if frequency == "hourly":
            scripts.extend(["get_interdaily_price_astock.py", "merge_jsonl_hourly.py"])

        try:
            for script in scripts:
                script_path = data_dir / script
                if script_path.exists():
                    print(f"  Running {script}...")
                    result = subprocess.run(
                        [sys.executable, str(script_path)],
                        cwd=data_dir,
                        capture_output=True,
                        text=True,
                        timeout=300,
                    )
                    if result.returncode != 0:
                        print(f"  [Warning] {script} returned non-zero: {result.stderr}")
            return True
        except Exception as e:
            print(f"  [Error] Script execution failed: {e}")
            return False

    async def _execute_single_model(
        self,
        model_config: Dict[str, Any],
        today_date: str,
        frequency: str,
        market: str
    ):
        """
        Execute a single model's trading session.

        Args:
            model_config: Model configuration
            today_date: Trading date/time string
            frequency: Trading frequency
            market: Market type
        """
        from tools.general_tools import write_config_value

        model_name = model_config.get("name", "unknown")
        basemodel = model_config.get("basemodel")
        openai_base_url = model_config.get("openai_base_url")
        openai_api_key = model_config.get("openai_api_key")

        if not basemodel:
            raise ValueError(f"Model {model_name} missing basemodel field")

        # Generate signature using live mode
        signature = generate_signature(model_name, frequency, TradingMode.LIVE)
        log_path = derive_log_path(frequency)
        agent_type = derive_agent_type(frequency)

        # Write runtime config
        write_config_value("SIGNATURE", signature)
        write_config_value("IF_TRADE", False)
        write_config_value("MARKET", market)
        write_config_value("LOG_PATH", log_path)

        # Get agent class
        agent_info = {
            "BaseAgentAStock": {
                "module": "agent.base_agent_astock.base_agent_astock",
                "class": "BaseAgentAStock"
            },
            "BaseAgentAStock_Hour": {
                "module": "agent.base_agent_astock.base_agent_astock_hour",
                "class": "BaseAgentAStock_Hour"
            }
        }

        if agent_type not in agent_info:
            raise ValueError(f"Unsupported agent type: {agent_type}")

        info = agent_info[agent_type]
        module = importlib.import_module(info["module"])
        AgentClass = getattr(module, info["class"])

        # Create agent instance
        agent = AgentClass(
            signature=signature,
            basemodel=basemodel,
            stock_symbols=None,
            log_path=log_path,
            max_steps=30,
            max_retries=3,
            base_delay=1.0,
            initial_cash=100000.0,
            init_date=today_date.split()[0],
            openai_base_url=openai_base_url,
            openai_api_key=openai_api_key,
        )

        # Initialize and run single trading session
        await agent.initialize()
        await agent.run_trading_session(today_date)

        # Get summary
        summary = agent.get_position_summary()
        print(f"    Cash: {summary.get('positions', {}).get('CASH', 0):,.2f}")


# Singleton instance
_scheduler_instance: Optional[SchedulerService] = None


def get_scheduler_service() -> SchedulerService:
    """Get or create the scheduler service singleton"""
    global _scheduler_instance
    if _scheduler_instance is None:
        _scheduler_instance = SchedulerService()
    return _scheduler_instance
