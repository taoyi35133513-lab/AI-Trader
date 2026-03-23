"""
Scheduler Service

Manages APScheduler lifecycle within FastAPI backend for live trading.
Integrates with AgentRunnerService for execution and handles price data updates.
"""

import asyncio
import importlib
import json
import logging
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

logger = logging.getLogger(__name__)

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
        Start the live trading scheduler or add jobs to an existing one.

        If the scheduler is already running, new frequency jobs are added
        so that daily and hourly can coexist in the same scheduler.

        Args:
            config: Configuration dictionary with models and settings
            frequency: Trading frequency ("daily" or "hourly")
            market: Market type (default: "cn")

        Returns:
            Current scheduler status
        """
        async with self._lock:
            try:
                self._config = config
                self._status.market = market

                if not self.is_running:
                    # Create and start a new scheduler
                    self._scheduler = AsyncIOScheduler(timezone=self._tz)
                    self._status.frequency = frequency

                    if frequency == "daily":
                        self._add_daily_job()
                    elif frequency == "hourly":
                        self._add_hourly_jobs()
                    else:
                        raise ValueError(f"Invalid frequency: {frequency}")

                    self._scheduler.start()
                    self._status.running = True
                    self._status.started_at = datetime.now(self._tz)
                    self._status.error_message = None
                    logger.info("SchedulerService started for %s market, %s frequency", market, frequency)
                else:
                    # Scheduler already running — add jobs for the new frequency
                    existing_freq = self._status.frequency or ""
                    if frequency in existing_freq.split("+"):
                        self._status.error_message = f"Frequency '{frequency}' already scheduled"
                        return self._status

                    if frequency == "daily":
                        self._add_daily_job()
                    elif frequency == "hourly":
                        self._add_hourly_jobs()
                    else:
                        raise ValueError(f"Invalid frequency: {frequency}")

                    self._status.frequency = "+".join(
                        sorted(set(existing_freq.split("+") + [frequency]))
                    )
                    self._status.error_message = None
                    logger.info("SchedulerService added %s jobs to existing scheduler", frequency)

                self._update_job_info()

            except Exception as e:
                self._status.error_message = str(e)
                if not self.is_running and self._scheduler:
                    self._scheduler.shutdown(wait=False)
                    self._scheduler = None
                    self._status.running = False

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

                logger.info("SchedulerService stopped")

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
            misfire_grace_time=300,  # Allow 5 min delay before skipping
            kwargs={"frequency_override": "daily"},
        )
        logger.info("SchedulerService added daily job: %02d:%02d (Mon-Fri)", hour, minute)

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
                misfire_grace_time=300,  # Allow 5 min delay before skipping
                kwargs={"frequency_override": "hourly"},
            )

        times_str = ", ".join([f"{h:02d}:{m:02d}" for h, m in self.ASTOCK_HOURLY_TIMES])
        logger.info("SchedulerService added hourly jobs: %s (Mon-Fri)", times_str)

    @staticmethod
    def _resolve_env_var(value: Optional[str]) -> Optional[str]:
        """Resolve $ENV_VAR references in config values."""
        if value and isinstance(value, str) and value.startswith("$"):
            env_name = value[1:]
            resolved = os.environ.get(env_name)
            if resolved is None:
                logger.warning("Environment variable %s not set", env_name)
            return resolved
        return value

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

    async def _run_live_trading_session(self, frequency_override: Optional[str] = None):
        """Execute a live trading session"""
        now = datetime.now(self._tz)
        frequency = frequency_override or self._status.frequency or "daily"
        # If frequency contains '+' (e.g. 'daily+hourly'), fall back to daily
        if "+" in frequency:
            frequency = "daily"
        market = self._status.market

        logger.info("=" * 60)
        logger.info("Live Trading Session Started at %s", now.strftime('%Y-%m-%d %H:%M:%S'))
        logger.info("=" * 60)

        execution_result = {
            "started_at": now.isoformat(),
            "completed_at": None,
            "models_executed": [],
            "errors": [],
        }

        try:
            # Step 1: Update price data
            logger.info("[Step 1] Updating price data...")
            price_update_success = await self._update_prices(market, frequency)
            if not price_update_success:
                logger.warning("Price update failed, continuing with existing data...")

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

            logger.info("[Step 2] Trading date/time: %s", today_date)

            # Step 3: Execute trading for each enabled model
            logger.info("[Step 3] Executing trading sessions...")
            enabled_models = [
                m for m in self._config.get("models", [])
                if m.get("enabled", False)
            ]

            if not enabled_models:
                logger.warning("No enabled models found")
            else:
                for model_config in enabled_models:
                    model_name = model_config.get("name", "unknown")
                    try:
                        logger.info("Executing model: %s", model_name)
                        await self._execute_single_model(model_config, today_date, frequency, market)
                        execution_result["models_executed"].append(model_name)
                        logger.info("Model %s - Completed", model_name)
                    except Exception as e:
                        error_msg = f"{model_name}: {str(e)}"
                        execution_result["errors"].append(error_msg)
                        logger.error("Model %s - Failed: %s", model_name, e)

            # Step 4: Sync prices to DuckDB and update benchmark index
            logger.info("[Step 4] Syncing prices to database...")
            await self._sync_post_session(frequency, now)

            execution_result["completed_at"] = datetime.now(self._tz).isoformat()
            self._status.last_execution = execution_result

            logger.info("Live Trading Session Completed: Models=%d, Errors=%d",
                        len(execution_result['models_executed']), len(execution_result['errors']))
            if self.is_running:
                self._update_job_info()
                if self._status.next_runs:
                    logger.info("Next run: %s", self._status.next_runs[0]['next_run'])

        except Exception as e:
            execution_result["errors"].append(str(e))
            execution_result["completed_at"] = datetime.now(self._tz).isoformat()
            self._status.last_execution = execution_result
            logger.error("Trading session failed: %s", e, exc_info=True)

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
            logger.warning("fetch_realtime module not found, using script fallback")
            return await self._run_data_scripts(frequency)
        except Exception as e:
            logger.error("Price update failed: %s", e)
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
                    logger.info("Running %s...", script)
                    result = subprocess.run(
                        [sys.executable, str(script_path)],
                        cwd=data_dir,
                        capture_output=True,
                        text=True,
                        timeout=300,
                    )
                    if result.returncode != 0:
                        logger.warning("%s returned non-zero: %s", script, result.stderr)
            return True
        except Exception as e:
            logger.error("Script execution failed: %s", e)
            return False

    async def _sync_post_session(self, frequency: str, now: datetime):
        """Sync price data to DuckDB after a trading session.

        - For hourly: sync all hourly candles for today into DuckDB.
        - For the last session of the day (15:05 hourly or daily):
          also sync daily prices and update SSE 50 index.
        """
        trade_date = now.strftime("%Y-%m-%d")
        try:
            from data.sync_prices_db import (
                sync_daily_prices,
                sync_hourly_prices,
                update_sse50_index,
                update_sse50_hourly_index,
            )

            if frequency == "hourly":
                await asyncio.to_thread(sync_hourly_prices, trade_date)
                # Sync hourly SSE50 index for current time point
                hour = now.hour
                if hour == 10:
                    time_key = f"{trade_date} 10:30:00"
                elif hour == 11:
                    time_key = f"{trade_date} 11:30:00"
                elif hour == 14:
                    time_key = f"{trade_date} 14:00:00"
                elif hour == 15:
                    time_key = f"{trade_date} 15:00:00"
                else:
                    time_key = None
                if time_key:
                    await asyncio.to_thread(update_sse50_hourly_index, trade_date, time_key)
                # After last hourly session (15:05), also sync daily + index
                if now.hour >= 15:
                    await asyncio.to_thread(sync_daily_prices, trade_date)
                    await asyncio.to_thread(update_sse50_index, trade_date)
            else:
                # Daily session — sync daily prices and index
                await asyncio.to_thread(sync_daily_prices, trade_date)
                await asyncio.to_thread(update_sse50_index, trade_date)
        except Exception as e:
            logger.warning("Post-session price sync failed: %s", e)

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
        openai_base_url = self._resolve_env_var(model_config.get("openai_base_url"))
        openai_api_key = self._resolve_env_var(model_config.get("openai_api_key"))

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

        # Load skills for this agent
        from api.services.skills_service import get_agent_skills
        memory_agent = signature.replace("-live", "")
        skill_ids = get_agent_skills(memory_agent, market)
        if not skill_ids:
            skill_ids = model_config.get("skills", []) or []
        if skill_ids:
            logger.info("Agent %s loaded skills: %s", signature, skill_ids)

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
            init_date=today_date,
            openai_base_url=openai_base_url,
            openai_api_key=openai_api_key,
            skill_ids=skill_ids,
        )

        # Initialize agent (MCP tools, LLM)
        await agent.initialize()

        # Register agent if position file doesn't exist.
        # Try to inherit the backtest agent's last position for continuity;
        # fall back to fresh initial cash if no backtest data exists.
        if not os.path.exists(agent.position_file):
            backtest_sig = generate_signature(model_name, frequency, TradingMode.BACKTEST)
            backtest_pos_file = Path(log_path) / backtest_sig / "position" / "position.jsonl"
            inherited = False
            if backtest_pos_file.exists():
                try:
                    last_pos = None
                    with open(backtest_pos_file, "r") as f:
                        for line in f:
                            if line.strip():
                                last_pos = json.loads(line)
                    if last_pos and last_pos.get("positions"):
                        # Create the position directory
                        pos_dir = Path(agent.position_file).parent
                        pos_dir.mkdir(parents=True, exist_ok=True)
                        # Write inherited position to BOTH JSONL and DuckDB
                        from tools.data_access import PositionDataAccess
                        pos_access = PositionDataAccess()
                        init_action = {"action": "init_inherit", "symbol": "", "amount": 0}
                        pos_access.add_position_record(
                            today_date, signature, init_action, last_pos["positions"]
                        )
                        inherited = True
                        logger.info(
                            "Live agent %s inherited position from backtest %s (CASH=%.0f)",
                            signature, backtest_sig, last_pos["positions"].get("CASH", 0),
                        )
                except Exception as e:
                    logger.warning("Failed to inherit backtest position: %s", e)
            if not inherited:
                agent.register_agent()

        await agent.run_trading_session(today_date)

        # Get summary
        summary = agent.get_position_summary()
        logger.info("Cash: %s", f"{summary.get('positions', {}).get('CASH', 0):,.2f}")


# Singleton instance
_scheduler_instance: Optional[SchedulerService] = None


def get_scheduler_service() -> SchedulerService:
    """Get or create the scheduler service singleton"""
    global _scheduler_instance
    if _scheduler_instance is None:
        _scheduler_instance = SchedulerService()
    return _scheduler_instance
