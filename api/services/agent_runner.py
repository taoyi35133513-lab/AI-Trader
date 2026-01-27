"""
Agent Runner Service

This module provides background task management for running trading agents.
Agents run as asyncio tasks within the FastAPI process, eliminating the need
for separate process management.
"""

import asyncio
import json
import os
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

# Add project root to path
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from tools.general_tools import write_config_value

from api.services.trading_mode import (
    TradingMode,
    derive_agent_type,
    derive_log_path,
    generate_signature,
)


class AgentStatus(str, Enum):
    """Agent running status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class AgentRun:
    """Represents a single agent run"""
    run_id: str
    model_name: str
    signature: str
    frequency: str
    status: AgentStatus = AgentStatus.PENDING
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    progress: Dict[str, Any] = field(default_factory=dict)
    task: Optional[asyncio.Task] = None
    mode: TradingMode = TradingMode.BACKTEST
    run_type: str = "manual"  # "manual" | "scheduled"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API response"""
        return {
            "run_id": self.run_id,
            "model_name": self.model_name,
            "signature": self.signature,
            "frequency": self.frequency,
            "status": self.status.value,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error_message": self.error_message,
            "progress": self.progress,
            "mode": self.mode.value,
            "run_type": self.run_type,
        }


# Default configuration values (from main.py)
DEFAULT_MAX_STEPS = 30
DEFAULT_MAX_RETRIES = 3
DEFAULT_BASE_DELAY = 1.0
DEFAULT_INITIAL_CASH = 100000.0

# Agent class mapping (from main.py)
AGENT_REGISTRY = {
    "BaseAgentAStock": {
        "module": "agent.base_agent_astock.base_agent_astock",
        "class": "BaseAgentAStock"
    },
    "BaseAgentAStock_Hour": {
        "module": "agent.base_agent_astock.base_agent_astock_hour",
        "class": "BaseAgentAStock_Hour"
    }
}


# Note: derive_agent_type, derive_log_path, and generate_signature are now
# imported from api.services.trading_mode for consistency across the codebase


def get_agent_class(agent_type: str):
    """Dynamically import and return the corresponding agent class"""
    if agent_type not in AGENT_REGISTRY:
        supported_types = ", ".join(AGENT_REGISTRY.keys())
        raise ValueError(f"Unsupported agent type: {agent_type}. Supported: {supported_types}")

    agent_info = AGENT_REGISTRY[agent_type]
    module_path = agent_info["module"]
    class_name = agent_info["class"]

    import importlib
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


class AgentRunnerService:
    """
    Manages agent execution as background tasks.

    This service maintains a registry of running and completed agent tasks,
    allowing the API to start, monitor, and cancel agent runs.
    """

    def __init__(self):
        self._runs: Dict[str, AgentRun] = {}
        self._lock = asyncio.Lock()

    async def start_agent(
        self,
        model_config: Dict[str, Any],
        frequency: str,
        init_date: str,
        end_date: str,
        market: str = "cn",
        mode: TradingMode = TradingMode.BACKTEST,
        run_type: str = "manual",
    ) -> AgentRun:
        """
        Start an agent as a background task.

        Args:
            model_config: Model configuration dict with name, basemodel, etc.
            frequency: Trading frequency (daily/hourly)
            init_date: Start date for trading
            end_date: End date for trading
            market: Market type (cn)
            mode: Trading mode (BACKTEST or LIVE)
            run_type: Type of run ("manual" or "scheduled")

        Returns:
            AgentRun object with run details
        """
        model_name = model_config.get("name", "unknown")
        signature = generate_signature(model_name, frequency, mode)
        run_id = str(uuid.uuid4())[:8]

        agent_run = AgentRun(
            run_id=run_id,
            model_name=model_name,
            signature=signature,
            frequency=frequency,
            mode=mode,
            run_type=run_type,
        )

        async with self._lock:
            self._runs[run_id] = agent_run

        # Start agent in background
        task = asyncio.create_task(
            self._run_agent(
                agent_run=agent_run,
                model_config=model_config,
                frequency=frequency,
                init_date=init_date,
                end_date=end_date,
                market=market,
            )
        )
        agent_run.task = task

        return agent_run

    async def _run_agent(
        self,
        agent_run: AgentRun,
        model_config: Dict[str, Any],
        frequency: str,
        init_date: str,
        end_date: str,
        market: str,
    ) -> None:
        """Internal method to run the agent"""
        agent_run.status = AgentStatus.RUNNING
        agent_run.started_at = datetime.now()
        agent_run.progress = {"current_date": init_date, "dates_processed": 0}

        try:
            model_name = model_config.get("name", "unknown")
            basemodel = model_config.get("basemodel")
            openai_base_url = model_config.get("openai_base_url")
            openai_api_key = model_config.get("openai_api_key")

            if not basemodel:
                raise ValueError(f"Model {model_name} missing basemodel field")

            agent_type = derive_agent_type(frequency)
            log_path = derive_log_path(frequency)
            signature = agent_run.signature

            # Get agent class
            AgentClass = get_agent_class(agent_type)

            # Write config values for tools to read
            write_config_value("SIGNATURE", signature)
            write_config_value("IF_TRADE", False)
            write_config_value("MARKET", market)
            write_config_value("LOG_PATH", log_path)

            # Check position file for fresh start
            position_file = project_root / log_path / signature / "position" / "position.jsonl"
            if not position_file.exists():
                from tools.general_tools import _resolve_runtime_env_path
                runtime_env_path = _resolve_runtime_env_path()
                if os.path.exists(runtime_env_path):
                    os.remove(runtime_env_path)

            # Create agent instance
            agent = AgentClass(
                signature=signature,
                basemodel=basemodel,
                stock_symbols=None,
                log_path=log_path,
                max_steps=DEFAULT_MAX_STEPS,
                max_retries=DEFAULT_MAX_RETRIES,
                base_delay=DEFAULT_BASE_DELAY,
                initial_cash=DEFAULT_INITIAL_CASH,
                init_date=init_date,
                openai_base_url=openai_base_url,
                openai_api_key=openai_api_key
            )

            await agent.initialize()
            await agent.run_date_range(init_date, end_date)

            # Get final summary
            summary = agent.get_position_summary()
            agent_run.progress["final_summary"] = summary

            agent_run.status = AgentStatus.COMPLETED
            agent_run.completed_at = datetime.now()

        except asyncio.CancelledError:
            agent_run.status = AgentStatus.CANCELLED
            agent_run.completed_at = datetime.now()
            raise

        except Exception as e:
            agent_run.status = AgentStatus.FAILED
            agent_run.error_message = str(e)
            agent_run.completed_at = datetime.now()

    async def get_run(self, run_id: str) -> Optional[AgentRun]:
        """Get agent run by ID"""
        return self._runs.get(run_id)

    async def get_all_runs(self) -> List[AgentRun]:
        """Get all agent runs"""
        return list(self._runs.values())

    async def cancel_run(self, run_id: str) -> bool:
        """
        Cancel a running agent.

        Args:
            run_id: The run ID to cancel

        Returns:
            True if cancelled, False if not found or already completed
        """
        agent_run = self._runs.get(run_id)
        if not agent_run:
            return False

        if agent_run.status != AgentStatus.RUNNING:
            return False

        if agent_run.task and not agent_run.task.done():
            agent_run.task.cancel()
            return True

        return False

    async def start_all_enabled_agents(
        self,
        config: Dict[str, Any],
        frequency: str,
    ) -> List[AgentRun]:
        """
        Start all enabled agents from config.

        Args:
            config: Full configuration dict
            frequency: Trading frequency

        Returns:
            List of AgentRun objects
        """
        init_date = config["date_range"]["init_date"]
        end_date = config["date_range"]["end_date"]
        market = config.get("market", "cn")

        enabled_models = [m for m in config["models"] if m.get("enabled", False)]

        runs = []
        for model_config in enabled_models:
            run = await self.start_agent(
                model_config=model_config,
                frequency=frequency,
                init_date=init_date,
                end_date=end_date,
                market=market,
            )
            runs.append(run)

        return runs


# Singleton instance
_runner_instance: Optional[AgentRunnerService] = None


def get_agent_runner() -> AgentRunnerService:
    """Get or create the agent runner singleton"""
    global _runner_instance
    if _runner_instance is None:
        _runner_instance = AgentRunnerService()
    return _runner_instance
