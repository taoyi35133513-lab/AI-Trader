import argparse
import asyncio
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

from tools.logging_config import setup_logging

setup_logging()

logger = logging.getLogger(__name__)

from tools.general_tools import get_config_value, write_config_value

# Default configuration values
DEFAULT_MAX_STEPS = 30
DEFAULT_MAX_RETRIES = 3
DEFAULT_BASE_DELAY = 1.0
DEFAULT_INITIAL_CASH = 100000.0
DEFAULT_START_DAYS_AGO = 30  # Default lookback period for new agents

# Hardcoded fallback registry (used when config lacks agent_types)
_DEFAULT_AGENT_REGISTRY = {
    "BaseAgentAStock": {
        "module": "agent.base_agent_astock.base_agent_astock",
        "class": "BaseAgentAStock",
        "frequency": "daily",
    },
    "BaseAgentAStock_Hour": {
        "module": "agent.base_agent_astock.base_agent_astock_hour",
        "class": "BaseAgentAStock_Hour",
        "frequency": "hourly",
    },
}


def _get_agent_registry(config: Optional[dict] = None) -> dict:
    """Return the agent registry, preferring config-driven over hardcoded."""
    if config and "agent_types" in config:
        return config["agent_types"]
    return _DEFAULT_AGENT_REGISTRY


def get_agent_class(agent_type: str, config: Optional[dict] = None):
    """
    Dynamically import and return the corresponding class based on agent type name.

    Looks up *agent_type* in the config-driven registry first, then falls back
    to the hardcoded default.

    Args:
        agent_type: Agent type name (e.g., "BaseAgentAStock")
        config: Optional loaded config dict (may contain "agent_types")

    Returns:
        Agent class

    Raises:
        ValueError: If agent type is not supported
        ImportError: If unable to import agent module
    """
    registry = _get_agent_registry(config)

    if agent_type not in registry:
        supported_types = ", ".join(registry.keys())
        raise ValueError(f"Unsupported agent type: {agent_type}\n   Supported types: {supported_types}")

    agent_info = registry[agent_type]
    module_path = agent_info["module"]
    class_name = agent_info["class"]

    try:
        import importlib

        module = importlib.import_module(module_path)
        agent_class = getattr(module, class_name)
        logger.info("Loaded Agent class: %s (from %s)", agent_type, module_path)
        return agent_class
    except ImportError as e:
        raise ImportError(f"Unable to import agent module {module_path}: {e}")
    except AttributeError as e:
        raise AttributeError(f"Class {class_name} not found in module {module_path}: {e}")


def load_config(config_path=None):
    """
    Load configuration file from configs directory

    Args:
        config_path: Configuration file path, if None use default config

    Returns:
        dict: Configuration dictionary
    """
    if config_path is None:
        config_path = Path(__file__).parent / "configs" / "config.json"
    else:
        config_path = Path(config_path)

    if not config_path.exists():
        logger.error("Configuration file does not exist: %s", config_path)
        exit(1)

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
        logger.info("Loaded configuration file: %s", config_path)

        # Validate with Pydantic (logs warnings but does not block)
        try:
            from configs.schema import TradingConfig
            TradingConfig(**config)
            logger.info("Configuration validated successfully")
        except Exception as ve:
            logger.warning("Configuration validation warning: %s", ve)

        return config
    except json.JSONDecodeError as e:
        logger.error("Configuration file JSON format error: %s", e)
        exit(1)
    except Exception as e:
        logger.error("Failed to load configuration file: %s", e)
        exit(1)


def derive_agent_type(frequency: str, config: Optional[dict] = None) -> str:
    """Derive agent type from frequency, using config registry when available."""
    registry = _get_agent_registry(config)
    for name, info in registry.items():
        if info.get("frequency") == frequency:
            return name
    # Hardcoded fallback
    return "BaseAgentAStock_Hour" if frequency == "hourly" else "BaseAgentAStock"


def resolve_env_var(value: Optional[str]) -> Optional[str]:
    """Resolve $ENV_VAR references in config values."""
    if value and isinstance(value, str) and value.startswith("$"):
        env_name = value[1:]
        resolved = os.environ.get(env_name)
        if resolved is None:
            logger.warning("Environment variable %s not set", env_name)
        return resolved
    return value


def derive_log_path(frequency: str) -> str:
    """Derive log path from frequency"""
    suffix = "_hour" if frequency == "hourly" else ""
    return f"./data/agent_data_astock{suffix}"


def derive_signature(model_name: str, frequency: str) -> str:
    """Derive signature from model name and frequency"""
    suffix = "-astock-hour" if frequency == "hourly" else ""
    return f"{model_name}{suffix}"


def get_latest_trading_day(frequency: str) -> Optional[str]:
    """Get the latest trading day from price data (merged.jsonl)

    Args:
        frequency: 'daily' or 'hourly'

    Returns:
        Latest trading date/timestamp:
        - For daily: YYYY-MM-DD format
        - For hourly: YYYY-MM-DD HH:MM:SS format
    """
    # Determine the merged file path
    if frequency == "hourly":
        merged_file = Path(__file__).parent / "data" / "A_stock" / "merged_hourly.jsonl"
    else:
        merged_file = Path(__file__).parent / "data" / "A_stock" / "merged.jsonl"

    if not merged_file.exists():
        logger.warning("Price data file not found: %s", merged_file)
        return None

    latest_date = None
    try:
        with open(merged_file, "r") as f:
            for line in f:
                data = json.loads(line)
                # Support both formats: "Time Series (Daily)" and "Time Series (60min)"
                prices = data.get("Time Series (Daily)",
                                 data.get("Time Series (60min)",
                                         data.get("prices", {})))
                for date_str in prices.keys():
                    if frequency == "hourly":
                        # For hourly, keep full timestamp (YYYY-MM-DD HH:MM:SS)
                        if latest_date is None or date_str > latest_date:
                            latest_date = date_str
                    else:
                        # For daily, just keep date part
                        date_part = date_str.split(" ")[0] if " " in date_str else date_str
                        if latest_date is None or date_part > latest_date:
                            latest_date = date_part
    except Exception as e:
        logger.warning("Failed to read price data: %s", e)
        return None

    return latest_date


def get_latest_position_date(signature: str, frequency: str) -> Optional[str]:
    """Get the latest position date for a given agent signature

    Args:
        signature: Agent signature (e.g., 'gemini-2.5-flash')
        frequency: 'daily' or 'hourly'

    Returns:
        Latest position date/timestamp:
        - For daily: YYYY-MM-DD format
        - For hourly: YYYY-MM-DD HH:MM:SS format
    """
    log_path = derive_log_path(frequency)
    position_file = Path(log_path) / signature / "position" / "position.jsonl"

    if not position_file.exists():
        return None

    latest_date = None
    try:
        with open(position_file, "r") as f:
            for line in f:
                data = json.loads(line)
                date_str = data.get("date", "")
                if frequency == "hourly":
                    # For hourly, keep full timestamp
                    if date_str and (latest_date is None or date_str > latest_date):
                        latest_date = date_str
                else:
                    # For daily, just keep date part
                    date_part = date_str.split(" ")[0] if " " in date_str else date_str
                    if date_part and (latest_date is None or date_part > latest_date):
                        latest_date = date_part
    except Exception as e:
        logger.warning("Failed to read position file for %s: %s", signature, e)
        return None

    return latest_date


def get_next_hourly_timestamp(timestamp: str) -> str:
    """Get the next hourly trading timestamp

    A-share hourly trading times: 10:30, 11:30, 14:00, 15:00

    Args:
        timestamp: Current timestamp in YYYY-MM-DD HH:MM:SS format

    Returns:
        Next trading timestamp in YYYY-MM-DD HH:MM:SS format
    """
    TRADING_HOURS = ["10:30:00", "11:30:00", "14:00:00", "15:00:00"]

    dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    current_time = dt.strftime("%H:%M:%S")
    current_date = dt.strftime("%Y-%m-%d")

    # Find the next trading hour
    for i, hour in enumerate(TRADING_HOURS):
        if current_time < hour:
            return f"{current_date} {hour}"

    # Move to next trading day (skip weekends)
    next_day = dt + timedelta(days=1)
    while next_day.weekday() >= 5:  # Saturday=5, Sunday=6
        next_day += timedelta(days=1)

    return f"{next_day.strftime('%Y-%m-%d')} {TRADING_HOURS[0]}"


def calculate_date_range(signature: str, frequency: str) -> tuple[str, str]:
    """Calculate the date range for trading

    Logic:
    - End date: Latest available trading day in price data, or today
    - Start date: Next timestamp after the latest position, or DEFAULT_START_DAYS_AGO days before end date

    Args:
        signature: Agent signature
        frequency: 'daily' or 'hourly'

    Returns:
        Tuple of (start_date, end_date):
        - For daily: YYYY-MM-DD format
        - For hourly: YYYY-MM-DD HH:MM:SS format
    """
    # Get end date (latest trading day or today)
    end_date = get_latest_trading_day(frequency)
    if end_date is None:
        if frequency == "hourly":
            end_date = datetime.now().strftime("%Y-%m-%d 15:00:00")
        else:
            end_date = datetime.now().strftime("%Y-%m-%d")

    # Get start date (next timestamp after latest position, or default lookback)
    latest_position = get_latest_position_date(signature, frequency)

    if latest_position:
        if frequency == "hourly":
            # For hourly, get the next trading timestamp
            start_date = get_next_hourly_timestamp(latest_position)
        else:
            # For daily, start from the day after the latest position
            latest_dt = datetime.strptime(latest_position, "%Y-%m-%d")
            start_dt = latest_dt + timedelta(days=1)
            start_date = start_dt.strftime("%Y-%m-%d")
    else:
        # No existing positions - start from DEFAULT_START_DAYS_AGO days ago
        if frequency == "hourly":
            end_dt = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
            start_dt = end_dt - timedelta(days=DEFAULT_START_DAYS_AGO)
            start_date = f"{start_dt.strftime('%Y-%m-%d')} 10:30:00"
        else:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            start_dt = end_dt - timedelta(days=DEFAULT_START_DAYS_AGO)
            start_date = start_dt.strftime("%Y-%m-%d")

    return start_date, end_date


async def main(config_path=None, frequency_override=None):
    """Run trading experiment using BaseAgent class

    Args:
        config_path: Configuration file path, if None use default config
        frequency_override: Override frequency from command line
    """
    config = load_config(config_path)

    # Get frequency: CLI override > config file > default
    frequency = frequency_override or config.get("frequency", "daily")
    if frequency not in ("daily", "hourly"):
        logger.error("Invalid frequency: %s. Must be 'daily' or 'hourly'", frequency)
        exit(1)

    # Derive agent type and log path from frequency
    agent_type = derive_agent_type(frequency, config)
    log_path = derive_log_path(frequency)

    try:
        AgentClass = get_agent_class(agent_type, config)
    except (ValueError, ImportError, AttributeError) as e:
        logger.error(str(e))
        exit(1)

    market = config.get("market", "cn")
    logger.info("Market type: %s (frequency: %s)", market, frequency)

    # Get latest trading day for display
    latest_trading_day = get_latest_trading_day(frequency)
    logger.info("Latest available trading day: %s", latest_trading_day or "unknown")

    # Get model list from configuration file (only select enabled models)
    enabled_models = [model for model in config["models"] if model.get("enabled", False)]

    if not enabled_models:
        logger.error("No enabled models found in configuration")
        exit(1)

    # Use default values for agent configuration
    max_steps = DEFAULT_MAX_STEPS
    max_retries = DEFAULT_MAX_RETRIES
    base_delay = DEFAULT_BASE_DELAY
    initial_cash = DEFAULT_INITIAL_CASH

    model_names = [m.get("name") for m in enabled_models]

    logger.info("Starting trading experiment")
    logger.info("Agent type: %s", agent_type)
    logger.info("Model list: %s", model_names)
    logger.info("Agent config: max_steps=%d, max_retries=%d, base_delay=%.1f, initial_cash=%.0f",
                max_steps, max_retries, base_delay, initial_cash)

    for model_config in enabled_models:
        model_name = model_config.get("name", "unknown")
        basemodel = model_config.get("basemodel")
        openai_base_url = resolve_env_var(model_config.get("openai_base_url", None))
        openai_api_key = resolve_env_var(model_config.get("openai_api_key", None))

        if not basemodel:
            logger.warning("Model %s missing basemodel field", model_name)
            continue

        # Derive signature from model name and frequency
        signature = derive_signature(model_name, frequency)

        # Calculate date range for this specific agent
        init_date, end_date = calculate_date_range(signature, frequency)

        logger.info("=" * 60)
        logger.info("Processing model: %s", model_name)
        logger.info("Signature: %s", signature)
        logger.info("BaseModel: %s", basemodel)
        logger.info("Date range: %s to %s (auto-calculated)", init_date, end_date)

        project_root = Path(__file__).resolve().parent

        # Check position file to determine if this is a fresh start
        position_file = project_root / log_path / signature / "position" / "position.jsonl"

        if not position_file.exists():
            from tools.general_tools import _resolve_runtime_env_path
            runtime_env_path = _resolve_runtime_env_path()
            if os.path.exists(runtime_env_path):
                os.remove(runtime_env_path)
                logger.info("Position file not found, starting fresh from %s", init_date)

        # Write config values to shared config file
        write_config_value("SIGNATURE", signature)
        write_config_value("IF_TRADE", False)
        write_config_value("MARKET", market)
        write_config_value("LOG_PATH", log_path)

        logger.info("Runtime config initialized: SIGNATURE=%s, MARKET=%s", signature, market)

        stock_symbols = None

        try:
            agent = AgentClass(
                signature=signature,
                basemodel=basemodel,
                stock_symbols=stock_symbols,
                log_path=log_path,
                max_steps=max_steps,
                max_retries=max_retries,
                base_delay=base_delay,
                initial_cash=initial_cash,
                init_date=init_date,
                openai_base_url=openai_base_url,
                openai_api_key=openai_api_key
            )

            logger.info("%s instance created successfully: %s", agent_type, agent)

            await agent.initialize()
            logger.info("Initialization successful")
            await agent.run_date_range(init_date, end_date)

            summary = agent.get_position_summary()
            currency_symbol = "CNY" if market == "cn" else "USD"
            logger.info("Final position summary:")
            logger.info("   - Latest date: %s", summary.get("latest_date"))
            logger.info("   - Total records: %s", summary.get("total_records"))
            logger.info("   - Cash balance: %s %.2f", currency_symbol,
                        summary.get("positions", {}).get("CASH", 0))

        except Exception as e:
            logger.error("Error processing model %s (%s): %s", model_name, signature, e)
            exit()

        logger.info("=" * 60)
        logger.info("Model %s (%s) processing completed", model_name, signature)
        logger.info("=" * 60)

    logger.info("All models processing completed!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AI-Trader trading agent")
    parser.add_argument("config", nargs="?", default=None, help="Configuration file path")
    parser.add_argument("-f", "--frequency", choices=["daily", "hourly"], help="Trading frequency (overrides config file)")
    args = parser.parse_args()

    if args.config:
        logger.info("Using specified configuration file: %s", args.config)
    else:
        logger.info("Using default configuration file: configs/config.json")

    if args.frequency:
        logger.info("Using frequency override: %s", args.frequency)

    asyncio.run(main(args.config, args.frequency))
