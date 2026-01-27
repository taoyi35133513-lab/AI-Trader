import argparse
import asyncio
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

from tools.general_tools import get_config_value, write_config_value

# Default configuration values
DEFAULT_MAX_STEPS = 30
DEFAULT_MAX_RETRIES = 3
DEFAULT_BASE_DELAY = 1.0
DEFAULT_INITIAL_CASH = 100000.0
DEFAULT_START_DAYS_AGO = 30  # Default lookback period for new agents

# Agent class mapping table - for dynamic import and instantiation (A-stock only)
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


def get_agent_class(agent_type):
    """
    Dynamically import and return the corresponding class based on agent type name

    Args:
        agent_type: Agent type name (e.g., "BaseAgentAStock")

    Returns:
        Agent class

    Raises:
        ValueError: If agent type is not supported
        ImportError: If unable to import agent module
    """
    if agent_type not in AGENT_REGISTRY:
        supported_types = ", ".join(AGENT_REGISTRY.keys())
        raise ValueError(f"Unsupported agent type: {agent_type}\n   Supported types: {supported_types}")

    agent_info = AGENT_REGISTRY[agent_type]
    module_path = agent_info["module"]
    class_name = agent_info["class"]

    try:
        import importlib

        module = importlib.import_module(module_path)
        agent_class = getattr(module, class_name)
        print(f"Successfully loaded Agent class: {agent_type} (from {module_path})")
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
        print(f"Configuration file does not exist: {config_path}")
        exit(1)

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
        print(f"Successfully loaded configuration file: {config_path}")
        return config
    except json.JSONDecodeError as e:
        print(f"Configuration file JSON format error: {e}")
        exit(1)
    except Exception as e:
        print(f"Failed to load configuration file: {e}")
        exit(1)


def derive_agent_type(frequency: str) -> str:
    """Derive agent type from frequency"""
    return "BaseAgentAStock_Hour" if frequency == "hourly" else "BaseAgentAStock"


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
        print(f"Warning: Price data file not found: {merged_file}")
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
        print(f"Warning: Failed to read price data: {e}")
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
        print(f"Warning: Failed to read position file for {signature}: {e}")
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
        print(f"Invalid frequency: {frequency}. Must be 'daily' or 'hourly'")
        exit(1)

    # Derive agent type and log path from frequency
    agent_type = derive_agent_type(frequency)
    log_path = derive_log_path(frequency)

    try:
        AgentClass = get_agent_class(agent_type)
    except (ValueError, ImportError, AttributeError) as e:
        print(str(e))
        exit(1)

    market = config.get("market", "cn")
    print(f"Market type: {market} (frequency: {frequency})")

    # Get latest trading day for display
    latest_trading_day = get_latest_trading_day(frequency)
    print(f"Latest available trading day: {latest_trading_day or 'unknown'}")

    # Get model list from configuration file (only select enabled models)
    enabled_models = [model for model in config["models"] if model.get("enabled", False)]

    if not enabled_models:
        print("No enabled models found in configuration")
        exit(1)

    # Use default values for agent configuration
    max_steps = DEFAULT_MAX_STEPS
    max_retries = DEFAULT_MAX_RETRIES
    base_delay = DEFAULT_BASE_DELAY
    initial_cash = DEFAULT_INITIAL_CASH

    model_names = [m.get("name") for m in enabled_models]

    print("Starting trading experiment")
    print(f"Agent type: {agent_type}")
    print(f"Model list: {model_names}")
    print(f"Agent config: max_steps={max_steps}, max_retries={max_retries}, base_delay={base_delay}, initial_cash={initial_cash}")

    for model_config in enabled_models:
        model_name = model_config.get("name", "unknown")
        basemodel = model_config.get("basemodel")
        openai_base_url = model_config.get("openai_base_url", None)
        openai_api_key = model_config.get("openai_api_key", None)

        if not basemodel:
            print(f"Model {model_name} missing basemodel field")
            continue

        # Derive signature from model name and frequency
        signature = derive_signature(model_name, frequency)

        # Calculate date range for this specific agent
        init_date, end_date = calculate_date_range(signature, frequency)

        print("=" * 60)
        print(f"Processing model: {model_name}")
        print(f"Signature: {signature}")
        print(f"BaseModel: {basemodel}")
        print(f"Date range: {init_date} to {end_date} (auto-calculated)")

        project_root = Path(__file__).resolve().parent

        # Check position file to determine if this is a fresh start
        position_file = project_root / log_path / signature / "position" / "position.jsonl"

        if not position_file.exists():
            from tools.general_tools import _resolve_runtime_env_path
            runtime_env_path = _resolve_runtime_env_path()
            if os.path.exists(runtime_env_path):
                os.remove(runtime_env_path)
                print(f"Position file not found, starting fresh from {init_date}")

        # Write config values to shared config file
        write_config_value("SIGNATURE", signature)
        write_config_value("IF_TRADE", False)
        write_config_value("MARKET", market)
        write_config_value("LOG_PATH", log_path)

        print(f"Runtime config initialized: SIGNATURE={signature}, MARKET={market}")

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

            print(f"{agent_type} instance created successfully: {agent}")

            await agent.initialize()
            print("Initialization successful")
            await agent.run_date_range(init_date, end_date)

            summary = agent.get_position_summary()
            currency_symbol = "CNY" if market == "cn" else "USD"
            print(f"Final position summary:")
            print(f"   - Latest date: {summary.get('latest_date')}")
            print(f"   - Total records: {summary.get('total_records')}")
            print(f"   - Cash balance: {currency_symbol} {summary.get('positions', {}).get('CASH', 0):,.2f}")

        except Exception as e:
            print(f"Error processing model {model_name} ({signature}): {str(e)}")
            print(f"Error details: {e}")
            exit()

        print("=" * 60)
        print(f"Model {model_name} ({signature}) processing completed")
        print("=" * 60)

    print("All models processing completed!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AI-Trader trading agent")
    parser.add_argument("config", nargs="?", default=None, help="Configuration file path")
    parser.add_argument("-f", "--frequency", choices=["daily", "hourly"], help="Trading frequency (overrides config file)")
    args = parser.parse_args()

    if args.config:
        print(f"Using specified configuration file: {args.config}")
    else:
        print(f"Using default configuration file: configs/config.json")

    if args.frequency:
        print(f"Using frequency override: {args.frequency}")

    asyncio.run(main(args.config, args.frequency))
