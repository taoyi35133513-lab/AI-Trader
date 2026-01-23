import argparse
import asyncio
import json
import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from tools.general_tools import get_config_value, write_config_value

# Default configuration values
DEFAULT_MAX_STEPS = 30
DEFAULT_MAX_RETRIES = 3
DEFAULT_BASE_DELAY = 1.0
DEFAULT_INITIAL_CASH = 100000.0

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

    # Get date range from configuration file
    INIT_DATE = config["date_range"]["init_date"]
    END_DATE = config["date_range"]["end_date"]

    # Environment variables can override dates in configuration file
    if os.getenv("INIT_DATE"):
        INIT_DATE = os.getenv("INIT_DATE")
        print(f"Using environment variable to override INIT_DATE: {INIT_DATE}")
    if os.getenv("END_DATE"):
        END_DATE = os.getenv("END_DATE")
        print(f"Using environment variable to override END_DATE: {END_DATE}")

    # Validate date range
    if ' ' in INIT_DATE:
        INIT_DATE_obj = datetime.strptime(INIT_DATE, "%Y-%m-%d %H:%M:%S")
    else:
        INIT_DATE_obj = datetime.strptime(INIT_DATE, "%Y-%m-%d")

    if ' ' in END_DATE:
        END_DATE_obj = datetime.strptime(END_DATE, "%Y-%m-%d %H:%M:%S")
    else:
        END_DATE_obj = datetime.strptime(END_DATE, "%Y-%m-%d")

    if INIT_DATE_obj > END_DATE_obj:
        print("INIT_DATE is greater than END_DATE")
        exit(1)

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
    print(f"Date range: {INIT_DATE} to {END_DATE}")
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

        print("=" * 60)
        print(f"Processing model: {model_name}")
        print(f"Signature: {signature}")
        print(f"BaseModel: {basemodel}")

        project_root = Path(__file__).resolve().parent

        # Check position file to determine if this is a fresh start
        position_file = project_root / log_path / signature / "position" / "position.jsonl"

        if not position_file.exists():
            from tools.general_tools import _resolve_runtime_env_path
            runtime_env_path = _resolve_runtime_env_path()
            if os.path.exists(runtime_env_path):
                os.remove(runtime_env_path)
                print(f"Position file not found, cleared config for fresh start from {INIT_DATE}")

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
                init_date=INIT_DATE,
                openai_base_url=openai_base_url,
                openai_api_key=openai_api_key
            )

            print(f"{agent_type} instance created successfully: {agent}")

            await agent.initialize()
            print("Initialization successful")
            await agent.run_date_range(INIT_DATE, END_DATE)

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
