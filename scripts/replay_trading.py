"""回测指定日期的模拟交易"""
import asyncio
import importlib
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from api.services.trading_mode import (
    TradingMode,
    derive_agent_type,
    derive_log_path,
    generate_signature,
)
from tools.general_tools import write_config_value


def resolve_env(val):
    if val and isinstance(val, str) and val.startswith("$"):
        return os.environ.get(val[1:])
    return val


async def replay(frequency, trade_date):
    with open("configs/config.json") as f:
        config = json.load(f)
    enabled = [m for m in config["models"] if m.get("enabled")]

    for model in enabled:
        name = model["name"]
        sig = generate_signature(name, frequency, TradingMode.LIVE)
        log_path = derive_log_path(frequency)
        agent_type = derive_agent_type(frequency)

        print(f"=== Replay {frequency} {name} (sig={sig}) date={trade_date} ===")

        # Remove existing records for this date
        pos_file = Path(log_path) / sig / "position" / "position.jsonl"
        if pos_file.exists():
            kept = []
            with open(pos_file) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    d = json.loads(line)
                    if not d["date"].startswith(trade_date.split(" ")[0]) or d["date"] < trade_date:
                        kept.append(line)
            with open(pos_file, "w") as f:
                for l in kept:
                    f.write(l + "\n")
            print(f"  Cleaned records >= {trade_date}")

        write_config_value("SIGNATURE", sig)
        write_config_value("IF_TRADE", False)
        write_config_value("MARKET", "cn")
        write_config_value("LOG_PATH", log_path)

        agent_info = {
            "BaseAgentAStock": {
                "module": "agent.base_agent_astock.base_agent_astock",
                "class": "BaseAgentAStock",
            },
            "BaseAgentAStock_Hour": {
                "module": "agent.base_agent_astock.base_agent_astock_hour",
                "class": "BaseAgentAStock_Hour",
            },
        }
        info = agent_info[agent_type]
        mod = importlib.import_module(info["module"])
        AgentClass = getattr(mod, info["class"])

        agent = AgentClass(
            signature=sig,
            basemodel=model["basemodel"],
            stock_symbols=None,
            log_path=log_path,
            max_steps=30,
            max_retries=3,
            base_delay=1.0,
            initial_cash=100000.0,
            init_date=trade_date,
            openai_base_url=resolve_env(model.get("openai_base_url")),
            openai_api_key=resolve_env(model.get("openai_api_key")),
        )

        await agent.initialize()
        await agent.run_trading_session(trade_date)
        summary = agent.get_position_summary()
        action = summary.get("this_action", {}).get("action", "unknown")
        cash = summary.get("positions", {}).get("CASH", 0)
        print(f"  Result: action={action}, cash={cash:,.0f}")
        print()


async def main():
    import argparse

    parser = argparse.ArgumentParser(description="Replay trading for a specific date")
    parser.add_argument("--date", "-d", required=True, help="Trade date (e.g. 2026-03-16)")
    parser.add_argument(
        "--frequency",
        "-f",
        default="all",
        choices=["daily", "hourly", "all"],
        help="Frequency to replay",
    )
    args = parser.parse_args()

    date = args.date

    if args.frequency in ("daily", "all"):
        print("=" * 60)
        print(f"REPLAYING DAILY for {date}")
        print("=" * 60)
        await replay("daily", date)

    if args.frequency in ("hourly", "all"):
        hourly_times = [
            f"{date} 10:30:00",
            f"{date} 11:30:00",
            f"{date} 14:00:00",
            f"{date} 15:00:00",
        ]
        for time_point in hourly_times:
            print("=" * 60)
            print(f"REPLAYING HOURLY for {time_point}")
            print("=" * 60)
            await replay("hourly", time_point)


if __name__ == "__main__":
    asyncio.run(main())
