"""
Price and position data access utilities.

This module provides functions for accessing price data and position records.
It uses DuckDB as the primary data source with automatic fallback to JSONL files.

Usage:
    from tools.price_tools import get_open_prices, get_latest_position

    prices = get_open_prices("2025-10-30", ["600519.SH"])
    position, action_id = get_latest_position("2025-10-30", "gpt-5")
"""

import os

from dotenv import load_dotenv

load_dotenv()
import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

# 将项目根目录加入 Python 路径，便于从子目录直接运行本文件
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from tools.general_tools import get_config_value

logger = logging.getLogger(__name__)

# Lazy-loaded data access instances (singleton pattern)
_price_access = None
_position_access = None


def _get_price_access():
    """Get or create PriceDataAccess singleton."""
    global _price_access
    if _price_access is None:
        from tools.data_access import PriceDataAccess
        _price_access = PriceDataAccess(market="cn")
    return _price_access


def _get_position_access():
    """Get or create PositionDataAccess singleton."""
    global _position_access
    if _position_access is None:
        from tools.data_access import PositionDataAccess
        _position_access = PositionDataAccess()
    return _position_access

def _normalize_timestamp_str(ts: str) -> str:
    """
    Normalize timestamp string to zero-padded HH for robust string/chrono comparisons.
    - If ts has time part like 'YYYY-MM-DD H:MM:SS', pad hour to 'HH'.
    - If ts is date-only, return as-is.
    """
    try:
        if " " not in ts:
            return ts
        date_part, time_part = ts.split(" ", 1)
        parts = time_part.split(":")
        if len(parts) != 3:
            return ts
        hour, minute, second = parts
        hour = hour.zfill(2)
        return f"{date_part} {hour}:{minute}:{second}"
    except Exception:
        return ts

def _parse_timestamp_to_dt(ts: str) -> datetime:
    """
    Parse timestamp string to datetime, supporting both date-only and datetime.
    Assumes ts is already normalized if time exists.
    """
    if " " in ts:
        return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
    return datetime.strptime(ts, "%Y-%m-%d")


def get_market_type() -> str:
    """
    获取市场类型（仅支持A股）

    Returns:
        "cn" for A-shares market
    """
    return "cn"


# SSE 50 股票代码列表 (上证50成分股)
all_sse_50_symbols = [
    "600519.SH",
    "601318.SH",
    "600036.SH",
    "601899.SH",
    "600900.SH",
    "601166.SH",
    "600276.SH",
    "600030.SH",
    "603259.SH",
    "688981.SH",
    "688256.SH",
    "601398.SH",
    "688041.SH",
    "601211.SH",
    "601288.SH",
    "601328.SH",
    "688008.SH",
    "600887.SH",
    "600150.SH",
    "601816.SH",
    "601127.SH",
    "600031.SH",
    "688012.SH",
    "603501.SH",
    "601088.SH",
    "600309.SH",
    "601601.SH",
    "601668.SH",
    "603993.SH",
    "601012.SH",
    "601728.SH",
    "600690.SH",
    "600809.SH",
    "600941.SH",
    "600406.SH",
    "601857.SH",
    "601766.SH",
    "601919.SH",
    "600050.SH",
    "600760.SH",
    "601225.SH",
    "600028.SH",
    "601988.SH",
    "688111.SH",
    "601985.SH",
    "601888.SH",
    "601628.SH",
    "601600.SH",
    "601658.SH",
    "600048.SH",
]


def get_merged_file_path(market: str = "cn") -> Path:
    """Get merged.jsonl path for A-stock market.

    Args:
        market: Market type (only "cn" for A-shares is supported)

    Returns:
        Path object pointing to the merged.jsonl file
    """
    base_dir = Path(__file__).resolve().parents[1]
    return base_dir / "data" / "A_stock" / "merged.jsonl"

def _resolve_merged_file_path_for_date(
    today_date: Optional[str], market: str, merged_path: Optional[str] = None
) -> Path:
    """
    Resolve the correct merged data file path taking into account market and granularity.
    For A-shares:
      - Daily: data/A_stock/merged.jsonl
      - Hourly (timestamp contains space): data/A_stock/merged_hourly.jsonl
    A custom merged_path, if provided, takes precedence.
    """
    if merged_path is not None:
        return Path(merged_path)
    base_dir = Path(__file__).resolve().parents[1]
    if market == "cn" and today_date and " " in today_date:
        # Hourly trading session for A-shares
        return base_dir / "data" / "A_stock" / "merged_hourly.jsonl"
    return get_merged_file_path(market)


def is_trading_day(date: str, market: str = "cn") -> bool:
    """Check if a given date is a trading day.

    Uses DuckDB as the primary data source with automatic fallback to JSONL.

    Args:
        date: Date string in "YYYY-MM-DD" format
        market: Market type (default: "cn" for A-shares)

    Returns:
        True if the date is a trading day, False otherwise
    """
    return _get_price_access().is_trading_day(date)


def get_all_trading_days(market: str = "cn") -> List[str]:
    """Get all available trading days.

    Uses DuckDB as the primary data source with automatic fallback to JSONL.

    Args:
        market: Market type (default: "cn" for A-shares)

    Returns:
        Sorted list of trading dates in "YYYY-MM-DD" format
    """
    return _get_price_access().get_all_trading_days()


def get_stock_name_mapping(market: str = "us") -> Dict[str, str]:
    """Get mapping from stock symbols to names.

    Args:
        market: Market type ("us" or "cn")

    Returns:
        Dictionary mapping symbols to names, e.g. {"600519.SH": "贵州茅台"}
    """
    merged_file_path = get_merged_file_path(market)

    if not merged_file_path.exists():
        return {}

    name_map = {}
    try:
        with open(merged_file_path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    data = json.loads(line.strip())
                    meta = data.get("Meta Data", {})
                    symbol = meta.get("2. Symbol")
                    name = meta.get("2.1. Name", "")
                    if symbol and name:
                        name_map[symbol] = name
                except json.JSONDecodeError:
                    continue
        return name_map
    except Exception as e:
        print(f"⚠️  Error reading stock names: {e}")
        return {}


def format_price_dict_with_names(
    price_dict: Dict[str, Optional[float]], market: str = "us"
) -> Dict[str, Optional[float]]:
    """Format price dictionary to include stock names for display.

    Args:
        price_dict: Original price dictionary with keys like "600519.SH_price"
        market: Market type ("us" or "cn")

    Returns:
        New dictionary with keys like "600519.SH (贵州茅台)_price" for CN market,
        unchanged for US market
    """
    if market != "cn":
        return price_dict

    name_map = get_stock_name_mapping(market)
    if not name_map:
        return price_dict

    formatted_dict = {}
    for key, value in price_dict.items():
        if key.endswith("_price"):
            symbol = key[:-6]  # Remove "_price" suffix
            stock_name = name_map.get(symbol, "")
            if stock_name:
                new_key = f"{symbol} ({stock_name})_price"
            else:
                new_key = key
            formatted_dict[new_key] = value
        else:
            formatted_dict[key] = value

    return formatted_dict


def get_yesterday_date(today_date: str, merged_path: Optional[str] = None, market: str = "cn") -> str:
    """
    获取输入日期的上一个交易日或时间点。

    Uses DuckDB as the primary data source with automatic fallback to JSONL.

    Args:
        today_date: 日期字符串，格式 YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS。
        merged_path: 可选，自定义 merged.jsonl 路径（仅用于 JSONL fallback）
        market: 市场类型（默认: "cn" A股）

    Returns:
        yesterday_date: 上一个交易日或时间点的字符串，格式与输入一致。
    """
    return _get_price_access().get_yesterday_date(today_date, merged_path)



def get_open_prices(
    today_date: str, symbols: List[str], merged_path: Optional[str] = None, market: str = "cn"
) -> Dict[str, Optional[float]]:
    """读取指定日期与标的的开盘价。

    Uses DuckDB as the primary data source with automatic fallback to JSONL.

    Args:
        today_date: 日期字符串，格式 YYYY-MM-DD或YYYY-MM-DD HH:MM:SS。
        symbols: 需要查询的股票代码列表。
        merged_path: 可选，自定义 merged.jsonl 路径（仅用于 JSONL fallback）
        market: 市场类型（默认: "cn" A股）

    Returns:
        {symbol_price: open_price 或 None} 的字典；若未找到对应日期或标的，则值为 None。
    """
    return _get_price_access().get_open_prices(today_date, symbols, merged_path)


def get_yesterday_open_and_close_price(
    today_date: str, symbols: List[str], merged_path: Optional[str] = None, market: str = "cn"
) -> Tuple[Dict[str, Optional[float]], Dict[str, Optional[float]]]:
    """读取昨日的买入价和卖出价。

    Uses DuckDB as the primary data source with automatic fallback to JSONL.

    Args:
        today_date: 日期字符串，格式 YYYY-MM-DD，代表今天日期。
        symbols: 需要查询的股票代码列表。
        merged_path: 可选，自定义 merged.jsonl 路径（仅用于 JSONL fallback）
        market: 市场类型（默认: "cn" A股）

    Returns:
        (买入价字典, 卖出价字典) 的元组；若未找到对应日期或标的，则值为 None。
    """
    return _get_price_access().get_yesterday_open_and_close_price(today_date, symbols, merged_path)


def get_yesterday_profit(
    today_date: str,
    yesterday_buy_prices: Dict[str, Optional[float]],
    yesterday_sell_prices: Dict[str, Optional[float]],
    yesterday_init_position: Dict[str, float],
    stock_symbols: Optional[List[str]] = None,
) -> Dict[str, float]:
    """
    获取持仓收益（适用于日线和小时级交易）

    收益计算方式为：(前一时间点收盘价 - 前一时间点开盘价) × 当前持仓数量

    对于日线交易：计算昨日的收益
    对于小时级交易：计算上一小时的收益

    Args:
        today_date: 日期/时间字符串，格式 YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS
        yesterday_buy_prices: 前一时间点开盘价格字典，格式为 {symbol_price: price}
        yesterday_sell_prices: 前一时间点收盘价格字典，格式为 {symbol_price: price}
        yesterday_init_position: 前一时间点初始持仓字典，格式为 {symbol: quantity}
        stock_symbols: 股票代码列表，默认为 all_sse_50_symbols

    Returns:
        {symbol: profit} 的字典；若未找到对应日期或标的，则值为 0.0。
    """
    profit_dict = {}

    # 使用传入的股票列表或默认的上证50列表
    if stock_symbols is None:
        stock_symbols = all_sse_50_symbols

    # 遍历所有股票代码
    for symbol in stock_symbols:
        symbol_price_key = f"{symbol}_price"

        # 获取昨日开盘价和收盘价
        buy_price = yesterday_buy_prices.get(symbol_price_key)
        sell_price = yesterday_sell_prices.get(symbol_price_key)

        # 获取昨日持仓权重
        position_weight = yesterday_init_position.get(symbol, 0.0)

        # 计算收益：(收盘价 - 开盘价) * 持仓权重
        if buy_price is not None and sell_price is not None and position_weight > 0:
            profit = (sell_price - buy_price) * position_weight
            profit_dict[symbol] = round(profit, 4)  # 保留4位小数
        else:
            profit_dict[symbol] = 0.0

    return profit_dict

def get_today_init_position(today_date: str, signature: str) -> Dict[str, float]:
    """
    获取今日开盘时的初始持仓（即上一个交易日结束时的持仓）。

    Uses DuckDB as the primary data source with automatic fallback to JSONL.

    Args:
        today_date: 日期字符串，格式 YYYY-MM-DD，代表今天日期。
        signature: 模型名称/Agent签名。

    Returns:
        {symbol: weight} 的字典；若未找到对应日期，则返回空字典。
    """
    return _get_position_access().get_today_init_position(today_date, signature)


def get_latest_position(today_date: str, signature: str) -> Tuple[Dict[str, float], int]:
    """
    获取最新持仓。

    Uses DuckDB as the primary data source with automatic fallback to JSONL.
    优先选择当天 (today_date) 中 id 最大的记录；
    若当天无记录，则回退到上一个交易日，选择该日中 id 最大的记录。

    Args:
        today_date: 日期字符串，格式 YYYY-MM-DD，代表今天日期。
        signature: 模型名称/Agent签名。

    Returns:
        (positions, max_id):
          - positions: {symbol: weight} 的字典；若未找到任何记录，则为空字典。
          - max_id: 选中记录的最大 id；若未找到任何记录，则为 -1.
    """
    return _get_position_access().get_latest_position(today_date, signature)

def add_no_trade_record(today_date: str, signature: str):
    """
    添加不交易记录。

    Uses DuckDB and JSONL dual-write strategy for redundancy.

    Args:
        today_date: 日期字符串，格式 YYYY-MM-DD，代表今天日期。
        signature: 模型名称/Agent签名。

    Returns:
        None
    """
    _get_position_access().add_no_trade_record(today_date, signature)


if __name__ == "__main__":
    today_date = get_config_value("TODAY_DATE")
    signature = get_config_value("SIGNATURE")
    if signature is None:
        raise ValueError("SIGNATURE environment variable is not set")
    print(today_date, signature)
    yesterday_date = get_yesterday_date(today_date)
    print(yesterday_date)
    # today_buy_price = get_open_prices(today_date, all_nasdaq_100_symbols)
    # print(today_buy_price)
    # yesterday_buy_prices, yesterday_sell_prices = get_yesterday_open_and_close_price(today_date, all_nasdaq_100_symbols)
    # print(yesterday_sell_prices)
    # today_init_position = get_today_init_position(today_date, signature='qwen3-max')
    # print(today_init_position)
    # latest_position, latest_action_id = get_latest_position('2025-10-24', 'qwen3-max')
    # print(latest_position, latest_action_id)
    latest_position, latest_action_id = get_latest_position('2025-10-16 16:00:00', 'test')
    print(latest_position, latest_action_id)
    
    # yesterday_profit = get_yesterday_profit(today_date, yesterday_buy_prices, yesterday_sell_prices, today_init_position)
    # # print(yesterday_profit)
    # add_no_trade_record(today_date, signature)
