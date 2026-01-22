"""
MCP tool for local price data access.

This module provides MCP tool endpoints for querying stock price data.
It uses DuckDB as the primary data source with automatic fallback to JSONL files.
"""

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from fastmcp import FastMCP

# Add parent directory to Python path to import tools module
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

load_dotenv()

mcp = FastMCP("LocalPrices")

# Ensure project root is on sys.path for absolute imports like `tools.*`
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from tools.general_tools import get_config_value
from tools.data_access import PriceDataAccess

logger = logging.getLogger(__name__)

# Lazy-loaded price data access singleton
_price_access = None


def _get_price_access() -> PriceDataAccess:
    """Get or create PriceDataAccess singleton."""
    global _price_access
    if _price_access is None:
        _price_access = PriceDataAccess(market="cn")
    return _price_access


def _workspace_data_path(filename: str, symbol: Optional[str] = None) -> Path:
    """Get data file path based on symbol (auto-detect market type).

    Args:
        filename: Data filename (e.g., 'merged.jsonl')
        symbol: Stock symbol for auto-detecting market type.
                If symbol ends with .SH or .SZ, use A-stock data path.
                If symbol ends with -USDT, use crypto data path.

    Returns:
        Path to the data file
    """
    base_dir = Path(__file__).resolve().parents[1]

    # Auto-detect market type from symbol
    if symbol and (symbol.endswith(".SH") or symbol.endswith(".SZ")):
        # Chinese A-shares
        return base_dir / "data" / "A_stock" / filename
    elif symbol and symbol.endswith("-USDT"):
        # Cryptocurrencies
        crypto_filename = "crypto_merged.jsonl" if filename == "merged.jsonl" else filename
        return base_dir / "data" / "crypto" / crypto_filename
    else:
        # US stocks (default)
        return base_dir / "data" / filename


def _validate_date_daily(date_str: str) -> None:
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("date must be in YYYY-MM-DD format") from exc

def _validate_date_hourly(date_str: str) -> None:
    try:
        datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    except ValueError as exc:
        raise ValueError("date must be in YYYY-MM-DD HH:MM:SS format") from exc

@mcp.tool()
def get_price_local(symbol: str, date: str) -> Dict[str, Any]:
    """Read OHLCV data for specified stock and date. Get historical information for specified stock.
    
    Automatically detects date format and calls appropriate function:
    - Daily data: YYYY-MM-DD format (e.g., '2025-10-30')
    - Hourly data: YYYY-MM-DD HH:MM:SS format (e.g., '2025-10-30 14:30:00')

    Args:
        symbol: Stock symbol, e.g. 'IBM' or '600243.SHH'.
        date: Date in 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS' format. Based on your current time format.

    Returns:
        Dictionary containing symbol, date and ohlcv data.
    """
    # Detect date format
    result = None
    if ' ' in date or 'T' in date:
        # Contains time component, use hourly
        result =  get_price_local_hourly(symbol, date)
    else:
        # Date only, use daily
        result = get_price_local_daily(symbol, date)
    
    # log_file = get_config_value("LOG_FILE")
    # signature = get_config_value("SIGNATURE")
    
    # log_entry = {
    #     "signature": signature,
    #     "new_messages": [{"role": "tool:get_price_local", "content": result}]
    # }
    # with open(log_file, "a", encoding="utf-8") as f:
    #     f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
    
    return result



def get_price_local_daily(symbol: str, date: str) -> Dict[str, Any]:
    """Read OHLCV data for specified stock and date. Get historical information for specified stock.

    Uses DuckDB as the primary data source with automatic fallback to JSONL.

    Args:
        symbol: Stock symbol, e.g. '600519.SH'.
        date: Date in 'YYYY-MM-DD' format.

    Returns:
        Dictionary containing symbol, date and ohlcv data.
    """
    try:
        _validate_date_daily(date)
    except ValueError as e:
        return {"error": str(e), "symbol": symbol, "date": date}

    # Use data access layer (DuckDB-first with JSONL fallback)
    result = _get_price_access().get_ohlcv(symbol, date)

    # Handle "today" scenario - mask future data
    if "error" not in result and date == get_config_value("TODAY_DATE"):
        ohlcv = result.get("ohlcv", {})
        result["ohlcv"] = {
            "open": ohlcv.get("open"),
            "high": "You can not get the current high price",
            "low": "You can not get the current low price",
            "close": "You can not get the next close price",
            "volume": "You can not get the current volume",
        }

    return result


def get_price_local_hourly(symbol: str, date: str) -> Dict[str, Any]:
    """Read hourly OHLCV data for specified stock and datetime.

    Uses DuckDB as the primary data source with automatic fallback to JSONL.

    Args:
        symbol: Stock symbol, e.g. '600519.SH'.
        date: Datetime in 'YYYY-MM-DD HH:MM:SS' format.

    Returns:
        Dictionary containing symbol, date and ohlcv data.
    """
    try:
        _validate_date_hourly(date)
    except ValueError as e:
        return {"error": str(e), "symbol": symbol, "date": date}

    # Use data access layer (DuckDB-first with JSONL fallback)
    result = _get_price_access().get_ohlcv(symbol, date)

    # Handle "today" scenario - mask future data
    if "error" not in result and date == get_config_value("TODAY_DATE"):
        ohlcv = result.get("ohlcv", {})
        result["ohlcv"] = {
            "open": ohlcv.get("open"),
            "high": "You can not get the current high price",
            "low": "You can not get the current low price",
            "close": "You can not get the next close price",
            "volume": "You can not get the current volume",
        }

    return result


def get_price_local_function(symbol: str, date: str, filename: str = "merged.jsonl") -> Dict[str, Any]:
    """Read OHLCV data for specified stock and date.

    Uses DuckDB as the primary data source with automatic fallback to JSONL.
    This function is kept for backward compatibility.

    Args:
        symbol: Stock symbol, e.g. '600519.SH'.
        date: Date in 'YYYY-MM-DD' format.
        filename: Data filename (ignored, kept for backward compatibility).

    Returns:
        Dictionary containing symbol, date and ohlcv data.
    """
    try:
        _validate_date_daily(date)
    except ValueError as e:
        return {"error": str(e), "symbol": symbol, "date": date}

    return _get_price_access().get_ohlcv(symbol, date)


if __name__ == "__main__":
    
    port = int(os.getenv("GETPRICE_HTTP_PORT", "8003"))
    mcp.run(transport="streamable-http", port=port)
