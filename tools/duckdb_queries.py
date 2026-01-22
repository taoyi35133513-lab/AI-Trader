"""
DuckDB query functions returning price_tools-compatible formats.

This module provides SQL query functions that return data in the same format
as the existing JSONL-based functions in price_tools.py.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

logger = logging.getLogger(__name__)


def query_daily_open_prices(
    db, symbols: List[str], date: str, market: str = "cn"
) -> Dict[str, Optional[float]]:
    """Query daily open prices, returns {symbol_price: value} format.

    Args:
        db: DatabaseManager instance
        symbols: List of stock symbols (e.g., ["600519.SH", "601318.SH"])
        date: Date string in YYYY-MM-DD format
        market: Market identifier (default: "cn")

    Returns:
        Dictionary in format {"{symbol}_price": float or None}
    """
    if not symbols:
        return {}

    placeholders = ", ".join(["?" for _ in symbols])
    sql = f"""
        SELECT ts_code, open
        FROM stock_daily_prices
        WHERE ts_code IN ({placeholders})
          AND trade_date = ?
          AND market = ?
    """
    params = tuple(symbols) + (date, market)

    df = db.query(sql, params)

    # Convert to price_tools format: {symbol_price: value}
    result = {}
    for _, row in df.iterrows():
        symbol = row['ts_code']
        open_price = row['open']
        result[f"{symbol}_price"] = float(open_price) if open_price is not None else None

    # Add None for missing symbols
    for symbol in symbols:
        key = f"{symbol}_price"
        if key not in result:
            result[key] = None

    return result


def query_hourly_open_prices(
    db, symbols: List[str], datetime_str: str
) -> Dict[str, Optional[float]]:
    """Query hourly open prices, returns {symbol_price: value} format.

    Args:
        db: DatabaseManager instance
        symbols: List of stock symbols
        datetime_str: Datetime string in YYYY-MM-DD HH:MM:SS format

    Returns:
        Dictionary in format {"{symbol}_price": float or None}
    """
    if not symbols:
        return {}

    placeholders = ", ".join(["?" for _ in symbols])
    sql = f"""
        SELECT ts_code, open
        FROM stock_hourly_prices
        WHERE ts_code IN ({placeholders})
          AND trade_time = ?
    """
    params = tuple(symbols) + (datetime_str,)

    df = db.query(sql, params)

    result = {}
    for _, row in df.iterrows():
        symbol = row['ts_code']
        open_price = row['open']
        result[f"{symbol}_price"] = float(open_price) if open_price is not None else None

    for symbol in symbols:
        key = f"{symbol}_price"
        if key not in result:
            result[key] = None

    return result


def query_daily_ohlcv(
    db, symbol: str, date: str, market: str = "cn"
) -> Dict[str, Any]:
    """Query daily OHLCV data for a single symbol.

    Args:
        db: DatabaseManager instance
        symbol: Stock symbol
        date: Date string in YYYY-MM-DD format
        market: Market identifier

    Returns:
        Dictionary with symbol, date, and ohlcv fields (same format as MCP tool)
    """
    sql = """
        SELECT ts_code, trade_date, open, high, low, close, volume
        FROM stock_daily_prices
        WHERE ts_code = ? AND trade_date = ? AND market = ?
    """
    df = db.query(sql, (symbol, date, market))

    if df.empty:
        return {
            "error": f"Data not found for {symbol} on {date}",
            "symbol": symbol,
            "date": date
        }

    row = df.iloc[0]
    return {
        "symbol": symbol,
        "date": date,
        "ohlcv": {
            "open": str(row["open"]) if row["open"] is not None else None,
            "high": str(row["high"]) if row["high"] is not None else None,
            "low": str(row["low"]) if row["low"] is not None else None,
            "close": str(row["close"]) if row["close"] is not None else None,
            "volume": str(int(row["volume"])) if row["volume"] is not None else None,
        }
    }


def query_hourly_ohlcv(
    db, symbol: str, datetime_str: str
) -> Dict[str, Any]:
    """Query hourly OHLCV data for a single symbol.

    Args:
        db: DatabaseManager instance
        symbol: Stock symbol
        datetime_str: Datetime string in YYYY-MM-DD HH:MM:SS format

    Returns:
        Dictionary with symbol, date, and ohlcv fields
    """
    sql = """
        SELECT ts_code, trade_time, open, high, low, close, volume
        FROM stock_hourly_prices
        WHERE ts_code = ? AND trade_time = ?
    """
    df = db.query(sql, (symbol, datetime_str))

    if df.empty:
        return {
            "error": f"Data not found for {symbol} on {datetime_str}",
            "symbol": symbol,
            "date": datetime_str
        }

    row = df.iloc[0]
    return {
        "symbol": symbol,
        "date": datetime_str,
        "ohlcv": {
            "open": str(row["open"]) if row["open"] is not None else None,
            "high": str(row["high"]) if row["high"] is not None else None,
            "low": str(row["low"]) if row["low"] is not None else None,
            "close": str(row["close"]) if row["close"] is not None else None,
            "volume": str(int(row["volume"])) if row["volume"] is not None else None,
        }
    }


def query_yesterday_prices(
    db, symbols: List[str], yesterday_date: str, market: str = "cn"
) -> Tuple[Dict[str, Optional[float]], Dict[str, Optional[float]]]:
    """Query yesterday's open and close prices.

    Args:
        db: DatabaseManager instance
        symbols: List of stock symbols
        yesterday_date: Yesterday's date string
        market: Market identifier

    Returns:
        Tuple of (buy_prices, sell_prices) dictionaries
    """
    if not symbols:
        return {}, {}

    placeholders = ", ".join(["?" for _ in symbols])
    sql = f"""
        SELECT ts_code, open, close
        FROM stock_daily_prices
        WHERE ts_code IN ({placeholders})
          AND trade_date = ?
          AND market = ?
    """
    params = tuple(symbols) + (yesterday_date, market)

    df = db.query(sql, params)

    buy_results = {}
    sell_results = {}

    for _, row in df.iterrows():
        symbol = row['ts_code']
        key = f"{symbol}_price"
        buy_results[key] = float(row['open']) if row['open'] is not None else None
        sell_results[key] = float(row['close']) if row['close'] is not None else None

    # Add None for missing symbols
    for symbol in symbols:
        key = f"{symbol}_price"
        if key not in buy_results:
            buy_results[key] = None
        if key not in sell_results:
            sell_results[key] = None

    return buy_results, sell_results


def query_previous_trading_day(
    db, today_date: str, market: str = "cn"
) -> Optional[str]:
    """Query the previous trading day from database.

    Args:
        db: DatabaseManager instance
        today_date: Today's date string (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
        market: Market identifier

    Returns:
        Previous trading day string, or None if not found
    """
    is_hourly = " " in today_date

    if is_hourly:
        sql = """
            SELECT MAX(trade_time) as prev_time
            FROM stock_hourly_prices
            WHERE trade_time < ?
        """
        df = db.query(sql, (today_date,))

        if not df.empty and df.iloc[0]["prev_time"] is not None:
            prev_time = df.iloc[0]["prev_time"]
            if hasattr(prev_time, 'strftime'):
                return prev_time.strftime("%Y-%m-%d %H:%M:%S")
            return str(prev_time)
    else:
        sql = """
            SELECT MAX(trade_date) as prev_date
            FROM stock_daily_prices
            WHERE trade_date < ? AND market = ?
        """
        df = db.query(sql, (today_date, market))

        if not df.empty and df.iloc[0]["prev_date"] is not None:
            prev_date = df.iloc[0]["prev_date"]
            if hasattr(prev_date, 'strftime'):
                return prev_date.strftime("%Y-%m-%d")
            return str(prev_date)

    return None


def query_is_trading_day(
    db, date: str, market: str = "cn"
) -> bool:
    """Check if a date is a trading day.

    Args:
        db: DatabaseManager instance
        date: Date string
        market: Market identifier

    Returns:
        True if the date is a trading day
    """
    is_hourly = " " in date

    if is_hourly:
        sql = """
            SELECT COUNT(*) as cnt
            FROM stock_hourly_prices
            WHERE trade_time = ?
            LIMIT 1
        """
        df = db.query(sql, (date,))
    else:
        sql = """
            SELECT COUNT(*) as cnt
            FROM stock_daily_prices
            WHERE trade_date = ? AND market = ?
            LIMIT 1
        """
        df = db.query(sql, (date, market))

    return df.iloc[0]["cnt"] > 0 if not df.empty else False


def query_all_trading_days(
    db, market: str = "cn"
) -> List[str]:
    """Get all trading days from database.

    Args:
        db: DatabaseManager instance
        market: Market identifier

    Returns:
        Sorted list of trading day strings
    """
    sql = """
        SELECT DISTINCT trade_date
        FROM stock_daily_prices
        WHERE market = ?
        ORDER BY trade_date
    """
    df = db.query(sql, (market,))

    if df.empty:
        return []

    # Convert dates to strings
    result = []
    for _, row in df.iterrows():
        trade_date = row["trade_date"]
        if hasattr(trade_date, 'strftime'):
            result.append(trade_date.strftime("%Y-%m-%d"))
        else:
            result.append(str(trade_date))

    return result


# ==================== Position Queries ====================


def query_latest_position(
    db, signature: str, max_date: str
) -> Tuple[Dict[str, float], int]:
    """Query latest position from database.

    Args:
        db: DatabaseManager instance
        signature: Agent signature/name
        max_date: Maximum date to query (exclusive)

    Returns:
        Tuple of (positions dict, max action_id)
    """
    # First try to find position on the given date
    sql = """
        SELECT ts_code, quantity, step_id, cash
        FROM positions
        WHERE agent_name = ? AND trade_date = ?
        ORDER BY step_id DESC
    """
    df = db.query(sql, (signature, max_date))

    if not df.empty:
        max_id = int(df.iloc[0]["step_id"])
        positions = {}
        cash = None
        for _, row in df.iterrows():
            if row["ts_code"] and row["quantity"] and row["quantity"] > 0:
                positions[row["ts_code"]] = float(row["quantity"])
            if row["cash"] is not None:
                cash = float(row["cash"])
        if cash is not None:
            positions["CASH"] = cash
        return positions, max_id

    # Fall back to finding the most recent position before max_date
    sql = """
        SELECT trade_date, ts_code, quantity, step_id, cash
        FROM positions
        WHERE agent_name = ? AND trade_date < ?
        ORDER BY trade_date DESC, step_id DESC
    """
    df = db.query(sql, (signature, max_date))

    if df.empty:
        return {}, -1

    # Get all positions from the most recent date
    latest_date = df.iloc[0]["trade_date"]
    max_id = int(df.iloc[0]["step_id"])

    positions = {}
    cash = None
    for _, row in df.iterrows():
        if row["trade_date"] != latest_date:
            break
        if row["ts_code"] and row["quantity"] and row["quantity"] > 0:
            positions[row["ts_code"]] = float(row["quantity"])
        if row["cash"] is not None:
            cash = float(row["cash"])

    if cash is not None:
        positions["CASH"] = cash

    return positions, max_id


def query_today_init_position(
    db, today_date: str, signature: str
) -> Dict[str, float]:
    """Query opening position for today (end of yesterday).

    Args:
        db: DatabaseManager instance
        today_date: Today's date string
        signature: Agent signature/name

    Returns:
        Position dictionary {symbol: quantity, "CASH": cash_amount}
    """
    sql = """
        SELECT trade_date, ts_code, quantity, step_id, cash
        FROM positions
        WHERE agent_name = ? AND trade_date < ?
        ORDER BY trade_date DESC, step_id DESC
    """
    df = db.query(sql, (signature, today_date))

    if df.empty:
        return {}

    # Get all positions from the most recent date
    latest_date = df.iloc[0]["trade_date"]

    positions = {}
    cash = None
    for _, row in df.iterrows():
        if row["trade_date"] != latest_date:
            break
        if row["ts_code"] and row["quantity"] and row["quantity"] > 0:
            positions[row["ts_code"]] = float(row["quantity"])
        if row["cash"] is not None:
            cash = float(row["cash"])

    if cash is not None:
        positions["CASH"] = cash

    return positions


def insert_position_record(
    db, signature: str, date: str, action: dict, positions: dict
) -> None:
    """Insert a position record into the database.

    Args:
        db: DatabaseManager instance
        signature: Agent signature/name
        date: Trade date
        action: Action dictionary {action, symbol, amount}
        positions: Position dictionary {symbol: quantity, CASH: amount}
    """
    # Get next step_id for this agent
    sql = """
        SELECT COALESCE(MAX(step_id), -1) + 1 as next_step_id
        FROM positions
        WHERE agent_name = ?
    """
    df = db.query(sql, (signature,))
    next_step_id = int(df.iloc[0]["next_step_id"])

    # Get next row id (primary key)
    sql = "SELECT COALESCE(MAX(id), 0) + 1 as next_id FROM positions"
    df = db.query(sql)
    next_row_id = int(df.iloc[0]["next_id"])

    # Extract cash
    cash = positions.get("CASH", 0.0)

    # Insert each stock position as a separate row
    sql = """
        INSERT INTO positions
        (id, agent_name, market, trade_date, step_id, ts_code, quantity, cash, action, action_amount)
        VALUES (?, ?, 'cn', ?, ?, ?, ?, ?, ?, ?)
    """

    has_positions = False
    for symbol, qty in positions.items():
        if symbol == "CASH":
            continue
        has_positions = True
        db.execute(sql, (
            next_row_id, signature, date, next_step_id, symbol, qty, cash,
            action.get("action"), action.get("amount", 0)
        ))
        next_row_id += 1

    # If no stock positions, insert a cash-only record
    if not has_positions:
        db.execute(sql, (
            next_row_id, signature, date, next_step_id, None, 0, cash,
            action.get("action"), action.get("amount", 0)
        ))

    logger.info(f"DuckDB: Inserted position record for {signature} on {date} (step_id={next_step_id})")
