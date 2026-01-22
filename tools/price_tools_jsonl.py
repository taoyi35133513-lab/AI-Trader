"""
JSONL fallback implementations for price and position data access.

This module contains the original JSONL-based implementations extracted from
price_tools.py for use as fallback when DuckDB is unavailable.
"""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _get_merged_file_path(market: str = "cn") -> Path:
    """Get merged.jsonl path for specified market."""
    base_dir = Path(__file__).resolve().parents[1]
    return base_dir / "data" / "A_stock" / "merged.jsonl"


def _resolve_merged_file_path_for_date(
    today_date: Optional[str], market: str, merged_path: Optional[str] = None
) -> Path:
    """Resolve the correct merged data file path."""
    if merged_path is not None:
        return Path(merged_path)
    base_dir = Path(__file__).resolve().parents[1]
    if market == "cn" and today_date and " " in today_date:
        return base_dir / "data" / "A_stock" / "merged_hourly.jsonl"
    return _get_merged_file_path(market)


def get_open_prices_jsonl(
    today_date: str,
    symbols: List[str],
    merged_path: Optional[str] = None,
    market: str = "cn"
) -> Dict[str, Optional[float]]:
    """Read opening prices from JSONL file."""
    wanted = set(symbols)
    results: Dict[str, Optional[float]] = {}

    merged_file = _resolve_merged_file_path_for_date(today_date, market, merged_path)

    if not merged_file.exists():
        return results

    with merged_file.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                doc = json.loads(line)
            except Exception:
                continue
            meta = doc.get("Meta Data", {}) if isinstance(doc, dict) else {}
            sym = meta.get("2. Symbol")
            if sym not in wanted:
                continue
            # Find time series
            series = None
            for key, value in doc.items():
                if key.startswith("Time Series"):
                    series = value
                    break
            if not isinstance(series, dict):
                continue
            bar = series.get(today_date)

            if isinstance(bar, dict):
                open_val = bar.get("1. buy price")

                try:
                    results[f"{sym}_price"] = float(open_val) if open_val is not None else None
                except Exception:
                    results[f"{sym}_price"] = None

    return results


def get_ohlcv_jsonl(
    symbol: str, date: str, market: str = "cn"
) -> Dict[str, Any]:
    """Read OHLCV data from JSONL file."""
    merged_file = _resolve_merged_file_path_for_date(date, market, None)

    if not merged_file.exists():
        return {
            "error": f"Data file not found: {merged_file}",
            "symbol": symbol,
            "date": date
        }

    with merged_file.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            doc = json.loads(line)
            meta = doc.get("Meta Data", {})
            if meta.get("2. Symbol") != symbol:
                continue

            # Find appropriate time series
            is_hourly = " " in date
            series_key = "Time Series (60min)" if is_hourly else "Time Series (Daily)"
            series = doc.get(series_key, {})
            day = series.get(date)

            if day is None:
                sample_dates = sorted(series.keys(), reverse=True)[:5]
                return {
                    "error": f"Data not found for date {date}. Sample dates: {sample_dates}",
                    "symbol": symbol,
                    "date": date,
                }

            return {
                "symbol": symbol,
                "date": date,
                "ohlcv": {
                    "open": day.get("1. buy price"),
                    "high": day.get("2. high"),
                    "low": day.get("3. low"),
                    "close": day.get("4. sell price"),
                    "volume": day.get("5. volume"),
                },
            }

    return {
        "error": f"No records found for stock {symbol}",
        "symbol": symbol,
        "date": date
    }


def get_yesterday_date_jsonl(
    today_date: str, merged_path: Optional[str] = None, market: str = "cn"
) -> str:
    """Get previous trading day from JSONL file."""
    # Parse input date/time
    if ' ' in today_date:
        input_dt = datetime.strptime(today_date, "%Y-%m-%d %H:%M:%S")
        date_only = False
    else:
        input_dt = datetime.strptime(today_date, "%Y-%m-%d")
        date_only = True

    # Get merged.jsonl file path
    merged_file = _resolve_merged_file_path_for_date(today_date, market, merged_path)

    if not merged_file.exists():
        # Fallback to simple date arithmetic
        if date_only:
            yesterday_dt = input_dt - timedelta(days=1)
            while yesterday_dt.weekday() >= 5:
                yesterday_dt -= timedelta(days=1)
            return yesterday_dt.strftime("%Y-%m-%d")
        else:
            yesterday_dt = input_dt - timedelta(hours=1)
            return yesterday_dt.strftime("%Y-%m-%d %H:%M:%S")

    # Read all timestamps from JSONL
    all_timestamps = set()

    with merged_file.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                doc = json.loads(line)
                for key, value in doc.items():
                    if key.startswith("Time Series"):
                        if isinstance(value, dict):
                            all_timestamps.update(value.keys())
                        break
            except Exception:
                continue

    if not all_timestamps:
        if date_only:
            yesterday_dt = input_dt - timedelta(days=1)
            while yesterday_dt.weekday() >= 5:
                yesterday_dt -= timedelta(days=1)
            return yesterday_dt.strftime("%Y-%m-%d")
        else:
            yesterday_dt = input_dt - timedelta(hours=1)
            return yesterday_dt.strftime("%Y-%m-%d %H:%M:%S")

    # Find max timestamp < today_date
    previous_timestamp = None

    for ts_str in all_timestamps:
        try:
            ts_dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            if ts_dt < input_dt:
                if previous_timestamp is None or ts_dt > previous_timestamp:
                    previous_timestamp = ts_dt
        except Exception:
            continue

    if previous_timestamp is None:
        if date_only:
            yesterday_dt = input_dt - timedelta(days=1)
            while yesterday_dt.weekday() >= 5:
                yesterday_dt -= timedelta(days=1)
            return yesterday_dt.strftime("%Y-%m-%d")
        else:
            yesterday_dt = input_dt - timedelta(hours=1)
            return yesterday_dt.strftime("%Y-%m-%d %H:%M:%SS")

    if date_only:
        return previous_timestamp.strftime("%Y-%m-%d")
    else:
        return previous_timestamp.strftime("%Y-%m-%d %H:%M:%S")


def get_yesterday_open_and_close_price_jsonl(
    today_date: str,
    symbols: List[str],
    merged_path: Optional[str] = None,
    market: str = "cn"
) -> Tuple[Dict[str, Optional[float]], Dict[str, Optional[float]]]:
    """Read yesterday's open and close prices from JSONL."""
    wanted = set(symbols)
    buy_results: Dict[str, Optional[float]] = {}
    sell_results: Dict[str, Optional[float]] = {}

    merged_file = _resolve_merged_file_path_for_date(today_date, market, merged_path)

    if not merged_file.exists():
        return buy_results, sell_results

    yesterday_date = get_yesterday_date_jsonl(today_date, merged_path, market)

    with merged_file.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                doc = json.loads(line)
            except Exception:
                continue
            meta = doc.get("Meta Data", {}) if isinstance(doc, dict) else {}
            sym = meta.get("2. Symbol")
            if sym not in wanted:
                continue
            # Find time series
            series = None
            for key, value in doc.items():
                if key.startswith("Time Series"):
                    series = value
                    break
            if not isinstance(series, dict):
                continue

            bar = series.get(yesterday_date)
            if isinstance(bar, dict):
                buy_val = bar.get("1. buy price")
                sell_val = bar.get("4. sell price")

                try:
                    buy_price = float(buy_val) if buy_val is not None else None
                    sell_price = float(sell_val) if sell_val is not None else None
                    buy_results[f"{sym}_price"] = buy_price
                    sell_results[f"{sym}_price"] = sell_price
                except Exception:
                    buy_results[f"{sym}_price"] = None
                    sell_results[f"{sym}_price"] = None
            else:
                buy_results[f'{sym}_price'] = None
                sell_results[f'{sym}_price'] = None

    return buy_results, sell_results


def is_trading_day_jsonl(date: str, market: str = "cn") -> bool:
    """Check if date is a trading day from JSONL."""
    merged_file_path = _get_merged_file_path(market)

    if not merged_file_path.exists():
        return False

    try:
        with open(merged_file_path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    data = json.loads(line.strip())
                    time_series = data.get("Time Series (Daily)", {})
                    if date in time_series:
                        return True
                    for key, value in data.items():
                        if key.startswith("Time Series") and isinstance(value, dict):
                            for timestamp in value.keys():
                                if timestamp.startswith(date):
                                    return True
                except json.JSONDecodeError:
                    continue
            return False
    except Exception:
        return False


def get_all_trading_days_jsonl(market: str = "cn") -> List[str]:
    """Get all trading days from JSONL."""
    merged_file_path = _get_merged_file_path(market)

    if not merged_file_path.exists():
        return []

    trading_days = set()
    try:
        with open(merged_file_path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    data = json.loads(line.strip())
                    time_series = data.get("Time Series (Daily)", {})
                    trading_days.update(time_series.keys())
                except json.JSONDecodeError:
                    continue
        return sorted(list(trading_days))
    except Exception:
        return []


# ==================== Position Functions ====================


def _get_position_file(signature: str) -> Path:
    """Get position.jsonl file path."""
    from tools.general_tools import get_config_value

    base_dir = Path(__file__).resolve().parents[1]
    log_path = get_config_value("LOG_PATH", "./data/agent_data")

    if os.path.isabs(log_path):
        return Path(log_path) / signature / "position" / "position.jsonl"
    else:
        if log_path.startswith("./data/"):
            log_path = log_path[7:]
        return base_dir / "data" / log_path / signature / "position" / "position.jsonl"


def get_latest_position_jsonl(
    today_date: str, signature: str
) -> Tuple[Dict[str, float], int]:
    """Read latest position from JSONL file."""
    position_file = _get_position_file(signature)

    if not position_file.exists():
        return {}, -1

    # Try today first
    max_id_today = -1
    latest_positions_today: Dict[str, float] = {}

    with position_file.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                doc = json.loads(line)
                if doc.get("date") == today_date:
                    current_id = doc.get("id", -1)
                    if current_id > max_id_today:
                        max_id_today = current_id
                        latest_positions_today = doc.get("positions", {})
            except Exception:
                continue

    if max_id_today >= 0 and latest_positions_today:
        return latest_positions_today, max_id_today

    # Fall back to finding most recent before today
    all_records = []
    with position_file.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                doc = json.loads(line)
                record_date = doc.get("date")
                if record_date and record_date < today_date:
                    positions = doc.get("positions", {})
                    if positions:
                        all_records.append(doc)
            except Exception:
                continue

    if all_records:
        all_records.sort(
            key=lambda x: (x.get("date", ""), x.get("id", 0)),
            reverse=True
        )
        return all_records[0].get("positions", {}), all_records[0].get("id", -1)

    return {}, -1


def get_today_init_position_jsonl(
    today_date: str, signature: str
) -> Dict[str, float]:
    """Read today's init position from JSONL file."""
    position_file = _get_position_file(signature)

    if not position_file.exists():
        return {}

    all_records = []
    with position_file.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                doc = json.loads(line)
                record_date = doc.get("date")
                if record_date and record_date < today_date:
                    all_records.append(doc)
            except Exception:
                continue

    if not all_records:
        return {}

    all_records.sort(key=lambda x: (x.get("date", ""), x.get("id", 0)), reverse=True)
    return all_records[0].get("positions", {})


def add_position_record_jsonl(
    date: str, signature: str, action: dict, positions: dict
) -> None:
    """Append position record to JSONL file."""
    position_file = _get_position_file(signature)

    # Get next ID
    _, current_max_id = get_latest_position_jsonl(date, signature)
    next_id = current_max_id + 1

    save_item = {
        "date": date,
        "id": next_id,
        "this_action": action,
        "positions": positions
    }

    # Ensure directory exists
    position_file.parent.mkdir(parents=True, exist_ok=True)

    with position_file.open("a", encoding="utf-8") as f:
        f.write(json.dumps(save_item) + "\n")
