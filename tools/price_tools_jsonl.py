"""
JSONL fallback implementations for price and position data access.

This module contains the original JSONL-based implementations extracted from
price_tools.py for use as fallback when DuckDB is unavailable.

Performance note: The in-memory cache (_JsonlCache) avoids re-parsing the
entire JSONL file on every query.  The cache is keyed by file path and
invalidated when the file's mtime changes.
"""

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def _parse_date(date_str: str) -> datetime:
    """Parse a date string in either 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS' format."""
    if " " in date_str:
        return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    return datetime.strptime(date_str, "%Y-%m-%d")


def _date_part(date_str: str) -> str:
    """Extract just the date portion (YYYY-MM-DD) from a date or datetime string."""
    return date_str.split(" ")[0]


# ---------------------------------------------------------------------------
# In-memory JSONL cache
# ---------------------------------------------------------------------------

class _JsonlCache:
    """Lazy, mtime-aware cache for parsed JSONL price files.

    Structure per file:
        {symbol: {date_or_timestamp: bar_dict, ...}, ...}
    """

    def __init__(self):
        self._data: Dict[str, Dict[str, Dict[str, Any]]] = {}   # path -> {sym -> {date -> bar}}
        self._mtime: Dict[str, float] = {}                       # path -> last mtime

    def get(self, file_path: Path) -> Dict[str, Dict[str, Any]]:
        """Return cached data for *file_path*, re-reading if stale."""
        key = str(file_path)

        if not file_path.exists():
            return {}

        current_mtime = file_path.stat().st_mtime
        if key in self._data and self._mtime.get(key) == current_mtime:
            return self._data[key]

        # (Re)parse
        logger.info("JSONL cache miss — parsing %s", file_path)
        symbol_map: Dict[str, Dict[str, Any]] = {}

        with file_path.open("r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    doc = json.loads(line)
                except Exception:
                    continue
                meta = doc.get("Meta Data", {}) if isinstance(doc, dict) else {}
                sym = meta.get("2. Symbol")
                if not sym:
                    continue
                # Find the time-series dict
                series = None
                for k, v in doc.items():
                    if k.startswith("Time Series"):
                        series = v
                        break
                if isinstance(series, dict):
                    symbol_map[sym] = series

        self._data[key] = symbol_map
        self._mtime[key] = current_mtime
        return symbol_map


_cache = _JsonlCache()


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
    """Read opening prices from JSONL file (cached)."""
    merged_file = _resolve_merged_file_path_for_date(today_date, market, merged_path)
    symbol_map = _cache.get(merged_file)

    results: Dict[str, Optional[float]] = {}
    for sym in symbols:
        series = symbol_map.get(sym)
        if series is None:
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
    """Read OHLCV data from JSONL file (cached)."""
    merged_file = _resolve_merged_file_path_for_date(date, market, None)
    symbol_map = _cache.get(merged_file)

    if not symbol_map:
        return {"error": f"Data file not found or empty: {merged_file}", "symbol": symbol, "date": date}

    series = symbol_map.get(symbol)
    if series is None:
        return {"error": f"No records found for stock {symbol}", "symbol": symbol, "date": date}

    day = series.get(date)
    if day is None:
        sample_dates = sorted(series.keys(), reverse=True)[:5]
        return {"error": f"Data not found for date {date}. Sample dates: {sample_dates}", "symbol": symbol, "date": date}

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


def get_yesterday_date_jsonl(
    today_date: str, merged_path: Optional[str] = None, market: str = "cn"
) -> str:
    """Get previous trading day from JSONL file (cached)."""
    if ' ' in today_date:
        input_dt = datetime.strptime(today_date, "%Y-%m-%d %H:%M:%S")
        date_only = False
    else:
        input_dt = datetime.strptime(today_date, "%Y-%m-%d")
        date_only = True

    merged_file = _resolve_merged_file_path_for_date(today_date, market, merged_path)
    symbol_map = _cache.get(merged_file)

    # Collect all timestamps across all symbols from cache
    all_timestamps: set[str] = set()
    for series in symbol_map.values():
        all_timestamps.update(series.keys())

    def _simple_fallback() -> str:
        if date_only:
            dt = input_dt - timedelta(days=1)
            while dt.weekday() >= 5:
                dt -= timedelta(days=1)
            return dt.strftime("%Y-%m-%d")
        else:
            return (input_dt - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")

    if not all_timestamps:
        return _simple_fallback()

    # Find max timestamp < today_date
    previous_timestamp = None
    fmt = "%Y-%m-%d" if date_only else "%Y-%m-%d %H:%M:%S"
    for ts_str in all_timestamps:
        try:
            ts_dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            try:
                ts_dt = datetime.strptime(ts_str, "%Y-%m-%d")
            except ValueError:
                continue
        if ts_dt < input_dt:
            if previous_timestamp is None or ts_dt > previous_timestamp:
                previous_timestamp = ts_dt

    if previous_timestamp is None:
        return _simple_fallback()

    if date_only:
        return previous_timestamp.strftime("%Y-%m-%d")
    return previous_timestamp.strftime("%Y-%m-%d %H:%M:%S")


def get_yesterday_open_and_close_price_jsonl(
    today_date: str,
    symbols: List[str],
    merged_path: Optional[str] = None,
    market: str = "cn"
) -> Tuple[Dict[str, Optional[float]], Dict[str, Optional[float]]]:
    """Read yesterday's open and close prices from JSONL (cached)."""
    buy_results: Dict[str, Optional[float]] = {}
    sell_results: Dict[str, Optional[float]] = {}

    merged_file = _resolve_merged_file_path_for_date(today_date, market, merged_path)
    symbol_map = _cache.get(merged_file)

    if not symbol_map:
        return buy_results, sell_results

    yesterday_date = get_yesterday_date_jsonl(today_date, merged_path, market)

    for sym in symbols:
        series = symbol_map.get(sym)
        if series is None:
            continue
        bar = series.get(yesterday_date)
        if isinstance(bar, dict):
            buy_val = bar.get("1. buy price")
            sell_val = bar.get("4. sell price")
            try:
                buy_results[f"{sym}_price"] = float(buy_val) if buy_val is not None else None
                sell_results[f"{sym}_price"] = float(sell_val) if sell_val is not None else None
            except Exception:
                buy_results[f"{sym}_price"] = None
                sell_results[f"{sym}_price"] = None
        else:
            buy_results[f"{sym}_price"] = None
            sell_results[f"{sym}_price"] = None

    return buy_results, sell_results


def is_trading_day_jsonl(date: str, market: str = "cn") -> bool:
    """Check if date is a trading day from JSONL (cached)."""
    merged_file_path = _get_merged_file_path(market)
    symbol_map = _cache.get(merged_file_path)

    for series in symbol_map.values():
        if date in series:
            return True
        # Check prefix match for hourly timestamps
        for ts in series:
            if ts.startswith(date):
                return True
    return False


def get_all_trading_days_jsonl(market: str = "cn") -> List[str]:
    """Get all trading days from JSONL (cached)."""
    merged_file_path = _get_merged_file_path(market)
    symbol_map = _cache.get(merged_file_path)

    trading_days: set[str] = set()
    for series in symbol_map.values():
        trading_days.update(series.keys())
    return sorted(trading_days)


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

    today_dt = _parse_date(today_date)
    today_date_part = _date_part(today_date)

    # Try same-day records first (compare date part to catch all same-day timestamps)
    max_id_today = -1
    latest_positions_today: Dict[str, float] = {}

    with position_file.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                doc = json.loads(line)
                record_date = doc.get("date")
                if not record_date:
                    continue
                # Match same-day records (handles hourly timestamps on same date)
                if _date_part(record_date) == today_date_part:
                    record_dt = _parse_date(record_date)
                    # Only include records at or before today_date
                    if record_dt <= today_dt:
                        current_id = doc.get("id", -1)
                        if current_id > max_id_today:
                            max_id_today = current_id
                            latest_positions_today = doc.get("positions", {})
            except Exception:
                continue

    if max_id_today >= 0 and latest_positions_today:
        return latest_positions_today, max_id_today

    # Fall back to finding most recent before today (use datetime comparison)
    all_records = []
    with position_file.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                doc = json.loads(line)
                record_date = doc.get("date")
                if record_date and _parse_date(record_date) < today_dt:
                    positions = doc.get("positions", {})
                    if positions:
                        all_records.append(doc)
            except Exception:
                continue

    if all_records:
        # Sort by (date_part, id) so all records on the same calendar date are
        # grouped together regardless of time component.  This prevents a
        # registration record at "2026-03-10 12:00:00" from outranking a later
        # buy record stored as "2026-03-10" (parsed as midnight).
        all_records.sort(
            key=lambda x: (_date_part(x.get("date", "1970-01-01")), x.get("id", 0)),
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

    today_dt = _parse_date(today_date)

    all_records = []
    with position_file.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                doc = json.loads(line)
                record_date = doc.get("date")
                if record_date and _parse_date(record_date) < today_dt:
                    all_records.append(doc)
            except Exception:
                continue

    if not all_records:
        return {}

    all_records.sort(
        key=lambda x: (_parse_date(x.get("date", "1970-01-01")), x.get("id", 0)),
        reverse=True
    )
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
