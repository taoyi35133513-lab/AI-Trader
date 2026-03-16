"""
Price data sync: fetch candle data from Tushare and insert into DuckDB.

Called by the scheduler after each trading session to keep the dashboard
gap-filling and benchmark alignment working correctly.
"""

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _get_sse50_symbols() -> List[str]:
    """Read SSE 50 symbol list from merged_hourly.jsonl."""
    merged = PROJECT_ROOT / "data" / "A_stock" / "merged_hourly.jsonl"
    symbols = []
    if not merged.exists():
        merged = PROJECT_ROOT / "data" / "A_stock" / "merged.jsonl"
    with open(merged) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            meta = json.loads(line).get("Meta Data", {})
            sym = meta.get("2. Symbol", "")
            if sym:
                symbols.append(sym)
    return symbols


def sync_hourly_prices(trade_date: str) -> int:
    """
    Fetch hourly candles via tushare rt_min and upsert into DuckDB.

    Uses rt_min(freq='60MIN') which supports batch queries with comma-separated
    ts_codes (up to 1000 rows per request).

    Falls back to reading from merged_hourly.jsonl if rt_min fails.

    Args:
        trade_date: Date string "YYYY-MM-DD"

    Returns:
        Number of records inserted
    """
    import duckdb

    db_path = PROJECT_ROOT / "data" / "database" / "ai_trader.duckdb"
    conn = duckdb.connect(str(db_path))

    # Clean existing data for this date to avoid duplicates
    conn.execute(
        "DELETE FROM stock_hourly_prices WHERE CAST(trade_time AS DATE) = ?",
        [trade_date],
    )

    symbols = _get_sse50_symbols()
    count = 0

    # Try tushare rt_min batch query first
    try:
        from tools.tushare_client import get_tushare_pro

        pro = get_tushare_pro()
        ts_codes = ",".join(symbols)
        df = pro.rt_min(ts_code=ts_codes, freq="60MIN")

        if df is not None and not df.empty:
            # rt_min returns latest candle; filter for today
            for _, row in df.iterrows():
                trade_time = str(row["time"])
                if not trade_time.startswith(trade_date):
                    continue
                conn.execute(
                    "INSERT INTO stock_hourly_prices "
                    "(ts_code, trade_time, open, high, low, close, volume, market) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, 'cn')",
                    [
                        row["ts_code"],
                        trade_time,
                        float(row["open"]),
                        float(row["high"]),
                        float(row["low"]),
                        float(row["close"]),
                        int(float(row["vol"])),
                    ],
                )
                count += 1

            if count > 0:
                conn.close()
                logger.info("sync_hourly_prices (rt_min): inserted %d records for %s", count, trade_date)
                return count
    except Exception as e:
        logger.debug("rt_min failed, falling back to JSONL: %s", e)

    # Fallback: read from merged_hourly.jsonl
    merged = PROJECT_ROOT / "data" / "A_stock" / "merged_hourly.jsonl"
    if not merged.exists():
        conn.close()
        logger.warning("merged_hourly.jsonl not found")
        return 0

    with open(merged, "r", encoding="utf-8") as f:
        for line in f:
            try:
                data = json.loads(line)
                sym = data.get("Meta Data", {}).get("2. Symbol", "")
                ts = data.get("Time Series (60min)", {})
                for time_key, prices in ts.items():
                    if not time_key.startswith(trade_date):
                        continue
                    conn.execute(
                        "INSERT INTO stock_hourly_prices "
                        "(ts_code, trade_time, open, high, low, close, volume, market) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, 'cn')",
                        [
                            sym,
                            time_key,
                            float(prices["1. buy price"]),
                            float(prices["2. high"]),
                            float(prices["3. low"]),
                            float(prices["4. sell price"]),
                            int(float(prices["5. volume"])),
                        ],
                    )
                    count += 1
            except (json.JSONDecodeError, KeyError) as e:
                logger.debug("sync_hourly parse error: %s", e)

    conn.close()
    logger.info("sync_hourly_prices (jsonl fallback): inserted %d records for %s", count, trade_date)
    return count


def sync_daily_prices(trade_date: str) -> int:
    """
    Fetch daily close for *trade_date* via Tushare and upsert into DuckDB.

    Args:
        trade_date: Date string "YYYY-MM-DD"

    Returns:
        Number of records inserted
    """
    import duckdb
    from tools.tushare_client import get_tushare_pro

    pro = get_tushare_pro()
    db_path = PROJECT_ROOT / "data" / "database" / "ai_trader.duckdb"
    conn = duckdb.connect(str(db_path))

    conn.execute(
        "DELETE FROM stock_daily_prices WHERE trade_date = ?", [trade_date]
    )

    ts_date = trade_date.replace("-", "")
    symbols = _get_sse50_symbols()

    # Batch fetch: tushare daily supports trade_date param for all stocks
    try:
        df = pro.daily(trade_date=ts_date)
    except Exception as e:
        logger.error("sync_daily_prices batch fetch failed: %s", e)
        conn.close()
        return 0

    if df is None or df.empty:
        conn.close()
        logger.info("sync_daily_prices: no data for %s", trade_date)
        return 0

    symbol_set = set(symbols)
    df_filtered = df[df["ts_code"].isin(symbol_set)]

    count = 0
    for _, row in df_filtered.iterrows():
        try:
            conn.execute(
                "INSERT INTO stock_daily_prices "
                "(ts_code, trade_date, open, high, low, close, volume, market) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, 'cn')",
                [
                    row["ts_code"],
                    trade_date,
                    float(row["open"]),
                    float(row["high"]),
                    float(row["low"]),
                    float(row["close"]),
                    int(float(row["vol"])),
                ],
            )
            count += 1
        except Exception as e:
            logger.debug("sync_daily %s: %s", row["ts_code"], e)

    conn.close()
    logger.info("sync_daily_prices: inserted %d records for %s", count, trade_date)
    return count


def update_sse50_index(trade_date: str) -> bool:
    """
    Fetch today's SSE 50 index close and append to index_daily_sse_50.json.

    Args:
        trade_date: Date string "YYYY-MM-DD"

    Returns:
        True if updated successfully
    """
    from tools.tushare_client import get_tushare_pro

    pro = get_tushare_pro()
    index_file = PROJECT_ROOT / "data" / "A_stock" / "index_daily_sse_50.json"
    if not index_file.exists():
        logger.warning("SSE 50 index file not found")
        return False

    try:
        ts_date = trade_date.replace("-", "")
        # SSE 50 index code in tushare: 000016.SH
        df = pro.index_daily(ts_code="000016.SH", start_date=ts_date, end_date=ts_date)
        if df is None or len(df) == 0:
            logger.warning("No SSE 50 data for %s", trade_date)
            return False

        row = df.iloc[-1]

        with open(index_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        ts = data.setdefault("Time Series (Daily)", {})
        ts[trade_date] = {
            "1. open": str(row["open"]),
            "2. high": str(row["high"]),
            "3. low": str(row["low"]),
            "4. close": str(row["close"]),
            "5. volume": str(int(float(row["vol"]))),
        }

        with open(index_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info("SSE 50 index updated for %s: close=%s", trade_date, row["close"])
        return True
    except Exception as e:
        logger.error("update_sse50_index failed: %s", e)
        return False
