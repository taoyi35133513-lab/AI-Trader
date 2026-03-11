"""
Price data sync: fetch candle data from AKShare and insert into DuckDB.

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
    Fetch all hourly candles for *trade_date* via AKShare and upsert into DuckDB.

    Args:
        trade_date: Date string "YYYY-MM-DD"

    Returns:
        Number of records inserted
    """
    import akshare as ak
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
    for sym in symbols:
        code = sym.replace(".SH", "").replace(".SZ", "")
        try:
            df = ak.stock_zh_a_hist_min_em(
                symbol=code,
                period="60",
                start_date=f"{trade_date} 09:30:00",
                end_date=f"{trade_date} 15:00:00",
            )
            for _, row in df.iterrows():
                conn.execute(
                    "INSERT INTO stock_hourly_prices "
                    "(ts_code, trade_time, open, high, low, close, volume, market) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, 'cn')",
                    [
                        sym,
                        str(row["时间"]),
                        float(row["开盘"]),
                        float(row["最高"]),
                        float(row["最低"]),
                        float(row["收盘"]),
                        int(row["成交量"]),
                    ],
                )
                count += 1
            time.sleep(0.3)
        except Exception as e:
            logger.debug("sync_hourly %s: %s", sym, e)

    conn.close()
    logger.info("sync_hourly_prices: inserted %d records for %s", count, trade_date)
    return count


def sync_daily_prices(trade_date: str) -> int:
    """
    Fetch daily close for *trade_date* via AKShare and upsert into DuckDB.

    Args:
        trade_date: Date string "YYYY-MM-DD"

    Returns:
        Number of records inserted
    """
    import akshare as ak
    import duckdb

    db_path = PROJECT_ROOT / "data" / "database" / "ai_trader.duckdb"
    conn = duckdb.connect(str(db_path))

    conn.execute(
        "DELETE FROM stock_daily_prices WHERE trade_date = ?", [trade_date]
    )

    ak_date = trade_date.replace("-", "")
    symbols = _get_sse50_symbols()
    count = 0
    for sym in symbols:
        code = sym.replace(".SH", "").replace(".SZ", "")
        try:
            df = ak.stock_zh_a_hist(
                symbol=code, period="daily", start_date=ak_date, end_date=ak_date
            )
            if len(df) > 0:
                row = df.iloc[-1]
                conn.execute(
                    "INSERT INTO stock_daily_prices "
                    "(ts_code, trade_date, open, high, low, close, volume, market) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, 'cn')",
                    [
                        sym,
                        trade_date,
                        float(row["开盘"]),
                        float(row["最高"]),
                        float(row["最低"]),
                        float(row["收盘"]),
                        int(row["成交量"]),
                    ],
                )
                count += 1
            time.sleep(0.3)
        except Exception as e:
            logger.debug("sync_daily %s: %s", sym, e)

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
    import akshare as ak

    index_file = PROJECT_ROOT / "data" / "A_stock" / "index_daily_sse_50.json"
    if not index_file.exists():
        logger.warning("SSE 50 index file not found")
        return False

    try:
        df = ak.stock_zh_index_daily_em(
            symbol="sh000016",
            start_date=trade_date.replace("-", ""),
            end_date=trade_date.replace("-", ""),
        )
        if len(df) == 0:
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
            "5. volume": str(int(row["volume"])),
        }

        with open(index_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info("SSE 50 index updated for %s: close=%s", trade_date, row["close"])
        return True
    except Exception as e:
        logger.error("update_sse50_index failed: %s", e)
        return False
