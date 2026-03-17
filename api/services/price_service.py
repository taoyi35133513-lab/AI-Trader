"""
价格数据服务
"""

import json
import logging
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


class PriceService:
    """价格数据服务"""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def get_daily_prices(
        self,
        symbols: List[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        market: str = "cn",
    ) -> Dict[str, List[dict]]:
        """获取日线价格数据

        Args:
            symbols: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            market: 市场

        Returns:
            {symbol: [price_data]}
        """
        # 构建 SQL
        placeholders = ", ".join(["?" for _ in symbols])
        sql = f"""
            SELECT ts_code, trade_date, open, high, low, close, volume, amount
            FROM stock_daily_prices
            WHERE ts_code IN ({placeholders})
        """
        params = list(symbols)

        if start_date:
            sql += " AND trade_date >= ?"
            params.append(start_date)
        if end_date:
            sql += " AND trade_date <= ?"
            params.append(end_date)

        sql += " ORDER BY ts_code, trade_date"

        df = self.conn.execute(sql, params).df()

        # 按股票代码分组
        result = {}
        for symbol in symbols:
            symbol_df = df[df["ts_code"] == symbol]
            result[symbol] = symbol_df.to_dict("records")

        return result

    def get_price_on_date(self, symbol: str, trade_date: date) -> Optional[dict]:
        """获取指定日期的价格

        Args:
            symbol: 股票代码
            trade_date: 交易日期

        Returns:
            价格数据或 None
        """
        sql = """
            SELECT ts_code, trade_date, open, high, low, close, volume
            FROM stock_daily_prices
            WHERE ts_code = ? AND trade_date = ?
        """
        result = self.conn.execute(sql, [symbol, trade_date]).fetchone()
        if result:
            return {
                "ts_code": result[0],
                "trade_date": result[1],
                "open": result[2],
                "high": result[3],
                "low": result[4],
                "close": result[5],
                "volume": result[6],
            }
        return None

    def get_latest_price(self, symbol: str) -> Optional[dict]:
        """获取最新价格

        Args:
            symbol: 股票代码

        Returns:
            最新价格数据
        """
        sql = """
            SELECT ts_code, trade_date, open, high, low, close, volume
            FROM stock_daily_prices
            WHERE ts_code = ?
            ORDER BY trade_date DESC
            LIMIT 1
        """
        result = self.conn.execute(sql, [symbol]).fetchone()
        if result:
            return {
                "ts_code": result[0],
                "trade_date": result[1],
                "open": result[2],
                "high": result[3],
                "low": result[4],
                "close": result[5],
                "volume": result[6],
            }
        return None

    def get_hourly_prices(
        self,
        symbols: List[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Dict[str, List[dict]]:
        """获取小时线价格数据"""
        placeholders = ", ".join(["?" for _ in symbols])
        sql = f"""
            SELECT ts_code, trade_time, open, high, low, close, volume
            FROM stock_hourly_prices
            WHERE ts_code IN ({placeholders})
        """
        params = list(symbols)

        if start_date:
            sql += " AND DATE(trade_time) >= ?"
            params.append(start_date)
        if end_date:
            sql += " AND DATE(trade_time) <= ?"
            params.append(end_date)

        sql += " ORDER BY ts_code, trade_time"

        df = self.conn.execute(sql, params).df()

        result = {}
        for symbol in symbols:
            symbol_df = df[df["ts_code"] == symbol]
            result[symbol] = symbol_df.to_dict("records")

        return result

    def get_benchmark_data(
        self,
        market: str = "cn",
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[dict]:
        """获取基准指数数据

        对于 A 股市场(cn 和 cn_hour)，使用上证50指数日线数据；
        对于美股市场，需要从文件加载 QQQ 数据。
        """
        if market in ("cn", "cn_hour"):
            project_root = Path(__file__).parent.parent.parent

            # For hourly market, try hourly index file first
            if market == "cn_hour":
                hourly_file = project_root / "data" / "A_stock" / "index_hourly_sse_50.json"
                if hourly_file.exists():
                    try:
                        with open(hourly_file, "r", encoding="utf-8") as f:
                            hourly_data = json.load(f)
                        time_series = hourly_data.get("Time Series (60min)", {})
                        result = []
                        for ts_str, values in time_series.items():
                            day_part = ts_str.split(" ")[0]
                            try:
                                record_date = date.fromisoformat(day_part)
                            except ValueError:
                                continue
                            if start_date and record_date < start_date:
                                continue
                            if end_date and record_date > end_date:
                                continue
                            close_val = values.get("4. close")
                            if close_val is not None:
                                result.append({"trade_date": ts_str, "close": float(close_val)})
                        if result:
                            result.sort(key=lambda x: x["trade_date"])
                            return result
                    except Exception as e:
                        logger.debug("Hourly index load failed, falling back to daily: %s", e)

            # Load SSE 50 daily index data from JSON file
            index_file = project_root / "data" / "A_stock" / "index_daily_sse_50.json"

            if not index_file.exists():
                logger.warning("SSE 50 index file not found: %s", index_file)
                return []

            try:
                with open(index_file, "r", encoding="utf-8") as f:
                    index_data = json.load(f)
            except Exception as e:
                logger.error("Failed to load SSE 50 index file: %s", e)
                return []

            time_series = index_data.get("Time Series (Daily)", {})
            result = []
            for date_str, values in time_series.items():
                try:
                    record_date = date.fromisoformat(date_str)
                except ValueError:
                    continue

                if start_date and record_date < start_date:
                    continue
                if end_date and record_date > end_date:
                    continue

                close_val = values.get("4. close")
                if close_val is not None:
                    result.append({
                        "trade_date": date_str,
                        "close": float(close_val),
                    })

            # Sort by date ascending
            result.sort(key=lambda x: x["trade_date"])
            return result

        # US market - 需要从文件加载
        return []
