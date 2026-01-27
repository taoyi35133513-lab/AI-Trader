"""
行情数据查询服务

提供历史及实时行情数据查询功能，支持日线和小时线。
"""

from datetime import date, datetime
from typing import Any, Dict, List, Optional

import duckdb


class MarketDataService:
    """行情数据查询服务"""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def get_prices(
        self,
        symbols: List[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        frequency: str = "daily",
        market: str = "cn",
    ) -> Dict[str, Any]:
        """查询多个股票的历史价格

        Args:
            symbols: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            frequency: 数据频率 ("daily" 或 "hourly")
            market: 市场标识

        Returns:
            包含价格数据的字典
        """
        if not symbols:
            return {"data": {}, "count": 0}

        placeholders = ", ".join(["?" for _ in symbols])
        params = list(symbols)

        if frequency == "hourly":
            sql = f"""
                SELECT ts_code, trade_time, open, high, low, close, volume
                FROM stock_hourly_prices
                WHERE ts_code IN ({placeholders})
            """
            if start_date:
                sql += " AND DATE(trade_time) >= ?"
                params.append(start_date)
            if end_date:
                sql += " AND DATE(trade_time) <= ?"
                params.append(end_date)
            sql += " ORDER BY ts_code, trade_time"
            date_field = "trade_time"
        else:
            sql = f"""
                SELECT ts_code, trade_date, open, high, low, close, volume, amount
                FROM stock_daily_prices
                WHERE ts_code IN ({placeholders})
            """
            if start_date:
                sql += " AND trade_date >= ?"
                params.append(start_date)
            if end_date:
                sql += " AND trade_date <= ?"
                params.append(end_date)
            sql += " ORDER BY ts_code, trade_date"
            date_field = "trade_date"

        df = self.conn.execute(sql, params).df()

        # 按股票代码分组
        result = {}
        total_count = 0
        for symbol in symbols:
            symbol_df = df[df["ts_code"] == symbol]
            records = symbol_df.to_dict("records")
            # 转换日期字段名称为统一的 "date"
            for record in records:
                if date_field in record:
                    record["date"] = str(record.pop(date_field))
                record.pop("ts_code", None)
            result[symbol] = records
            total_count += len(records)

        return {
            "data": result,
            "count": total_count,
            "query": {
                "symbols": symbols,
                "start_date": str(start_date) if start_date else None,
                "end_date": str(end_date) if end_date else None,
                "frequency": frequency,
                "market": market,
            },
        }

    def get_snapshot(
        self,
        symbols: List[str],
        date_str: str,
        frequency: str = "daily",
    ) -> Dict[str, Any]:
        """获取特定日期/时间的价格快照

        Args:
            symbols: 股票代码列表
            date_str: 日期或时间字符串 (daily: "2025-01-20", hourly: "2025-01-20 10:30:00")
            frequency: 数据频率 ("daily" 或 "hourly")

        Returns:
            包含价格快照的字典
        """
        if not symbols:
            return {"date": date_str, "prices": {}}

        placeholders = ", ".join(["?" for _ in symbols])
        params = list(symbols)
        params.append(date_str)

        if frequency == "hourly":
            sql = f"""
                SELECT ts_code, trade_time, open, high, low, close, volume
                FROM stock_hourly_prices
                WHERE ts_code IN ({placeholders})
                  AND trade_time = ?
            """
        else:
            sql = f"""
                SELECT ts_code, trade_date, open, high, low, close, volume, amount
                FROM stock_daily_prices
                WHERE ts_code IN ({placeholders})
                  AND trade_date = ?
            """

        df = self.conn.execute(sql, params).df()

        prices = {}
        for _, row in df.iterrows():
            symbol = row["ts_code"]
            price_data = {
                "open": float(row["open"]) if row["open"] else None,
                "high": float(row["high"]) if row["high"] else None,
                "low": float(row["low"]) if row["low"] else None,
                "close": float(row["close"]) if row["close"] else None,
                "volume": int(row["volume"]) if row["volume"] else None,
            }
            if "amount" in row and row["amount"]:
                price_data["amount"] = float(row["amount"])
            prices[symbol] = price_data

        return {
            "date": date_str,
            "frequency": frequency,
            "prices": prices,
        }

    def get_latest_prices(
        self,
        symbols: List[str],
        frequency: str = "daily",
    ) -> Dict[str, Any]:
        """获取最新价格

        Args:
            symbols: 股票代码列表
            frequency: 数据频率 ("daily" 或 "hourly")

        Returns:
            包含最新价格的字典
        """
        if not symbols:
            return {"prices": {}}

        prices = {}
        for symbol in symbols:
            if frequency == "hourly":
                sql = """
                    SELECT ts_code, trade_time, open, high, low, close, volume
                    FROM stock_hourly_prices
                    WHERE ts_code = ?
                    ORDER BY trade_time DESC
                    LIMIT 1
                """
                date_field = "trade_time"
            else:
                sql = """
                    SELECT ts_code, trade_date, open, high, low, close, volume, amount
                    FROM stock_daily_prices
                    WHERE ts_code = ?
                    ORDER BY trade_date DESC
                    LIMIT 1
                """
                date_field = "trade_date"

            result = self.conn.execute(sql, [symbol]).fetchone()
            if result:
                price_data = {
                    "date": str(result[1]),
                    "open": float(result[2]) if result[2] else None,
                    "high": float(result[3]) if result[3] else None,
                    "low": float(result[4]) if result[4] else None,
                    "close": float(result[5]) if result[5] else None,
                    "volume": int(result[6]) if result[6] else None,
                }
                if len(result) > 7 and result[7]:
                    price_data["amount"] = float(result[7])
                prices[symbol] = price_data

        return {
            "frequency": frequency,
            "prices": prices,
        }

    def get_ohlcv(
        self,
        symbol: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        frequency: str = "daily",
    ) -> Dict[str, Any]:
        """获取单个股票的详细 OHLCV 数据

        Args:
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            frequency: 数据频率 ("daily" 或 "hourly")

        Returns:
            包含 OHLCV 数据的字典
        """
        params = [symbol]

        if frequency == "hourly":
            sql = """
                SELECT trade_time, open, high, low, close, volume
                FROM stock_hourly_prices
                WHERE ts_code = ?
            """
            if start_date:
                sql += " AND DATE(trade_time) >= ?"
                params.append(start_date)
            if end_date:
                sql += " AND DATE(trade_time) <= ?"
                params.append(end_date)
            sql += " ORDER BY trade_time"
        else:
            sql = """
                SELECT trade_date, open, high, low, close, volume, amount
                FROM stock_daily_prices
                WHERE ts_code = ?
            """
            if start_date:
                sql += " AND trade_date >= ?"
                params.append(start_date)
            if end_date:
                sql += " AND trade_date <= ?"
                params.append(end_date)
            sql += " ORDER BY trade_date"

        df = self.conn.execute(sql, params).df()
        records = df.to_dict("records")

        # 格式化记录
        formatted = []
        for record in records:
            item = {
                "date": str(record.get("trade_date") or record.get("trade_time")),
                "open": float(record["open"]) if record["open"] else None,
                "high": float(record["high"]) if record["high"] else None,
                "low": float(record["low"]) if record["low"] else None,
                "close": float(record["close"]) if record["close"] else None,
                "volume": int(record["volume"]) if record["volume"] else None,
            }
            if "amount" in record and record["amount"]:
                item["amount"] = float(record["amount"])
            formatted.append(item)

        return {
            "symbol": symbol,
            "frequency": frequency,
            "count": len(formatted),
            "data": formatted,
        }
