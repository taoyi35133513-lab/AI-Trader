"""
价格数据服务
"""

from datetime import date
from decimal import Decimal
from typing import Dict, List, Optional

import duckdb
import pandas as pd


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

        对于 A 股市场，使用上证50指数；
        对于美股市场，需要从文件加载 QQQ 数据。
        """
        if market == "cn":
            # 从数据库查询上证50指数数据
            # 暂时使用股票数据的平均值作为基准
            sql = """
                SELECT trade_date, AVG(close) as value
                FROM stock_daily_prices
                WHERE market = 'cn'
            """
            params = []

            if start_date:
                sql += " AND trade_date >= ?"
                params.append(start_date)
            if end_date:
                sql += " AND trade_date <= ?"
                params.append(end_date)

            sql += " GROUP BY trade_date ORDER BY trade_date"

            df = self.conn.execute(sql, params).df()
            return df.to_dict("records")

        # US market - 需要从文件加载
        return []
