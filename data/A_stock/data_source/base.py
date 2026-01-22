"""
A股数据源抽象基类

定义统一的数据获取接口，支持不同数据源（akshare, tushare）实现。
"""

from abc import ABC, abstractmethod
from typing import List, Optional

import pandas as pd


class AStockDataSource(ABC):
    """A股数据源抽象基类

    所有数据源实现必须继承此类，并实现以下抽象方法。
    返回的 DataFrame 使用统一的列名格式（兼容 Tushare）：
    - ts_code: 股票代码（格式：600519.SH）
    - trade_date: 交易日期（格式：YYYYMMDD）
    - open: 开盘价
    - high: 最高价
    - low: 最低价
    - close: 收盘价
    - vol: 成交量（单位：手，1手=100股）
    - amount: 成交额
    """

    def __init__(
        self,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        request_interval: float = 0.5,
    ):
        """初始化数据源

        Args:
            max_retries: 最大重试次数
            retry_delay: 重试延迟基数（秒）
            request_interval: 请求间隔（秒），用于避免频率限制
        """
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.request_interval = request_interval

    @abstractmethod
    def get_index_constituents(
        self,
        index_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """获取指数成分股

        Args:
            index_code: 指数代码（标准格式：000016.SH 表示上证50）
            start_date: 开始日期 YYYYMMDD（可选）
            end_date: 结束日期 YYYYMMDD（可选）

        Returns:
            DataFrame，包含以下列：
            - con_code: 成分股代码（格式：600519.SH）
            - con_name: 成分股名称
            - weight: 权重（百分比）
            - trade_date: 日期（可选）
        """
        pass

    @abstractmethod
    def get_stock_daily(
        self,
        stock_codes: List[str],
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """获取个股日线数据

        Args:
            stock_codes: 股票代码列表（标准格式：['600519.SH', '000001.SZ']）
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD

        Returns:
            DataFrame，包含以下列：
            - ts_code: 股票代码
            - trade_date: 交易日期
            - open: 开盘价
            - high: 最高价
            - low: 最低价
            - close: 收盘价
            - vol: 成交量（手）
            - amount: 成交额
        """
        pass

    @abstractmethod
    def get_index_daily(
        self,
        index_code: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """获取指数日线数据

        Args:
            index_code: 指数代码（标准格式：000016.SH）
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD

        Returns:
            DataFrame，包含以下列：
            - ts_code: 指数代码
            - trade_date: 交易日期
            - open: 开盘价
            - high: 最高价
            - low: 最低价
            - close: 收盘价
            - vol: 成交量
            - amount: 成交额
        """
        pass

    @staticmethod
    def convert_code_to_standard(code: str) -> str:
        """将股票代码转换为标准格式（带后缀）

        Args:
            code: 原始代码（如 600519 或 600519.SH）

        Returns:
            标准格式代码（如 600519.SH）
        """
        code = str(code).strip()

        # 已经有后缀
        if "." in code:
            return code.upper()

        # 补齐6位
        code = code.zfill(6)

        # 根据代码判断市场
        # 6开头：上海（SH）
        # 0/3开头：深圳（SZ）
        # 5开头：上海ETF（SH）
        # 1开头：深圳ETF（SZ）
        if code.startswith(("6", "5", "9")):
            return f"{code}.SH"
        else:
            return f"{code}.SZ"

    @staticmethod
    def convert_code_to_plain(code: str) -> str:
        """将股票代码转换为纯数字格式（无后缀）

        Args:
            code: 标准格式代码（如 600519.SH）

        Returns:
            纯数字代码（如 600519）
        """
        if "." in code:
            return code.split(".")[0]
        return code

    @staticmethod
    def convert_date_format(date_str: str, from_format: str = "%Y-%m-%d", to_format: str = "%Y%m%d") -> str:
        """日期格式转换

        Args:
            date_str: 日期字符串
            from_format: 原始格式
            to_format: 目标格式

        Returns:
            转换后的日期字符串
        """
        from datetime import datetime

        # 处理已经是目标格式的情况
        if len(date_str) == 8 and date_str.isdigit() and to_format == "%Y%m%d":
            return date_str
        if "-" in date_str and to_format == "%Y-%m-%d":
            return date_str

        try:
            dt = datetime.strptime(date_str, from_format)
            return dt.strftime(to_format)
        except ValueError:
            # 尝试另一种常见格式
            try:
                if "-" in date_str:
                    dt = datetime.strptime(date_str, "%Y-%m-%d")
                else:
                    dt = datetime.strptime(date_str, "%Y%m%d")
                return dt.strftime(to_format)
            except ValueError:
                return date_str
