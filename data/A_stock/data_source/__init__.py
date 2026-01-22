"""
A股数据源模块

提供统一的数据源接口，支持 akshare 和 tushare 两种数据源。
通过工厂函数 create_data_source() 创建数据源实例。
"""

from typing import Optional

from .base import AStockDataSource


def create_data_source(
    source_type: str = "akshare",
    **kwargs,
) -> AStockDataSource:
    """创建数据源实例

    工厂函数，根据 source_type 参数创建对应的数据源实例。

    Args:
        source_type: 数据源类型，支持 "akshare" 或 "tushare"
        **kwargs: 传递给数据源构造函数的参数
            - akshare: max_retries, retry_delay, request_interval
            - tushare: token, api_url, max_retries, retry_delay, timeout

    Returns:
        AStockDataSource 子类实例

    Raises:
        ValueError: 不支持的数据源类型

    Example:
        >>> # 创建 AKShare 数据源（推荐）
        >>> source = create_data_source("akshare")
        >>> df = source.get_stock_daily(["600519.SH"], "20250101", "20250110")

        >>> # 创建 Tushare 数据源
        >>> source = create_data_source("tushare", token="your_token")
        >>> df = source.get_index_constituents("000016.SH")
    """
    source_type = source_type.lower()

    if source_type == "akshare":
        from .akshare_source import AKShareDataSource

        return AKShareDataSource(**kwargs)

    elif source_type == "tushare":
        from .tushare_source import TushareDataSource

        return TushareDataSource(**kwargs)

    else:
        raise ValueError(f"不支持的数据源类型: {source_type}，可选: akshare, tushare")


def get_available_sources() -> list:
    """获取可用的数据源类型列表

    Returns:
        数据源类型名称列表
    """
    return ["akshare", "tushare"]


# 导出
__all__ = [
    "AStockDataSource",
    "create_data_source",
    "get_available_sources",
]
