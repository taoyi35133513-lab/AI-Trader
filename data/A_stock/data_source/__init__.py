"""
A股数据源模块

提供统一的数据源接口，使用 AKShare 作为数据源。
通过工厂函数 create_data_source() 创建数据源实例。
"""

from typing import Optional

from .base import AStockDataSource


def create_data_source(
    source_type: str = "akshare",
    **kwargs,
) -> AStockDataSource:
    """创建数据源实例

    工厂函数，创建 AKShare 数据源实例。

    Args:
        source_type: 数据源类型，仅支持 "akshare"
        **kwargs: 传递给数据源构造函数的参数
            - max_retries: 最大重试次数
            - retry_delay: 重试延迟
            - request_interval: 请求间隔

    Returns:
        AStockDataSource 子类实例

    Raises:
        ValueError: 不支持的数据源类型

    Example:
        >>> source = create_data_source("akshare")
        >>> df = source.get_stock_daily(["600519.SH"], "20250101", "20250110")
    """
    source_type = source_type.lower()

    if source_type == "akshare":
        from .akshare_source import AKShareDataSource

        return AKShareDataSource(**kwargs)

    else:
        raise ValueError(f"不支持的数据源类型: {source_type}，仅支持: akshare")


def get_available_sources() -> list:
    """获取可用的数据源类型列表

    Returns:
        数据源类型名称列表
    """
    return ["akshare"]


# 导出
__all__ = [
    "AStockDataSource",
    "create_data_source",
    "get_available_sources",
]
