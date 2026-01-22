"""
价格数据模型
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class PriceData(BaseModel):
    """单条价格数据"""

    ts_code: str = Field(..., description="股票代码")
    trade_date: date = Field(..., description="交易日期")
    open: Optional[Decimal] = Field(None, description="开盘价")
    high: Optional[Decimal] = Field(None, description="最高价")
    low: Optional[Decimal] = Field(None, description="最低价")
    close: Optional[Decimal] = Field(None, description="收盘价")
    volume: Optional[int] = Field(None, description="成交量")
    amount: Optional[Decimal] = Field(None, description="成交额")


class HourlyPriceData(BaseModel):
    """小时线价格数据"""

    ts_code: str = Field(..., description="股票代码")
    trade_time: datetime = Field(..., description="交易时间")
    open: Optional[Decimal] = None
    high: Optional[Decimal] = None
    low: Optional[Decimal] = None
    close: Optional[Decimal] = None
    volume: Optional[int] = None


class PriceResponse(BaseModel):
    """价格查询响应"""

    symbol: str
    data: List[PriceData]
    count: int


class PricesResponse(BaseModel):
    """多股票价格响应"""

    prices: Dict[str, List[PriceData]]
    count: int


class BenchmarkData(BaseModel):
    """基准指数数据"""

    date: date
    value: Decimal
    return_pct: Optional[Decimal] = Field(None, description="相对收益率 %")


class BenchmarkResponse(BaseModel):
    """基准指数响应"""

    market: str
    benchmark_name: str
    data: List[BenchmarkData]
    initial_value: Decimal
    final_value: Decimal
    total_return: Decimal = Field(..., description="总收益率 %")
