"""
行情数据查询 API 路由

提供历史及实时行情数据查询接口。
"""

from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Query

from api.dependencies import get_db
from api.services.market_data_service import MarketDataService

router = APIRouter()


@router.get("/prices")
async def get_prices(
    symbols: str = Query(..., description="股票代码，逗号分隔，如 600519.SH,601318.SH"),
    start_date: Optional[date] = Query(None, description="开始日期 (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="结束日期 (YYYY-MM-DD)"),
    frequency: str = Query("daily", description="数据频率: daily 或 hourly"),
    market: str = Query("cn", description="市场标识"),
    db=Depends(get_db),
):
    """查询多个股票的历史价格数据

    支持按日期范围查询日线或小时线数据。

    - **symbols**: 股票代码列表，逗号分隔
    - **start_date**: 开始日期（可选）
    - **end_date**: 结束日期（可选）
    - **frequency**: 数据频率，daily（日线）或 hourly（小时线）
    - **market**: 市场标识，默认 cn
    """
    service = MarketDataService(db)
    symbol_list = [s.strip() for s in symbols.split(",")]
    return service.get_prices(symbol_list, start_date, end_date, frequency, market)


@router.get("/snapshot")
async def get_snapshot(
    symbols: str = Query(..., description="股票代码，逗号分隔"),
    date_str: str = Query(
        ...,
        alias="date",
        description="日期或时间 (daily: 2025-01-20, hourly: 2025-01-20 10:30:00)",
    ),
    frequency: str = Query("daily", description="数据频率: daily 或 hourly"),
    db=Depends(get_db),
):
    """获取特定日期/时间的价格快照

    返回指定时间点所有股票的价格数据。

    - **symbols**: 股票代码列表，逗号分隔
    - **date**: 日期或时间字符串
    - **frequency**: 数据频率，daily 或 hourly
    """
    service = MarketDataService(db)
    symbol_list = [s.strip() for s in symbols.split(",")]
    return service.get_snapshot(symbol_list, date_str, frequency)


@router.get("/latest")
async def get_latest_prices(
    symbols: str = Query(..., description="股票代码，逗号分隔"),
    frequency: str = Query("daily", description="数据频率: daily 或 hourly"),
    db=Depends(get_db),
):
    """获取最新价格

    返回每个股票的最新可用价格数据。

    - **symbols**: 股票代码列表，逗号分隔
    - **frequency**: 数据频率，daily 或 hourly
    """
    service = MarketDataService(db)
    symbol_list = [s.strip() for s in symbols.split(",")]
    return service.get_latest_prices(symbol_list, frequency)


@router.get("/ohlcv/{symbol}")
async def get_ohlcv(
    symbol: str,
    start_date: Optional[date] = Query(None, description="开始日期"),
    end_date: Optional[date] = Query(None, description="结束日期"),
    frequency: str = Query("daily", description="数据频率: daily 或 hourly"),
    db=Depends(get_db),
):
    """获取单个股票的详细 OHLCV 数据

    返回开盘价、最高价、最低价、收盘价、成交量等详细数据。

    - **symbol**: 股票代码，如 600519.SH
    - **start_date**: 开始日期（可选）
    - **end_date**: 结束日期（可选）
    - **frequency**: 数据频率，daily 或 hourly
    """
    service = MarketDataService(db)
    return service.get_ohlcv(symbol, start_date, end_date, frequency)
