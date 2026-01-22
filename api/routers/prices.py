"""
价格数据 API 路由
"""

from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, Query

from api.dependencies import get_db
from api.services.price_service import PriceService

router = APIRouter()


@router.get("/daily")
async def get_daily_prices(
    symbols: str = Query(..., description="股票代码，逗号分隔"),
    start_date: Optional[date] = Query(None, description="开始日期"),
    end_date: Optional[date] = Query(None, description="结束日期"),
    market: str = Query("cn", description="市场"),
    db=Depends(get_db),
):
    """获取日线价格数据"""
    service = PriceService(db)
    symbol_list = [s.strip() for s in symbols.split(",")]
    prices = service.get_daily_prices(symbol_list, start_date, end_date, market)
    return {"prices": prices, "symbols": symbol_list}


@router.get("/hourly")
async def get_hourly_prices(
    symbols: str = Query(..., description="股票代码，逗号分隔"),
    start_date: Optional[date] = Query(None, description="开始日期"),
    end_date: Optional[date] = Query(None, description="结束日期"),
    db=Depends(get_db),
):
    """获取小时线价格数据"""
    service = PriceService(db)
    symbol_list = [s.strip() for s in symbols.split(",")]
    prices = service.get_hourly_prices(symbol_list, start_date, end_date)
    return {"prices": prices, "symbols": symbol_list}


@router.get("/{symbol}")
async def get_stock_price(
    symbol: str,
    trade_date: Optional[date] = Query(None, description="交易日期，不传则返回最新"),
    db=Depends(get_db),
):
    """获取单只股票价格"""
    service = PriceService(db)
    if trade_date:
        price = service.get_price_on_date(symbol, trade_date)
    else:
        price = service.get_latest_price(symbol)

    if price:
        return price
    return {"error": f"No price data for {symbol}", "symbol": symbol}
