"""
基准指数 API 路由
"""

from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Query

from api.dependencies import get_db
from api.services.price_service import PriceService

router = APIRouter()


@router.get("/{market}")
async def get_benchmark(
    market: str,
    start_date: Optional[date] = Query(None, description="开始日期"),
    end_date: Optional[date] = Query(None, description="结束日期"),
    db=Depends(get_db),
):
    """获取基准指数数据

    - cn: 上证50指数
    - us: QQQ ETF
    """
    service = PriceService(db)
    data = service.get_benchmark_data(market, start_date, end_date)

    benchmark_names = {
        "cn": "上证50指数",
        "cn_hour": "上证50指数",
        "us": "QQQ ETF",
    }

    return {
        "market": market,
        "benchmark_name": benchmark_names.get(market, "Unknown"),
        "data": data,
        "count": len(data),
    }
