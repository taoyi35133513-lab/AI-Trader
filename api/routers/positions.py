"""
持仓数据查询 API 路由

提供持仓历史、快照、交易记录和资产估值查询接口。
"""

from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from api.dependencies import get_db
from api.services.position_service import PositionService

router = APIRouter()


def _handle_value_error(e: ValueError) -> HTTPException:
    """Convert ValueError to HTTPException with 400 status"""
    return HTTPException(status_code=400, detail=str(e))


@router.get("/")
async def list_agents(
    market: str = Query("cn", description="市场: cn, cn_hour, us"),
    db=Depends(get_db),
):
    """列出所有有持仓数据的 Agent

    - **market**: 市场标识，cn（日线）、cn_hour（小时线）或 us
    """
    try:
        service = PositionService(db)
        agents = service.list_agents(market)
        return {"market": market, "agents": agents, "count": len(agents)}
    except ValueError as e:
        raise _handle_value_error(e)


@router.get("/{agent}")
async def get_position_history(
    agent: str,
    market: str = Query("cn", description="市场: cn, cn_hour, us"),
    start_date: Optional[date] = Query(None, description="开始日期"),
    end_date: Optional[date] = Query(None, description="结束日期"),
    db=Depends(get_db),
):
    """查询 Agent 持仓历史

    返回指定时间范围内的所有持仓记录，包括持仓数量、现金余额和交易动作。

    - **agent**: Agent 名称，如 gemini-2.5-flash
    - **market**: 市场标识，cn（日线）、cn_hour（小时线）或 us
    - **start_date**: 开始日期（可选）
    - **end_date**: 结束日期（可选）
    """
    try:
        service = PositionService(db)
        return service.get_position_history(agent, market, start_date, end_date)
    except ValueError as e:
        raise _handle_value_error(e)


@router.get("/{agent}/snapshot")
async def get_position_snapshot(
    agent: str,
    date_str: str = Query(
        ...,
        alias="date",
        description="日期或时间 (daily: 2025-01-20, hourly: 2025-01-20 10:30:00)",
    ),
    market: str = Query("cn", description="市场: cn, cn_hour, us"),
    db=Depends(get_db),
):
    """获取特定日期/时间的持仓快照

    返回指定时间点的持仓状态，包括所有股票持仓和现金余额。

    - **agent**: Agent 名称
    - **date**: 日期或时间字符串
    - **market**: 市场标识
    """
    try:
        service = PositionService(db)
        return service.get_position_snapshot(agent, date_str, market)
    except ValueError as e:
        raise _handle_value_error(e)


@router.get("/{agent}/trades")
async def get_trade_actions(
    agent: str,
    market: str = Query("cn", description="市场: cn, cn_hour, us"),
    start_date: Optional[date] = Query(None, description="开始日期"),
    end_date: Optional[date] = Query(None, description="结束日期"),
    db=Depends(get_db),
):
    """获取交易记录

    返回指定时间范围内的所有交易动作（买入/卖出），不包括无交易记录。

    - **agent**: Agent 名称
    - **market**: 市场标识
    - **start_date**: 开始日期（可选）
    - **end_date**: 结束日期（可选）
    """
    try:
        service = PositionService(db)
        return service.get_trade_actions(agent, market, start_date, end_date)
    except ValueError as e:
        raise _handle_value_error(e)


@router.get("/{agent}/valuation")
async def get_valuation(
    agent: str,
    date_str: str = Query(
        ...,
        alias="date",
        description="日期或时间 (daily: 2025-01-20, hourly: 2025-01-20 10:30:00)",
    ),
    market: str = Query("cn", description="市场: cn, cn_hour, us"),
    db=Depends(get_db),
):
    """获取资产估值

    结合持仓数据和市场价格，计算指定时间点的资产总值。

    返回每个持仓的数量、当前价格、市值，以及现金、总资产和收益率。

    - **agent**: Agent 名称
    - **date**: 日期或时间字符串
    - **market**: 市场标识
    """
    try:
        service = PositionService(db)
        return service.get_valuation(agent, date_str, market)
    except ValueError as e:
        raise _handle_value_error(e)
