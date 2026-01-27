"""
Agent 持仓数据 API 路由
"""

from datetime import date
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from api.dependencies import get_db
from api.services.position_service_v2 import PositionServiceV2

router = APIRouter(prefix="/api/positions", tags=["agent-positions"])


# ===== Response Models =====


class ActionResponse(BaseModel):
    action: Optional[str] = None
    symbol: Optional[str] = None
    amount: Optional[int] = None
    price: Optional[float] = None


class PositionStepResponse(BaseModel):
    date: str
    step_id: int
    positions: Dict[str, Any]
    cash: float
    this_action: Optional[ActionResponse] = None


class PositionHistoryResponse(BaseModel):
    agent_name: str
    market: str
    positions: List[PositionStepResponse]


class HoldingRecord(BaseModel):
    date: str
    time: Optional[str] = None
    quantity: int
    action: Optional[str] = None
    amount: Optional[int] = None


class HoldingTimelineResponse(BaseModel):
    symbol: str
    history: List[HoldingRecord]


class TradeRecord(BaseModel):
    agent_name: str
    date: str
    time: Optional[str] = None
    step_id: int
    action: str
    symbol: str
    amount: int
    price: Optional[float] = None


# ===== Endpoints =====


@router.get("/{agent_name}/history", response_model=PositionHistoryResponse)
async def get_position_history(
    agent_name: str,
    market: str = Query("cn", description="市场 (cn/cn_hour)"),
    start_date: Optional[date] = Query(None, description="开始日期"),
    end_date: Optional[date] = Query(None, description="结束日期"),
    db=Depends(get_db),
):
    """获取 Agent 持仓历史"""
    service = PositionServiceV2(db)

    positions = service.get_positions_by_agent(
        agent_name=agent_name,
        market=market,
        start_date=start_date,
        end_date=end_date,
    )

    return {
        "agent_name": agent_name,
        "market": market,
        "positions": positions,
    }


@router.get("/{agent_name}/latest", response_model=PositionStepResponse)
async def get_latest_position(
    agent_name: str,
    market: str = Query("cn", description="市场"),
    db=Depends(get_db),
):
    """获取最新持仓快照"""
    service = PositionServiceV2(db)

    position = service.get_latest_position(agent_name=agent_name, market=market)

    if not position:
        raise HTTPException(status_code=404, detail=f"No position found for {agent_name}")

    return position


@router.get("/{agent_name}/at-date/{target_date}", response_model=PositionStepResponse)
async def get_position_at_date(
    agent_name: str,
    target_date: date,
    market: str = Query("cn", description="市场"),
    db=Depends(get_db),
):
    """获取指定日期的持仓快照"""
    service = PositionServiceV2(db)

    position = service.get_position_at_date(
        agent_name=agent_name,
        target_date=target_date,
        market=market,
    )

    if not position:
        raise HTTPException(
            status_code=404,
            detail=f"No position found for {agent_name} on {target_date}",
        )

    return position


@router.get("/{agent_name}/holdings/{symbol}", response_model=HoldingTimelineResponse)
async def get_holding_timeline(
    agent_name: str,
    symbol: str,
    market: str = Query("cn", description="市场"),
    start_date: Optional[date] = Query(None, description="开始日期"),
    end_date: Optional[date] = Query(None, description="结束日期"),
    db=Depends(get_db),
):
    """获取特定股票的持仓历史"""
    service = PositionServiceV2(db)

    history = service.get_holdings_history(
        agent_name=agent_name,
        symbol=symbol,
        market=market,
        start_date=start_date,
        end_date=end_date,
    )

    return {
        "symbol": symbol,
        "history": history,
    }


@router.get("/all/trades", response_model=List[TradeRecord])
async def get_all_trades(
    market: str = Query("cn", description="市场"),
    agent_name: Optional[str] = Query(None, description="指定 Agent"),
    limit: int = Query(100, le=500, description="返回数量"),
    db=Depends(get_db),
):
    """获取交易记录（所有 Agent 或指定 Agent）"""
    service = PositionServiceV2(db)

    trades = service.get_trade_actions(
        agent_name=agent_name,
        market=market,
        limit=limit,
    )

    return trades


@router.get("/all/history")
async def get_all_positions_history(
    market: str = Query("cn", description="市场"),
    start_date: Optional[date] = Query(None, description="开始日期"),
    end_date: Optional[date] = Query(None, description="结束日期"),
    db=Depends(get_db),
):
    """获取所有 Agent 的持仓历史（用于 dashboard）"""
    service = PositionServiceV2(db)

    all_positions = service.get_all_positions(
        market=market,
        start_date=start_date,
        end_date=end_date,
    )

    return all_positions
