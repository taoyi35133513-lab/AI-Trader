"""
Agent 相关 API 路由
"""

from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, Query

from api.dependencies import get_db
from api.services.agent_service import AgentService

router = APIRouter()


@router.get("")
async def list_agents(
    market: str = Query("cn", description="市场: cn, cn_hour, us"),
    db=Depends(get_db),
):
    """获取所有 Agent 列表"""
    service = AgentService(db)
    agents = service.get_all_agents(market)
    return {"agents": agents, "count": len(agents)}


@router.get("/{agent_name}/positions")
async def get_agent_positions(
    agent_name: str,
    market: str = Query("cn", description="市场"),
    start_date: Optional[date] = Query(None, description="开始日期"),
    end_date: Optional[date] = Query(None, description="结束日期"),
    db=Depends(get_db),
):
    """获取 Agent 持仓历史"""
    service = AgentService(db)
    positions = service.get_agent_positions(agent_name, market, start_date, end_date)
    return {"agent_name": agent_name, "positions": positions, "count": len(positions)}


@router.get("/{agent_name}/asset-history")
async def get_agent_asset_history(
    agent_name: str,
    market: str = Query("cn", description="市场"),
    db=Depends(get_db),
):
    """获取 Agent 资产变化历史"""
    service = AgentService(db)
    return service.get_agent_asset_history(agent_name, market)


@router.get("/leaderboard")
async def get_leaderboard(
    market: str = Query("cn", description="市场"),
    db=Depends(get_db),
):
    """获取排行榜"""
    service = AgentService(db)
    leaderboard = service.get_leaderboard(market)
    return {"market": market, "leaderboard": leaderboard}


@router.get("/recent-trades")
async def get_recent_trades(
    market: str = Query("cn", description="市场"),
    limit: int = Query(20, description="返回数量", ge=1, le=100),
    db=Depends(get_db),
):
    """获取最近交易记录"""
    service = AgentService(db)
    trades = service.get_recent_trades(market, limit)
    return {"market": market, "trades": trades, "count": len(trades)}
