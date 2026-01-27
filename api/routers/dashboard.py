"""
仪表盘聚合 API 路由
"""

from fastapi import APIRouter, Depends, Query

from api.dependencies import get_db
from api.services.agent_service import AgentService
from api.services.price_service import PriceService

router = APIRouter()


@router.get("/{market}")
async def get_dashboard(
    market: str,
    db=Depends(get_db),
):
    """获取仪表盘所有数据（一次请求）

    返回:
    - agents: Agent 列表
    - asset_histories: 所有 Agent 资产曲线
    - benchmark: 基准数据
    - leaderboard: 排行榜
    - recent_trades: 最近交易
    - stats: 统计信息
    """
    agent_service = AgentService(db)
    price_service = PriceService(db)

    # 获取 Agent 列表
    agents = agent_service.get_all_agents(market)

    # 获取所有 Agent 资产历史
    asset_histories = []
    for agent in agents:
        history = agent_service.get_agent_asset_history(agent["name"], market)
        if history.get("history"):
            # 获取 positions 数据并添加到 history
            positions = agent_service.get_agent_positions(agent["name"], market)
            history["positions"] = positions
            asset_histories.append(history)

    # 获取排行榜
    leaderboard = agent_service.get_leaderboard(market)

    # 获取最近交易
    recent_trades = agent_service.get_recent_trades(market, limit=20)

    # 获取基准数据
    benchmark_data = price_service.get_benchmark_data(market)

    # 计算统计信息
    stats = {
        "agent_count": len(agents),
        "trading_days": 0,
        "best_performer": None,
        "best_return": 0,
    }

    if asset_histories:
        # 交易天数
        max_days = max(len(h.get("history", [])) for h in asset_histories)
        stats["trading_days"] = max_days

        # 最佳表现者
        if leaderboard:
            best = leaderboard[0]
            stats["best_performer"] = best.get("display_name", best.get("agent_name"))
            stats["best_return"] = best.get("total_return", 0)

    return {
        "market": market,
        "agents": agents,
        "asset_histories": asset_histories,
        "benchmark": {
            "name": "上证50" if market.startswith("cn") else "QQQ",
            "data": benchmark_data,
        },
        "leaderboard": leaderboard,
        "recent_trades": recent_trades,
        "stats": stats,
    }
