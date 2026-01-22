"""
Agent 数据模型
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class AgentInfo(BaseModel):
    """Agent 基本信息"""

    name: str = Field(..., description="Agent 名称")
    display_name: str = Field(..., description="显示名称")
    market: str = Field(..., description="市场 (cn/us)")
    initial_cash: Decimal = Field(..., description="初始资金")
    icon: Optional[str] = Field(None, description="图标 emoji")
    color: Optional[str] = Field(None, description="颜色代码")


class PositionItem(BaseModel):
    """持仓项"""

    ts_code: str = Field(..., description="股票代码")
    quantity: int = Field(..., description="持仓数量")
    market_value: Optional[Decimal] = Field(None, description="市值")


class PositionRecord(BaseModel):
    """持仓记录"""

    date: date = Field(..., description="日期")
    step_id: Optional[int] = Field(None, description="步骤 ID")
    cash: Decimal = Field(..., description="现金余额")
    positions: Dict[str, int] = Field(..., description="持仓 {股票代码: 数量}")
    total_value: Optional[Decimal] = Field(None, description="总资产")


class AssetHistoryItem(BaseModel):
    """资产历史记录"""

    date: str = Field(..., description="日期 YYYY-MM-DD")
    total_value: Decimal = Field(..., description="总资产价值")
    cash: Decimal = Field(..., description="现金")
    stock_value: Decimal = Field(..., description="股票市值")
    return_pct: Optional[Decimal] = Field(None, description="收益率 %")


class AgentAssetHistory(BaseModel):
    """Agent 资产历史"""

    agent_name: str
    display_name: str
    market: str
    initial_cash: Decimal
    final_value: Decimal
    total_return: Decimal = Field(..., description="总收益率 %")
    history: List[AssetHistoryItem]
    icon: Optional[str] = None
    color: Optional[str] = None


class TradeAction(BaseModel):
    """交易动作"""

    date: str
    agent_name: str
    action: str = Field(..., description="buy/sell/hold")
    ts_code: Optional[str] = None
    quantity: Optional[int] = None
    price: Optional[Decimal] = None
    reasoning: Optional[str] = Field(None, description="交易理由")


class LeaderboardItem(BaseModel):
    """排行榜项"""

    rank: int
    agent_name: str
    display_name: str
    final_value: Decimal
    total_return: Decimal
    icon: Optional[str] = None
    color: Optional[str] = None


class DashboardResponse(BaseModel):
    """仪表盘数据响应"""

    market: str
    agents: List[AgentInfo]
    asset_histories: List[AgentAssetHistory]
    benchmark: Optional[Dict[str, Any]] = None
    leaderboard: List[LeaderboardItem]
    recent_trades: List[TradeAction]
    stats: Dict[str, Any] = Field(
        ...,
        description="统计信息: agent_count, trading_days, best_performer, best_return",
    )
