"""
Agent 数据服务
"""

import json
import os
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
import pandas as pd

from api.config import get_data_dir, get_project_root, load_config_json


class AgentService:
    """Agent 数据服务"""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn
        self.project_root = get_project_root()

    def get_all_agents(self, market: str = "cn") -> List[dict]:
        """获取所有 Agent 信息

        Args:
            market: 市场 (cn/cn_hour/us)

        Returns:
            Agent 信息列表
        """
        # 从配置文件加载 Agent 信息
        config_map = {
            "cn": "astock_config.json",
            "cn_hour": "astock_hour_config.json",
            "us": "default_config.json",
        }

        config_name = config_map.get(market, "astock_config.json")
        config = load_config_json(config_name)

        agents = []
        models = config.get("models", [])
        initial_cash = config.get("agent_config", {}).get("initial_cash", 100000)

        # Agent 图标和颜色映射
        icons = ["🤖", "🧠", "💡", "🎯", "🚀", "⚡", "🔮", "🎨"]
        colors = [
            "#4CAF50",
            "#2196F3",
            "#FF9800",
            "#E91E63",
            "#9C27B0",
            "#00BCD4",
            "#FF5722",
            "#795548",
        ]

        for i, model in enumerate(models):
            if model.get("enabled", True):
                agents.append(
                    {
                        "name": model.get("signature", model.get("name")),
                        "display_name": model.get("name", model.get("signature")),
                        "market": market,
                        "initial_cash": initial_cash,
                        "icon": icons[i % len(icons)],
                        "color": colors[i % len(colors)],
                    }
                )

        return agents

    def get_agent_positions(
        self,
        agent_name: str,
        market: str = "cn",
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[dict]:
        """获取 Agent 持仓历史

        Args:
            agent_name: Agent 名称
            market: 市场
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            持仓记录列表
        """
        # 从 JSONL 文件加载持仓数据
        data_dir = get_data_dir(market)
        position_file = data_dir / agent_name / "position" / "position.jsonl"

        if not position_file.exists():
            return []

        positions = []
        with open(position_file, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    record = json.loads(line)
                    record_date = record.get("date", "")

                    # 解析日期（支持多种格式）
                    if " " in record_date:
                        record_date_obj = datetime.strptime(
                            record_date.split(" ")[0], "%Y-%m-%d"
                        ).date()
                    else:
                        record_date_obj = datetime.strptime(
                            record_date, "%Y-%m-%d"
                        ).date()

                    # 日期过滤
                    if start_date and record_date_obj < start_date:
                        continue
                    if end_date and record_date_obj > end_date:
                        continue

                    positions.append(
                        {
                            "date": record_date,
                            "step_id": record.get("id"),
                            "positions": record.get("positions", {}),
                            "cash": record.get("positions", {}).get("CASH", 0),
                        }
                    )

        return positions

    def get_agent_asset_history(
        self, agent_name: str, market: str = "cn"
    ) -> Dict[str, Any]:
        """获取 Agent 资产变化历史

        Args:
            agent_name: Agent 名称
            market: 市场

        Returns:
            资产历史数据
        """
        # 获取持仓数据
        positions = self.get_agent_positions(agent_name, market)

        if not positions:
            return {"agent_name": agent_name, "history": [], "error": "No position data"}

        # 获取 Agent 配置
        agents = self.get_all_agents(market)
        agent_info = next(
            (a for a in agents if a["name"] == agent_name),
            {"initial_cash": 100000, "icon": "🤖", "color": "#4CAF50"},
        )

        initial_cash = float(agent_info.get("initial_cash", 100000))

        # 计算每日资产价值
        history = []
        for pos in positions:
            pos_dict = pos.get("positions", {})
            cash = float(pos_dict.get("CASH", 0))

            # 计算股票市值
            stock_value = 0
            for symbol, quantity in pos_dict.items():
                if symbol == "CASH" or quantity == 0:
                    continue

                # 从数据库获取价格
                date_str = pos.get("date", "").split(" ")[0]
                price_data = self._get_price_for_date(symbol, date_str)
                if price_data:
                    stock_value += float(price_data.get("close", 0)) * quantity

            total_value = cash + stock_value
            return_pct = ((total_value - initial_cash) / initial_cash) * 100

            history.append(
                {
                    "date": pos.get("date", "").split(" ")[0],
                    "total_value": round(total_value, 2),
                    "cash": round(cash, 2),
                    "stock_value": round(stock_value, 2),
                    "return_pct": round(return_pct, 2),
                }
            )

        # 计算最终收益
        final_value = history[-1]["total_value"] if history else initial_cash
        total_return = ((final_value - initial_cash) / initial_cash) * 100

        return {
            "agent_name": agent_name,
            "display_name": agent_info.get("display_name", agent_name),
            "market": market,
            "initial_cash": initial_cash,
            "final_value": round(final_value, 2),
            "total_return": round(total_return, 2),
            "history": history,
            "icon": agent_info.get("icon"),
            "color": agent_info.get("color"),
        }

    def _get_price_for_date(self, symbol: str, date_str: str) -> Optional[dict]:
        """获取指定日期的价格"""
        try:
            sql = """
                SELECT close FROM stock_daily_prices
                WHERE ts_code = ? AND trade_date = ?
            """
            result = self.conn.execute(sql, [symbol, date_str]).fetchone()
            if result:
                return {"close": result[0]}
        except Exception:
            pass
        return None

    def get_leaderboard(self, market: str = "cn") -> List[dict]:
        """获取排行榜

        Args:
            market: 市场

        Returns:
            排行榜数据
        """
        agents = self.get_all_agents(market)
        leaderboard = []

        for agent in agents:
            asset_history = self.get_agent_asset_history(agent["name"], market)
            if asset_history.get("history"):
                leaderboard.append(
                    {
                        "agent_name": agent["name"],
                        "display_name": asset_history.get("display_name", agent["name"]),
                        "final_value": asset_history.get("final_value", 0),
                        "total_return": asset_history.get("total_return", 0),
                        "icon": asset_history.get("icon"),
                        "color": asset_history.get("color"),
                    }
                )

        # 按收益率排序
        leaderboard.sort(key=lambda x: x["total_return"], reverse=True)

        # 添加排名
        for i, item in enumerate(leaderboard):
            item["rank"] = i + 1

        return leaderboard

    def get_recent_trades(
        self, market: str = "cn", limit: int = 20
    ) -> List[dict]:
        """获取最近交易记录

        Args:
            market: 市场
            limit: 返回数量

        Returns:
            交易记录列表
        """
        agents = self.get_all_agents(market)
        all_trades = []

        for agent in agents:
            positions = self.get_agent_positions(agent["name"], market)

            # 检测交易动作
            prev_pos = None
            for pos in positions:
                if prev_pos:
                    # 比较持仓变化
                    prev_holdings = prev_pos.get("positions", {})
                    curr_holdings = pos.get("positions", {})

                    for symbol, quantity in curr_holdings.items():
                        if symbol == "CASH":
                            continue

                        prev_qty = prev_holdings.get(symbol, 0)
                        if quantity > prev_qty:
                            all_trades.append(
                                {
                                    "date": pos.get("date", "").split(" ")[0],
                                    "agent_name": agent["name"],
                                    "action": "buy",
                                    "ts_code": symbol,
                                    "quantity": quantity - prev_qty,
                                }
                            )
                        elif quantity < prev_qty:
                            all_trades.append(
                                {
                                    "date": pos.get("date", "").split(" ")[0],
                                    "agent_name": agent["name"],
                                    "action": "sell",
                                    "ts_code": symbol,
                                    "quantity": prev_qty - quantity,
                                }
                            )

                prev_pos = pos

        # 按日期排序，返回最近的
        all_trades.sort(key=lambda x: x["date"], reverse=True)
        return all_trades[:limit]
