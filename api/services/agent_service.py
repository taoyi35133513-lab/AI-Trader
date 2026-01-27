"""
Agent 数据服务

支持 DuckDB 优先、JSONL 降级的混合数据访问模式。
"""

import json
import logging
import os
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
import pandas as pd

from api.config import get_data_dir, get_project_root, load_config_json
from api.services.position_service_v2 import PositionServiceV2

logger = logging.getLogger(__name__)


class AgentService:
    """Agent 数据服务"""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn
        self.project_root = get_project_root()
        self._position_service = PositionServiceV2(conn)

    def get_all_agents(self, market: str = "cn") -> List[dict]:
        """获取所有 Agent 信息

        Args:
            market: 市场 (cn/cn_hour/us)

        Returns:
            Agent 信息列表
        """
        # 使用统一的配置文件
        config = load_config_json("config.json")

        agents = []
        models = config.get("models", [])
        initial_cash = config.get("agent_config", {}).get("initial_cash", 100000)

        # 根据 market 确定 signature 后缀
        signature_suffix = "-astock-hour" if market == "cn_hour" else ""

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
                base_name = model.get("signature", model.get("name"))
                agent_name = f"{base_name}{signature_suffix}"
                agents.append(
                    {
                        "name": agent_name,
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

        优先从 DuckDB 读取，如果数据为空则降级到 JSONL 文件。

        Args:
            agent_name: Agent 名称
            market: 市场
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            持仓记录列表
        """
        # 尝试从 DuckDB 获取
        try:
            positions = self._position_service.get_positions_by_agent(
                agent_name=agent_name,
                market=market,
                start_date=start_date,
                end_date=end_date,
            )
            if positions:
                logger.debug(f"DuckDB: Retrieved {len(positions)} positions for {agent_name}")
                return positions
        except Exception as e:
            logger.warning(f"DuckDB position query failed: {e}")

        # 降级到 JSONL 文件
        return self._get_positions_from_jsonl(agent_name, market, start_date, end_date)

    def _get_positions_from_jsonl(
        self,
        agent_name: str,
        market: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[dict]:
        """从 JSONL 文件加载持仓数据（降级方法）"""
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
                            "this_action": record.get("this_action"),
                        }
                    )

        logger.debug(f"JSONL: Retrieved {len(positions)} positions for {agent_name}")
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

        # 对于同一日期/时间的多条记录，只保留最后一条（id/step_id 最大的）
        # 这与前端 data-loader.js 的处理逻辑一致
        # 小时级市场使用完整时间戳作为 key，日线市场使用日期
        positions_by_date = {}
        for pos in positions:
            raw_date = pos.get("date", "")
            if market == "cn_hour":
                # 小时级：使用完整时间戳
                date_key = raw_date
            else:
                # 日线：只使用日期部分
                date_key = raw_date.split(" ")[0]
            step_id = pos.get("step_id", 0)
            if date_key not in positions_by_date or step_id > positions_by_date[date_key].get("step_id", 0):
                positions_by_date[date_key] = pos

        # 按日期排序
        sorted_dates = sorted(positions_by_date.keys())
        unique_positions = [positions_by_date[d] for d in sorted_dates]

        # 计算每日资产价值
        history = []
        for pos in unique_positions:
            pos_dict = pos.get("positions", {})
            cash = float(pos_dict.get("CASH", 0))

            # 计算股票市值
            stock_value = 0
            raw_date = pos.get("date", "")
            # 小时级市场使用完整时间戳查询价格，日线市场使用日期
            if market == "cn_hour":
                price_date_str = raw_date  # 完整时间戳如 "2025-12-31 15:00:00"
            else:
                price_date_str = raw_date.split(" ")[0]  # 只取日期部分

            for symbol, quantity in pos_dict.items():
                if symbol == "CASH" or quantity == 0:
                    continue

                # 从数据库获取价格
                price_data = self._get_price_for_date(symbol, price_date_str, market)
                if price_data:
                    stock_value += float(price_data.get("close", 0)) * quantity

            total_value = cash + stock_value
            return_pct = ((total_value - initial_cash) / initial_cash) * 100

            # 返回的 date 字段：小时级保留完整时间戳，日线只保留日期
            history.append(
                {
                    "date": price_date_str,
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

    def _get_price_for_date(
        self, symbol: str, date_str: str, market: str = "cn"
    ) -> Optional[dict]:
        """获取指定日期的价格

        Args:
            symbol: 股票代码
            date_str: 日期字符串 (daily: "2025-12-31", hourly: "2025-12-31 15:00:00")
            market: 市场类型 (cn/cn_hour)

        Returns:
            价格数据字典，包含 close 字段
        """
        try:
            if market == "cn_hour":
                # 小时级：查询 stock_hourly_prices 表
                sql = """
                    SELECT close FROM stock_hourly_prices
                    WHERE ts_code = ? AND trade_time = ?
                """
                result = self.conn.execute(sql, [symbol, date_str]).fetchone()
            else:
                # 日线：查询 stock_daily_prices 表
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
