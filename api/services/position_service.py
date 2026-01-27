"""
持仓数据查询服务

提供持仓历史、快照、交易记录和资产估值查询功能。
"""

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb

from api.config import get_data_dir, load_config_json

# Valid market identifiers
VALID_MARKETS = {"cn", "cn_hour", "us"}


class PositionService:
    """持仓数据查询服务"""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def _validate_market(self, market: str) -> None:
        """验证市场参数

        Args:
            market: 市场标识

        Raises:
            ValueError: 如果市场标识无效
        """
        if market not in VALID_MARKETS:
            raise ValueError(
                f"Invalid market: {market}. Must be one of: {', '.join(sorted(VALID_MARKETS))}"
            )

    def _load_positions_from_jsonl(
        self,
        agent_name: str,
        market: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[dict]:
        """从 JSONL 文件加载持仓数据

        Args:
            agent_name: Agent 名称
            market: 市场 (cn/cn_hour)
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            持仓记录列表
        """
        self._validate_market(market)

        data_dir = get_data_dir(market)
        position_file = data_dir / agent_name / "position" / "position.jsonl"

        if not position_file.exists():
            return []

        positions = []
        try:
            with open(position_file, "r", encoding="utf-8") as f:
                for line_num, line in enumerate(f, 1):
                    if not line.strip():
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        # Skip malformed lines
                        continue

                    record_date = record.get("date", "")
                    if not record_date:
                        continue

                    # 解析日期
                    try:
                        if " " in record_date:
                            record_date_obj = datetime.strptime(
                                record_date.split(" ")[0], "%Y-%m-%d"
                            ).date()
                        else:
                            record_date_obj = datetime.strptime(
                                record_date, "%Y-%m-%d"
                            ).date()
                    except ValueError:
                        # Skip records with invalid date format
                        continue

                    # 日期过滤
                    if start_date and record_date_obj < start_date:
                        continue
                    if end_date and record_date_obj > end_date:
                        continue

                    positions.append(record)
        except OSError:
            # File read error
            return []

        return positions

    def get_position_history(
        self,
        agent_name: str,
        market: str = "cn",
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Dict[str, Any]:
        """获取持仓历史

        Args:
            agent_name: Agent 名称
            market: 市场
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            持仓历史数据
        """
        positions = self._load_positions_from_jsonl(
            agent_name, market, start_date, end_date
        )

        formatted_positions = []
        for pos in positions:
            pos_dict = pos.get("positions", {})
            # 分离股票持仓和现金
            holdings = {k: v for k, v in pos_dict.items() if k != "CASH" and v > 0}
            cash = pos_dict.get("CASH", 0)

            formatted_positions.append({
                "date": pos.get("date"),
                "step_id": pos.get("id"),
                "holdings": holdings,
                "cash": cash,
                "action": pos.get("this_action"),
            })

        return {
            "agent": agent_name,
            "market": market,
            "count": len(formatted_positions),
            "positions": formatted_positions,
        }

    def get_position_snapshot(
        self,
        agent_name: str,
        date_str: str,
        market: str = "cn",
    ) -> Dict[str, Any]:
        """获取特定日期/时间的持仓快照

        Args:
            agent_name: Agent 名称
            date_str: 日期或时间字符串
            market: 市场

        Returns:
            持仓快照数据
        """
        positions = self._load_positions_from_jsonl(agent_name, market)

        # 查找匹配的持仓记录
        # 对于小时级市场，精确匹配时间戳
        # 对于日线市场，匹配日期
        is_hourly = market == "cn_hour"
        target_position = None

        for pos in positions:
            pos_date = pos.get("date", "")

            if is_hourly:
                # 精确匹配时间戳
                if pos_date == date_str:
                    target_position = pos
            else:
                # 日期匹配
                pos_date_only = pos_date.split(" ")[0]
                date_str_only = date_str.split(" ")[0]
                if pos_date_only == date_str_only:
                    # 取最后一条（step_id 最大的）
                    if target_position is None or pos.get("id", 0) > target_position.get("id", 0):
                        target_position = pos

        if not target_position:
            return {
                "agent": agent_name,
                "date": date_str,
                "market": market,
                "error": f"No position found for date: {date_str}",
            }

        pos_dict = target_position.get("positions", {})

        return {
            "agent": agent_name,
            "date": target_position.get("date"),
            "market": market,
            "step_id": target_position.get("id"),
            "holdings": pos_dict,
            "action": target_position.get("this_action"),
        }

    def get_trade_actions(
        self,
        agent_name: str,
        market: str = "cn",
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Dict[str, Any]:
        """获取交易记录

        Args:
            agent_name: Agent 名称
            market: 市场
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            交易记录列表
        """
        positions = self._load_positions_from_jsonl(
            agent_name, market, start_date, end_date
        )

        trades = []
        for pos in positions:
            action = pos.get("this_action")
            if action and action.get("action") != "no_trade":
                trades.append({
                    "date": pos.get("date"),
                    "step_id": pos.get("id"),
                    "action": action.get("action"),
                    "symbol": action.get("symbol"),
                    "amount": action.get("amount"),
                })

        return {
            "agent": agent_name,
            "market": market,
            "count": len(trades),
            "trades": trades,
        }

    def get_valuation(
        self,
        agent_name: str,
        date_str: str,
        market: str = "cn",
    ) -> Dict[str, Any]:
        """获取资产估值

        结合持仓和市场价格计算资产总值。

        Args:
            agent_name: Agent 名称
            date_str: 日期或时间字符串
            market: 市场

        Returns:
            资产估值数据
        """
        # 获取持仓快照
        snapshot = self.get_position_snapshot(agent_name, date_str, market)
        if "error" in snapshot:
            return snapshot

        holdings = snapshot.get("holdings", {})
        cash_value = holdings.get("CASH", 0)
        cash = float(cash_value) if cash_value is not None else 0.0

        # 获取初始资金
        config = load_config_json("config.json")
        if config:
            initial_cash = config.get("agent_config", {}).get("initial_cash", 100000)
        else:
            initial_cash = 100000

        # 计算每个持仓的市值
        holdings_detail = {}
        stock_value = 0.0

        for symbol, quantity in holdings.items():
            if symbol == "CASH":
                continue
            if quantity is None or quantity == 0:
                continue

            # 查询价格
            price = self._get_price_for_date(symbol, date_str, market)
            if price is not None:
                qty_float = float(quantity) if quantity is not None else 0.0
                value = qty_float * float(price)
                stock_value += value
                holdings_detail[symbol] = {
                    "quantity": quantity,
                    "price": float(price),
                    "value": round(value, 2),
                }
            else:
                holdings_detail[symbol] = {
                    "quantity": quantity,
                    "price": None,
                    "value": None,
                    "error": "Price not available",
                }

        total_value = cash + stock_value
        return_pct = ((total_value - initial_cash) / initial_cash) * 100 if initial_cash else 0.0

        return {
            "agent": agent_name,
            "date": snapshot.get("date"),
            "market": market,
            "holdings": holdings_detail,
            "cash": round(cash, 2),
            "stock_value": round(stock_value, 2),
            "total_value": round(total_value, 2),
            "initial_cash": initial_cash,
            "return_pct": round(return_pct, 2),
            "action": snapshot.get("action"),
        }

    def _get_price_for_date(
        self, symbol: str, date_str: str, market: str = "cn"
    ) -> Optional[float]:
        """获取指定日期的收盘价

        Args:
            symbol: 股票代码
            date_str: 日期或时间字符串
            market: 市场

        Returns:
            收盘价或 None
        """
        try:
            if market == "cn_hour":
                sql = """
                    SELECT close FROM stock_hourly_prices
                    WHERE ts_code = ? AND trade_time = ?
                """
                result = self.conn.execute(sql, [symbol, date_str]).fetchone()
            else:
                # 日线：只用日期部分
                date_only = date_str.split(" ")[0]
                sql = """
                    SELECT close FROM stock_daily_prices
                    WHERE ts_code = ? AND trade_date = ?
                """
                result = self.conn.execute(sql, [symbol, date_only]).fetchone()

            if result and result[0]:
                return float(result[0])
        except Exception:
            pass
        return None

    def list_agents(self, market: str = "cn") -> List[str]:
        """列出所有有持仓数据的 Agent

        Args:
            market: 市场

        Returns:
            Agent 名称列表
        """
        self._validate_market(market)

        data_dir = get_data_dir(market)
        agents = []

        if data_dir.exists():
            for agent_dir in data_dir.iterdir():
                if agent_dir.is_dir():
                    position_file = agent_dir / "position" / "position.jsonl"
                    if position_file.exists():
                        agents.append(agent_dir.name)

        return sorted(agents)
