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
from api.utils.model_display import (
    PROVIDER_COLORS,
    PROVIDER_ICONS,
    display_name as _display_name,
    get_provider as _get_provider,
    iter_valid_models as _iter_valid_models,
    resolve_model_name as _resolve_model_name,
)

logger = logging.getLogger(__name__)


class AgentService:
    """Agent 数据服务"""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn
        self.project_root = get_project_root()
        self._position_service = PositionServiceV2(conn)

    def get_all_agents(self, market: str = "cn") -> List[dict]:
        """获取所有 Agent 信息

        Discovers agents from three sources and merges (dedup by name):
        1. Data directory scan (folders containing position/position.jsonl)
        2. DuckDB positions table (distinct agent_name)
        3. Config models (for metadata, not filtered by enabled)

        Args:
            market: 市场 (cn/cn_hour/us)

        Returns:
            Agent 信息列表
        """
        config = load_config_json("config.json")
        initial_cash = config.get("agent_config", {}).get("initial_cash", 100000)
        is_hourly = market == "cn_hour"

        # Build a lookup of config model metadata keyed by agent folder name
        config_meta: Dict[str, dict] = {}
        for model, name in _iter_valid_models(config):
            folder = f"{name}-astock-hour" if is_hourly else name
            config_meta[folder] = model

        discovered_names: set = set()

        # --- Source 1: Scan data directory ---
        data_dir = get_data_dir(market)
        if data_dir.exists():
            for child in data_dir.iterdir():
                if child.is_dir():
                    position_file = child / "position" / "position.jsonl"
                    if position_file.exists():
                        agent_name = child.name
                        # Filter by hourly suffix
                        if is_hourly and not agent_name.endswith("-astock-hour"):
                            continue
                        if not is_hourly and agent_name.endswith("-astock-hour"):
                            continue
                        discovered_names.add(agent_name)

        # --- Source 2: DuckDB positions table ---
        try:
            if is_hourly:
                sql = "SELECT DISTINCT agent_name FROM positions WHERE market = 'cn' AND agent_name LIKE '%-astock-hour'"
            else:
                sql = "SELECT DISTINCT agent_name FROM positions WHERE market = 'cn' AND agent_name NOT LIKE '%-astock-hour'"
            rows = self.conn.execute(sql).fetchall()
            for row in rows:
                discovered_names.add(row[0])
        except Exception as e:
            logger.debug(f"DuckDB agent discovery query failed (table may not exist): {e}")

        # --- Source 3: Config models (add any not yet discovered) ---
        for folder_name in config_meta:
            discovered_names.add(folder_name)

        # --- Build agent list (exclude live agents; their data is merged) ---
        agents = []
        for agent_name in sorted(discovered_names):
            # Skip live agent folders — data merged into backtest agent
            stripped = agent_name
            if is_hourly and stripped.endswith("-astock-hour"):
                stripped = stripped[: -len("-astock-hour")]
            if stripped.endswith("-live"):
                continue

            # Determine the base model name (strip -astock-hour suffix for display)
            base_name = agent_name
            if is_hourly and base_name.endswith("-astock-hour"):
                base_name = base_name[: -len("-astock-hour")]

            # Use config metadata if available
            model_cfg = config_meta.get(agent_name, {})
            cfg_name = _resolve_model_name(model_cfg) if model_cfg else None
            display_name = _display_name(cfg_name) if cfg_name else _display_name(base_name)

            provider = _get_provider(base_name)
            icon = PROVIDER_ICONS.get(provider, "./figs/stock.svg")
            color = PROVIDER_COLORS.get(provider, "#999999")

            # Determine enabled status from config
            model_enabled = model_cfg.get("enabled", False) if model_cfg else False

            agents.append(
                {
                    "name": agent_name,
                    "display_name": display_name,
                    "market": market,
                    "initial_cash": initial_cash,
                    "icon": icon,
                    "color": color,
                    "enabled": model_enabled,
                }
            )

        return agents

    @staticmethod
    def _get_live_agent_name(agent_name: str, market: str) -> str:
        """从 backtest agent name 推算对应的 live agent name。

        Naming convention (see trading_mode.py):
          backtest daily:  {model}            → live: {model}-live
          backtest hourly: {model}-astock-hour → live: {model}-live-astock-hour
        """
        is_hourly = market == "cn_hour"
        if is_hourly and agent_name.endswith("-astock-hour"):
            base = agent_name[: -len("-astock-hour")]
            return f"{base}-live-astock-hour"
        return f"{agent_name}-live"

    def get_agent_positions(
        self,
        agent_name: str,
        market: str = "cn",
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[dict]:
        """获取 Agent 持仓历史

        优先从 DuckDB 读取，如果数据为空则降级到 JSONL 文件。
        自动合并对应的 live agent 数据（如果存在），保证 backtest + live 时间线连续。

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
        except Exception as e:
            logger.warning(f"DuckDB position query failed: {e}")
            positions = []

        # 降级到 JSONL 文件（如果 DuckDB 无数据）
        if not positions:
            positions = self._get_positions_from_jsonl(agent_name, market, start_date, end_date)

        # 合并 live agent 数据（如果存在）
        # 只取 backtest 最后日期之后的 live 记录，避免初始化记录导致的重叠和资产值跳变
        live_name = self._get_live_agent_name(agent_name, market)
        # Try DuckDB first for live positions, fallback to JSONL
        live_positions = []
        try:
            live_positions = self._position_service.get_positions_by_agent(
                agent_name=live_name, market=market, start_date=start_date, end_date=end_date,
            )
        except Exception:
            pass
        if not live_positions:
            live_positions = self._get_positions_from_jsonl(live_name, market, start_date, end_date)

        if live_positions and positions:
            last_backtest_date = max(p.get("date", "") for p in positions)
            live_positions = [p for p in live_positions if p.get("date", "") > last_backtest_date]
        if live_positions:
            positions = positions + live_positions
            positions.sort(key=lambda p: p.get("date", ""))
            logger.debug(
                f"Merged {len(live_positions)} live positions from {live_name} "
                f"into {agent_name} (total: {len(positions)})"
            )

        return positions

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
        sorted_pos_dates = sorted(positions_by_date.keys())

        # Get all trading dates from price data to fill gaps between position records.
        # Only fill gaps up to the last actual position date — do not extrapolate
        # beyond the last trade (otherwise carry-forward creates phantom data points
        # when real-time price data extends beyond the agent's last trading date).
        all_trading_dates = sorted_pos_dates
        if len(sorted_pos_dates) >= 1:
            first_date = sorted_pos_dates[0]
            last_date = sorted_pos_dates[-1]
            try:
                if market == "cn_hour":
                    rows = self.conn.execute(
                        "SELECT DISTINCT trade_time FROM stock_hourly_prices "
                        "WHERE trade_time >= ? AND trade_time <= ? ORDER BY trade_time",
                        [first_date, last_date],
                    ).fetchall()
                else:
                    rows = self.conn.execute(
                        "SELECT DISTINCT trade_date FROM stock_daily_prices "
                        "WHERE trade_date >= ? AND trade_date <= ? ORDER BY trade_date",
                        [first_date, last_date],
                    ).fetchall()
                if rows:
                    # Convert to string for consistent comparison
                    all_trading_dates = [str(r[0]) for r in rows]
            except Exception:
                pass

        # Compute portfolio value for each trading date using the most recent position
        history = []
        current_pos = None
        pos_idx = 0
        for date_str in all_trading_dates:
            # Advance to the latest position record on or before this date
            while pos_idx < len(sorted_pos_dates) and sorted_pos_dates[pos_idx] <= date_str:
                current_pos = positions_by_date[sorted_pos_dates[pos_idx]]
                pos_idx += 1

            if current_pos is None:
                continue

            pos_dict = current_pos.get("positions", {})
            cash = float(pos_dict.get("CASH", 0))

            stock_value = 0
            for symbol, quantity in pos_dict.items():
                if symbol == "CASH" or quantity == 0:
                    continue
                price_data = self._get_price_for_date(symbol, date_str, market)
                if price_data:
                    stock_value += float(price_data.get("close", 0)) * quantity

            total_value = cash + stock_value
            return_pct = ((total_value - initial_cash) / initial_cash) * 100

            history.append(
                {
                    "date": date_str,
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
                sql = """
                    SELECT close FROM stock_hourly_prices
                    WHERE ts_code = ? AND trade_time <= ?
                    ORDER BY trade_time DESC LIMIT 1
                """
                result = self.conn.execute(sql, [symbol, date_str]).fetchone()
            else:
                sql = """
                    SELECT close FROM stock_daily_prices
                    WHERE ts_code = ? AND trade_date <= ?
                    ORDER BY trade_date DESC LIMIT 1
                """
                result = self.conn.execute(sql, [symbol, date_str]).fetchone()

            if result:
                return {"close": result[0]}
        except Exception:
            pass
        return None

    def get_leaderboard(self, market: str = "cn", agents: Optional[List[dict]] = None) -> List[dict]:
        """获取排行榜

        Args:
            market: 市场
            agents: 可选的预过滤 Agent 列表，为 None 时使用 get_all_agents()

        Returns:
            排行榜数据
        """
        if agents is None:
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
        self, market: str = "cn", limit: int = 20, agents: Optional[List[dict]] = None
    ) -> List[dict]:
        """获取最近交易记录

        Args:
            market: 市场
            limit: 返回数量
            agents: 可选的预过滤 Agent 列表，为 None 时使用 get_all_agents()

        Returns:
            交易记录列表
        """
        if agents is None:
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
