"""
持仓数据服务 V2

使用新的规范化表结构存储和查询 Agent 持仓数据。
"""

import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import duckdb

logger = logging.getLogger(__name__)


class PositionServiceV2:
    """Agent 持仓数据服务（DuckDB 版本）"""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def save_position_step(
        self,
        agent_name: str,
        market: str,
        position_date: str,
        step_id: int,
        action_data: Optional[Dict[str, Any]],
        holdings: Dict[str, int],
        cash: float,
        session_id: Optional[int] = None,
        position_time: Optional[str] = None,
    ) -> int:
        """保存持仓步骤

        Args:
            agent_name: Agent 名称
            market: 市场 (cn/cn_hour)
            position_date: 日期字符串 (YYYY-MM-DD)
            step_id: 步骤 ID
            action_data: 动作数据 {"action": "buy", "symbol": "600519.SH", "amount": 100}
            holdings: 持仓 {"600519.SH": 100, ...}
            cash: 现金余额
            session_id: 关联的会话 ID（可选）
            position_time: 时间戳字符串（小时级使用）

        Returns:
            position_history_id
        """
        # 解析动作数据
        action = action_data.get("action") if action_data else None
        action_symbol = action_data.get("symbol") if action_data else None
        action_amount = action_data.get("amount") if action_data else None
        action_price = action_data.get("price") if action_data else None

        # 解析时间戳
        pos_time = None
        if position_time:
            try:
                pos_time = datetime.strptime(position_time, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                pos_time = None

        # 使用事务确保数据完整性
        try:
            self.conn.execute("BEGIN TRANSACTION")

            # 插入持仓历史记录
            insert_sql = """
                INSERT INTO agent_positions_history
                (session_id, agent_name, market, position_date, position_time, step_id,
                 action, action_symbol, action_amount, action_price, cash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING id
            """
            result = self.conn.execute(
                insert_sql,
                [
                    session_id,
                    agent_name,
                    market,
                    position_date,
                    pos_time,
                    step_id,
                    action,
                    action_symbol,
                    action_amount,
                    action_price,
                    cash,
                ],
            ).fetchone()

            position_history_id = result[0]

            # 插入持仓明细
            if holdings:
                holdings_sql = """
                    INSERT INTO agent_position_holdings
                    (position_history_id, ts_code, quantity)
                    VALUES (?, ?, ?)
                """
                for ts_code, quantity in holdings.items():
                    if quantity > 0:  # 只保存非零持仓
                        self.conn.execute(holdings_sql, [position_history_id, ts_code, quantity])

            self.conn.execute("COMMIT")

            logger.debug(
                f"Saved position step {step_id} for {agent_name} on {position_date}, "
                f"holdings: {len(holdings)} stocks, cash: {cash}"
            )
            return position_history_id

        except Exception as e:
            self.conn.execute("ROLLBACK")
            logger.error(f"Failed to save position step: {e}")
            raise

    def get_positions_by_agent(
        self,
        agent_name: str,
        market: str = "cn",
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """获取 Agent 持仓历史

        Args:
            agent_name: Agent 名称
            market: 市场
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            持仓记录列表
        """
        sql = """
            SELECT
                ph.id,
                ph.position_date,
                ph.position_time,
                ph.step_id,
                ph.action,
                ph.action_symbol,
                ph.action_amount,
                ph.action_price,
                ph.cash
            FROM agent_positions_history ph
            WHERE ph.agent_name = ? AND ph.market = ?
        """
        params = [agent_name, market]

        if start_date:
            sql += " AND ph.position_date >= ?"
            params.append(start_date)
        if end_date:
            sql += " AND ph.position_date <= ?"
            params.append(end_date)

        sql += " ORDER BY ph.position_date, ph.step_id"

        results = self.conn.execute(sql, params).fetchall()

        positions = []
        for r in results:
            pos_id = r[0]

            # 获取持仓明细
            holdings_sql = """
                SELECT ts_code, quantity
                FROM agent_position_holdings
                WHERE position_history_id = ?
            """
            holdings = self.conn.execute(holdings_sql, [pos_id]).fetchall()
            holdings_dict = {h[0]: h[1] for h in holdings}
            holdings_dict["CASH"] = float(r[8]) if r[8] else 0

            # 构建日期字符串
            date_str = str(r[1])
            if r[2]:  # position_time
                date_str = f"{r[1]} {r[2].strftime('%H:%M:%S')}"

            positions.append({
                "date": date_str,
                "step_id": r[3],
                "positions": holdings_dict,
                "cash": float(r[8]) if r[8] else 0,
                "this_action": {
                    "action": r[4],
                    "symbol": r[5],
                    "amount": r[6],
                    "price": float(r[7]) if r[7] else None,
                } if r[4] else None,
            })

        return positions

    def get_latest_position(
        self,
        agent_name: str,
        market: str = "cn",
    ) -> Optional[Dict[str, Any]]:
        """获取最新持仓快照

        Args:
            agent_name: Agent 名称
            market: 市场

        Returns:
            最新持仓数据
        """
        sql = """
            SELECT
                ph.id,
                ph.position_date,
                ph.position_time,
                ph.step_id,
                ph.action,
                ph.action_symbol,
                ph.action_amount,
                ph.action_price,
                ph.cash
            FROM agent_positions_history ph
            WHERE ph.agent_name = ? AND ph.market = ?
            ORDER BY ph.position_date DESC, ph.step_id DESC
            LIMIT 1
        """
        result = self.conn.execute(sql, [agent_name, market]).fetchone()

        if not result:
            return None

        pos_id = result[0]

        # 获取持仓明细
        holdings_sql = """
            SELECT ts_code, quantity
            FROM agent_position_holdings
            WHERE position_history_id = ?
        """
        holdings = self.conn.execute(holdings_sql, [pos_id]).fetchall()
        holdings_dict = {h[0]: h[1] for h in holdings}
        holdings_dict["CASH"] = float(result[8]) if result[8] else 0

        # 构建日期字符串
        date_str = str(result[1])
        if result[2]:  # position_time
            date_str = f"{result[1]} {result[2].strftime('%H:%M:%S')}"

        return {
            "date": date_str,
            "step_id": result[3],
            "positions": holdings_dict,
            "cash": float(result[8]) if result[8] else 0,
            "this_action": {
                "action": result[4],
                "symbol": result[5],
                "amount": result[6],
                "price": float(result[7]) if result[7] else None,
            } if result[4] else None,
        }

    def get_position_at_date(
        self,
        agent_name: str,
        target_date: date,
        market: str = "cn",
    ) -> Optional[Dict[str, Any]]:
        """获取指定日期的持仓快照

        Args:
            agent_name: Agent 名称
            target_date: 目标日期
            market: 市场

        Returns:
            持仓数据
        """
        sql = """
            SELECT
                ph.id,
                ph.position_date,
                ph.position_time,
                ph.step_id,
                ph.action,
                ph.action_symbol,
                ph.action_amount,
                ph.action_price,
                ph.cash
            FROM agent_positions_history ph
            WHERE ph.agent_name = ?
              AND ph.market = ?
              AND ph.position_date = ?
            ORDER BY ph.step_id DESC
            LIMIT 1
        """
        result = self.conn.execute(sql, [agent_name, market, target_date]).fetchone()

        if not result:
            return None

        pos_id = result[0]

        # 获取持仓明细
        holdings_sql = """
            SELECT ts_code, quantity
            FROM agent_position_holdings
            WHERE position_history_id = ?
        """
        holdings = self.conn.execute(holdings_sql, [pos_id]).fetchall()
        holdings_dict = {h[0]: h[1] for h in holdings}
        holdings_dict["CASH"] = float(result[8]) if result[8] else 0

        return {
            "date": str(result[1]),
            "step_id": result[3],
            "positions": holdings_dict,
            "cash": float(result[8]) if result[8] else 0,
            "this_action": {
                "action": result[4],
                "symbol": result[5],
                "amount": result[6],
            } if result[4] else None,
        }

    def get_holdings_history(
        self,
        agent_name: str,
        symbol: str,
        market: str = "cn",
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """获取特定股票的持仓历史

        Args:
            agent_name: Agent 名称
            symbol: 股票代码
            market: 市场
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            持仓历史列表
        """
        sql = """
            SELECT
                ph.position_date,
                ph.position_time,
                h.quantity,
                ph.action,
                ph.action_symbol,
                ph.action_amount
            FROM agent_position_holdings h
            JOIN agent_positions_history ph ON h.position_history_id = ph.id
            WHERE ph.agent_name = ?
              AND ph.market = ?
              AND h.ts_code = ?
        """
        params = [agent_name, market, symbol]

        if start_date:
            sql += " AND ph.position_date >= ?"
            params.append(start_date)
        if end_date:
            sql += " AND ph.position_date <= ?"
            params.append(end_date)

        sql += " ORDER BY ph.position_date, ph.step_id"

        results = self.conn.execute(sql, params).fetchall()

        return [
            {
                "date": str(r[0]),
                "time": r[1].strftime("%H:%M:%S") if r[1] else None,
                "quantity": r[2],
                "action": r[3] if r[4] == symbol else None,
                "amount": r[5] if r[4] == symbol else None,
            }
            for r in results
        ]

    def get_all_positions(
        self,
        market: str = "cn",
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """获取所有 Agent 的持仓数据（用于 dashboard）

        Args:
            market: 市场
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            {agent_name: [positions]}
        """
        sql = """
            SELECT DISTINCT agent_name
            FROM agent_positions_history
            WHERE market = ?
        """
        agents = self.conn.execute(sql, [market]).fetchall()

        result = {}
        for (agent_name,) in agents:
            result[agent_name] = self.get_positions_by_agent(
                agent_name, market, start_date, end_date
            )

        return result

    def get_trade_actions(
        self,
        agent_name: Optional[str] = None,
        market: str = "cn",
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """获取交易动作（买卖记录）

        Args:
            agent_name: Agent 名称（可选，为 None 则返回所有 Agent）
            market: 市场
            limit: 返回数量

        Returns:
            交易记录列表
        """
        sql = """
            SELECT
                ph.agent_name,
                ph.position_date,
                ph.position_time,
                ph.step_id,
                ph.action,
                ph.action_symbol,
                ph.action_amount,
                ph.action_price
            FROM agent_positions_history ph
            WHERE ph.market = ?
              AND ph.action IN ('buy', 'sell')
              AND ph.action_symbol IS NOT NULL
        """
        params = [market]

        if agent_name:
            sql += " AND ph.agent_name = ?"
            params.append(agent_name)

        sql += " ORDER BY ph.position_date DESC, ph.step_id DESC LIMIT ?"
        params.append(limit)

        results = self.conn.execute(sql, params).fetchall()

        return [
            {
                "agent_name": r[0],
                "date": str(r[1]),
                "time": r[2].strftime("%H:%M:%S") if r[2] else None,
                "step_id": r[3],
                "action": r[4],
                "symbol": r[5],
                "amount": r[6],
                "price": float(r[7]) if r[7] else None,
            }
            for r in results
        ]
