"""
交易点评服务

提供交易点评的 CRUD 操作，数据存储在 DuckDB trade_comments 表中。
"""

import logging
from typing import Optional

import duckdb

logger = logging.getLogger(__name__)


class CommentService:
    """交易点评服务"""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def add_comment(
        self,
        agent_name: str,
        market: str,
        trade_date: str,
        ts_code: str,
        action: str,
        comment_text: str,
    ) -> dict:
        """添加点评"""
        result = self.conn.execute(
            """
            INSERT INTO trade_comments (agent_name, market, trade_date, ts_code, action, comment_text)
            VALUES (?, ?, ?, ?, ?, ?)
            RETURNING id, agent_name, market, trade_date, ts_code, action, comment_text, created_at
            """,
            (agent_name, market, trade_date, ts_code, action, comment_text),
        ).fetchone()

        return {
            "id": result[0],
            "agent_name": result[1],
            "market": result[2],
            "trade_date": result[3],
            "ts_code": result[4],
            "action": result[5],
            "comment_text": result[6],
            "created_at": str(result[7]),
        }

    def get_comments_for_trade(
        self,
        agent_name: str,
        trade_date: str,
        ts_code: str,
        action: str,
    ) -> list:
        """获取特定交易的点评"""
        rows = self.conn.execute(
            """
            SELECT id, agent_name, market, trade_date, ts_code, action, comment_text, created_at, updated_at
            FROM trade_comments
            WHERE agent_name = ? AND trade_date = ? AND ts_code = ? AND action = ?
            ORDER BY created_at DESC
            """,
            (agent_name, trade_date, ts_code, action),
        ).fetchall()

        return [self._row_to_dict(r) for r in rows]

    def get_comments_by_agent(
        self,
        agent_name: str,
        trade_date: Optional[str] = None,
        ts_code: Optional[str] = None,
        limit: int = 50,
    ) -> list:
        """获取 agent 的点评列表"""
        sql = "SELECT id, agent_name, market, trade_date, ts_code, action, comment_text, created_at, updated_at FROM trade_comments WHERE agent_name = ?"
        params = [agent_name]

        if trade_date:
            sql += " AND trade_date = ?"
            params.append(trade_date)
        if ts_code:
            sql += " AND ts_code = ?"
            params.append(ts_code)

        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        rows = self.conn.execute(sql, params).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def get_latest_comments(
        self,
        agent_name: str,
        market: str = "cn",
        limit: int = 10,
    ) -> list:
        """获取最新点评（用于 prompt 注入）"""
        rows = self.conn.execute(
            """
            SELECT id, agent_name, market, trade_date, ts_code, action, comment_text, created_at, updated_at
            FROM trade_comments
            WHERE agent_name = ? AND market = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (agent_name, market, limit),
        ).fetchall()

        return [self._row_to_dict(r) for r in rows]

    def update_comment(self, comment_id: int, comment_text: str) -> Optional[dict]:
        """更新点评"""
        result = self.conn.execute(
            """
            UPDATE trade_comments
            SET comment_text = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            RETURNING id, agent_name, market, trade_date, ts_code, action, comment_text, created_at, updated_at
            """,
            (comment_text, comment_id),
        ).fetchone()

        if not result:
            return None
        return self._row_to_dict(result)

    def delete_comment(self, comment_id: int) -> bool:
        """删除点评"""
        result = self.conn.execute(
            "DELETE FROM trade_comments WHERE id = ? RETURNING id",
            (comment_id,),
        ).fetchone()
        return result is not None

    @staticmethod
    def _row_to_dict(row) -> dict:
        return {
            "id": row[0],
            "agent_name": row[1],
            "market": row[2],
            "trade_date": row[3],
            "ts_code": row[4],
            "action": row[5],
            "comment_text": row[6],
            "created_at": str(row[7]),
            "updated_at": str(row[8]) if row[8] else None,
        }
