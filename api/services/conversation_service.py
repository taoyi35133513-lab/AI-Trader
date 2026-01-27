"""
对话日志服务

管理 Agent 交易会话和对话消息的 DuckDB 存储与查询。
"""

import logging
from datetime import date, datetime, time
from typing import Any, Dict, List, Optional

import duckdb

logger = logging.getLogger(__name__)


class ConversationService:
    """Agent 对话日志服务"""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def create_session(
        self,
        agent_name: str,
        market: str,
        session_timestamp: datetime,
    ) -> int:
        """创建新的交易会话

        Args:
            agent_name: Agent 名称
            market: 市场 (cn/cn_hour)
            session_timestamp: 会话时间戳

        Returns:
            session_id
        """
        session_date = session_timestamp.date()
        session_time = session_timestamp.time() if market == "cn_hour" else None

        # 检查是否已存在
        check_sql = """
            SELECT id FROM agent_trading_sessions
            WHERE agent_name = ? AND session_timestamp = ?
        """
        existing = self.conn.execute(check_sql, [agent_name, session_timestamp]).fetchone()
        if existing:
            return existing[0]

        # 创建新会话
        insert_sql = """
            INSERT INTO agent_trading_sessions
            (agent_name, market, session_date, session_time, session_timestamp)
            VALUES (?, ?, ?, ?, ?)
            RETURNING id
        """
        result = self.conn.execute(
            insert_sql,
            [agent_name, market, session_date, session_time, session_timestamp],
        ).fetchone()

        session_id = result[0]
        logger.info(f"Created session {session_id} for {agent_name} at {session_timestamp}")
        return session_id

    def get_or_create_session(
        self,
        agent_name: str,
        market: str,
        session_timestamp: datetime,
    ) -> int:
        """获取或创建交易会话

        Args:
            agent_name: Agent 名称
            market: 市场
            session_timestamp: 会话时间戳

        Returns:
            session_id
        """
        return self.create_session(agent_name, market, session_timestamp)

    def add_messages(
        self,
        session_id: int,
        messages: List[Dict[str, str]],
        base_timestamp: datetime,
    ) -> None:
        """添加对话消息（批量插入）

        Args:
            session_id: 会话 ID
            messages: 消息列表 [{"role": "user", "content": "..."}]
            base_timestamp: 基准时间戳
        """
        if not messages:
            return

        # 获取当前最大序号
        max_seq_sql = """
            SELECT COALESCE(MAX(message_sequence), 0)
            FROM agent_conversation_messages
            WHERE session_id = ?
        """
        max_seq = self.conn.execute(max_seq_sql, [session_id]).fetchone()[0]

        # 准备批量插入数据
        insert_sql = """
            INSERT INTO agent_conversation_messages
            (session_id, message_sequence, role, content, tool_call_id, tool_name, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """

        for i, msg in enumerate(messages):
            seq = max_seq + i + 1
            role = msg.get("role", "unknown")
            content = msg.get("content", "")
            tool_call_id = msg.get("tool_call_id")
            tool_name = msg.get("tool_name")

            self.conn.execute(
                insert_sql,
                [session_id, seq, role, content, tool_call_id, tool_name, base_timestamp],
            )

        logger.debug(f"Added {len(messages)} messages to session {session_id}")

    def get_conversation_by_date(
        self,
        agent_name: str,
        session_date: str,
        market: str = "cn",
        session_time: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """获取指定日期的完整对话

        Args:
            agent_name: Agent 名称
            session_date: 日期字符串 (YYYY-MM-DD)
            market: 市场
            session_time: 时间字符串 (HH:MM:SS) - 用于小时级

        Returns:
            会话数据，包含消息列表
        """
        # 查询会话
        if session_time:
            session_sql = """
                SELECT id, agent_name, session_date, session_time, session_timestamp
                FROM agent_trading_sessions
                WHERE agent_name = ? AND session_date = ? AND session_time = ? AND market = ?
            """
            session = self.conn.execute(
                session_sql, [agent_name, session_date, session_time, market]
            ).fetchone()
        else:
            session_sql = """
                SELECT id, agent_name, session_date, session_time, session_timestamp
                FROM agent_trading_sessions
                WHERE agent_name = ? AND session_date = ? AND market = ?
                ORDER BY session_timestamp DESC
                LIMIT 1
            """
            session = self.conn.execute(
                session_sql, [agent_name, session_date, market]
            ).fetchone()

        if not session:
            return None

        session_id = session[0]

        # 查询消息
        messages_sql = """
            SELECT role, content, tool_call_id, tool_name, timestamp
            FROM agent_conversation_messages
            WHERE session_id = ?
            ORDER BY message_sequence
        """
        messages = self.conn.execute(messages_sql, [session_id]).fetchall()

        return {
            "session_id": session_id,
            "agent_name": session[1],
            "session_date": str(session[2]),
            "session_time": str(session[3]) if session[3] else None,
            "session_timestamp": session[4].isoformat() if session[4] else None,
            "messages": [
                {
                    "role": m[0],
                    "content": m[1],
                    "tool_call_id": m[2],
                    "tool_name": m[3],
                    "timestamp": m[4].isoformat() if m[4] else None,
                }
                for m in messages
            ],
        }

    def get_latest_conversations(
        self,
        agent_name: str,
        limit: int = 10,
        market: str = "cn",
    ) -> List[Dict[str, Any]]:
        """获取最近 N 个交易会话

        Args:
            agent_name: Agent 名称
            limit: 返回数量
            market: 市场

        Returns:
            会话列表
        """
        sessions_sql = """
            SELECT id, agent_name, session_date, session_time, session_timestamp
            FROM agent_trading_sessions
            WHERE agent_name = ? AND market = ?
            ORDER BY session_timestamp DESC
            LIMIT ?
        """
        sessions = self.conn.execute(sessions_sql, [agent_name, market, limit]).fetchall()

        results = []
        for session in sessions:
            session_id = session[0]

            # 获取消息
            messages_sql = """
                SELECT role, content, tool_call_id, tool_name, timestamp
                FROM agent_conversation_messages
                WHERE session_id = ?
                ORDER BY message_sequence
            """
            messages = self.conn.execute(messages_sql, [session_id]).fetchall()

            results.append({
                "session_id": session_id,
                "agent_name": session[1],
                "session_date": str(session[2]),
                "session_time": str(session[3]) if session[3] else None,
                "session_timestamp": session[4].isoformat() if session[4] else None,
                "messages": [
                    {
                        "role": m[0],
                        "content": m[1],
                        "tool_call_id": m[2],
                        "tool_name": m[3],
                        "timestamp": m[4].isoformat() if m[4] else None,
                    }
                    for m in messages
                ],
            })

        return results

    @staticmethod
    def _escape_like_pattern(keyword: str) -> str:
        """转义 LIKE 模式中的特殊字符

        Args:
            keyword: 原始关键词

        Returns:
            转义后的关键词
        """
        # 转义 LIKE 通配符: % _ 和转义字符本身 \
        return keyword.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

    def search_conversations(
        self,
        agent_name: str,
        keyword: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        role: Optional[str] = None,
        market: str = "cn",
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """搜索对话消息

        Args:
            agent_name: Agent 名称
            keyword: 搜索关键词
            start_date: 开始日期
            end_date: 结束日期
            role: 消息角色过滤
            market: 市场
            limit: 返回数量

        Returns:
            匹配的消息列表
        """
        # 转义关键词中的 LIKE 特殊字符
        escaped_keyword = self._escape_like_pattern(keyword)

        # 构建查询
        sql = """
            SELECT
                s.id as session_id,
                s.agent_name,
                s.session_date,
                s.session_time,
                m.role,
                m.content,
                m.timestamp
            FROM agent_conversation_messages m
            JOIN agent_trading_sessions s ON m.session_id = s.id
            WHERE s.agent_name = ?
              AND s.market = ?
              AND m.content LIKE ? ESCAPE '\\'
        """
        params = [agent_name, market, f"%{escaped_keyword}%"]

        if start_date:
            sql += " AND s.session_date >= ?"
            params.append(start_date)
        if end_date:
            sql += " AND s.session_date <= ?"
            params.append(end_date)
        if role:
            sql += " AND m.role = ?"
            params.append(role)

        sql += " ORDER BY m.timestamp DESC LIMIT ?"
        params.append(limit)

        results = self.conn.execute(sql, params).fetchall()

        return [
            {
                "session_id": r[0],
                "agent_name": r[1],
                "session_date": str(r[2]),
                "session_time": str(r[3]) if r[3] else None,
                "role": r[4],
                "content": r[5],
                "timestamp": r[6].isoformat() if r[6] else None,
            }
            for r in results
        ]

    def get_sessions_by_date_range(
        self,
        agent_name: str,
        start_date: date,
        end_date: date,
        market: str = "cn",
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """获取日期范围内的会话摘要

        Args:
            agent_name: Agent 名称
            start_date: 开始日期
            end_date: 结束日期
            market: 市场
            limit: 返回数量
            offset: 偏移量

        Returns:
            会话摘要列表
        """
        sql = """
            SELECT
                s.id,
                s.agent_name,
                s.session_date,
                s.session_time,
                s.session_timestamp,
                COUNT(m.id) as message_count,
                (SELECT content FROM agent_conversation_messages
                 WHERE session_id = s.id ORDER BY message_sequence LIMIT 1) as first_message
            FROM agent_trading_sessions s
            LEFT JOIN agent_conversation_messages m ON s.id = m.session_id
            WHERE s.agent_name = ?
              AND s.market = ?
              AND s.session_date >= ?
              AND s.session_date <= ?
            GROUP BY s.id, s.agent_name, s.session_date, s.session_time, s.session_timestamp
            ORDER BY s.session_timestamp DESC
            LIMIT ? OFFSET ?
        """
        results = self.conn.execute(
            sql, [agent_name, market, start_date, end_date, limit, offset]
        ).fetchall()

        return [
            {
                "session_id": r[0],
                "agent_name": r[1],
                "session_date": str(r[2]),
                "session_time": str(r[3]) if r[3] else None,
                "session_timestamp": r[4].isoformat() if r[4] else None,
                "message_count": r[5],
                "first_message_preview": (r[6][:100] + "...") if r[6] and len(r[6]) > 100 else r[6],
            }
            for r in results
        ]

    def get_all_sessions(
        self,
        market: str = "cn",
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        """获取所有会话（用于 dashboard）

        Args:
            market: 市场
            limit: 返回数量

        Returns:
            会话摘要列表
        """
        sql = """
            SELECT
                s.id,
                s.agent_name,
                s.session_date,
                s.session_time,
                s.session_timestamp,
                COUNT(m.id) as message_count
            FROM agent_trading_sessions s
            LEFT JOIN agent_conversation_messages m ON s.id = m.session_id
            WHERE s.market = ?
            GROUP BY s.id, s.agent_name, s.session_date, s.session_time, s.session_timestamp
            ORDER BY s.session_timestamp DESC
            LIMIT ?
        """
        results = self.conn.execute(sql, [market, limit]).fetchall()

        return [
            {
                "session_id": r[0],
                "agent_name": r[1],
                "session_date": str(r[2]),
                "session_time": str(r[3]) if r[3] else None,
                "session_timestamp": r[4].isoformat() if r[4] else None,
                "message_count": r[5],
            }
            for r in results
        ]
