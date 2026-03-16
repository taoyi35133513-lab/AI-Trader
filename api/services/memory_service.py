"""
AI 交易 Agent 记忆与经验服务

三层记忆架构：
- L1 Reflection: 每次交易后的自我复盘
- L2 Lesson: 由多条 L1 压缩而来的可复用经验
- L3 Strategy: Agent 的核心交易哲学备忘
"""

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Optional

import duckdb

from api.config import get_database_path, load_config_json

logger = logging.getLogger(__name__)

# Memory level constants
LEVEL_REFLECTION = "reflection"
LEVEL_LESSON = "lesson"
LEVEL_STRATEGY = "strategy"

# Consolidation thresholds
L1_CONSOLIDATE_THRESHOLD = 5   # Compress L1->L2 when active L1 count >= 5
L2_CONSOLIDATE_THRESHOLD = 8   # Update L3 when active L2 count >= 8
L1_EXPIRE_DAYS = 30            # Auto-archive L1 older than 30 days

# Token budgets per level
TOKEN_BUDGET = {
    LEVEL_STRATEGY: 500,
    LEVEL_LESSON: 800,
    LEVEL_REFLECTION: 700,
}


def init_memory_table(conn: duckdb.DuckDBPyConnection):
    """Create agent_memory table if not exists."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS agent_memory (
            id              INTEGER PRIMARY KEY,
            agent_name      VARCHAR NOT NULL,
            market          VARCHAR NOT NULL DEFAULT 'cn',
            level           VARCHAR NOT NULL,
            content         TEXT NOT NULL,
            source_dates    VARCHAR,
            tags            VARCHAR,
            status          VARCHAR NOT NULL DEFAULT 'active',
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at      TIMESTAMP,
            source_session_id INTEGER,
            parent_ids      VARCHAR
        )
    """)
    # Create sequence for auto-increment if not exists
    try:
        conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_agent_memory_id START 1")
    except Exception:
        pass


class MemoryService:
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn
        try:
            init_memory_table(conn)
        except Exception:
            pass  # read-only connection, table already exists

    def add_reflection(
        self,
        agent_name: str,
        market: str,
        content: str,
        source_date: str,
        session_id: Optional[int] = None,
        tags: Optional[str] = None,
    ) -> int:
        """Add a L1 reflection after a trading session."""
        expires_at = datetime.now() + timedelta(days=L1_EXPIRE_DAYS)
        try:
            next_id = self.conn.execute("SELECT nextval('seq_agent_memory_id')").fetchone()[0]
        except Exception:
            row = self.conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM agent_memory").fetchone()
            next_id = row[0]

        self.conn.execute(
            """INSERT INTO agent_memory (id, agent_name, market, level, content, source_dates, tags, status, expires_at, source_session_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'active', ?, ?)""",
            [next_id, agent_name, market, LEVEL_REFLECTION, content, source_date, tags, expires_at, session_id],
        )
        return next_id

    def add_lesson(
        self, agent_name: str, market: str, content: str, source_dates: str, parent_ids: str
    ) -> int:
        """Add a L2 lesson (compressed from L1 reflections)."""
        try:
            next_id = self.conn.execute("SELECT nextval('seq_agent_memory_id')").fetchone()[0]
        except Exception:
            row = self.conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM agent_memory").fetchone()
            next_id = row[0]

        self.conn.execute(
            """INSERT INTO agent_memory (id, agent_name, market, level, content, source_dates, parent_ids, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'active')""",
            [next_id, agent_name, market, LEVEL_LESSON, content, source_dates, parent_ids],
        )
        return next_id

    def update_strategy(self, agent_name: str, market: str, content: str, source_dates: str) -> int:
        """Update or create the L3 strategy memo (only one active per agent)."""
        # Archive existing strategy
        self.conn.execute(
            "UPDATE agent_memory SET status = 'archived' WHERE agent_name = ? AND market = ? AND level = ? AND status = 'active'",
            [agent_name, market, LEVEL_STRATEGY],
        )
        try:
            next_id = self.conn.execute("SELECT nextval('seq_agent_memory_id')").fetchone()[0]
        except Exception:
            row = self.conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM agent_memory").fetchone()
            next_id = row[0]

        self.conn.execute(
            """INSERT INTO agent_memory (id, agent_name, market, level, content, source_dates, status)
               VALUES (?, ?, ?, ?, ?, ?, 'active')""",
            [next_id, agent_name, market, LEVEL_STRATEGY, content, source_dates],
        )
        return next_id

    def get_active_memories(self, agent_name: str, market: str) -> dict:
        """Get all active memories for injection into system prompt.

        Returns dict with keys: strategy (str|None), lessons (list[dict]), reflections (list[dict])
        """
        # Auto-archive expired L1 (skip on read-only connections)
        try:
            self.conn.execute(
                "UPDATE agent_memory SET status = 'archived' WHERE level = ? AND status = 'active' AND expires_at < CURRENT_TIMESTAMP",
                [LEVEL_REFLECTION],
            )
        except Exception:
            pass

        rows = self.conn.execute(
            """SELECT id, level, content, source_dates, tags, created_at
               FROM agent_memory
               WHERE agent_name = ? AND market = ? AND status = 'active'
               ORDER BY level, created_at DESC""",
            [agent_name, market],
        ).fetchall()

        result = {"strategy": None, "lessons": [], "reflections": []}
        for row in rows:
            entry = {
                "id": row[0],
                "content": row[2],
                "source_dates": row[3],
                "tags": row[4],
                "created_at": str(row[5]) if row[5] else None,
            }
            if row[1] == LEVEL_STRATEGY:
                result["strategy"] = entry
            elif row[1] == LEVEL_LESSON:
                result["lessons"].append(entry)
            elif row[1] == LEVEL_REFLECTION:
                result["reflections"].append(entry)

        # Limit reflections to most recent 2
        result["reflections"] = result["reflections"][:2]
        # Limit lessons to most recent 4
        result["lessons"] = result["lessons"][:4]

        return result

    def get_all_memories(self, agent_name: str, market: str, include_archived: bool = False) -> list:
        """Get all memories for an agent (for API/frontend display)."""
        status_filter = "" if include_archived else "AND status = 'active'"
        rows = self.conn.execute(
            f"""SELECT id, level, content, source_dates, tags, status, created_at, expires_at, parent_ids
               FROM agent_memory
               WHERE agent_name = ? AND market = ? {status_filter}
               ORDER BY created_at DESC""",
            [agent_name, market],
        ).fetchall()

        return [
            {
                "id": row[0],
                "level": row[1],
                "content": row[2],
                "source_dates": row[3],
                "tags": row[4],
                "status": row[5],
                "created_at": str(row[6]) if row[6] else None,
                "expires_at": str(row[7]) if row[7] else None,
                "parent_ids": row[8],
            }
            for row in rows
        ]

    def get_memory_stats(self, agent_name: str, market: str) -> dict:
        """Get memory statistics for an agent."""
        rows = self.conn.execute(
            """SELECT level, status, COUNT(*) as cnt
               FROM agent_memory
               WHERE agent_name = ? AND market = ?
               GROUP BY level, status""",
            [agent_name, market],
        ).fetchall()

        stats = {
            "reflection": {"active": 0, "archived": 0},
            "lesson": {"active": 0, "archived": 0},
            "strategy": {"active": 0, "archived": 0},
        }
        for row in rows:
            if row[0] in stats and row[1] in stats.get(row[0], {}):
                stats[row[0]][row[1]] = row[2]
        return stats

    def should_consolidate_l1(self, agent_name: str, market: str) -> bool:
        """Check if L1->L2 consolidation should trigger."""
        row = self.conn.execute(
            "SELECT COUNT(*) FROM agent_memory WHERE agent_name = ? AND market = ? AND level = ? AND status = 'active'",
            [agent_name, market, LEVEL_REFLECTION],
        ).fetchone()
        return row[0] >= L1_CONSOLIDATE_THRESHOLD

    def should_consolidate_l2(self, agent_name: str, market: str) -> bool:
        """Check if L2->L3 consolidation should trigger."""
        row = self.conn.execute(
            "SELECT COUNT(*) FROM agent_memory WHERE agent_name = ? AND market = ? AND level = ? AND status = 'active'",
            [agent_name, market, LEVEL_LESSON],
        ).fetchone()
        return row[0] >= L2_CONSOLIDATE_THRESHOLD

    def archive_memories(self, ids: list[int]):
        """Archive specific memories by ID."""
        if not ids:
            return
        placeholders = ",".join(["?"] * len(ids))
        self.conn.execute(
            f"UPDATE agent_memory SET status = 'archived' WHERE id IN ({placeholders})",
            ids,
        )

    def delete_memory(self, memory_id: int) -> bool:
        """Delete a memory by ID."""
        self.conn.execute("DELETE FROM agent_memory WHERE id = ?", [memory_id])
        return True
