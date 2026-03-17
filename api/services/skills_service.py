"""Skills 技能服务

管理 agent 的技能分配（DuckDB 存储）。
"""

import logging
from contextlib import contextmanager
from typing import List

import duckdb

from api.config import get_database_path

logger = logging.getLogger(__name__)


def init_skills_table(conn: duckdb.DuckDBPyConnection):
    """Create agent_skills table if not exists."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS agent_skills (
            agent_name VARCHAR NOT NULL,
            market VARCHAR NOT NULL DEFAULT 'cn',
            skill_id VARCHAR NOT NULL,
            enabled BOOLEAN DEFAULT TRUE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (agent_name, market, skill_id)
        )
    """)


@contextmanager
def _get_conn():
    """Context manager for DuckDB connection with auto-close."""
    conn = duckdb.connect(str(get_database_path()), read_only=False)
    try:
        yield conn
    finally:
        conn.close()


def get_agent_skills(agent_name: str, market: str = "cn") -> List[str]:
    """Get active skill IDs for an agent."""
    try:
        with _get_conn() as conn:
            init_skills_table(conn)
            rows = conn.execute(
                "SELECT skill_id FROM agent_skills WHERE agent_name = ? AND market = ? AND enabled = TRUE",
                [agent_name, market],
            ).fetchall()
            return [r[0] for r in rows]
    except Exception as e:
        logger.debug("get_agent_skills failed: %s", e)
        return []


def set_agent_skills(agent_name: str, market: str, skill_ids: List[str]):
    """Set active skills for an agent (replaces all existing)."""
    with _get_conn() as conn:
        init_skills_table(conn)
        conn.execute(
            "DELETE FROM agent_skills WHERE agent_name = ? AND market = ?",
            [agent_name, market],
        )
        for sid in skill_ids:
            conn.execute(
                "INSERT INTO agent_skills (agent_name, market, skill_id) VALUES (?, ?, ?)",
                [agent_name, market, sid],
            )
        logger.info("Set %d skills for %s (%s): %s", len(skill_ids), agent_name, market, skill_ids)


def add_agent_skill(agent_name: str, market: str, skill_id: str):
    """Enable a single skill for an agent."""
    with _get_conn() as conn:
        init_skills_table(conn)
        conn.execute(
            "DELETE FROM agent_skills WHERE agent_name = ? AND market = ? AND skill_id = ?",
            [agent_name, market, skill_id],
        )
        conn.execute(
            "INSERT INTO agent_skills (agent_name, market, skill_id) VALUES (?, ?, ?)",
            [agent_name, market, skill_id],
        )


def remove_agent_skill(agent_name: str, market: str, skill_id: str):
    """Disable a skill for an agent."""
    with _get_conn() as conn:
        conn.execute(
            "DELETE FROM agent_skills WHERE agent_name = ? AND market = ? AND skill_id = ?",
            [agent_name, market, skill_id],
        )
