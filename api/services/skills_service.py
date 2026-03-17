"""Skills 技能服务

管理 agent 的技能分配（DuckDB 存储）。
"""

import logging
from typing import List, Optional

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


def get_agent_skills(agent_name: str, market: str = "cn") -> List[str]:
    """Get active skill IDs for an agent."""
    try:
        conn = duckdb.connect(str(get_database_path()), read_only=False)
        init_skills_table(conn)
        rows = conn.execute(
            "SELECT skill_id FROM agent_skills WHERE agent_name = ? AND market = ? AND enabled = TRUE",
            [agent_name, market],
        ).fetchall()
        conn.close()
        return [r[0] for r in rows]
    except Exception as e:
        logger.debug("get_agent_skills failed: %s", e)
        return []


def set_agent_skills(agent_name: str, market: str, skill_ids: List[str]):
    """Set active skills for an agent (replaces all existing)."""
    conn = duckdb.connect(str(get_database_path()), read_only=False)
    try:
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
    except Exception as e:
        logger.error("Failed to set agent skills for %s: %s", agent_name, e)
        raise
    finally:
        conn.close()


def add_agent_skill(agent_name: str, market: str, skill_id: str):
    """Enable a single skill for an agent."""
    conn = duckdb.connect(str(get_database_path()), read_only=False)
    try:
        conn.execute(
            """INSERT INTO agent_skills (agent_name, market, skill_id)
               VALUES (?, ?, ?)
               ON CONFLICT (agent_name, market, skill_id)
               DO UPDATE SET enabled = TRUE, updated_at = CURRENT_TIMESTAMP""",
            [agent_name, market, skill_id],
        )
    finally:
        conn.close()


def remove_agent_skill(agent_name: str, market: str, skill_id: str):
    """Disable a skill for an agent."""
    conn = duckdb.connect(str(get_database_path()), read_only=False)
    try:
        conn.execute(
            "DELETE FROM agent_skills WHERE agent_name = ? AND market = ? AND skill_id = ?",
            [agent_name, market, skill_id],
        )
    finally:
        conn.close()
