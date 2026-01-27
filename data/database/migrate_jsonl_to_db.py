#!/usr/bin/env python
"""
JSONL 数据迁移脚本

将 log.jsonl 和 position.jsonl 数据导入到 DuckDB。

用法:
    python -m data.database.migrate_jsonl_to_db --market cn
    python -m data.database.migrate_jsonl_to_db --market cn_hour
    python -m data.database.migrate_jsonl_to_db --agent gemini-2.5-flash
    python -m data.database.migrate_jsonl_to_db --dry-run
    python -m data.database.migrate_jsonl_to_db --logs-only
    python -m data.database.migrate_jsonl_to_db --positions-only
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Add project root to path
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from data.database.connection import DatabaseManager
from data.database.models import create_all_tables

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def get_data_dir(market: str) -> Path:
    """获取数据目录"""
    if market == "cn_hour":
        return project_root / "data" / "agent_data_astock_hour"
    else:
        return project_root / "data" / "agent_data_astock"


def parse_timestamp(date_str: str) -> Tuple[datetime, Optional[str]]:
    """解析时间戳

    Args:
        date_str: 日期字符串 (YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS)

    Returns:
        (datetime 对象, 时间字符串 或 None)
    """
    if " " in date_str:
        dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        return dt, dt.strftime("%H:%M:%S")
    else:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt, None


def migrate_agent_logs(
    conn,
    agent_path: Path,
    agent_name: str,
    market: str,
    dry_run: bool = False,
) -> int:
    """迁移单个 Agent 的日志数据

    Args:
        conn: DuckDB 连接
        agent_path: Agent 数据目录路径
        agent_name: Agent 名称
        market: 市场
        dry_run: 是否只做预演

    Returns:
        迁移的会话数量
    """
    log_dir = agent_path / "log"
    if not log_dir.exists():
        return 0

    session_count = 0

    for date_dir in sorted(log_dir.iterdir()):
        if not date_dir.is_dir():
            continue

        log_file = date_dir / "log.jsonl"
        if not log_file.exists():
            continue

        # 解析日期/时间戳从目录名
        date_str = date_dir.name.replace("-", ":")  # 还原 Windows 兼容的目录名
        # 尝试解析日期
        try:
            if len(date_str) > 10:  # 包含时间
                # 格式: YYYY-MM-DD HH:MM:SS -> 可能是 YYYY-MM-DD HH-MM-SS
                parts = date_str.split(" ", 1)
                if len(parts) == 2:
                    session_timestamp = datetime.strptime(parts[0] + " " + parts[1], "%Y-%m-%d %H:%M:%S")
                else:
                    session_timestamp = datetime.strptime(date_str[:10], "%Y-%m-%d")
            else:
                session_timestamp = datetime.strptime(date_str[:10], "%Y-%m-%d")
        except ValueError:
            logger.warning(f"Cannot parse date from directory: {date_dir.name}")
            continue

        session_date = session_timestamp.date()
        session_time = session_timestamp.time() if market == "cn_hour" else None

        if dry_run:
            # 统计消息数
            msg_count = 0
            with open(log_file, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        entry = json.loads(line)
                        msgs = entry.get("new_messages", [])
                        if isinstance(msgs, dict):
                            msgs = [msgs]
                        msg_count += len(msgs)
            logger.info(f"  [DRY-RUN] Session {date_dir.name}: {msg_count} messages")
            session_count += 1
            continue

        # 创建会话
        try:
            # 检查会话是否已存在
            check_sql = """
                SELECT id FROM agent_trading_sessions
                WHERE agent_name = ? AND session_timestamp = ?
            """
            existing = conn.execute(check_sql, [agent_name, session_timestamp]).fetchone()
            if existing:
                logger.debug(f"Session already exists for {agent_name} at {session_timestamp}")
                session_count += 1
                continue

            # 插入会话
            insert_session_sql = """
                INSERT INTO agent_trading_sessions
                (agent_name, market, session_date, session_time, session_timestamp)
                VALUES (?, ?, ?, ?, ?)
                RETURNING id
            """
            result = conn.execute(
                insert_session_sql,
                [agent_name, market, session_date, session_time, session_timestamp],
            ).fetchone()
            session_id = result[0]

            # 读取并插入消息
            message_seq = 0
            with open(log_file, "r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue

                    entry = json.loads(line)
                    entry_timestamp = datetime.fromisoformat(entry.get("timestamp", session_timestamp.isoformat()))
                    messages = entry.get("new_messages", [])

                    # 处理单个消息或消息列表
                    if isinstance(messages, dict):
                        messages = [messages]

                    for msg in messages:
                        message_seq += 1
                        role = msg.get("role", "unknown")
                        content = msg.get("content", "")
                        tool_call_id = msg.get("tool_call_id")
                        tool_name = msg.get("tool_name")

                        insert_msg_sql = """
                            INSERT INTO agent_conversation_messages
                            (session_id, message_sequence, role, content, tool_call_id, tool_name, timestamp)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """
                        conn.execute(
                            insert_msg_sql,
                            [session_id, message_seq, role, content, tool_call_id, tool_name, entry_timestamp],
                        )

            session_count += 1
            logger.debug(f"Migrated session {date_dir.name}: {message_seq} messages")

        except Exception as e:
            logger.error(f"Failed to migrate session {date_dir.name}: {e}")
            continue

    return session_count


def migrate_agent_positions(
    conn,
    agent_path: Path,
    agent_name: str,
    market: str,
    dry_run: bool = False,
) -> int:
    """迁移单个 Agent 的持仓数据

    Args:
        conn: DuckDB 连接
        agent_path: Agent 数据目录路径
        agent_name: Agent 名称
        market: 市场
        dry_run: 是否只做预演

    Returns:
        迁移的步骤数量
    """
    position_file = agent_path / "position" / "position.jsonl"
    if not position_file.exists():
        return 0

    step_count = 0

    with open(position_file, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue

            record = json.loads(line)
            date_str = record.get("date", "")
            step_id = record.get("id", 0)
            positions = record.get("positions", {})
            action_data = record.get("this_action", {})

            if dry_run:
                step_count += 1
                continue

            try:
                # 解析日期
                pos_timestamp, time_str = parse_timestamp(date_str)
                pos_date = pos_timestamp.date()

                # 检查是否已存在
                check_sql = """
                    SELECT id FROM agent_positions_history
                    WHERE agent_name = ? AND position_date = ? AND step_id = ?
                """
                existing = conn.execute(check_sql, [agent_name, pos_date, step_id]).fetchone()
                if existing:
                    logger.debug(f"Position already exists for {agent_name} at {pos_date} step {step_id}")
                    step_count += 1
                    continue

                # 提取动作数据
                action = action_data.get("action") if action_data else None
                action_symbol = action_data.get("symbol") if action_data else None
                action_amount = action_data.get("amount") if action_data else None
                action_price = action_data.get("price") if action_data else None

                # 提取现金和持仓
                cash = float(positions.get("CASH", 0))
                holdings = {k: v for k, v in positions.items() if k != "CASH" and v > 0}

                # 插入持仓历史
                insert_pos_sql = """
                    INSERT INTO agent_positions_history
                    (session_id, agent_name, market, position_date, position_time, step_id,
                     action, action_symbol, action_amount, action_price, cash)
                    VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    RETURNING id
                """
                result = conn.execute(
                    insert_pos_sql,
                    [
                        agent_name,
                        market,
                        pos_date,
                        pos_timestamp if time_str else None,
                        step_id,
                        action,
                        action_symbol,
                        action_amount,
                        action_price,
                        cash,
                    ],
                ).fetchone()
                pos_history_id = result[0]

                # 插入持仓明细
                if holdings:
                    insert_holding_sql = """
                        INSERT INTO agent_position_holdings
                        (position_history_id, ts_code, quantity)
                        VALUES (?, ?, ?)
                    """
                    for ts_code, quantity in holdings.items():
                        conn.execute(insert_holding_sql, [pos_history_id, ts_code, quantity])

                step_count += 1

            except Exception as e:
                logger.error(f"Failed to migrate position step {step_id}: {e}")
                continue

    return step_count


def main():
    parser = argparse.ArgumentParser(description="迁移 JSONL 数据到 DuckDB")
    parser.add_argument(
        "--market",
        default="cn",
        choices=["cn", "cn_hour"],
        help="市场类型 (cn=日线, cn_hour=小时线)",
    )
    parser.add_argument("--agent", help="指定 Agent 名称（可选，不指定则迁移所有）")
    parser.add_argument("--dry-run", action="store_true", help="预演模式，不实际写入数据")
    parser.add_argument("--logs-only", action="store_true", help="只迁移日志数据")
    parser.add_argument("--positions-only", action="store_true", help="只迁移持仓数据")
    parser.add_argument("-v", "--verbose", action="store_true", help="详细输出")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info(f"=== JSONL to DuckDB Migration ===")
    logger.info(f"Market: {args.market}")
    logger.info(f"Agent: {args.agent or 'ALL'}")
    logger.info(f"Dry run: {args.dry_run}")

    # 确保表已创建
    if not args.dry_run:
        logger.info("Creating tables if not exist...")
        create_all_tables()

    data_dir = get_data_dir(args.market)
    if not data_dir.exists():
        logger.error(f"Data directory not found: {data_dir}")
        return

    # 查找 Agent 目录
    agent_dirs = [d for d in data_dir.iterdir() if d.is_dir()]
    if args.agent:
        agent_dirs = [d for d in agent_dirs if d.name == args.agent]
        if not agent_dirs:
            logger.error(f"Agent not found: {args.agent}")
            return

    logger.info(f"Found {len(agent_dirs)} agents to migrate")

    with DatabaseManager() as db:
        total_sessions = 0
        total_positions = 0

        for agent_dir in agent_dirs:
            agent_name = agent_dir.name
            logger.info(f"\n--- Migrating {agent_name} ---")

            # 迁移日志
            if not args.positions_only:
                sessions = migrate_agent_logs(
                    db.conn, agent_dir, agent_name, args.market, args.dry_run
                )
                total_sessions += sessions
                logger.info(f"  Logs: {sessions} sessions")

            # 迁移持仓
            if not args.logs_only:
                positions = migrate_agent_positions(
                    db.conn, agent_dir, agent_name, args.market, args.dry_run
                )
                total_positions += positions
                logger.info(f"  Positions: {positions} steps")

    logger.info(f"\n=== Migration Complete ===")
    logger.info(f"Total sessions: {total_sessions}")
    logger.info(f"Total position steps: {total_positions}")

    if args.dry_run:
        logger.info("(This was a dry run - no data was written)")


if __name__ == "__main__":
    main()
