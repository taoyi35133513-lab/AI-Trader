"""
Repair DuckDB positions by re-importing from JSONL source of truth.

For a given agent_name (signature), this script:
1. Reads all records from the agent's position.jsonl
2. Deletes all existing DuckDB position records for that agent
3. Re-inserts all records from JSONL

Usage:
    python tools/repair_duckdb_positions.py                                    # Repair deepseek-chat-v3.2-live
    python tools/repair_duckdb_positions.py --agent deepseek-chat-v3.2-live
    python tools/repair_duckdb_positions.py --agent deepseek-chat-v3.2-live-astock-hour --log-path ./data/agent_data_astock_hour
    python tools/repair_duckdb_positions.py --dry-run                          # Preview without writing
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path

# Ensure project root is on path
project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def read_jsonl_positions(position_file: str) -> list:
    """Read all position records from a JSONL file.

    Returns:
        List of dicts with keys: date, id, this_action (optional), positions
    """
    records = []
    with open(position_file, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                records.append(record)
            except json.JSONDecodeError as e:
                logger.warning("Skipping invalid JSON at line %d: %s", line_no, e)
    return records


def repair_agent_positions(
    agent_name: str,
    log_path: str = "./data/agent_data_astock",
    dry_run: bool = False,
) -> None:
    """Delete and re-import DuckDB positions for a given agent from JSONL.

    Args:
        agent_name: Agent signature (e.g., 'deepseek-chat-v3.2-live')
        log_path: Base log path containing agent directories
        dry_run: If True, only preview changes without writing
    """
    position_file = os.path.join(log_path, agent_name, "position", "position.jsonl")

    if not os.path.exists(position_file):
        logger.error("Position file not found: %s", position_file)
        return

    # Read JSONL records
    records = read_jsonl_positions(position_file)
    logger.info("Read %d records from %s", len(records), position_file)

    if not records:
        logger.warning("No records to import")
        return

    # Preview records
    for i, rec in enumerate(records):
        date = rec.get("date", "?")
        action = rec.get("this_action", {}).get("action", "init")
        positions = rec.get("positions", {})
        cash = positions.get("CASH", 0)
        stocks = {k: v for k, v in positions.items() if k != "CASH" and v > 0}
        logger.info(
            "  Record %d: date=%s, action=%s, CASH=%.2f, stocks=%d held",
            i, date, action, cash, len(stocks),
        )

    if dry_run:
        logger.info("[DRY RUN] Would delete all DuckDB positions for '%s' and re-insert %d records", agent_name, len(records))
        return

    # Import database tools
    from data.database.connection import DatabaseManager
    from tools import duckdb_queries as dq

    with DatabaseManager(read_only=False) as db:
        # Step 1: Count existing records
        count_df = db.query(
            "SELECT COUNT(*) as cnt FROM positions WHERE agent_name = ?",
            (agent_name,),
        )
        existing_count = int(count_df.iloc[0]["cnt"])
        logger.info("DuckDB has %d existing position rows for '%s'", existing_count, agent_name)

        # Step 2: Delete all existing records for this agent
        db.execute("DELETE FROM positions WHERE agent_name = ?", (agent_name,))
        logger.info("Deleted %d rows from DuckDB for '%s'", existing_count, agent_name)

        # Step 3: Re-insert each record from JSONL
        for i, rec in enumerate(records):
            date = rec.get("date", "")
            positions = rec.get("positions", {})
            action = rec.get("this_action", {"action": "init", "symbol": "", "amount": 0})

            dq.insert_position_record(db, agent_name, date, action, positions)
            logger.info("Inserted record %d/%d: date=%s, action=%s", i + 1, len(records), date, action.get("action", "?"))

    logger.info("Repair complete: %d records imported for '%s'", len(records), agent_name)


def main():
    parser = argparse.ArgumentParser(description="Repair DuckDB positions from JSONL")
    parser.add_argument(
        "--agent",
        default="deepseek-chat-v3.2-live",
        help="Agent signature to repair (default: deepseek-chat-v3.2-live)",
    )
    parser.add_argument(
        "--log-path",
        default="./data/agent_data_astock",
        help="Base log path (default: ./data/agent_data_astock)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without writing to DuckDB",
    )
    args = parser.parse_args()

    # Resolve relative path from project root
    log_path = args.log_path
    if not os.path.isabs(log_path):
        log_path = os.path.join(str(project_root), log_path)

    repair_agent_positions(
        agent_name=args.agent,
        log_path=log_path,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
