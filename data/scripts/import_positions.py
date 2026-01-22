"""
Position data import script.

Imports historical position data from JSONL files to DuckDB.
This script scans the agent_data directories and imports all position records.

Usage:
    python -m data.scripts.import_positions
    python -m data.scripts.import_positions --agent gpt-5
    python -m data.scripts.import_positions --dry-run
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Add project root to path
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from data.database.connection import DatabaseManager
from data.database.models import create_table

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def find_position_files(base_path: Path, agent_filter: Optional[str] = None) -> List[Tuple[str, Path]]:
    """Find all position.jsonl files in agent data directories.

    Args:
        base_path: Base path to search (e.g., data/agent_data_astock)
        agent_filter: If set, only import this agent's data

    Returns:
        List of (agent_name, file_path) tuples
    """
    position_files = []

    if not base_path.exists():
        logger.warning(f"Path does not exist: {base_path}")
        return position_files

    for agent_dir in base_path.iterdir():
        if not agent_dir.is_dir():
            continue

        agent_name = agent_dir.name

        if agent_filter and agent_name != agent_filter:
            continue

        position_file = agent_dir / "position" / "position.jsonl"
        if position_file.exists():
            position_files.append((agent_name, position_file))
            logger.info(f"Found position file: {position_file}")

    return position_files


def parse_position_record(record: dict, agent_name: str) -> List[dict]:
    """Parse a JSONL position record into database rows.

    Args:
        record: JSONL record with date, id, this_action, positions
        agent_name: Agent name

    Returns:
        List of database row dicts
    """
    rows = []

    trade_date = record.get("date")
    step_id = record.get("id", 0)
    action_info = record.get("this_action", {})
    positions = record.get("positions", {})

    if not trade_date:
        return rows

    # Extract cash
    cash = positions.get("CASH", 0.0)
    action = action_info.get("action", "")
    action_amount = action_info.get("amount", 0)

    # Create a row for each stock position
    has_stocks = False
    for symbol, quantity in positions.items():
        if symbol == "CASH":
            continue
        has_stocks = True
        rows.append({
            "agent_name": agent_name,
            "market": "cn",
            "trade_date": trade_date,
            "step_id": step_id,
            "ts_code": symbol,
            "quantity": int(quantity) if quantity else 0,
            "cash": float(cash),
            "action": action,
            "action_amount": int(action_amount) if action_amount else 0,
        })

    # If no stock positions, create a cash-only record
    if not has_stocks:
        rows.append({
            "agent_name": agent_name,
            "market": "cn",
            "trade_date": trade_date,
            "step_id": step_id,
            "ts_code": None,
            "quantity": 0,
            "cash": float(cash),
            "action": action,
            "action_amount": int(action_amount) if action_amount else 0,
        })

    return rows


def get_next_position_id(db: DatabaseManager) -> int:
    """Get the next available position ID.

    Args:
        db: Database manager

    Returns:
        Next available ID
    """
    result = db.query("SELECT COALESCE(MAX(id), 0) + 1 as next_id FROM positions")
    return int(result.iloc[0]["next_id"])


def import_position_file(
    db: DatabaseManager,
    agent_name: str,
    file_path: Path,
    dry_run: bool = False,
    start_id: int = 1
) -> Tuple[int, int, int]:
    """Import a single position.jsonl file.

    Args:
        db: Database manager
        agent_name: Agent name
        file_path: Path to position.jsonl
        dry_run: If True, don't actually write to database
        start_id: Starting ID for new records

    Returns:
        Tuple of (records_imported, rows_inserted, next_id)
    """
    records_imported = 0
    rows_inserted = 0
    current_id = start_id

    with open(file_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
                rows = parse_position_record(record, agent_name)

                if not rows:
                    continue

                records_imported += 1

                if not dry_run:
                    for row in rows:
                        sql = """
                            INSERT INTO positions
                            (id, agent_name, market, trade_date, step_id, ts_code, quantity, cash, action, action_amount)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """
                        try:
                            db.execute(sql, (
                                current_id,
                                row["agent_name"],
                                row["market"],
                                row["trade_date"],
                                row["step_id"],
                                row["ts_code"],
                                row["quantity"],
                                row["cash"],
                                row["action"],
                                row["action_amount"]
                            ))
                            rows_inserted += 1
                            current_id += 1
                        except Exception as e:
                            # Skip duplicate records
                            if "duplicate" in str(e).lower() or "unique" in str(e).lower():
                                logger.debug(f"Skipping duplicate: {row['trade_date']} step {row['step_id']}")
                            else:
                                logger.warning(f"Failed to insert row: {e}")
                else:
                    rows_inserted += len(rows)
                    current_id += len(rows)

            except json.JSONDecodeError as e:
                logger.warning(f"Invalid JSON at line {line_num} in {file_path}: {e}")
                continue

    return records_imported, rows_inserted, current_id


def import_all_positions(
    agent_filter: Optional[str] = None,
    dry_run: bool = False
) -> Dict[str, Tuple[int, int]]:
    """Import positions from all agent data directories.

    Args:
        agent_filter: If set, only import this agent's data
        dry_run: If True, don't actually write to database

    Returns:
        Dict mapping agent_name to (records, rows) tuple
    """
    results = {}

    # Define data directories to scan
    data_dirs = [
        project_root / "data" / "agent_data_astock",
        project_root / "data" / "agent_data_astock_hour",
    ]

    # Ensure positions table exists
    if not dry_run:
        create_table("positions")

    with DatabaseManager() as db:
        # Get the starting ID for new records
        if not dry_run:
            current_id = get_next_position_id(db)
        else:
            current_id = 1

        for data_dir in data_dirs:
            position_files = find_position_files(data_dir, agent_filter)

            for agent_name, file_path in position_files:
                logger.info(f"Importing positions for {agent_name} from {file_path}")

                records, rows, current_id = import_position_file(
                    db, agent_name, file_path, dry_run, start_id=current_id
                )

                if agent_name in results:
                    prev_records, prev_rows = results[agent_name]
                    results[agent_name] = (prev_records + records, prev_rows + rows)
                else:
                    results[agent_name] = (records, rows)

                logger.info(f"  Imported {records} records ({rows} rows)")

    return results


def main():
    parser = argparse.ArgumentParser(description="Import position data from JSONL to DuckDB")
    parser.add_argument("--agent", type=str, help="Only import specific agent's data")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to database")
    args = parser.parse_args()

    logger.info("Starting position data import...")
    if args.dry_run:
        logger.info("DRY RUN MODE - no data will be written")

    results = import_all_positions(
        agent_filter=args.agent,
        dry_run=args.dry_run
    )

    # Print summary
    logger.info("\n=== Import Summary ===")
    total_records = 0
    total_rows = 0

    for agent_name, (records, rows) in sorted(results.items()):
        logger.info(f"  {agent_name}: {records} records, {rows} rows")
        total_records += records
        total_rows += rows

    logger.info(f"\nTotal: {total_records} records, {total_rows} rows imported")

    if args.dry_run:
        logger.info("(Dry run - no data was actually written)")


if __name__ == "__main__":
    main()
