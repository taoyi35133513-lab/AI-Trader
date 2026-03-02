"""Shared pytest fixtures."""

import os
import sys

import pytest

# Ensure project root is on sys.path so that imports work
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


@pytest.fixture
def tmp_duckdb(tmp_path):
    """Yield a temporary DuckDB DatabaseManager with the positions table."""
    from data.database.connection import DatabaseManager
    from data.database.models import TABLE_DEFINITIONS, INDEX_DEFINITIONS

    db_path = tmp_path / "test.duckdb"
    with DatabaseManager(db_path=db_path, read_only=False) as db:
        # Create positions table + indexes
        db.execute(TABLE_DEFINITIONS["positions"])
        for idx_sql in INDEX_DEFINITIONS.get("positions", []):
            db.execute(idx_sql)
        yield db
