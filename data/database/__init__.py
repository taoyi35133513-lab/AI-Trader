"""
AI-Trader 数据库模块

使用 DuckDB 作为本地数据库，提供高效的数据存储和查询能力。
"""

from .connection import get_connection, close_connection, DatabaseManager
from .models import (
    create_all_tables,
    drop_all_tables,
    TABLE_DEFINITIONS,
)

__all__ = [
    "get_connection",
    "close_connection",
    "DatabaseManager",
    "create_all_tables",
    "drop_all_tables",
    "TABLE_DEFINITIONS",
]
