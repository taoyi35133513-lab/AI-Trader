"""
数据库表定义

定义所有表的 DDL 语句和相关操作。
"""

import logging
from typing import Dict

from .connection import get_connection

logger = logging.getLogger(__name__)

# 表定义
TABLE_DEFINITIONS: Dict[str, str] = {
    "stock_daily_prices": """
        CREATE TABLE IF NOT EXISTS stock_daily_prices (
            ts_code VARCHAR(20) NOT NULL,
            trade_date DATE NOT NULL,
            open DECIMAL(10, 4),
            high DECIMAL(10, 4),
            low DECIMAL(10, 4),
            close DECIMAL(10, 4),
            volume BIGINT,
            amount DECIMAL(20, 4),
            market VARCHAR(10) DEFAULT 'cn',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ts_code, trade_date)
        )
    """,

    "stock_hourly_prices": """
        CREATE TABLE IF NOT EXISTS stock_hourly_prices (
            ts_code VARCHAR(20) NOT NULL,
            trade_time TIMESTAMP NOT NULL,
            open DECIMAL(10, 4),
            high DECIMAL(10, 4),
            low DECIMAL(10, 4),
            close DECIMAL(10, 4),
            volume BIGINT,
            market VARCHAR(10) DEFAULT 'cn',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ts_code, trade_time)
        )
    """,

    "index_weights": """
        CREATE TABLE IF NOT EXISTS index_weights (
            index_code VARCHAR(20) NOT NULL,
            con_code VARCHAR(20) NOT NULL,
            stock_name VARCHAR(50),
            weight DECIMAL(10, 4),
            trade_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (index_code, con_code, trade_date)
        )
    """,

    "positions": """
        CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY,
            agent_name VARCHAR(50) NOT NULL,
            market VARCHAR(10) NOT NULL,
            trade_date DATE NOT NULL,
            step_id INTEGER,
            ts_code VARCHAR(20),
            quantity INTEGER,
            cash DECIMAL(20, 4),
            action VARCHAR(10),
            action_amount INTEGER,
            price DECIMAL(10, 4),
            total_value DECIMAL(20, 4),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,

    "trade_logs": """
        CREATE TABLE IF NOT EXISTS trade_logs (
            id INTEGER PRIMARY KEY,
            agent_name VARCHAR(50) NOT NULL,
            market VARCHAR(10) NOT NULL,
            trade_date DATE NOT NULL,
            log_type VARCHAR(20),
            log_content TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
}

# 索引定义
INDEX_DEFINITIONS: Dict[str, list] = {
    "stock_daily_prices": [
        "CREATE INDEX IF NOT EXISTS idx_daily_ts_code ON stock_daily_prices(ts_code)",
        "CREATE INDEX IF NOT EXISTS idx_daily_trade_date ON stock_daily_prices(trade_date)",
        "CREATE INDEX IF NOT EXISTS idx_daily_market ON stock_daily_prices(market)",
    ],
    "stock_hourly_prices": [
        "CREATE INDEX IF NOT EXISTS idx_hourly_ts_code ON stock_hourly_prices(ts_code)",
        "CREATE INDEX IF NOT EXISTS idx_hourly_trade_time ON stock_hourly_prices(trade_time)",
    ],
    "index_weights": [
        "CREATE INDEX IF NOT EXISTS idx_weights_index_code ON index_weights(index_code)",
        "CREATE INDEX IF NOT EXISTS idx_weights_trade_date ON index_weights(trade_date)",
    ],
    "positions": [
        "CREATE INDEX IF NOT EXISTS idx_positions_agent ON positions(agent_name)",
        "CREATE INDEX IF NOT EXISTS idx_positions_date ON positions(trade_date)",
    ],
    "trade_logs": [
        "CREATE INDEX IF NOT EXISTS idx_logs_agent ON trade_logs(agent_name)",
        "CREATE INDEX IF NOT EXISTS idx_logs_date ON trade_logs(trade_date)",
    ],
}


def create_table(table_name: str) -> bool:
    """创建单个表

    Args:
        table_name: 表名

    Returns:
        True 如果创建成功
    """
    if table_name not in TABLE_DEFINITIONS:
        logger.error(f"未知的表名: {table_name}")
        return False

    conn = get_connection()

    try:
        # 创建表
        conn.execute(TABLE_DEFINITIONS[table_name])
        logger.info(f"表 {table_name} 创建成功")

        # 创建索引
        if table_name in INDEX_DEFINITIONS:
            for index_sql in INDEX_DEFINITIONS[table_name]:
                conn.execute(index_sql)
            logger.info(f"表 {table_name} 索引创建成功")

        return True

    except Exception as e:
        logger.error(f"创建表 {table_name} 失败: {e}")
        return False


def create_all_tables() -> bool:
    """创建所有表

    Returns:
        True 如果全部创建成功
    """
    success = True

    for table_name in TABLE_DEFINITIONS:
        if not create_table(table_name):
            success = False

    return success


def drop_table(table_name: str) -> bool:
    """删除单个表

    Args:
        table_name: 表名

    Returns:
        True 如果删除成功
    """
    conn = get_connection()

    try:
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        logger.info(f"表 {table_name} 已删除")
        return True
    except Exception as e:
        logger.error(f"删除表 {table_name} 失败: {e}")
        return False


def drop_all_tables() -> bool:
    """删除所有表

    Returns:
        True 如果全部删除成功
    """
    success = True

    for table_name in TABLE_DEFINITIONS:
        if not drop_table(table_name):
            success = False

    return success


def get_table_schema(table_name: str) -> list:
    """获取表结构信息

    Args:
        table_name: 表名

    Returns:
        列信息列表
    """
    conn = get_connection()

    result = conn.execute(f"DESCRIBE {table_name}").fetchall()
    return result


def show_all_schemas():
    """打印所有表结构"""
    conn = get_connection()

    tables = conn.execute("SHOW TABLES").fetchall()

    for (table_name,) in tables:
        print(f"\n=== {table_name} ===")
        schema = get_table_schema(table_name)
        for col in schema:
            print(f"  {col[0]}: {col[1]}")
