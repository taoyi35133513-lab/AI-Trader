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

    "index_daily_prices": """
        CREATE TABLE IF NOT EXISTS index_daily_prices (
            index_code VARCHAR(20) NOT NULL,
            trade_date DATE NOT NULL,
            open DECIMAL(10, 4),
            high DECIMAL(10, 4),
            low DECIMAL(10, 4),
            close DECIMAL(10, 4),
            volume BIGINT,
            amount DECIMAL(20, 4),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (index_code, trade_date)
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

    # ===== 新增表：交易会话和对话消息 =====

    "agent_trading_sessions": """
        CREATE TABLE IF NOT EXISTS agent_trading_sessions (
            id INTEGER PRIMARY KEY,
            agent_name VARCHAR(50) NOT NULL,
            market VARCHAR(10) NOT NULL,
            session_date DATE NOT NULL,
            session_time TIME,
            session_timestamp TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (agent_name, session_timestamp)
        )
    """,

    "agent_conversation_messages": """
        CREATE TABLE IF NOT EXISTS agent_conversation_messages (
            id INTEGER PRIMARY KEY,
            session_id INTEGER NOT NULL,
            message_sequence INTEGER NOT NULL,
            role VARCHAR(20) NOT NULL,
            content TEXT,
            tool_call_id VARCHAR(100),
            tool_name VARCHAR(100),
            timestamp TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (session_id) REFERENCES agent_trading_sessions(id)
        )
    """,

    "agent_positions_history": """
        CREATE TABLE IF NOT EXISTS agent_positions_history (
            id INTEGER PRIMARY KEY,
            session_id INTEGER,
            agent_name VARCHAR(50) NOT NULL,
            market VARCHAR(10) NOT NULL,
            position_date DATE NOT NULL,
            position_time TIMESTAMP,
            step_id INTEGER NOT NULL,
            action VARCHAR(20),
            action_symbol VARCHAR(20),
            action_amount INTEGER,
            action_price DECIMAL(10, 4),
            cash DECIMAL(20, 4) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (session_id) REFERENCES agent_trading_sessions(id)
        )
    """,

    "agent_position_holdings": """
        CREATE TABLE IF NOT EXISTS agent_position_holdings (
            id INTEGER PRIMARY KEY,
            position_history_id INTEGER NOT NULL,
            ts_code VARCHAR(20) NOT NULL,
            quantity INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (position_history_id) REFERENCES agent_positions_history(id)
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
    "index_daily_prices": [
        "CREATE INDEX IF NOT EXISTS idx_index_daily_code ON index_daily_prices(index_code)",
        "CREATE INDEX IF NOT EXISTS idx_index_daily_date ON index_daily_prices(trade_date)",
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

    # ===== 新增表的索引 =====

    "agent_trading_sessions": [
        "CREATE INDEX IF NOT EXISTS idx_sessions_agent ON agent_trading_sessions(agent_name)",
        "CREATE INDEX IF NOT EXISTS idx_sessions_date ON agent_trading_sessions(session_date)",
        "CREATE INDEX IF NOT EXISTS idx_sessions_timestamp ON agent_trading_sessions(session_timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_sessions_market ON agent_trading_sessions(market)",
    ],

    "agent_conversation_messages": [
        "CREATE INDEX IF NOT EXISTS idx_messages_session ON agent_conversation_messages(session_id)",
        "CREATE INDEX IF NOT EXISTS idx_messages_role ON agent_conversation_messages(role)",
        "CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON agent_conversation_messages(timestamp)",
    ],

    "agent_positions_history": [
        "CREATE INDEX IF NOT EXISTS idx_pos_hist_session ON agent_positions_history(session_id)",
        "CREATE INDEX IF NOT EXISTS idx_pos_hist_agent ON agent_positions_history(agent_name)",
        "CREATE INDEX IF NOT EXISTS idx_pos_hist_date ON agent_positions_history(position_date)",
        "CREATE INDEX IF NOT EXISTS idx_pos_hist_market ON agent_positions_history(market)",
        # 复合索引用于迁移时的唯一性检查和查询优化
        "CREATE INDEX IF NOT EXISTS idx_pos_hist_agent_date_step ON agent_positions_history(agent_name, position_date, step_id)",
    ],

    "agent_position_holdings": [
        "CREATE INDEX IF NOT EXISTS idx_holdings_pos ON agent_position_holdings(position_history_id)",
        "CREATE INDEX IF NOT EXISTS idx_holdings_symbol ON agent_position_holdings(ts_code)",
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
