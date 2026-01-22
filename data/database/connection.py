"""
DuckDB 数据库连接管理

提供单例连接、上下文管理器和常用查询方法。
"""

import logging
import os
from pathlib import Path
from typing import Optional, Union

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

# 默认数据库路径
DEFAULT_DB_PATH = Path(__file__).parent / "ai_trader.duckdb"

# 全局连接实例
_connection: Optional[duckdb.DuckDBPyConnection] = None


def get_connection(db_path: Union[str, Path, None] = None) -> duckdb.DuckDBPyConnection:
    """获取数据库连接（单例模式）

    Args:
        db_path: 数据库文件路径，None 使用默认路径

    Returns:
        DuckDB 连接对象
    """
    global _connection

    if _connection is not None:
        return _connection

    if db_path is None:
        db_path = DEFAULT_DB_PATH

    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"连接数据库: {db_path}")
    _connection = duckdb.connect(str(db_path))

    return _connection


def close_connection():
    """关闭数据库连接"""
    global _connection

    if _connection is not None:
        _connection.close()
        _connection = None
        logger.info("数据库连接已关闭")


class DatabaseManager:
    """数据库管理器

    提供上下文管理和常用查询方法。

    用法:
        with DatabaseManager() as db:
            df = db.query("SELECT * FROM stock_daily_prices LIMIT 10")
            db.execute("INSERT INTO ...")
    """

    def __init__(self, db_path: Union[str, Path, None] = None):
        """初始化数据库管理器

        Args:
            db_path: 数据库文件路径，None 使用默认路径
        """
        self.db_path = db_path or DEFAULT_DB_PATH
        self.conn: Optional[duckdb.DuckDBPyConnection] = None

    def __enter__(self):
        self.conn = get_connection(self.db_path)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 不关闭连接，保持单例
        pass

    def query(self, sql: str, params: tuple = None) -> pd.DataFrame:
        """执行查询并返回 DataFrame

        Args:
            sql: SQL 查询语句
            params: 查询参数

        Returns:
            查询结果 DataFrame
        """
        if self.conn is None:
            self.conn = get_connection(self.db_path)

        if params:
            return self.conn.execute(sql, params).df()
        return self.conn.execute(sql).df()

    def execute(self, sql: str, params: tuple = None):
        """执行 SQL 语句（不返回结果）

        Args:
            sql: SQL 语句
            params: 参数
        """
        if self.conn is None:
            self.conn = get_connection(self.db_path)

        if params:
            self.conn.execute(sql, params)
        else:
            self.conn.execute(sql)

    def executemany(self, sql: str, params_list: list):
        """批量执行 SQL 语句

        Args:
            sql: SQL 语句（带占位符）
            params_list: 参数列表
        """
        if self.conn is None:
            self.conn = get_connection(self.db_path)

        self.conn.executemany(sql, params_list)

    def insert_df(self, table_name: str, df: pd.DataFrame, if_exists: str = "append"):
        """将 DataFrame 插入表

        Args:
            table_name: 表名
            df: 要插入的 DataFrame
            if_exists: 处理已存在数据的方式
                - 'append': 追加数据
                - 'replace': 替换整个表
                - 'ignore': 忽略重复
        """
        if self.conn is None:
            self.conn = get_connection(self.db_path)

        if df.empty:
            logger.warning(f"空 DataFrame，跳过插入 {table_name}")
            return

        if if_exists == "replace":
            self.conn.execute(f"DELETE FROM {table_name}")

        # 使用 DuckDB 的 DataFrame 注册功能
        self.conn.register("temp_df", df)

        if if_exists == "ignore":
            # 使用 INSERT OR IGNORE 语义
            columns = ", ".join(df.columns)
            self.conn.execute(f"""
                INSERT OR IGNORE INTO {table_name} ({columns})
                SELECT {columns} FROM temp_df
            """)
        else:
            columns = ", ".join(df.columns)
            self.conn.execute(f"""
                INSERT INTO {table_name} ({columns})
                SELECT {columns} FROM temp_df
            """)

        self.conn.unregister("temp_df")
        logger.info(f"已插入 {len(df)} 条记录到 {table_name}")

    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在

        Args:
            table_name: 表名

        Returns:
            True 如果表存在
        """
        if self.conn is None:
            self.conn = get_connection(self.db_path)

        result = self.conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            (table_name,)
        ).fetchone()
        return result[0] > 0

    def get_table_count(self, table_name: str) -> int:
        """获取表中的记录数

        Args:
            table_name: 表名

        Returns:
            记录数
        """
        if self.conn is None:
            self.conn = get_connection(self.db_path)

        result = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        return result[0]

    def show_tables(self) -> list:
        """显示所有表

        Returns:
            表名列表
        """
        if self.conn is None:
            self.conn = get_connection(self.db_path)

        result = self.conn.execute("SHOW TABLES").fetchall()
        return [row[0] for row in result]


# 便捷函数
def query(sql: str, params: tuple = None) -> pd.DataFrame:
    """执行查询并返回 DataFrame（便捷函数）

    Args:
        sql: SQL 查询语句
        params: 查询参数

    Returns:
        查询结果 DataFrame
    """
    with DatabaseManager() as db:
        return db.query(sql, params)


def execute(sql: str, params: tuple = None):
    """执行 SQL 语句（便捷函数）

    Args:
        sql: SQL 语句
        params: 参数
    """
    with DatabaseManager() as db:
        db.execute(sql, params)
