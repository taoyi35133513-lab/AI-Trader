"""
DuckDB 数据库连接管理

提供上下文管理器和常用查询方法。
支持多进程并发访问（每次创建独立连接）。
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


def get_connection(
    db_path: Union[str, Path, None] = None,
    read_only: bool = False
) -> duckdb.DuckDBPyConnection:
    """获取数据库连接（每次创建新连接，支持并发访问）

    Args:
        db_path: 数据库文件路径，None 使用默认路径
        read_only: 是否只读模式（只读模式支持多进程并发）

    Returns:
        DuckDB 连接对象
    """
    if db_path is None:
        db_path = DEFAULT_DB_PATH

    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"连接数据库: {db_path} (read_only={read_only})")
    return duckdb.connect(str(db_path), read_only=read_only)


def close_connection():
    """关闭数据库连接 - 保留用于兼容性"""
    pass


class DatabaseManager:
    """数据库管理器

    提供上下文管理和常用查询方法。
    每次使用时创建新连接，退出时自动关闭，支持多进程并发访问。

    用法:
        with DatabaseManager() as db:
            df = db.query("SELECT * FROM stock_daily_prices LIMIT 10")
            db.execute("INSERT INTO ...")

        # 只读模式（支持多进程并发读取）
        with DatabaseManager(read_only=True) as db:
            df = db.query("SELECT * FROM stock_daily_prices LIMIT 10")
    """

    def __init__(
        self,
        db_path: Union[str, Path, None] = None,
        read_only: bool = False
    ):
        """初始化数据库管理器

        Args:
            db_path: 数据库文件路径，None 使用默认路径
            read_only: 是否只读模式（只读模式支持多进程并发）
        """
        self.db_path = db_path or DEFAULT_DB_PATH
        self.read_only = read_only
        self.conn: Optional[duckdb.DuckDBPyConnection] = None

    def __enter__(self):
        self.conn = get_connection(self.db_path, read_only=self.read_only)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 关闭连接，释放锁
        if self.conn is not None:
            self.conn.close()
            self.conn = None

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
