"""
FastAPI 依赖注入
"""

from typing import Generator

import duckdb

from api.config import get_database_path


def get_db(read_only: bool = False) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """获取数据库连接

    使用依赖注入模式，每个请求获取独立连接。

    Args:
        read_only: 是否只读模式，默认 False 以支持写操作

    Yields:
        DuckDB 连接对象
    """
    db_path = get_database_path()
    conn = duckdb.connect(str(db_path), read_only=read_only)
    try:
        yield conn
    finally:
        conn.close()


def get_db_readonly() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """获取只读数据库连接（用于纯查询 API）

    Yields:
        DuckDB 只读连接对象
    """
    db_path = get_database_path()
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        yield conn
    finally:
        conn.close()


class DatabaseManager:
    """数据库管理器（用于服务层）"""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def query(self, sql: str, params: tuple = None):
        """执行查询"""
        if params:
            return self.conn.execute(sql, params).df()
        return self.conn.execute(sql).df()

    def fetchone(self, sql: str, params: tuple = None):
        """获取单条记录"""
        if params:
            return self.conn.execute(sql, params).fetchone()
        return self.conn.execute(sql).fetchone()

    def fetchall(self, sql: str, params: tuple = None):
        """获取所有记录"""
        if params:
            return self.conn.execute(sql, params).fetchall()
        return self.conn.execute(sql).fetchall()
