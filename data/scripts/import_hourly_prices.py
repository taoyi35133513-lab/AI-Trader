#!/usr/bin/env python3
"""
导入小时线行情数据到数据库

支持从 CSV 文件导入小时线数据。
"""

import logging
import sys
from pathlib import Path

import pandas as pd

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from data.database import DatabaseManager, create_all_tables

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 默认数据文件路径
DEFAULT_HOURLY_FILE = project_root / "data/A_stock/A_stock_data/A_stock_hourly.csv"


def clean_hourly_prices(df: pd.DataFrame) -> pd.DataFrame:
    """清洗小时线数据

    Args:
        df: 原始 DataFrame

    Returns:
        清洗后的 DataFrame
    """
    # 复制避免修改原数据
    df = df.copy()

    # 列名映射（适配原始 CSV 列名）
    # 原始列: stock_name, stock_code, trade_date, open, close, high, low, volume
    column_mapping = {
        "stock_code": "ts_code",
        "trade_date": "trade_time",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "volume": "volume",
    }

    # 重命名列
    df = df.rename(columns=column_mapping)

    # 转换时间格式 (YYYY-MM-DD HH:MM)
    if "trade_time" in df.columns:
        df["trade_time"] = pd.to_datetime(df["trade_time"], format="%Y-%m-%d %H:%M")

    # 添加市场标识
    df["market"] = "cn"

    # 数据类型转换
    numeric_cols = ["open", "high", "low", "close", "volume"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 只保留需要的列
    keep_cols = ["ts_code", "trade_time", "open", "high", "low", "close", "volume", "market"]
    df = df[[col for col in keep_cols if col in df.columns]]

    # 删除重复记录
    df = df.drop_duplicates(subset=["ts_code", "trade_time"])

    # 删除空值行
    df = df.dropna(subset=["ts_code", "trade_time", "close"])

    logger.info(f"清洗完成，共 {len(df)} 条有效记录")
    return df


def import_hourly_prices(
    file_path: Path = None,
    if_exists: str = "append"
) -> int:
    """导入小时线数据

    Args:
        file_path: CSV 文件路径
        if_exists: 处理已存在数据方式 ('append', 'replace', 'ignore')

    Returns:
        导入的记录数
    """
    if file_path is None:
        file_path = DEFAULT_HOURLY_FILE

    file_path = Path(file_path)
    if not file_path.exists():
        logger.error(f"文件不存在: {file_path}")
        return 0

    logger.info(f"读取文件: {file_path}")

    # 读取 CSV
    df = pd.read_csv(file_path)
    logger.info(f"原始数据: {len(df)} 条记录")

    # 清洗数据
    df = clean_hourly_prices(df)

    if df.empty:
        logger.warning("没有有效数据可导入")
        return 0

    # 确保表存在
    create_all_tables()

    # 导入数据库
    with DatabaseManager() as db:
        db.insert_df("stock_hourly_prices", df, if_exists=if_exists)

        count = db.get_table_count("stock_hourly_prices")
        logger.info(f"表 stock_hourly_prices 当前共 {count} 条记录")

    return len(df)


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="导入小时线行情数据")
    parser.add_argument(
        "--file",
        type=str,
        default=None,
        help=f"CSV 文件路径 (默认: {DEFAULT_HOURLY_FILE})"
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["append", "replace", "ignore"],
        default="append",
        help="导入模式: append(追加), replace(替换), ignore(忽略重复)"
    )

    args = parser.parse_args()

    file_path = Path(args.file) if args.file else None
    count = import_hourly_prices(file_path, if_exists=args.mode)

    logger.info(f"成功导入 {count} 条小时线数据")


if __name__ == "__main__":
    main()
