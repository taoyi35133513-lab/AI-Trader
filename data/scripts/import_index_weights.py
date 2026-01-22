#!/usr/bin/env python3
"""
导入指数成分股权重数据到数据库

支持从 CSV 文件导入指数权重数据。
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
DEFAULT_WEIGHT_FILE = project_root / "data/A_stock/A_stock_data/sse_50_weight.csv"


def clean_index_weights(df: pd.DataFrame) -> pd.DataFrame:
    """清洗指数权重数据

    Args:
        df: 原始 DataFrame

    Returns:
        清洗后的 DataFrame
    """
    # 复制避免修改原数据
    df = df.copy()

    # 原始列: index_code, con_code, trade_date, weight, stock_name
    # 目标列: index_code, con_code, stock_name, weight, trade_date

    # 转换日期格式
    if "trade_date" in df.columns:
        df["trade_date"] = pd.to_datetime(df["trade_date"].astype(str), format="%Y%m%d")

    # 数据类型转换
    if "weight" in df.columns:
        df["weight"] = pd.to_numeric(df["weight"], errors="coerce")

    # 只保留需要的列
    keep_cols = ["index_code", "con_code", "stock_name", "weight", "trade_date"]
    df = df[[col for col in keep_cols if col in df.columns]]

    # 删除重复记录
    df = df.drop_duplicates(subset=["index_code", "con_code", "trade_date"])

    # 删除空值行
    df = df.dropna(subset=["index_code", "con_code"])

    logger.info(f"清洗完成，共 {len(df)} 条有效记录")
    return df


def import_index_weights(
    file_path: Path = None,
    if_exists: str = "append"
) -> int:
    """导入指数权重数据

    Args:
        file_path: CSV 文件路径
        if_exists: 处理已存在数据方式 ('append', 'replace', 'ignore')

    Returns:
        导入的记录数
    """
    if file_path is None:
        file_path = DEFAULT_WEIGHT_FILE

    file_path = Path(file_path)
    if not file_path.exists():
        logger.error(f"文件不存在: {file_path}")
        return 0

    logger.info(f"读取文件: {file_path}")

    # 读取 CSV
    df = pd.read_csv(file_path)
    logger.info(f"原始数据: {len(df)} 条记录")

    # 清洗数据
    df = clean_index_weights(df)

    if df.empty:
        logger.warning("没有有效数据可导入")
        return 0

    # 确保表存在
    create_all_tables()

    # 导入数据库
    with DatabaseManager() as db:
        db.insert_df("index_weights", df, if_exists=if_exists)

        count = db.get_table_count("index_weights")
        logger.info(f"表 index_weights 当前共 {count} 条记录")

    return len(df)


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="导入指数成分股权重数据")
    parser.add_argument(
        "--file",
        type=str,
        default=None,
        help=f"CSV 文件路径 (默认: {DEFAULT_WEIGHT_FILE})"
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
    count = import_index_weights(file_path, if_exists=args.mode)

    logger.info(f"成功导入 {count} 条权重数据")


if __name__ == "__main__":
    main()
