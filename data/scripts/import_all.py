#!/usr/bin/env python3
"""
一键导入所有数据到数据库

导入日线、小时线和指数权重数据。
"""

import logging
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from data.database import DatabaseManager, create_all_tables, close_connection
from data.scripts.import_daily_prices import import_daily_prices
from data.scripts.import_hourly_prices import import_hourly_prices
from data.scripts.import_index_weights import import_index_weights

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def import_all(reset: bool = False, mode: str = "append"):
    """一键导入所有数据

    Args:
        reset: 是否重置数据库（删除所有表再重建）
        mode: 导入模式 ('append', 'replace', 'ignore')
    """
    logger.info("=" * 60)
    logger.info("开始一键导入所有数据...")
    logger.info("=" * 60)

    # 初始化数据库
    if reset:
        from data.database import drop_all_tables
        logger.warning("重置数据库...")
        drop_all_tables()

    create_all_tables()

    results = {}

    # 1. 导入日线数据
    logger.info("\n" + "-" * 40)
    logger.info("步骤 1/3: 导入日线行情数据")
    logger.info("-" * 40)
    try:
        count = import_daily_prices(if_exists=mode)
        results["日线数据"] = {"status": "成功", "count": count}
    except Exception as e:
        logger.error(f"导入日线数据失败: {e}")
        results["日线数据"] = {"status": "失败", "error": str(e)}

    # 2. 导入小时线数据
    logger.info("\n" + "-" * 40)
    logger.info("步骤 2/3: 导入小时线行情数据")
    logger.info("-" * 40)
    try:
        count = import_hourly_prices(if_exists=mode)
        results["小时线数据"] = {"status": "成功", "count": count}
    except Exception as e:
        logger.error(f"导入小时线数据失败: {e}")
        results["小时线数据"] = {"status": "失败", "error": str(e)}

    # 3. 导入指数权重数据
    logger.info("\n" + "-" * 40)
    logger.info("步骤 3/3: 导入指数权重数据")
    logger.info("-" * 40)
    try:
        count = import_index_weights(if_exists=mode)
        results["指数权重"] = {"status": "成功", "count": count}
    except Exception as e:
        logger.error(f"导入指数权重数据失败: {e}")
        results["指数权重"] = {"status": "失败", "error": str(e)}

    # 打印汇总
    logger.info("\n" + "=" * 60)
    logger.info("导入完成，汇总信息:")
    logger.info("=" * 60)

    with DatabaseManager() as db:
        tables = db.show_tables()
        for table in tables:
            count = db.get_table_count(table)
            logger.info(f"  {table}: {count} 条记录")

    logger.info("\n导入结果:")
    for name, result in results.items():
        if result["status"] == "成功":
            logger.info(f"  ✓ {name}: {result['count']} 条")
        else:
            logger.error(f"  ✗ {name}: {result.get('error', '未知错误')}")

    return results


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="一键导入所有数据到数据库")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="重置数据库（删除所有表并重建）"
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["append", "replace", "ignore"],
        default="append",
        help="导入模式: append(追加), replace(替换), ignore(忽略重复)"
    )

    args = parser.parse_args()

    if args.reset:
        confirm = input("确定要重置数据库吗？这将删除所有数据！(y/N): ")
        if confirm.lower() != "y":
            logger.info("已取消")
            return

    try:
        import_all(reset=args.reset, mode=args.mode)
    finally:
        close_connection()


if __name__ == "__main__":
    main()
