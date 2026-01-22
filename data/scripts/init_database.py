#!/usr/bin/env python3
"""
数据库初始化脚本

用于创建数据库表结构。
"""

import logging
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from data.database import create_all_tables, drop_all_tables, DatabaseManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def init_database(reset: bool = False):
    """初始化数据库

    Args:
        reset: 如果为 True，先删除所有表再重建
    """
    logger.info("=" * 50)
    logger.info("开始初始化数据库...")
    logger.info("=" * 50)

    with DatabaseManager() as db:
        if reset:
            logger.warning("重置模式：删除所有现有表...")
            drop_all_tables()

        # 创建所有表
        success = create_all_tables()

        if success:
            logger.info("\n数据库初始化成功！")

            # 显示创建的表
            tables = db.show_tables()
            logger.info(f"\n已创建 {len(tables)} 个表:")
            for table in tables:
                count = db.get_table_count(table)
                logger.info(f"  - {table}: {count} 条记录")
        else:
            logger.error("数据库初始化失败！")
            return False

    return True


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="初始化 AI-Trader 数据库")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="重置数据库（删除所有表并重建）"
    )
    parser.add_argument(
        "--show-schema",
        action="store_true",
        help="显示所有表结构"
    )

    args = parser.parse_args()

    if args.show_schema:
        from data.database.models import show_all_schemas
        show_all_schemas()
        return

    if args.reset:
        confirm = input("确定要重置数据库吗？这将删除所有数据！(y/N): ")
        if confirm.lower() != "y":
            logger.info("已取消")
            return

    success = init_database(reset=args.reset)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
