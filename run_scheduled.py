#!/usr/bin/env python3
"""
定时交易执行入口

使用方式：
    # 日频交易（定时模式）
    python run_scheduled.py --config configs/astock_config.json

    # 小时频交易（定时模式）
    python run_scheduled.py --config configs/astock_hour_config.json

    # 指定市场和频率
    python run_scheduled.py -c configs/astock_config.json -m cn -f daily

    # 立即执行一次（测试用）
    python run_scheduled.py -c configs/astock_config.json --run-now

    # 后台运行
    nohup python run_scheduled.py -c configs/astock_config.json > logs/scheduled.log 2>&1 &

示例：
    # A股日频定时交易
    python run_scheduled.py -c configs/astock_config.json

    # A股小时频定时交易
    python run_scheduled.py -c configs/astock_hour_config.json

    # 立即测试执行一次
    python run_scheduled.py -c configs/astock_config.json --run-now
"""

import argparse
import asyncio
import sys
from pathlib import Path

# 添加项目根目录到 path
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))


def print_banner():
    """打印启动横幅"""
    banner = """
╔══════════════════════════════════════════════════════════════╗
║                   AI-Trader 定时交易系统                       ║
║                  Scheduled Trading System                     ║
╚══════════════════════════════════════════════════════════════╝
"""
    print(banner)


def main():
    parser = argparse.ArgumentParser(
        description="AI-Trader 定时交易执行",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python run_scheduled.py -c configs/astock_config.json           # A股日频定时
  python run_scheduled.py -c configs/astock_hour_config.json      # A股小时频定时
  python run_scheduled.py -c configs/astock_config.json --run-now # 立即执行一次
        """,
    )

    parser.add_argument(
        "--config",
        "-c",
        required=True,
        help="配置文件路径 (如 configs/astock_config.json)",
    )
    parser.add_argument(
        "--market",
        "-m",
        choices=["cn", "us", "crypto"],
        help="市场类型（可选，默认从配置文件推断）",
    )
    parser.add_argument(
        "--frequency",
        "-f",
        choices=["daily", "hourly"],
        help="交易频率（可选，默认从配置文件推断）",
    )
    parser.add_argument(
        "--run-now",
        action="store_true",
        help="立即执行一次（不启动定时器，用于测试）",
    )

    args = parser.parse_args()

    # 打印横幅
    print_banner()

    # 检查配置文件是否存在
    config_path = Path(args.config)
    if not config_path.exists():
        # 尝试相对于项目根目录
        config_path = PROJECT_ROOT / args.config
        if not config_path.exists():
            print(f"❌ 配置文件不存在: {args.config}")
            sys.exit(1)

    print(f"📄 配置文件: {config_path}")

    # 导入调度器
    from scheduler.live_scheduler import LiveTradingScheduler

    # 创建调度器实例
    try:
        scheduler = LiveTradingScheduler(
            config_path=str(config_path),
            market=args.market,
            frequency=args.frequency,
        )
    except Exception as e:
        print(f"❌ 初始化调度器失败: {e}")
        sys.exit(1)

    # 根据模式运行
    if args.run_now:
        print("\n🚀 立即执行模式（不启动定时器）")
        print("=" * 60)
        asyncio.run(scheduler.run_now())
    else:
        print("\n🕐 定时调度模式")
        print("=" * 60)
        scheduler.start()
        scheduler.run_forever()


if __name__ == "__main__":
    main()
