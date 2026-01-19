"""
Live Trading Scheduler - 定时交易调度器

支持：
- 日频交易：每个交易日 09:35 执行
- 小时频交易：每个交易日 10:35, 11:35, 14:05, 15:05 执行
"""

import asyncio
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# 添加项目根目录到 path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


class LiveTradingScheduler:
    """
    轻量级定时调度器

    - 日频：09:35 执行（开盘后5分钟，确保有价格数据）
    - 小时频：10:35, 11:35, 14:05, 15:05 执行
    """

    # A股交易时间配置
    ASTOCK_DAILY_TIME = (9, 35)  # 日频触发时间
    ASTOCK_HOURLY_TIMES = [
        (10, 35),  # 10:30 K线完成后
        (11, 35),  # 11:30 K线完成后
        (14, 5),   # 14:00 K线完成后
        (15, 5),   # 15:00 K线完成后（收盘）
    ]

    # 美股交易时间配置（北京时间）
    US_DAILY_TIME = (21, 35)  # 美股开盘后5分钟
    US_HOURLY_TIMES = [
        (22, 5), (23, 5), (0, 5), (1, 5), (2, 5), (3, 5), (4, 5)
    ]

    def __init__(self, config_path: str, market: str = None, frequency: str = None):
        """
        初始化调度器

        Args:
            config_path: 配置文件路径
            market: 市场类型 (cn/us/crypto)，如果不指定则从配置文件读取
            frequency: 交易频率 (daily/hourly)，如果不指定则从配置文件推断
        """
        self.config_path = Path(config_path).resolve()
        self.config = self._load_config()

        # 从配置文件推断市场和频率
        agent_type = self.config.get("agent_type", "BaseAgent")

        if market is None:
            if "AStock" in agent_type:
                market = "cn"
            elif "Crypto" in agent_type:
                market = "crypto"
            else:
                market = self.config.get("market", "us")

        if frequency is None:
            if "Hour" in agent_type:
                frequency = "hourly"
            else:
                frequency = "daily"

        self.market = market
        self.frequency = frequency
        self.scheduler = AsyncIOScheduler()

        # 设置时区
        if market == "cn":
            self.tz = pytz.timezone("Asia/Shanghai")
        elif market == "us":
            self.tz = pytz.timezone("America/New_York")
        else:
            self.tz = pytz.timezone("UTC")

        print(f"[Scheduler] 初始化完成")
        print(f"  - 配置文件: {self.config_path}")
        print(f"  - 市场: {self.market}")
        print(f"  - 频率: {self.frequency}")
        print(f"  - 时区: {self.tz}")

    def _load_config(self) -> dict:
        """加载配置文件"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"配置文件不存在: {self.config_path}")

        with open(self.config_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def start(self):
        """启动调度器"""
        if self.frequency == "daily":
            self._add_daily_job()
        elif self.frequency == "hourly":
            self._add_hourly_jobs()

        print(f"\n[Scheduler] 启动定时任务")
        print(f"  - 市场: {self.market}")
        print(f"  - 频率: {self.frequency}")
        print(f"  - 下次执行时间: {self._get_next_run_time()}")

        self.scheduler.start()

    def _add_daily_job(self):
        """添加日频任务"""
        if self.market == "cn":
            hour, minute = self.ASTOCK_DAILY_TIME
            tz = pytz.timezone("Asia/Shanghai")
        elif self.market == "us":
            hour, minute = self.US_DAILY_TIME
            tz = pytz.timezone("Asia/Shanghai")  # 使用北京时间便于理解
        else:
            hour, minute = 9, 35
            tz = self.tz

        self.scheduler.add_job(
            self._run_trading_session,
            CronTrigger(
                hour=hour,
                minute=minute,
                day_of_week="mon-fri",
                timezone=tz,
            ),
            id="daily_trading",
            name="Daily Trading Session",
        )
        print(f"[Scheduler] 已添加日频任务: 每个工作日 {hour:02d}:{minute:02d}")

    def _add_hourly_jobs(self):
        """添加小时频任务"""
        if self.market == "cn":
            schedule_times = self.ASTOCK_HOURLY_TIMES
            tz = pytz.timezone("Asia/Shanghai")
        elif self.market == "us":
            schedule_times = self.US_HOURLY_TIMES
            tz = pytz.timezone("Asia/Shanghai")
        else:
            schedule_times = [(h, 5) for h in range(24)]
            tz = self.tz

        for hour, minute in schedule_times:
            self.scheduler.add_job(
                self._run_trading_session,
                CronTrigger(
                    hour=hour,
                    minute=minute,
                    day_of_week="mon-fri",
                    timezone=tz,
                ),
                id=f"hourly_trading_{hour:02d}_{minute:02d}",
                name=f"Hourly Trading {hour:02d}:{minute:02d}",
            )

        times_str = ", ".join([f"{h:02d}:{m:02d}" for h, m in schedule_times])
        print(f"[Scheduler] 已添加小时频任务: {times_str}")

    def _get_next_run_time(self) -> str:
        """获取下次执行时间"""
        jobs = self.scheduler.get_jobs()
        if jobs:
            next_run = min(job.next_run_time for job in jobs if job.next_run_time)
            return next_run.strftime("%Y-%m-%d %H:%M:%S %Z")
        return "未知"

    async def _run_trading_session(self):
        """执行交易会话"""
        now = datetime.now(self.tz)
        print(f"\n{'=' * 60}")
        print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] 触发定时交易任务")
        print(f"{'=' * 60}")

        try:
            # 1. 获取并更新实时价格
            print("\n[Step 1] 获取实时价格数据...")
            from data.fetch_realtime import update_realtime_prices

            success = await update_realtime_prices(self.market, self.frequency)
            if not success:
                print("[Warning] 实时价格获取失败，尝试继续执行...")

            # 2. 执行交易会话
            print("\n[Step 2] 执行交易会话...")
            await self._execute_trading(now)

            print(f"\n[完成] 交易会话执行完毕")
            print(f"[下次执行] {self._get_next_run_time()}")

        except Exception as e:
            print(f"\n[ERROR] 交易执行失败: {e}")
            import traceback
            traceback.print_exc()

    async def _execute_trading(self, current_time: datetime):
        """执行单次交易"""
        # 重新加载配置（支持运行时修改）
        self.config = self._load_config()

        # 设置当前日期/时间
        if self.frequency == "daily":
            today_date = current_time.strftime("%Y-%m-%d")
        else:
            # 小时频：对齐到整点
            hour = current_time.hour
            if self.market == "cn":
                # A股交易时段对齐
                if hour == 10:
                    aligned_time = "10:30:00"
                elif hour == 11:
                    aligned_time = "11:30:00"
                elif hour == 14:
                    aligned_time = "14:00:00"
                elif hour == 15:
                    aligned_time = "15:00:00"
                else:
                    aligned_time = f"{hour:02d}:00:00"
            else:
                aligned_time = f"{hour:02d}:00:00"

            today_date = current_time.strftime(f"%Y-%m-%d {aligned_time}")

        print(f"[Trading] 交易时间点: {today_date}")

        # 获取启用的模型
        enabled_models = [
            m for m in self.config.get("models", []) if m.get("enabled", False)
        ]

        if not enabled_models:
            print("[Warning] 没有启用的模型")
            return

        # 遍历启用的模型执行
        for model_config in enabled_models:
            model_name = model_config.get("name", "unknown")
            print(f"\n[Trading] 执行模型: {model_name}")
            print(f"[Trading] 时间: {today_date}")

            try:
                await self._run_single_model(model_config, today_date)
                print(f"[Trading] 模型 {model_name} 执行完成")
            except Exception as e:
                print(f"[ERROR] 模型 {model_name} 执行失败: {e}")
                import traceback
                traceback.print_exc()

    async def _run_single_model(self, model_config: dict, today_date: str):
        """
        运行单个模型的单次交易会话

        Args:
            model_config: 模型配置
            today_date: 交易日期/时间
        """
        from main import get_agent_class
        from tools.general_tools import write_config_value

        agent_type = self.config.get("agent_type", "BaseAgent")
        agent_config = self.config.get("agent_config", {})
        log_config = self.config.get("log_config", {})

        # 读取模型参数
        basemodel = model_config.get("basemodel")
        signature = model_config.get("signature")
        openai_base_url = model_config.get("openai_base_url")
        openai_api_key = model_config.get("openai_api_key")

        if not basemodel or not signature:
            raise ValueError(f"模型配置缺少 basemodel 或 signature")

        # 获取 Agent 类
        AgentClass = get_agent_class(agent_type)

        # 获取日志路径
        log_path = log_config.get("log_path", "./data/agent_data")

        # 写入运行时配置
        write_config_value("SIGNATURE", signature)
        write_config_value("IF_TRADE", False)
        write_config_value("MARKET", self.market)
        write_config_value("LOG_PATH", log_path)

        # 提取 agent 配置参数
        max_steps = agent_config.get("max_steps", 30)
        max_retries = agent_config.get("max_retries", 3)
        base_delay = agent_config.get("base_delay", 1.0)
        initial_cash = agent_config.get("initial_cash", 100000.0)

        # 创建 Agent 实例
        if agent_type == "BaseAgentCrypto":
            agent = AgentClass(
                signature=signature,
                basemodel=basemodel,
                log_path=log_path,
                max_steps=max_steps,
                max_retries=max_retries,
                base_delay=base_delay,
                initial_cash=initial_cash,
                init_date=today_date.split()[0],  # 只取日期部分
                openai_base_url=openai_base_url,
                openai_api_key=openai_api_key,
            )
        else:
            agent = AgentClass(
                signature=signature,
                basemodel=basemodel,
                stock_symbols=None,  # 使用默认股票列表
                log_path=log_path,
                max_steps=max_steps,
                max_retries=max_retries,
                base_delay=base_delay,
                initial_cash=initial_cash,
                init_date=today_date.split()[0],
                openai_base_url=openai_base_url,
                openai_api_key=openai_api_key,
            )

        # 初始化
        await agent.initialize()

        # 执行单次交易会话
        await agent.run_trading_session(today_date)

        # 输出结果摘要
        summary = agent.get_position_summary()
        print(f"[Summary] 最新日期: {summary.get('latest_date')}")
        print(f"[Summary] 现金余额: {summary.get('positions', {}).get('CASH', 0):,.2f}")

    def run_forever(self):
        """运行调度器（阻塞）"""
        print("\n[Scheduler] 调度器运行中，按 Ctrl+C 停止...")
        try:
            asyncio.get_event_loop().run_forever()
        except (KeyboardInterrupt, SystemExit):
            print("\n[Scheduler] 收到停止信号，正在关闭...")
            self.scheduler.shutdown()
            print("[Scheduler] 调度器已停止")

    async def run_now(self):
        """立即执行一次（用于测试）"""
        print("[Mode] 立即执行模式（不启动定时器）")
        await self._run_trading_session()


def main():
    """命令行入口"""
    import argparse

    parser = argparse.ArgumentParser(description="定时交易调度器")
    parser.add_argument("--config", "-c", required=True, help="配置文件路径")
    parser.add_argument(
        "--market", "-m", choices=["cn", "us", "crypto"], help="市场类型（可选，默认从配置推断）"
    )
    parser.add_argument(
        "--frequency", "-f", choices=["daily", "hourly"], help="交易频率（可选，默认从配置推断）"
    )
    parser.add_argument("--run-now", action="store_true", help="立即执行一次（不启动定时器）")
    args = parser.parse_args()

    scheduler = LiveTradingScheduler(
        config_path=args.config,
        market=args.market,
        frequency=args.frequency,
    )

    if args.run_now:
        asyncio.run(scheduler.run_now())
    else:
        scheduler.start()
        scheduler.run_forever()


if __name__ == "__main__":
    main()
