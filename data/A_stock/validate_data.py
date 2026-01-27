"""
A 股数据完整性验证模块

用于检测和报告 SSE 50 指数成分股数据的完整性问题：
- 缺失的股票（需要行情数据但不在价格数据中）
- 持仓中已剔除的股票（仍需更新行情）
- 数据新鲜度检查

重要：被剔除的成分股如果仍在持仓中，需要继续更新行情数据以便正确估值和卖出。
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """数据验证结果"""

    is_valid: bool
    missing_stocks: List[str] = field(default_factory=list)  # 需要数据但缺失
    missing_held_stocks: List[str] = field(default_factory=list)  # 持仓中但缺失行情
    extra_stocks: List[str] = field(default_factory=list)  # 不需要的股票（可选清理）
    stale_stocks: List[str] = field(default_factory=list)  # 数据过时的股票
    held_stocks: List[str] = field(default_factory=list)  # 当前持仓的股票
    weight_file_outdated: bool = False
    error_message: Optional[str] = None

    def __str__(self) -> str:
        lines = []
        lines.append(f"验证结果: {'通过' if self.is_valid else '未通过'}")
        if self.missing_stocks:
            lines.append(f"缺失股票 ({len(self.missing_stocks)}): {', '.join(self.missing_stocks)}")
        if self.missing_held_stocks:
            lines.append(f"持仓缺失行情 ({len(self.missing_held_stocks)}): {', '.join(self.missing_held_stocks)}")
        if self.extra_stocks:
            lines.append(f"多余股票 ({len(self.extra_stocks)}): {', '.join(self.extra_stocks)}")
        if self.stale_stocks:
            lines.append(f"数据过时 ({len(self.stale_stocks)}): {', '.join(self.stale_stocks)}")
        if self.held_stocks:
            lines.append(f"当前持仓 ({len(self.held_stocks)}): {', '.join(self.held_stocks)}")
        if self.weight_file_outdated:
            lines.append("成分股权重文件需要更新")
        if self.error_message:
            lines.append(f"错误: {self.error_message}")
        return "\n".join(lines)


class DataValidator:
    """A 股数据验证器"""

    def __init__(self, data_dir: Optional[Path] = None):
        """初始化验证器

        Args:
            data_dir: 数据目录，默认为 ./A_stock_data
        """
        if data_dir is None:
            self.data_dir = Path(__file__).parent / "A_stock_data"
        else:
            self.data_dir = Path(data_dir)

        self.weight_file = self.data_dir / "sse_50_weight.csv"
        self.daily_price_file = self.data_dir / "daily_prices_sse_50.csv"
        self.hourly_price_file = self.data_dir / "A_stock_hourly.csv"

        # 持仓数据目录
        self.project_root = Path(__file__).parent.parent.parent
        self.daily_position_dir = self.project_root / "data" / "agent_data_astock"
        self.hourly_position_dir = self.project_root / "data" / "agent_data_astock_hour"

    def get_all_held_stocks(self, frequency: str = "daily") -> Set[str]:
        """获取所有 agent 持仓中的股票

        遍历所有 agent 的持仓文件，收集当前持有的股票代码。
        这些股票即使已从指数中剔除，也需要继续更新行情数据。

        Args:
            frequency: "daily" 或 "hourly"

        Returns:
            持仓股票代码集合
        """
        if frequency == "daily":
            position_dir = self.daily_position_dir
        else:
            position_dir = self.hourly_position_dir

        held_stocks: Set[str] = set()

        if not position_dir.exists():
            logger.debug(f"持仓目录不存在: {position_dir}")
            return held_stocks

        # 遍历所有 agent 目录
        for agent_dir in position_dir.iterdir():
            if not agent_dir.is_dir():
                continue

            position_file = agent_dir / "position" / "position.jsonl"
            if not position_file.exists():
                continue

            try:
                # 读取最后一行获取最新持仓
                with open(position_file, "r", encoding="utf-8") as f:
                    lines = f.readlines()
                    if not lines:
                        continue

                    # 获取最后一行的持仓数据
                    last_line = lines[-1].strip()
                    if not last_line:
                        continue

                    data = json.loads(last_line)
                    positions = data.get("positions", {})

                    # 收集持仓数量 > 0 的股票
                    for stock, amount in positions.items():
                        if stock != "CASH" and amount > 0:
                            held_stocks.add(stock)

            except Exception as e:
                logger.warning(f"读取持仓文件失败 {position_file}: {e}")
                continue

        if held_stocks:
            logger.info(f"检测到 {len(held_stocks)} 只持仓股票: {sorted(held_stocks)}")

        return held_stocks

    def get_index_constituents_from_api(self) -> Set[str]:
        """从 API 获取当前 SSE 50 成分股

        Returns:
            成分股代码集合
        """
        from data_source import create_data_source

        source = create_data_source("akshare")
        df = source.get_index_constituents("000016.SH")

        if df.empty:
            raise RuntimeError("从 API 获取成分股失败")

        return set(df["con_code"].unique())

    def get_index_constituents_from_file(self) -> Set[str]:
        """从本地文件获取 SSE 50 成分股

        Returns:
            成分股代码集合
        """
        if not self.weight_file.exists():
            logger.warning(f"权重文件不存在: {self.weight_file}")
            return set()

        df = pd.read_csv(self.weight_file)
        return set(df["con_code"].unique())

    def get_stocks_in_price_data(self, frequency: str = "daily") -> Set[str]:
        """获取价格数据中的股票列表

        Args:
            frequency: "daily" 或 "hourly"

        Returns:
            股票代码集合
        """
        if frequency == "daily":
            price_file = self.daily_price_file
            code_col = "ts_code"
        else:
            price_file = self.hourly_price_file
            code_col = "stock_code"

        if not price_file.exists():
            logger.warning(f"价格文件不存在: {price_file}")
            return set()

        df = pd.read_csv(price_file)
        return set(df[code_col].unique())

    def get_latest_date_in_price_data(self, frequency: str = "daily") -> Optional[str]:
        """获取价格数据中的最新日期

        Args:
            frequency: "daily" 或 "hourly"

        Returns:
            最新日期字符串 (YYYYMMDD)
        """
        if frequency == "daily":
            price_file = self.daily_price_file
        else:
            price_file = self.hourly_price_file

        if not price_file.exists():
            return None

        df = pd.read_csv(price_file)
        if df.empty or "trade_date" not in df.columns:
            return None

        df["trade_date"] = df["trade_date"].astype(str)
        return df["trade_date"].max()

    def check_stale_data(
        self, stocks: Set[str], frequency: str = "daily", max_age_days: int = 5
    ) -> List[str]:
        """检查数据是否过时

        Args:
            stocks: 要检查的股票集合
            frequency: "daily" 或 "hourly"
            max_age_days: 最大允许天数

        Returns:
            数据过时的股票列表
        """
        if frequency == "daily":
            price_file = self.daily_price_file
            code_col = "ts_code"
        else:
            price_file = self.hourly_price_file
            code_col = "stock_code"

        if not price_file.exists():
            return list(stocks)

        df = pd.read_csv(price_file)
        df["trade_date"] = df["trade_date"].astype(str)

        # 计算截止日期
        cutoff_date = (datetime.now() - timedelta(days=max_age_days)).strftime("%Y%m%d")

        stale = []
        for stock in stocks:
            stock_data = df[df[code_col] == stock]
            if stock_data.empty:
                stale.append(stock)
            else:
                latest = stock_data["trade_date"].max()
                if latest < cutoff_date:
                    stale.append(stock)

        return stale

    def validate(
        self,
        use_api: bool = True,
        frequency: str = "daily",
        check_freshness: bool = False,
    ) -> ValidationResult:
        """验证数据完整性

        验证逻辑：
        1. 获取当前指数成分股
        2. 获取所有 agent 持仓中的股票
        3. 需要行情数据的股票 = 成分股 + 持仓股票
        4. 检查这些股票是否都有行情数据

        Args:
            use_api: 是否从 API 获取当前成分股（推荐）
            frequency: "daily" 或 "hourly"
            check_freshness: 是否检查数据新鲜度

        Returns:
            ValidationResult 验证结果
        """
        try:
            # 获取当前指数成分股
            weight_file_outdated = False
            if use_api:
                try:
                    current_constituents = self.get_index_constituents_from_api()
                    file_constituents = self.get_index_constituents_from_file()
                    weight_file_outdated = current_constituents != file_constituents
                    if weight_file_outdated:
                        logger.info("成分股权重文件与 API 不一致，需要更新")
                except Exception as e:
                    logger.warning(f"API 获取失败，使用本地文件: {e}")
                    current_constituents = self.get_index_constituents_from_file()
            else:
                current_constituents = self.get_index_constituents_from_file()

            if not current_constituents:
                return ValidationResult(
                    is_valid=False,
                    error_message="无法获取成分股数据",
                )

            # 获取所有持仓股票
            held_stocks = self.get_all_held_stocks(frequency)

            # 需要行情数据的股票 = 成分股 + 持仓股票
            required_stocks = current_constituents | held_stocks

            # 获取价格数据中的股票
            price_stocks = self.get_stocks_in_price_data(frequency)

            # 计算差异
            # 缺失的成分股
            missing_constituents = sorted(current_constituents - price_stocks)
            # 持仓中缺失行情的股票（已剔除但仍持有）
            missing_held = sorted((held_stocks - current_constituents) - price_stocks)
            # 所有缺失的股票
            all_missing = sorted(required_stocks - price_stocks)
            # 多余的股票（既不在成分股中，也不在持仓中）
            extra_stocks = sorted(price_stocks - required_stocks)

            # 检查数据新鲜度
            stale_stocks = []
            if check_freshness:
                stocks_to_check = required_stocks & price_stocks
                stale_stocks = self.check_stale_data(stocks_to_check, frequency)

            # 判断是否有效 - 所有需要的股票都有数据且不过时
            is_valid = len(all_missing) == 0 and len(stale_stocks) == 0

            return ValidationResult(
                is_valid=is_valid,
                missing_stocks=missing_constituents,
                missing_held_stocks=missing_held,
                extra_stocks=extra_stocks,
                stale_stocks=stale_stocks,
                held_stocks=sorted(held_stocks),
                weight_file_outdated=weight_file_outdated,
            )

        except Exception as e:
            logger.exception("验证过程发生错误")
            return ValidationResult(
                is_valid=False,
                error_message=str(e),
            )


def validate_and_report(
    use_api: bool = True,
    frequency: str = "daily",
    check_freshness: bool = False,
) -> ValidationResult:
    """验证数据并打印报告

    Args:
        use_api: 是否从 API 获取当前成分股
        frequency: "daily" 或 "hourly"
        check_freshness: 是否检查数据新鲜度

    Returns:
        ValidationResult 验证结果
    """
    validator = DataValidator()
    result = validator.validate(
        use_api=use_api,
        frequency=frequency,
        check_freshness=check_freshness,
    )

    print("\n" + "=" * 60)
    print("A 股数据完整性验证报告")
    print("=" * 60)

    if result.error_message:
        print(f"错误: {result.error_message}")
        return result

    # 基本信息
    freq_label = "日线" if frequency == "daily" else "小时线"
    print(f"数据类型: {freq_label}")
    print(f"验证状态: {'✅ 通过' if result.is_valid else '❌ 未通过'}")

    if result.weight_file_outdated:
        print("⚠️  成分股权重文件需要更新")

    # 当前持仓
    if result.held_stocks:
        print(f"\n📊 当前持仓股票 ({len(result.held_stocks)}):")
        for stock in result.held_stocks:
            print(f"    - {stock}")

    # 缺失的成分股
    if result.missing_stocks:
        print(f"\n❌ 缺失成分股 ({len(result.missing_stocks)}):")
        for stock in result.missing_stocks:
            print(f"    - {stock}")

    # 持仓中缺失行情的股票
    if result.missing_held_stocks:
        print(f"\n❌ 持仓缺失行情 (已剔除但仍持有，需更新数据) ({len(result.missing_held_stocks)}):")
        for stock in result.missing_held_stocks:
            print(f"    - {stock}")

    # 修复提示
    if result.missing_stocks or result.missing_held_stocks:
        print("\n修复命令:")
        print("    python get_daily_price_akshare.py --fix-missing")

    # 多余股票（既不在成分股中也不在持仓中）
    if result.extra_stocks:
        print(f"\nℹ️  多余股票 (可选清理，共 {len(result.extra_stocks)}):")
        for stock in result.extra_stocks:
            print(f"    - {stock}")

    # 数据过时
    if result.stale_stocks:
        print(f"\n⚠️  数据过时 ({len(result.stale_stocks)}):")
        for stock in result.stale_stocks:
            print(f"    - {stock}")

    if result.is_valid:
        print("\n✅ 所有需要的股票数据完整")

    print("=" * 60)
    return result


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="验证 A 股数据完整性")
    parser.add_argument(
        "--no-api",
        action="store_true",
        help="不使用 API，仅使用本地文件",
    )
    parser.add_argument(
        "-f",
        "--frequency",
        choices=["daily", "hourly"],
        default="daily",
        help="数据频率 (daily/hourly)",
    )
    parser.add_argument(
        "--check-freshness",
        action="store_true",
        help="检查数据新鲜度",
    )
    args = parser.parse_args()

    result = validate_and_report(
        use_api=not args.no_api,
        frequency=args.frequency,
        check_freshness=args.check_freshness,
    )

    # 返回码：0=成功，1=失败
    exit(0 if result.is_valid else 1)
