"""
实时数据获取模块

功能：
1. 从实时 API 获取最新价格
2. 将新数据追加到 merged.jsonl
3. 保持与历史数据格式完全一致
"""

import asyncio
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# 添加项目根目录到 path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


class RealtimeDataFetcher:
    """实时数据获取器"""

    def __init__(self, market: str, frequency: str):
        """
        初始化

        Args:
            market: 市场类型 (cn/us/crypto)
            frequency: 频率 (daily/hourly)
        """
        self.market = market
        self.frequency = frequency
        self.data_file = self._get_data_file()

    def _get_data_file(self) -> Path:
        """获取数据文件路径"""
        if self.market == "cn":
            if self.frequency == "daily":
                return PROJECT_ROOT / "data" / "A_stock" / "merged.jsonl"
            else:
                return PROJECT_ROOT / "data" / "A_stock" / "merged_hourly.jsonl"
        elif self.market == "us":
            if self.frequency == "daily":
                return PROJECT_ROOT / "data" / "merged.jsonl"
            else:
                return PROJECT_ROOT / "data" / "merged_hourly.jsonl"
        else:
            return PROJECT_ROOT / "data" / "crypto" / "crypto_merged.jsonl"

    def get_symbols_from_merged(self) -> List[str]:
        """从 merged.jsonl 读取股票代码列表"""
        symbols = []
        if not self.data_file.exists():
            print(f"[Warning] 数据文件不存在: {self.data_file}")
            return symbols

        with open(self.data_file, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    data = json.loads(line)
                    symbol = data.get("Meta Data", {}).get("2. Symbol", "")
                    if symbol:
                        symbols.append(symbol)
                except json.JSONDecodeError:
                    continue

        return symbols

    async def fetch_astock_realtime(self, symbols: List[str]) -> Dict:
        """
        获取 A 股实时价格

        使用 akshare 获取实时行情

        Args:
            symbols: 股票代码列表 (如 ['600519.SH', '000001.SZ'])

        Returns:
            {symbol: {open, high, low, close, volume}, ...}
        """
        prices = {}

        try:
            import akshare as ak

            # 获取全部 A 股实时行情
            print("[Data] 正在获取 A 股实时行情...")
            df = ak.stock_zh_a_spot_em()

            if df is None or df.empty:
                print("[Warning] 获取实时行情失败，返回空数据")
                return prices

            for symbol in symbols:
                try:
                    # 提取股票代码（去掉后缀）
                    code = symbol.split(".")[0]

                    # 在行情数据中查找
                    row = df[df["代码"] == code]

                    if not row.empty:
                        row = row.iloc[0]
                        prices[symbol] = {
                            "open": float(row["今开"]) if row["今开"] != "-" else 0,
                            "high": float(row["最高"]) if row["最高"] != "-" else 0,
                            "low": float(row["最低"]) if row["最低"] != "-" else 0,
                            "close": float(row["最新价"]) if row["最新价"] != "-" else 0,
                            "volume": int(row["成交量"]) if row["成交量"] != "-" else 0,
                        }
                except Exception as e:
                    print(f"[Warning] 获取 {symbol} 价格失败: {e}")
                    continue

            print(f"[Data] 成功获取 {len(prices)}/{len(symbols)} 只股票价格")

        except ImportError:
            print("[Error] 请安装 akshare: pip install akshare")
        except Exception as e:
            print(f"[Error] 获取 A 股实时行情失败: {e}")

        return prices

    async def fetch_us_realtime(self, symbols: List[str]) -> Dict:
        """
        获取美股实时价格

        使用 yfinance 获取实时行情

        Args:
            symbols: 股票代码列表 (如 ['AAPL', 'MSFT'])

        Returns:
            {symbol: {open, high, low, close, volume}, ...}
        """
        prices = {}

        try:
            import yfinance as yf

            print("[Data] 正在获取美股实时行情...")

            # 批量获取
            tickers = yf.Tickers(" ".join(symbols))

            for symbol in symbols:
                try:
                    ticker = tickers.tickers.get(symbol)
                    if ticker:
                        info = ticker.info
                        prices[symbol] = {
                            "open": info.get("regularMarketOpen", 0),
                            "high": info.get("regularMarketDayHigh", 0),
                            "low": info.get("regularMarketDayLow", 0),
                            "close": info.get("regularMarketPrice", 0),
                            "volume": info.get("regularMarketVolume", 0),
                        }
                except Exception as e:
                    print(f"[Warning] 获取 {symbol} 价格失败: {e}")
                    continue

            print(f"[Data] 成功获取 {len(prices)}/{len(symbols)} 只股票价格")

        except ImportError:
            print("[Error] 请安装 yfinance: pip install yfinance")
        except Exception as e:
            print(f"[Error] 获取美股实时行情失败: {e}")

        return prices

    async def fetch_realtime_prices(self, symbols: List[str]) -> Dict:
        """
        获取实时价格（根据市场类型选择数据源）

        Args:
            symbols: 股票代码列表

        Returns:
            {symbol: {open, high, low, close, volume}, ...}
        """
        if self.market == "cn":
            return await self.fetch_astock_realtime(symbols)
        elif self.market == "us":
            return await self.fetch_us_realtime(symbols)
        else:
            # Crypto 暂不支持
            print("[Warning] 加密货币实时数据暂不支持")
            return {}

    def get_time_key(self, now: datetime = None) -> str:
        """
        获取时间键

        Args:
            now: 当前时间，默认使用系统时间

        Returns:
            日频: "2025-01-13"
            小时频: "2025-01-13 10:30:00"
        """
        if now is None:
            now = datetime.now()

        if self.frequency == "daily":
            return now.strftime("%Y-%m-%d")
        else:
            # 小时频：对齐到交易时间点
            hour = now.hour
            if self.market == "cn":
                # A股交易时段对齐
                if 9 <= hour < 11:
                    aligned = "10:30:00"
                elif 11 <= hour < 13:
                    aligned = "11:30:00"
                elif 13 <= hour < 14:
                    aligned = "14:00:00"
                elif 14 <= hour < 16:
                    aligned = "15:00:00"
                else:
                    aligned = f"{hour:02d}:00:00"
            else:
                aligned = f"{hour:02d}:00:00"

            return now.strftime(f"%Y-%m-%d {aligned}")

    def append_prices_to_merged(self, prices: Dict, time_key: str = None) -> bool:
        """
        将新价格追加到 merged.jsonl

        Args:
            prices: {symbol: {open, high, low, close, volume}, ...}
            time_key: 时间键，默认使用当前时间

        Returns:
            是否成功
        """
        if not prices:
            print("[Warning] 没有价格数据需要追加")
            return False

        if time_key is None:
            time_key = self.get_time_key()

        # 确定时间序列键名
        if self.frequency == "daily":
            time_series_key = "Time Series (Daily)"
        else:
            time_series_key = "Time Series (60min)"

        print(f"[Data] 追加价格数据到 {self.data_file}")
        print(f"[Data] 时间键: {time_key}")

        # 读取并更新
        updated_count = 0
        updated_lines = []

        with open(self.data_file, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    data = json.loads(line)
                    symbol = data.get("Meta Data", {}).get("2. Symbol", "")

                    if symbol in prices:
                        price_data = prices[symbol]

                        # 确保时间序列存在
                        if time_series_key not in data:
                            data[time_series_key] = {}

                        # 添加新的时间点数据
                        data[time_series_key][time_key] = {
                            "1. buy price": str(price_data["open"]),
                            "2. high": str(price_data["high"]),
                            "3. low": str(price_data["low"]),
                            "4. sell price": str(price_data["close"]),
                            "5. volume": str(price_data["volume"]),
                        }

                        # 更新最后刷新时间
                        data["Meta Data"]["3. Last Refreshed"] = time_key

                        updated_count += 1

                    updated_lines.append(json.dumps(data, ensure_ascii=False))

                except json.JSONDecodeError:
                    updated_lines.append(line.strip())

        # 写回文件
        with open(self.data_file, "w", encoding="utf-8") as f:
            f.write("\n".join(updated_lines))

        print(f"[Data] 成功更新 {updated_count} 只股票的价格数据")
        return updated_count > 0

    def check_data_exists(self, time_key: str = None) -> bool:
        """
        检查指定时间的数据是否已存在

        Args:
            time_key: 时间键

        Returns:
            是否存在
        """
        if time_key is None:
            time_key = self.get_time_key()

        if self.frequency == "daily":
            time_series_key = "Time Series (Daily)"
        else:
            time_series_key = "Time Series (60min)"

        # 只检查第一只股票
        with open(self.data_file, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    data = json.loads(line)
                    time_series = data.get(time_series_key, {})
                    return time_key in time_series
                except json.JSONDecodeError:
                    continue

        return False


async def update_realtime_prices(market: str, frequency: str) -> bool:
    """
    更新实时价格（主入口函数）

    流程：
    1. 读取现有 merged.jsonl 获取股票列表
    2. 检查当前时间点数据是否已存在
    3. 调用实时 API 获取最新价格
    4. 将新数据追加到 merged.jsonl

    Args:
        market: 市场类型 (cn/us/crypto)
        frequency: 频率 (daily/hourly)

    Returns:
        是否成功
    """
    print(f"\n[Data] 开始更新实时价格")
    print(f"  - 市场: {market}")
    print(f"  - 频率: {frequency}")

    fetcher = RealtimeDataFetcher(market, frequency)

    # 获取当前时间键
    time_key = fetcher.get_time_key()
    print(f"  - 时间键: {time_key}")

    # 检查数据是否已存在
    if fetcher.check_data_exists(time_key):
        print(f"[Data] 时间点 {time_key} 的数据已存在，跳过更新")
        return True

    # 获取股票列表
    symbols = fetcher.get_symbols_from_merged()
    if not symbols:
        print("[Error] 无法获取股票列表")
        return False

    print(f"[Data] 股票列表: {len(symbols)} 只")

    # 获取实时价格
    prices = await fetcher.fetch_realtime_prices(symbols)
    if not prices:
        print("[Error] 无法获取实时价格")
        return False

    # 追加到文件
    success = fetcher.append_prices_to_merged(prices, time_key)

    if success:
        print(f"[Data] 实时价格更新完成")
    else:
        print(f"[Warning] 实时价格更新失败")

    return success


async def main():
    """测试入口"""
    import argparse

    parser = argparse.ArgumentParser(description="实时数据获取")
    parser.add_argument("--market", "-m", default="cn", choices=["cn", "us", "crypto"])
    parser.add_argument("--frequency", "-f", default="daily", choices=["daily", "hourly"])
    args = parser.parse_args()

    await update_realtime_prices(args.market, args.frequency)


if __name__ == "__main__":
    asyncio.run(main())
