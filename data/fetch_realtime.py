"""
实时数据获取模块

功能：
1. 从实时 API 获取最新价格
2. 将新数据追加到 merged.jsonl
3. 保持与历史数据格式完全一致
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# 添加项目根目录到 path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

logger = logging.getLogger(__name__)


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
            logger.warning("数据文件不存在: %s", self.data_file)
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

        优先使用 ts.get_realtime_quotes()（无配额限制，盘中实时）；
        回退到 pro.rt_min（每天 10 次限制）；
        最终回退到 pro.daily（仅收盘后有数据）。

        Args:
            symbols: 股票代码列表 (如 ['600519.SH', '000001.SZ'])

        Returns:
            {symbol: {open, high, low, close, volume}, ...}
        """
        prices = {}

        try:
            import tushare as ts

            # 方案1: ts.get_realtime_quotes() — 无配额限制，实时数据
            codes = [s.split(".")[0] for s in symbols]
            code_to_symbol = {s.split(".")[0]: s for s in symbols}

            try:
                logger.info("正在通过 tushare get_realtime_quotes 获取实时行情...")
                df = ts.get_realtime_quotes(codes)

                if df is not None and not df.empty:
                    for _, row in df.iterrows():
                        code = row["code"]
                        sym = code_to_symbol.get(code)
                        if not sym:
                            continue
                        try:
                            price_val = float(row["price"]) if row["price"] and row["price"] != "0.000" else 0
                            if price_val <= 0:
                                continue
                            prices[sym] = {
                                "open": float(row["open"]) if row["open"] else price_val,
                                "high": float(row["high"]) if row["high"] else price_val,
                                "low": float(row["low"]) if row["low"] else price_val,
                                "close": price_val,
                                "volume": int(float(row["volume"])) if row["volume"] else 0,
                            }
                        except (ValueError, TypeError) as e:
                            logger.warning("获取 %s 价格失败: %s", sym, e)
            except Exception as e:
                logger.warning("get_realtime_quotes 失败: %s", e)

            # 方案2: pro.rt_min (每天 10 次限制)
            if not prices:
                from tools.tushare_client import get_tushare_pro
                pro = get_tushare_pro()
                try:
                    logger.info("回退到 tushare rt_min...")
                    ts_codes = ",".join(symbols)
                    df = pro.rt_min(ts_code=ts_codes, freq="60MIN")
                    if df is not None and not df.empty:
                        for _, row in df.iterrows():
                            sym = row["ts_code"]
                            prices[sym] = {
                                "open": float(row["open"]),
                                "high": float(row["high"]),
                                "low": float(row["low"]),
                                "close": float(row["close"]),
                                "volume": int(float(row["vol"])),
                            }
                except Exception as e:
                    logger.warning("rt_min 失败: %s", e)

            # 方案3: pro.daily (仅收盘后)
            if not prices:
                from tools.tushare_client import get_tushare_pro
                pro = get_tushare_pro()
                today = datetime.now().strftime("%Y%m%d")
                logger.info("回退到 tushare daily (trade_date=%s)...", today)
                try:
                    df = pro.daily(trade_date=today)
                    if df is not None and not df.empty:
                        symbol_set = set(symbols)
                        for _, row in df[df["ts_code"].isin(symbol_set)].iterrows():
                            prices[row["ts_code"]] = {
                                "open": float(row["open"]),
                                "high": float(row["high"]),
                                "low": float(row["low"]),
                                "close": float(row["close"]),
                                "volume": int(float(row["vol"])),
                            }
                except Exception as e:
                    logger.warning("daily 失败: %s", e)

            logger.info("成功获取 %d/%d 只股票价格", len(prices), len(symbols))

        except ImportError:
            logger.error("请安装 tushare: pip install tushare")
        except Exception as e:
            logger.error("获取 A 股实时行情失败: %s", e)

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

            logger.info("正在获取美股实时行情...")

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
                    logger.warning("获取 %s 价格失败: %s", symbol, e)
                    continue

            logger.info("成功获取 %d/%d 只股票价格", len(prices), len(symbols))

        except ImportError:
            logger.error("请安装 yfinance: pip install yfinance")
        except Exception as e:
            logger.error("获取美股实时行情失败: %s", e)

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
            logger.warning("加密货币实时数据暂不支持")
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
            logger.warning("没有价格数据需要追加")
            return False

        if time_key is None:
            time_key = self.get_time_key()

        # 确定时间序列键名
        if self.frequency == "daily":
            time_series_key = "Time Series (Daily)"
        else:
            time_series_key = "Time Series (60min)"

        logger.info("追加价格数据到 %s, 时间键: %s", self.data_file, time_key)

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

        logger.info("成功更新 %d 只股票的价格数据", updated_count)
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
    logger.info("开始更新实时价格: 市场=%s, 频率=%s", market, frequency)

    fetcher = RealtimeDataFetcher(market, frequency)

    # 获取当前时间键
    time_key = fetcher.get_time_key()
    logger.info("时间键: %s", time_key)

    # 检查数据是否已存在
    if fetcher.check_data_exists(time_key):
        logger.info("时间点 %s 的数据已存在，跳过更新", time_key)
        return True

    # 获取股票列表
    symbols = fetcher.get_symbols_from_merged()
    if not symbols:
        logger.error("无法获取股票列表")
        return False

    logger.info("股票列表: %d 只", len(symbols))

    # 获取实时价格
    prices = await fetcher.fetch_realtime_prices(symbols)
    if not prices:
        logger.error("无法获取实时价格")
        return False

    # 追加到文件
    success = fetcher.append_prices_to_merged(prices, time_key)

    if success:
        logger.info("实时价格更新完成")
    else:
        logger.warning("实时价格更新失败")

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
