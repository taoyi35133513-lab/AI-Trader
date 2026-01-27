"""
AKShare 数据源实现

使用 akshare 库获取 A 股行情数据，替代 tushare。
支持多数据源备用和指数退避机制，应对限流问题。
"""

import logging
import random
import time
from typing import List, Optional, Callable, Any

import pandas as pd

from .base import AStockDataSource

logger = logging.getLogger(__name__)


class AKShareDataSource(AStockDataSource):
    """AKShare 数据源实现

    API 映射：
    - Tushare pro.index_weight() -> akshare ak.index_stock_cons_weight_csindex()
    - Tushare pro.daily() -> akshare ak.stock_zh_a_hist()
    - Tushare pro.index_daily() -> akshare ak.index_zh_a_hist()

    支持多数据源：
    - 主数据源：东方财富 (stock_zh_a_hist)
    - 备用数据源：腾讯 (stock_zh_a_hist_tx)
    """

    # 指数代码映射：标准格式 -> 纯数字格式
    INDEX_CODE_MAP = {
        "000016.SH": "000016",  # 上证50
        "000300.SH": "000300",  # 沪深300
        "000905.SH": "000905",  # 中证500
        "000852.SH": "000852",  # 中证1000
        "399006.SZ": "399006",  # 创业板指
    }

    # 限流错误关键词
    RATE_LIMIT_ERRORS = [
        "RemoteDisconnected",
        "Connection aborted",
        "ConnectionResetError",
        "Too Many Requests",
        "429",
        "rate limit",
    ]

    def __init__(
        self,
        max_retries: int = 5,
        retry_delay: float = 5.0,
        request_interval: float = 2.0,
        max_retry_delay: float = 60.0,
    ):
        """初始化 AKShare 数据源

        Args:
            max_retries: 最大重试次数（默认5次）
            retry_delay: 重试延迟基数（秒，默认5秒）
            request_interval: 请求间隔（秒，默认2秒）
            max_retry_delay: 最大重试延迟（秒，默认60秒）
        """
        super().__init__(max_retries, retry_delay, request_interval)
        self.max_retry_delay = max_retry_delay
        self._consecutive_failures = 0  # 连续失败计数

        # 延迟导入 akshare
        try:
            import akshare as ak

            self.ak = ak
        except ImportError:
            raise ImportError("请安装 akshare: pip install akshare")

    def _is_rate_limit_error(self, error: Exception) -> bool:
        """判断是否为限流错误"""
        error_str = str(error)
        return any(keyword in error_str for keyword in self.RATE_LIMIT_ERRORS)

    def _calculate_backoff_delay(self, attempt: int) -> float:
        """计算指数退避延迟（含随机抖动）

        Args:
            attempt: 当前尝试次数（从1开始）

        Returns:
            延迟时间（秒）
        """
        # 指数退避：base * 2^(attempt-1)
        base_delay = self.retry_delay * (2 ** (attempt - 1))
        # 添加随机抖动 (±20%)
        jitter = base_delay * 0.2 * (random.random() * 2 - 1)
        delay = base_delay + jitter
        # 限制最大延迟
        return min(delay, self.max_retry_delay)

    def _adaptive_request_interval(self) -> float:
        """自适应请求间隔（根据连续失败次数调整）"""
        if self._consecutive_failures == 0:
            return self.request_interval
        # 连续失败时增加间隔
        return min(self.request_interval * (1 + self._consecutive_failures * 0.5), 10.0)

    def _api_call_with_retry(self, api_func: Callable, **kwargs) -> Any:
        """带指数退避重试机制的 API 调用

        Args:
            api_func: API 函数
            **kwargs: 函数参数

        Returns:
            API 返回结果

        Raises:
            Exception: 所有重试都失败时抛出
        """
        last_exception = None

        for attempt in range(1, self.max_retries + 1):
            try:
                result = api_func(**kwargs)
                # 成功后重置连续失败计数
                self._consecutive_failures = 0
                # 自适应间隔等待
                time.sleep(self._adaptive_request_interval())
                return result
            except Exception as e:
                last_exception = e
                self._consecutive_failures += 1

                if attempt < self.max_retries:
                    # 判断是否为限流错误
                    if self._is_rate_limit_error(e):
                        wait_time = self._calculate_backoff_delay(attempt)
                        logger.warning(
                            f"API 限流 (尝试 {attempt}/{self.max_retries})，"
                            f"等待 {wait_time:.1f}s 后重试: {e}"
                        )
                    else:
                        wait_time = self.retry_delay * attempt
                        logger.warning(
                            f"API 调用失败 (尝试 {attempt}/{self.max_retries})，"
                            f"等待 {wait_time:.1f}s 后重试: {e}"
                        )
                    time.sleep(wait_time)
                else:
                    logger.error(f"API 调用失败，已达到最大重试次数: {e}")

        raise last_exception

    def get_index_constituents(
        self,
        index_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """获取指数成分股

        使用 ak.index_stock_cons_weight_csindex() 获取中证指数成分股权重

        Args:
            index_code: 指数代码（标准格式：000016.SH）
            start_date: 开始日期（可选，akshare 此接口不需要）
            end_date: 结束日期（可选）

        Returns:
            DataFrame，包含 con_code, con_name, weight, trade_date
        """
        ak_code = self.convert_code_to_plain(index_code)

        logger.info(f"正在获取指数 {index_code} 的成分股...")

        try:
            df = self._api_call_with_retry(self.ak.index_stock_cons_weight_csindex, symbol=ak_code)

            if df is None or df.empty:
                logger.warning(f"未获取到指数 {index_code} 的成分股数据")
                return pd.DataFrame()

            # 转换为标准格式
            # akshare 返回列：日期, 指数代码, 指数名称, 成分券代码, 成分券名称, 权重
            # 注意：日期列可能是 datetime 类型，需要先转换为字符串
            result = pd.DataFrame(
                {
                    "con_code": df["成分券代码"].astype(str).apply(self.convert_code_to_standard),
                    "con_name": df["成分券名称"],
                    "weight": df["权重"],
                    "trade_date": pd.to_datetime(df["日期"]).dt.strftime("%Y%m%d"),
                }
            )

            logger.info(f"获取到 {len(result)} 条成分股记录")
            return result

        except Exception as e:
            logger.error(f"获取指数成分股失败: {e}")
            return pd.DataFrame()

    def _fetch_stock_daily_eastmoney(
        self,
        ts_code: str,
        ak_code: str,
        start_date: str,
        end_date: str,
        adjust: str,
    ) -> Optional[pd.DataFrame]:
        """使用东方财富数据源获取日线数据

        Args:
            ts_code: 标准股票代码 (600519.SH)
            ak_code: 纯数字代码 (600519)
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
            adjust: 复权类型

        Returns:
            标准化的 DataFrame，失败返回 None
        """
        df = self._api_call_with_retry(
            self.ak.stock_zh_a_hist,
            symbol=ak_code,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust=adjust,
        )

        if df is None or df.empty:
            return None

        # 转换为标准格式
        return pd.DataFrame(
            {
                "ts_code": ts_code,
                "trade_date": pd.to_datetime(df["日期"]).dt.strftime("%Y%m%d"),
                "open": df["开盘"],
                "high": df["最高"],
                "low": df["最低"],
                "close": df["收盘"],
                "vol": df["成交量"] / 100,  # 股 -> 手
                "amount": df["成交额"],
            }
        )

    def _fetch_stock_daily_tencent(
        self,
        ts_code: str,
        ak_code: str,
        start_date: str,
        end_date: str,
    ) -> Optional[pd.DataFrame]:
        """使用腾讯数据源获取日线数据（备用）

        Args:
            ts_code: 标准股票代码 (600519.SH)
            ak_code: 纯数字代码 (600519)
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD

        Returns:
            标准化的 DataFrame，失败返回 None
        """
        # 腾讯数据源需要带市场前缀的代码
        if ts_code.endswith(".SH"):
            tx_code = f"sh{ak_code}"
        elif ts_code.endswith(".SZ"):
            tx_code = f"sz{ak_code}"
        else:
            tx_code = f"sh{ak_code}"  # 默认上海

        # 腾讯数据源日期格式为 YYYY-MM-DD
        start_date_tx = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
        end_date_tx = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"

        df = self._api_call_with_retry(
            self.ak.stock_zh_a_hist_tx,
            symbol=tx_code,
            start_date=start_date_tx,
            end_date=end_date_tx,
        )

        if df is None or df.empty:
            return None

        # 转换为标准格式
        # 腾讯返回列：date, open, close, high, low, amount
        return pd.DataFrame(
            {
                "ts_code": ts_code,
                "trade_date": pd.to_datetime(df["date"]).dt.strftime("%Y%m%d"),
                "open": df["open"],
                "high": df["high"],
                "low": df["low"],
                "close": df["close"],
                "vol": df["amount"] / 100 if "amount" in df.columns else 0,  # 股 -> 手
                "amount": 0,  # 腾讯源无成交额
            }
        )

    def get_stock_daily(
        self,
        stock_codes: List[str],
        start_date: str,
        end_date: str,
        adjust: str = "",
    ) -> pd.DataFrame:
        """获取个股日线数据（支持多数据源备用）

        首先尝试东方财富数据源，失败后自动切换到腾讯数据源。
        使用指数退避机制应对限流。

        Args:
            stock_codes: 股票代码列表（标准格式：['600519.SH']）
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
            adjust: 复权类型，""=不复权, "qfq"=前复权, "hfq"=后复权

        Returns:
            DataFrame，包含 ts_code, trade_date, open, high, low, close, vol, amount
        """
        all_data = []
        total = len(stock_codes)
        failed_stocks = []

        # 转换日期格式
        start_date_fmt = self.convert_date_format(start_date, "%Y%m%d", "%Y%m%d")
        end_date_fmt = self.convert_date_format(end_date, "%Y%m%d", "%Y%m%d")

        for i, ts_code in enumerate(stock_codes, 1):
            ak_code = self.convert_code_to_plain(ts_code)
            df_std = None

            # 尝试东方财富数据源
            try:
                logger.info(f"获取 {ts_code} ({i}/{total}) 的日线数据 [东方财富]...")
                df_std = self._fetch_stock_daily_eastmoney(
                    ts_code, ak_code, start_date_fmt, end_date_fmt, adjust
                )
                if df_std is not None and not df_std.empty:
                    all_data.append(df_std)
                    logger.info(f"  {ts_code} 获取成功，{len(df_std)} 条记录")
                    continue
            except Exception as e:
                logger.warning(f"  {ts_code} 东方财富数据源失败: {e}")

            # 尝试腾讯备用数据源
            try:
                logger.info(f"  {ts_code} 尝试腾讯备用数据源...")
                df_std = self._fetch_stock_daily_tencent(
                    ts_code, ak_code, start_date_fmt, end_date_fmt
                )
                if df_std is not None and not df_std.empty:
                    all_data.append(df_std)
                    logger.info(f"  {ts_code} 腾讯数据源获取成功，{len(df_std)} 条记录")
                    continue
            except Exception as e:
                logger.warning(f"  {ts_code} 腾讯数据源也失败: {e}")

            # 两个数据源都失败
            logger.error(f"  {ts_code} 所有数据源均失败")
            failed_stocks.append(ts_code)

        if failed_stocks:
            logger.warning(f"以下股票获取失败: {failed_stocks}")

        if not all_data:
            logger.warning("未获取到任何日线数据")
            return pd.DataFrame()

        result = pd.concat(all_data, ignore_index=True)
        result = result.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)

        logger.info(
            f"共获取 {len(result)} 条日线记录，"
            f"成功 {len(stock_codes) - len(failed_stocks)}/{len(stock_codes)} 只股票"
        )
        return result

    def get_index_daily(
        self,
        index_code: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """获取指数日线数据

        使用 ak.index_zh_a_hist() 获取指数历史行情

        Args:
            index_code: 指数代码（标准格式：000016.SH）
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD

        Returns:
            DataFrame，包含 ts_code, trade_date, open, high, low, close, vol, amount
        """
        ak_code = self.convert_code_to_plain(index_code)

        # 转换日期格式
        start_date_fmt = self.convert_date_format(start_date, "%Y%m%d", "%Y%m%d")
        end_date_fmt = self.convert_date_format(end_date, "%Y%m%d", "%Y%m%d")

        logger.info(f"正在获取指数 {index_code} 的日线数据 ({start_date} - {end_date})...")

        try:
            df = self._api_call_with_retry(
                self.ak.index_zh_a_hist,
                symbol=ak_code,
                period="daily",
                start_date=start_date_fmt,
                end_date=end_date_fmt,
            )

            if df is None or df.empty:
                logger.warning(f"未获取到指数 {index_code} 的日线数据")
                return pd.DataFrame()

            # 转换为标准格式
            # akshare 返回列：日期, 开盘, 收盘, 最高, 最低, 成交量, 成交额, 振幅, 涨跌幅, 涨跌额, 换手率
            # 注意：日期列可能是 datetime 类型，需要先转换为字符串
            result = pd.DataFrame(
                {
                    "ts_code": index_code,
                    "trade_date": pd.to_datetime(df["日期"]).dt.strftime("%Y%m%d"),
                    "open": df["开盘"],
                    "high": df["最高"],
                    "low": df["最低"],
                    "close": df["收盘"],
                    "vol": df["成交量"],
                    "amount": df["成交额"],
                }
            )

            logger.info(f"获取到 {len(result)} 条指数日线记录")
            return result

        except Exception as e:
            logger.error(f"获取指数日线数据失败: {e}")
            return pd.DataFrame()


# 测试代码
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    source = AKShareDataSource()

    # 测试获取成分股
    print("\n=== 测试获取上证50成分股 ===")
    df_cons = source.get_index_constituents("000016.SH")
    print(f"成分股数量: {len(df_cons)}")
    if not df_cons.empty:
        print(df_cons.head())

    # 测试获取个股日线
    print("\n=== 测试获取贵州茅台日线 ===")
    df_daily = source.get_stock_daily(["600519.SH"], "20250101", "20250110")
    print(f"日线数据: {df_daily.shape}")
    if not df_daily.empty:
        print(df_daily)

    # 测试获取指数日线
    print("\n=== 测试获取上证50指数日线 ===")
    df_index = source.get_index_daily("000016.SH", "20250101", "20250110")
    print(f"指数日线: {df_index.shape}")
    if not df_index.empty:
        print(df_index)
