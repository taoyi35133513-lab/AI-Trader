"""
Tushare 数据源实现

使用 tushare 库获取 A 股行情数据（重构自 get_daily_price_tushare.py）。
"""

import logging
import os
import time
from datetime import datetime, timedelta
from typing import List, Optional

import pandas as pd
from dotenv import load_dotenv

from .base import AStockDataSource

load_dotenv()

logger = logging.getLogger(__name__)


class TushareDataSource(AStockDataSource):
    """Tushare 数据源实现

    使用 Tushare Pro API 获取 A 股行情数据。
    """

    def __init__(
        self,
        token: Optional[str] = None,
        api_url: Optional[str] = None,
        max_retries: int = 3,
        retry_delay: float = 5.0,
        timeout: int = 120,
    ):
        """初始化 Tushare 数据源

        Args:
            token: Tushare API token，默认从环境变量 TUSHARE_TOKEN 获取
            api_url: Tushare API 地址，默认使用咸鱼版
            max_retries: 最大重试次数
            retry_delay: 重试延迟基数（秒）
            timeout: 请求超时（秒）
        """
        super().__init__(max_retries, retry_delay, request_interval=1.0)

        self.timeout = timeout

        # 延迟导入 tushare
        try:
            import tushare as ts
            import tushare.pro.client as client

            self.ts = ts
            self.client = client
        except ImportError:
            raise ImportError("请安装 tushare: pip install tushare")

        # 获取 token
        self.token = token or os.getenv("TUSHARE_TOKEN")
        if not self.token:
            raise ValueError("未找到 TUSHARE_TOKEN，请在 .env 文件中配置或通过参数传入")

        # 设置 API 地址（咸鱼版）
        if api_url:
            self.client.DataApi._DataApi__http_url = api_url
        else:
            self.client.DataApi._DataApi__http_url = "http://tushare.xyz:5000"

        # 初始化 pro_api
        # 使用内置 token（咸鱼版）
        self.pro = self.ts.pro_api("33d1aba4ba8d602806253cdb63f0848be5606cfda8f1ba2c3bc36c43")

        # 设置超时
        if hasattr(self.pro, "api") and hasattr(self.pro.api, "timeout"):
            self.pro.api.timeout = self.timeout

    def _api_call_with_retry(self, api_func, **kwargs):
        """带重试机制的 API 调用

        Args:
            api_func: API 函数
            **kwargs: 函数参数

        Returns:
            API 返回结果

        Raises:
            Exception: 所有重试都失败时抛出
        """
        import requests

        # 设置超时
        if hasattr(self.pro, "api") and hasattr(self.pro.api, "timeout"):
            self.pro.api.timeout = self.timeout

        for attempt in range(1, self.max_retries + 1):
            try:
                result = api_func(**kwargs)
                return result

            except (
                requests.exceptions.Timeout,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError,
            ) as e:
                if attempt < self.max_retries:
                    wait_time = self.retry_delay * attempt
                    logger.warning(f"网络超时错误 (尝试 {attempt}/{self.max_retries})，等待 {wait_time}s 后重试: {e}")
                    time.sleep(wait_time)
                else:
                    logger.error("所有重试尝试均失败")
                    raise

            except Exception as e:
                error_str = str(e).lower()
                if "timeout" in error_str or "timed out" in error_str:
                    if attempt < self.max_retries:
                        wait_time = self.retry_delay * attempt
                        logger.warning(f"超时错误 (尝试 {attempt}/{self.max_retries})，等待 {wait_time}s 后重试: {e}")
                        time.sleep(wait_time)
                    else:
                        raise
                else:
                    if attempt < self.max_retries:
                        wait_time = self.retry_delay * attempt
                        logger.warning(f"API 错误 (尝试 {attempt}/{self.max_retries})，等待 {wait_time}s 后重试: {e}")
                        time.sleep(wait_time)
                    else:
                        raise

        raise Exception("所有重试尝试均失败")

    def _get_last_month_dates(self) -> tuple:
        """获取上个月的起止日期

        Returns:
            (start_date, end_date) 格式 YYYYMMDD
        """
        today = datetime.now()
        first_day_of_this_month = today.replace(day=1)
        last_day_of_last_month = first_day_of_this_month - timedelta(days=1)
        first_day_of_last_month = last_day_of_last_month.replace(day=1)

        return (first_day_of_last_month.strftime("%Y%m%d"), last_day_of_last_month.strftime("%Y%m%d"))

    def get_index_constituents(
        self,
        index_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """获取指数成分股

        使用 pro.index_weight() 获取指数成分股权重

        Args:
            index_code: 指数代码（标准格式：000016.SH）
            start_date: 开始日期 YYYYMMDD（默认上月）
            end_date: 结束日期 YYYYMMDD（默认上月）

        Returns:
            DataFrame，包含 con_code, con_name, weight, trade_date
        """
        # 默认使用上个月日期
        if start_date is None or end_date is None:
            start_date, end_date = self._get_last_month_dates()

        logger.info(f"正在获取指数 {index_code} 的成分股 ({start_date} - {end_date})...")

        try:
            df = self._api_call_with_retry(
                self.pro.index_weight,
                index_code=index_code,
                start_date=start_date,
                end_date=end_date,
            )

            if df is None or df.empty:
                logger.warning(f"未获取到指数 {index_code} 的成分股数据")
                return pd.DataFrame()

            # Tushare 返回列：index_code, con_code, trade_date, weight
            # 需要获取成分股名称（可选，这里暂时留空）
            result = pd.DataFrame(
                {
                    "con_code": df["con_code"],
                    "con_name": "",  # Tushare 此接口不返回名称
                    "weight": df["weight"] if "weight" in df.columns else 0,
                    "trade_date": df["trade_date"],
                }
            )

            logger.info(f"获取到 {len(result)} 条成分股记录")
            return result

        except Exception as e:
            logger.error(f"获取指数成分股失败: {e}")
            return pd.DataFrame()

    def get_stock_daily(
        self,
        stock_codes: List[str],
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """获取个股日线数据

        使用 pro.daily() 批量获取股票日线数据
        注意：Tushare 有每次最多 6000 条记录的限制

        Args:
            stock_codes: 股票代码列表（标准格式：['600519.SH']）
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD

        Returns:
            DataFrame，包含 ts_code, trade_date, open, high, low, close, vol, amount
        """
        num_stocks = len(stock_codes)
        code_str = ",".join(stock_codes)

        # 计算批次大小（每次最多 6000 条记录）
        max_records = 6000
        batch_days = max(1, max_records // num_stocks)

        # 解析日期
        start_dt = datetime.strptime(start_date, "%Y%m%d")
        end_dt = datetime.strptime(end_date, "%Y%m%d")

        all_data = []
        current_start = start_dt
        total_batches = ((end_dt - start_dt).days // batch_days) + 1
        batch_num = 0

        while current_start <= end_dt:
            current_end = min(current_start + timedelta(days=batch_days - 1), end_dt)

            batch_start_str = current_start.strftime("%Y%m%d")
            batch_end_str = current_end.strftime("%Y%m%d")
            batch_num += 1

            logger.info(f"正在获取批次 {batch_num}/{total_batches}: {batch_start_str} - {batch_end_str}")

            try:
                df_batch = self._api_call_with_retry(
                    self.pro.daily,
                    ts_code=code_str,
                    start_date=batch_start_str,
                    end_date=batch_end_str,
                )

                if df_batch is not None and not df_batch.empty:
                    all_data.append(df_batch)
                    logger.info(f"  批次 {batch_num} 获取成功，{len(df_batch)} 条记录")

            except Exception as e:
                logger.error(f"  批次 {batch_num} 获取失败: {e}")

            # 批次间延迟
            if current_start < end_dt:
                time.sleep(1)

            current_start = current_end + timedelta(days=1)

        if not all_data:
            logger.warning("未获取到任何日线数据")
            return pd.DataFrame()

        result = pd.concat(all_data, ignore_index=True)
        result = result.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)

        logger.info(f"共获取 {len(result)} 条日线记录")
        return result

    def get_index_daily(
        self,
        index_code: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """获取指数日线数据

        使用 pro.index_daily() 获取指数日线数据

        Args:
            index_code: 指数代码（标准格式：000016.SH）
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD

        Returns:
            DataFrame，包含 ts_code, trade_date, open, high, low, close, vol, amount
        """
        logger.info(f"正在获取指数 {index_code} 的日线数据 ({start_date} - {end_date})...")

        try:
            df = self._api_call_with_retry(
                self.pro.index_daily,
                ts_code=index_code,
                start_date=start_date,
                end_date=end_date,
            )

            if df is None or df.empty:
                logger.warning(f"未获取到指数 {index_code} 的日线数据")
                return pd.DataFrame()

            logger.info(f"获取到 {len(df)} 条指数日线记录")
            return df

        except Exception as e:
            logger.error(f"获取指数日线数据失败: {e}")
            return pd.DataFrame()


# 测试代码
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    try:
        source = TushareDataSource()

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

    except Exception as e:
        print(f"测试失败: {e}")
