"""
AKShare 数据源实现

使用 akshare 库获取 A 股行情数据，替代 tushare。
"""

import logging
import time
from typing import List, Optional

import pandas as pd

from .base import AStockDataSource

logger = logging.getLogger(__name__)


class AKShareDataSource(AStockDataSource):
    """AKShare 数据源实现

    API 映射：
    - Tushare pro.index_weight() -> akshare ak.index_stock_cons_weight_csindex()
    - Tushare pro.daily() -> akshare ak.stock_zh_a_hist()
    - Tushare pro.index_daily() -> akshare ak.index_zh_a_hist()
    """

    # 指数代码映射：标准格式 -> 纯数字格式
    INDEX_CODE_MAP = {
        "000016.SH": "000016",  # 上证50
        "000300.SH": "000300",  # 沪深300
        "000905.SH": "000905",  # 中证500
        "000852.SH": "000852",  # 中证1000
        "399006.SZ": "399006",  # 创业板指
    }

    def __init__(
        self,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        request_interval: float = 0.5,
    ):
        """初始化 AKShare 数据源

        Args:
            max_retries: 最大重试次数
            retry_delay: 重试延迟基数（秒）
            request_interval: 请求间隔（秒）
        """
        super().__init__(max_retries, retry_delay, request_interval)

        # 延迟导入 akshare
        try:
            import akshare as ak

            self.ak = ak
        except ImportError:
            raise ImportError("请安装 akshare: pip install akshare")

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
        for attempt in range(1, self.max_retries + 1):
            try:
                result = api_func(**kwargs)
                time.sleep(self.request_interval)
                return result
            except Exception as e:
                if attempt < self.max_retries:
                    wait_time = self.retry_delay * attempt
                    logger.warning(f"API 调用失败 (尝试 {attempt}/{self.max_retries})，等待 {wait_time}s 后重试: {e}")
                    time.sleep(wait_time)
                else:
                    logger.error(f"API 调用失败，已达到最大重试次数: {e}")
                    raise

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

    def get_stock_daily(
        self,
        stock_codes: List[str],
        start_date: str,
        end_date: str,
        adjust: str = "",
    ) -> pd.DataFrame:
        """获取个股日线数据

        使用 ak.stock_zh_a_hist() 逐个获取股票日线数据
        注意：akshare 不支持批量获取，需要逐个股票查询

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

        # 转换日期格式：YYYYMMDD -> YYYY-MM-DD（akshare 需要）
        start_date_fmt = self.convert_date_format(start_date, "%Y%m%d", "%Y%m%d")
        end_date_fmt = self.convert_date_format(end_date, "%Y%m%d", "%Y%m%d")

        for i, ts_code in enumerate(stock_codes, 1):
            ak_code = self.convert_code_to_plain(ts_code)

            try:
                logger.info(f"获取 {ts_code} ({i}/{total}) 的日线数据...")

                df = self._api_call_with_retry(
                    self.ak.stock_zh_a_hist,
                    symbol=ak_code,
                    period="daily",
                    start_date=start_date_fmt,
                    end_date=end_date_fmt,
                    adjust=adjust,
                )

                if df is not None and not df.empty:
                    # 转换为标准格式
                    # akshare 返回列：日期, 股票代码, 开盘, 收盘, 最高, 最低, 成交量, 成交额, 振幅, 涨跌幅, 涨跌额, 换手率
                    # 注意：日期列可能是 datetime 类型，需要先转换为字符串
                    df_std = pd.DataFrame(
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
                    all_data.append(df_std)
                    logger.info(f"  {ts_code} 获取成功，{len(df_std)} 条记录")

            except Exception as e:
                logger.error(f"  {ts_code} 获取失败: {e}")
                continue

        if not all_data:
            logger.warning("未获取到任何日线数据")
            return pd.DataFrame()

        result = pd.concat(all_data, ignore_index=True)
        result = result.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)

        logger.info(f"共获取 {len(result)} 条日线记录，覆盖 {len(stock_codes)} 只股票")
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
