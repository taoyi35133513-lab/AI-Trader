"""
使用 AKShare 获取 A 股日线数据

功能与 get_daily_price_tushare.py 完全一致，作为 tushare 的替代方案。
输出格式兼容现有的 merge_jsonl_tushare.py 转换脚本。
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from data_source import create_data_source

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def get_daily_price_a_stock(
    index_code: str = "000016.SH",
    output_dir: Optional[Path] = None,
    daily_start_date: str = "20250101",
    fallback_csv: Optional[Path] = None,
) -> Optional[pd.DataFrame]:
    """获取 A 股指数成分股日线数据

    使用 AKShare 获取指定指数的成分股日线数据，输出格式与 tushare 版本一致。

    Args:
        index_code: 指数代码，默认上证50 (000016.SH)
        output_dir: 输出目录，默认为 ./A_stock_data
        daily_start_date: 数据开始日期，格式 YYYYMMDD
        fallback_csv: 成分股列表备用 CSV 文件路径

    Returns:
        DataFrame 包含日线数据，失败返回 None
    """
    # 创建 AKShare 数据源
    source = create_data_source("akshare")

    # 结束日期为今天
    daily_end_date = datetime.now().strftime("%Y%m%d")

    try:
        # 1. 获取指数成分股
        print(f"正在获取指数成分股数据: {index_code}")
        df_cons = source.get_index_constituents(index_code)

        # 如果 API 返回空数据，尝试读取备用文件
        if df_cons.empty:
            if fallback_csv and Path(fallback_csv).exists():
                print(f"API 返回空数据，使用备用文件: {fallback_csv}")
                df_cons = pd.read_csv(fallback_csv)
            else:
                print(f"未获取到指数 {index_code} 的成分股数据")
                return None

        # 提取唯一的成分股代码
        code_list = df_cons["con_code"].unique().tolist()
        num_stocks = len(code_list)
        print(f"共 {num_stocks} 只成分股")

        # 2. 获取日线数据
        print(f"正在获取日线数据: {daily_start_date} - {daily_end_date}")
        df_daily = source.get_stock_daily(code_list, daily_start_date, daily_end_date)

        if df_daily.empty:
            print("未获取到日线数据")
            return None

        # 3. 保存数据
        if output_dir is None:
            output_dir = Path(__file__).parent / "A_stock_data"
        else:
            output_dir = Path(output_dir)

        output_dir.mkdir(parents=True, exist_ok=True)

        # 文件命名与 tushare 版本一致
        index_name = "sse_50" if index_code == "000016.SH" else index_code.replace(".", "_")
        daily_file = output_dir / f"daily_prices_{index_name}.csv"
        df_daily.to_csv(daily_file, index=False, encoding="utf-8")
        print(f"数据已保存: {daily_file} (shape: {df_daily.shape})")

        return df_daily

    except Exception as e:
        print(f"获取数据失败: {e}")
        logger.exception("详细错误信息")
        return None


def convert_index_daily_to_json(
    df: pd.DataFrame,
    symbol: str = "000016.SH",
    output_file: Optional[Path] = None,
) -> Dict:
    """将指数日线数据转换为 JSON 格式（与 Alpha Vantage 格式兼容）

    Args:
        df: 指数日线 DataFrame
        symbol: 指数代码
        output_file: 输出 JSON 文件路径

    Returns:
        JSON 格式的数据字典
    """
    if df.empty:
        print("警告: DataFrame 为空")
        return {}

    # 按日期降序排列
    df = df.sort_values(by="trade_date", ascending=False).reset_index(drop=True)

    # 获取最后更新日期
    last_refreshed = df.iloc[0]["trade_date"]
    last_refreshed_formatted = f"{last_refreshed[:4]}-{last_refreshed[4:6]}-{last_refreshed[6:]}"

    # 构建 JSON 结构
    json_data = {
        "Meta Data": {
            "1. Information": "Daily Prices (open, high, low, close) and Volumes",
            "2. Symbol": symbol,
            "3. Last Refreshed": last_refreshed_formatted,
            "4. Output Size": "Compact",
            "5. Time Zone": "Asia/Shanghai",
        },
        "Time Series (Daily)": {},
    }

    # 转换每一行数据
    for _, row in df.iterrows():
        trade_date = str(row["trade_date"])
        date_formatted = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"

        json_data["Time Series (Daily)"][date_formatted] = {
            "1. open": f"{row['open']:.4f}",
            "2. high": f"{row['high']:.4f}",
            "3. low": f"{row['low']:.4f}",
            "4. close": f"{row['close']:.4f}",
            "5. volume": str(int(row["vol"])) if pd.notna(row["vol"]) else "0",
        }

    # 保存到文件
    if output_file:
        output_file = Path(output_file)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(json_data, f, indent=4, ensure_ascii=False)
        print(f"JSON 数据已保存: {output_file}")

    return json_data


def get_index_daily_data(
    index_code: str = "000016.SH",
    start_date: str = "20250101",
    end_date: Optional[str] = None,
    output_dir: Optional[Path] = None,
) -> Optional[pd.DataFrame]:
    """获取指数日线数据并转换为 JSON 格式

    Args:
        index_code: 指数代码，默认上证50 (000016.SH)
        start_date: 开始日期 YYYYMMDD
        end_date: 结束日期 YYYYMMDD，默认今天
        output_dir: 输出目录

    Returns:
        DataFrame 包含指数日线数据，失败返回 None
    """
    # 创建 AKShare 数据源
    source = create_data_source("akshare")

    # 默认结束日期为今天
    if end_date is None:
        end_date = datetime.now().strftime("%Y%m%d")

    try:
        print(f"正在获取指数日线数据: {index_code} ({start_date} - {end_date})")
        df = source.get_index_daily(index_code, start_date, end_date)

        if df.empty:
            print(f"未获取到指数 {index_code} 的日线数据")
            return None

        # 设置输出目录
        if output_dir is None:
            output_dir = Path(__file__).parent
        else:
            output_dir = Path(output_dir)

        output_dir.mkdir(parents=True, exist_ok=True)

        # 保存为 JSON
        index_name = "sse_50" if index_code == "000016.SH" else index_code.replace(".", "_")
        json_file = output_dir / f"index_daily_{index_name}.json"
        convert_index_daily_to_json(df, symbol=index_code, output_file=json_file)

        return df

    except Exception as e:
        print(f"获取指数日线数据失败: {e}")
        logger.exception("详细错误信息")
        return None


if __name__ == "__main__":
    # 备用成分股文件路径
    fallback_path = Path(__file__).parent / "A_stock_data" / "sse_50_weight.csv"

    # 获取成分股日线数据
    print("=" * 50)
    print("使用 AKShare 获取 A 股数据")
    print("=" * 50)

    df = get_daily_price_a_stock(index_code="000016.SH", daily_start_date="20251001", fallback_csv=fallback_path)

    # 获取指数日线数据
    print("\n" + "=" * 50)
    print("获取指数日线数据...")
    print("=" * 50)
    df_index = get_index_daily_data(index_code="000016.SH", start_date="20251001")
