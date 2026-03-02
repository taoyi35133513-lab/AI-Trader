"""
使用 AKShare 获取 A 股日线数据

支持增量更新：检查已存在的数据，只获取新日期的数据。
输出格式兼容 merge_jsonl.py 转换脚本。
"""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from data_source import create_data_source

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def get_latest_date_from_csv(csv_path: Path) -> Optional[str]:
    """从现有 CSV 文件获取最新日期

    Args:
        csv_path: CSV 文件路径

    Returns:
        最新日期字符串 (YYYYMMDD)，文件不存在或为空返回 None
    """
    if not csv_path.exists():
        return None

    try:
        df = pd.read_csv(csv_path)
        if df.empty or "trade_date" not in df.columns:
            return None

        # 确保 trade_date 是字符串格式
        df["trade_date"] = df["trade_date"].astype(str)
        latest_date = df["trade_date"].max()
        return latest_date
    except Exception as e:
        logger.warning("读取 CSV 获取最新日期失败: %s", e)
        return None


def _get_db_manager():
    """获取数据库管理器（延迟导入）"""
    import sys
    project_root = Path(__file__).parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    from data.database.connection import DatabaseManager
    return DatabaseManager


def get_latest_date_from_duckdb() -> Optional[str]:
    """从 DuckDB 获取股票日线最新日期

    Returns:
        最新日期字符串 (YYYYMMDD)，无数据返回 None
    """
    try:
        DatabaseManager = _get_db_manager()

        with DatabaseManager() as db:
            if not db.table_exists("stock_daily_prices"):
                return None

            df = db.query(
                "SELECT MAX(trade_date) as max_date FROM stock_daily_prices WHERE market = 'cn'"
            )
            if not df.empty and df.iloc[0]["max_date"] is not None:
                # 转换为 YYYYMMDD 格式
                max_date = df.iloc[0]["max_date"]
                if isinstance(max_date, str) and "-" in max_date:
                    return max_date.replace("-", "")
                if hasattr(max_date, 'strftime'):
                    return max_date.strftime("%Y%m%d")
                return str(max_date).replace("-", "")
        return None
    except Exception as e:
        logger.warning("从 DuckDB 获取股票最新日期失败: %s", e)
        return None


def get_latest_index_date_from_duckdb(index_code: str = "000016.SH") -> Optional[str]:
    """从 DuckDB 获取指数日线最新日期

    Args:
        index_code: 指数代码

    Returns:
        最新日期字符串 (YYYYMMDD)，无数据返回 None
    """
    try:
        DatabaseManager = _get_db_manager()

        with DatabaseManager() as db:
            if not db.table_exists("index_daily_prices"):
                return None

            df = db.query(
                "SELECT MAX(trade_date) as max_date FROM index_daily_prices WHERE index_code = ?",
                (index_code,)
            )
            if not df.empty:
                max_date = df.iloc[0]["max_date"]
                # 检查是否为空值（None 或 NaT）
                if max_date is None or pd.isna(max_date):
                    return None
                # 转换为 YYYYMMDD 格式
                if isinstance(max_date, str) and "-" in max_date:
                    return max_date.replace("-", "")
                # 处理 date 对象
                if hasattr(max_date, 'strftime'):
                    return max_date.strftime("%Y%m%d")
                return str(max_date).replace("-", "")
        return None
    except Exception as e:
        logger.warning("从 DuckDB 获取指数最新日期失败: %s", e)
        return None


def save_index_daily_to_duckdb(df: pd.DataFrame, index_code: str) -> bool:
    """将指数日线数据保存到 DuckDB

    Args:
        df: 指数日线 DataFrame
        index_code: 指数代码

    Returns:
        True 如果保存成功
    """
    if df.empty:
        return False

    try:
        DatabaseManager = _get_db_manager()

        with DatabaseManager() as db:
            # 确保表存在
            from data.database.models import create_table
            create_table("index_daily_prices")

            # 准备 DataFrame 用于插入
            df_insert = df.copy()
            df_insert["index_code"] = index_code
            df_insert["trade_date"] = df_insert["trade_date"].astype(str).apply(
                lambda x: f"{x[:4]}-{x[4:6]}-{x[6:]}" if len(x) == 8 and "-" not in x else x
            )

            # 重命名和选择列
            col_mapping = {"vol": "volume"}
            df_insert = df_insert.rename(columns=col_mapping)

            # 确保必需列存在
            required_cols = ["index_code", "trade_date", "open", "high", "low", "close", "volume"]
            for col in required_cols:
                if col not in df_insert.columns:
                    df_insert[col] = None

            if "amount" not in df_insert.columns:
                df_insert["amount"] = None

            # 只保留需要的列
            df_insert = df_insert[["index_code", "trade_date", "open", "high", "low", "close", "volume", "amount"]]

            # 获取要删除的日期列表
            dates_to_delete = df_insert["trade_date"].unique().tolist()

            # 批量删除已存在的记录
            if dates_to_delete:
                placeholders = ", ".join(["?" for _ in dates_to_delete])
                db.execute(
                    f"DELETE FROM index_daily_prices WHERE index_code = ? AND trade_date IN ({placeholders})",
                    (index_code, *dates_to_delete)
                )

            # 使用 insert_df 批量插入
            db.insert_df("index_daily_prices", df_insert)

            logger.info("已保存 %d 条指数日线数据到 DuckDB", len(df_insert))
            return True

    except Exception as e:
        logger.error("保存指数日线数据到 DuckDB 失败: %s", e)
        return False


def calculate_start_date(existing_latest: Optional[str], default_start: str) -> Tuple[str, bool]:
    """计算数据获取的起始日期

    Args:
        existing_latest: 现有数据的最新日期
        default_start: 默认起始日期

    Returns:
        (起始日期, 是否需要更新)
    """
    today = datetime.now().strftime("%Y%m%d")

    if existing_latest is None:
        return default_start, True

    # 如果现有数据已经是今天或之后，无需更新
    if existing_latest >= today:
        return existing_latest, False

    # 从现有最新日期的下一天开始获取
    try:
        latest_dt = datetime.strptime(existing_latest, "%Y%m%d")
        next_day = (latest_dt + timedelta(days=1)).strftime("%Y%m%d")
        return next_day, True
    except ValueError:
        return default_start, True


def get_daily_price_a_stock(
    index_code: str = "000016.SH",
    output_dir: Optional[Path] = None,
    daily_start_date: str = "20250101",
    fallback_csv: Optional[Path] = None,
    force_update: bool = False,
) -> Optional[pd.DataFrame]:
    """获取 A 股指数成分股日线数据（支持增量更新）

    使用 AKShare 获取指定指数的成分股日线数据。
    如果已有数据，只获取新日期的数据并合并。

    Args:
        index_code: 指数代码，默认上证50 (000016.SH)
        output_dir: 输出目录，默认为 ./A_stock_data
        daily_start_date: 数据开始日期，格式 YYYYMMDD（仅首次获取时使用）
        fallback_csv: 成分股列表备用 CSV 文件路径
        force_update: 强制全量更新，忽略现有数据

    Returns:
        DataFrame 包含日线数据，失败返回 None
    """
    # 设置输出目录和文件路径
    if output_dir is None:
        output_dir = Path(__file__).parent / "A_stock_data"
    else:
        output_dir = Path(output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)

    index_name = "sse_50" if index_code == "000016.SH" else index_code.replace(".", "_")
    daily_file = output_dir / f"daily_prices_{index_name}.csv"

    # 检查现有数据，决定起始日期
    existing_latest = None
    if not force_update:
        existing_latest = get_latest_date_from_csv(daily_file)
        if existing_latest is None:
            existing_latest = get_latest_date_from_duckdb()

    start_date, need_update = calculate_start_date(existing_latest, daily_start_date)
    daily_end_date = datetime.now().strftime("%Y%m%d")

    if not need_update:
        logger.info("数据已是最新 (最新日期: %s)，无需更新", existing_latest)
        # 返回现有数据
        if daily_file.exists():
            return pd.read_csv(daily_file)
        return None

    logger.info("增量更新: %s - %s (现有最新: %s)", start_date, daily_end_date, existing_latest or "无")

    # 创建 AKShare 数据源
    source = create_data_source("akshare")

    try:
        # 1. 获取指数成分股
        logger.info("正在获取指数成分股数据: %s", index_code)
        df_cons = source.get_index_constituents(index_code)

        # 如果 API 返回空数据，尝试读取备用文件
        if df_cons.empty:
            if fallback_csv and Path(fallback_csv).exists():
                logger.info("API 返回空数据，使用备用文件: %s", fallback_csv)
                df_cons = pd.read_csv(fallback_csv)
            else:
                logger.error("未获取到指数 %s 的成分股数据", index_code)
                return None

        # 提取唯一的成分股代码
        code_list = df_cons["con_code"].unique().tolist()
        num_stocks = len(code_list)
        logger.info("共 %d 只成分股", num_stocks)

        # 1.5 添加持仓中的股票（即使已从指数中剔除）
        try:
            from validate_data import DataValidator
            validator = DataValidator(output_dir)
            held_stocks = validator.get_all_held_stocks("daily")
            # 找出持仓中但不在成分股中的股票
            extra_held = held_stocks - set(code_list)
            if extra_held:
                logger.info("添加 %d 只持仓股票（已从指数剔除）: %s", len(extra_held), sorted(extra_held))
                code_list = list(set(code_list) | extra_held)
        except Exception as e:
            logger.warning("获取持仓股票失败: %s", e)

        # 2. 获取日线数据
        logger.info("正在获取日线数据: %s - %s", start_date, daily_end_date)
        df_new = source.get_stock_daily(code_list, start_date, daily_end_date)

        if df_new.empty:
            logger.warning("未获取到新的日线数据")
            if daily_file.exists():
                return pd.read_csv(daily_file)
            return None

        # 3. 合并数据
        if daily_file.exists() and not force_update:
            logger.info("合并现有数据与新数据...")
            df_existing = pd.read_csv(daily_file)
            # 确保 trade_date 格式一致
            df_existing["trade_date"] = df_existing["trade_date"].astype(str)
            df_new["trade_date"] = df_new["trade_date"].astype(str)

            # 合并并去重（以 ts_code + trade_date 为键）
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            df_combined = df_combined.drop_duplicates(
                subset=["ts_code", "trade_date"],
                keep="last"  # 保留新数据
            )
            df_combined = df_combined.sort_values(["ts_code", "trade_date"])
            df_daily = df_combined
            logger.info("合并后记录数: %d (新增: %d)", len(df_daily), len(df_new))
        else:
            df_daily = df_new

        # 4. 保存数据
        df_daily.to_csv(daily_file, index=False, encoding="utf-8")
        logger.info("数据已保存: %s (shape: %s)", daily_file, df_daily.shape)

        return df_daily

    except Exception as e:
        logger.error("获取数据失败: %s", e)
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
        logger.warning("DataFrame 为空")
        return {}

    # 按日期降序排列
    df = df.sort_values(by="trade_date", ascending=False).reset_index(drop=True)

    # 获取最后更新日期
    last_refreshed = str(df.iloc[0]["trade_date"])
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
        logger.info("JSON 数据已保存: %s", output_file)

    return json_data


def get_latest_index_date_from_json(json_path: Path) -> Optional[str]:
    """从现有 JSON 文件获取最新日期

    Args:
        json_path: JSON 文件路径

    Returns:
        最新日期字符串 (YYYYMMDD)，文件不存在或为空返回 None
    """
    if not json_path.exists():
        return None

    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        time_series = data.get("Time Series (Daily)", {})
        if not time_series:
            return None

        # 获取所有日期并找最大值
        dates = list(time_series.keys())
        if not dates:
            return None

        # 日期格式为 YYYY-MM-DD，转换为 YYYYMMDD
        latest_date = max(dates)
        return latest_date.replace("-", "")
    except Exception as e:
        logger.warning("读取 JSON 获取最新日期失败: %s", e)
        return None


def get_index_daily_data(
    index_code: str = "000016.SH",
    start_date: str = "20250101",
    end_date: Optional[str] = None,
    output_dir: Optional[Path] = None,
    force_update: bool = False,
) -> Optional[pd.DataFrame]:
    """获取指数日线数据（支持增量更新）

    使用 AKShare 获取指数日线数据，支持增量更新。
    如果已有数据，只获取新日期的数据并合并。
    同时保存到 JSON 文件和 DuckDB。

    Args:
        index_code: 指数代码，默认上证50 (000016.SH)
        start_date: 开始日期 YYYYMMDD（仅首次获取时使用）
        end_date: 结束日期 YYYYMMDD，默认今天
        output_dir: 输出目录
        force_update: 强制全量更新，忽略现有数据

    Returns:
        DataFrame 包含指数日线数据，失败返回 None
    """
    # 设置输出目录
    if output_dir is None:
        output_dir = Path(__file__).parent
    else:
        output_dir = Path(output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)

    index_name = "sse_50" if index_code == "000016.SH" else index_code.replace(".", "_")
    json_file = output_dir / f"index_daily_{index_name}.json"

    # 检查现有数据，决定起始日期
    existing_latest = None
    if not force_update:
        # 优先检查 DuckDB
        existing_latest = get_latest_index_date_from_duckdb(index_code)
        # 备用检查 JSON 文件
        if existing_latest is None:
            existing_latest = get_latest_index_date_from_json(json_file)

    actual_start_date, need_update = calculate_start_date(existing_latest, start_date)

    # 默认结束日期为今天
    if end_date is None:
        end_date = datetime.now().strftime("%Y%m%d")

    if not need_update:
        logger.info("指数数据已是最新 (最新日期: %s)，无需更新", existing_latest)
        # 返回现有数据（从 JSON 读取）
        if json_file.exists():
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                time_series = data.get("Time Series (Daily)", {})
                if time_series:
                    records = []
                    for date_str, values in time_series.items():
                        records.append({
                            "trade_date": date_str.replace("-", ""),
                            "open": float(values["1. open"]),
                            "high": float(values["2. high"]),
                            "low": float(values["3. low"]),
                            "close": float(values["4. close"]),
                            "vol": int(values["5. volume"]),
                        })
                    return pd.DataFrame(records)
            except Exception as e:
                logger.warning("读取现有 JSON 数据失败: %s", e)
        return None

    logger.info("指数增量更新: %s - %s (现有最新: %s)", actual_start_date, end_date, existing_latest or "无")

    # 创建 AKShare 数据源
    source = create_data_source("akshare")

    try:
        df_new = source.get_index_daily(index_code, actual_start_date, end_date)

        if df_new.empty:
            logger.warning("未获取到新的指数日线数据")
            return None

        # 合并现有数据
        df_combined = df_new
        if json_file.exists() and not force_update:
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                time_series = data.get("Time Series (Daily)", {})
                if time_series:
                    existing_records = []
                    for date_str, values in time_series.items():
                        existing_records.append({
                            "trade_date": date_str.replace("-", ""),
                            "open": float(values["1. open"]),
                            "high": float(values["2. high"]),
                            "low": float(values["3. low"]),
                            "close": float(values["4. close"]),
                            "vol": int(values["5. volume"]),
                        })
                    df_existing = pd.DataFrame(existing_records)

                    # 合并并去重
                    df_new["trade_date"] = df_new["trade_date"].astype(str)
                    df_existing["trade_date"] = df_existing["trade_date"].astype(str)
                    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                    df_combined = df_combined.drop_duplicates(subset=["trade_date"], keep="last")
                    df_combined = df_combined.sort_values("trade_date")
                    logger.info("合并后指数记录数: %d (新增: %d)", len(df_combined), len(df_new))
            except Exception as e:
                logger.warning("合并现有数据失败: %s", e)

        # 保存为 JSON
        convert_index_daily_to_json(df_combined, symbol=index_code, output_file=json_file)

        # 保存到 DuckDB
        if save_index_daily_to_duckdb(df_new, index_code):
            logger.info("指数数据已保存到 DuckDB")

        return df_combined

    except Exception as e:
        logger.error("获取指数日线数据失败: %s", e)
        logger.exception("详细错误信息")
        return None


def update_weight_file(
    index_code: str = "000016.SH",
    output_dir: Optional[Path] = None,
) -> bool:
    """从 API 更新本地成分股权重文件

    确保本地备用文件与最新指数成分保持一致。

    Args:
        index_code: 指数代码
        output_dir: 输出目录

    Returns:
        True 如果更新成功
    """
    if output_dir is None:
        output_dir = Path(__file__).parent / "A_stock_data"

    index_name = "sse_50" if index_code == "000016.SH" else index_code.replace(".", "_")
    weight_file = output_dir / f"{index_name}_weight.csv"

    source = create_data_source("akshare")
    df_cons = source.get_index_constituents(index_code)

    if df_cons.empty:
        logger.warning("从 API 获取成分股失败，权重文件未更新")
        return False

    # 读取现有文件以保留股票名称
    stock_name_map = {}
    if weight_file.exists():
        try:
            old_df = pd.read_csv(weight_file)
            if "stock_name" in old_df.columns:
                stock_name_map = dict(zip(old_df["con_code"], old_df["stock_name"]))
        except Exception as e:
            logger.warning("读取现有权重文件失败: %s", e)

    # 添加股票名称
    df_cons["stock_name"] = df_cons["con_code"].apply(
        lambda x: stock_name_map.get(x, df_cons[df_cons["con_code"] == x]["con_name"].iloc[0]
                                      if "con_name" in df_cons.columns and len(df_cons[df_cons["con_code"] == x]) > 0
                                      else "Unknown")
    )

    # 添加 index_code 列
    df_cons["index_code"] = index_code

    # 重排列以匹配预期格式
    columns = ["index_code", "con_code", "trade_date", "weight"]
    if "stock_name" in df_cons.columns:
        columns.append("stock_name")
    df_cons = df_cons[[c for c in columns if c in df_cons.columns]]

    df_cons.to_csv(weight_file, index=False, encoding="utf-8")
    logger.info("成分股权重文件已更新: %s (%d 只成分股)", weight_file, len(df_cons))
    return True


def fetch_missing_stocks(
    missing_codes: List[str],
    output_dir: Optional[Path] = None,
    start_date: str = "20250101",
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """获取缺失股票的历史数据并合并到现有数据

    Args:
        missing_codes: 缺失的股票代码列表
        output_dir: 输出目录
        start_date: 起始日期 YYYYMMDD
        end_date: 结束日期，默认今天

    Returns:
        获取到的新数据 DataFrame
    """
    if not missing_codes:
        logger.info("没有缺失的股票需要获取")
        return pd.DataFrame()

    if output_dir is None:
        output_dir = Path(__file__).parent / "A_stock_data"

    if end_date is None:
        end_date = datetime.now().strftime("%Y%m%d")

    daily_file = output_dir / "daily_prices_sse_50.csv"

    logger.info("获取 %d 只缺失股票的数据: %s", len(missing_codes), missing_codes)
    logger.info("日期范围: %s - %s", start_date, end_date)

    source = create_data_source("akshare")
    df_new = source.get_stock_daily(missing_codes, start_date, end_date)

    if df_new.empty:
        logger.warning("未获取到缺失股票的数据")
        return df_new

    # 合并到现有数据
    if daily_file.exists():
        df_existing = pd.read_csv(daily_file)
        df_existing["trade_date"] = df_existing["trade_date"].astype(str)
        df_new["trade_date"] = df_new["trade_date"].astype(str)

        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        df_combined = df_combined.drop_duplicates(
            subset=["ts_code", "trade_date"],
            keep="last"
        )
        df_combined = df_combined.sort_values(["ts_code", "trade_date"])
    else:
        df_combined = df_new

    df_combined.to_csv(daily_file, index=False, encoding="utf-8")
    logger.info("已添加 %d 条记录到 %s", len(df_new), daily_file)
    logger.info("现有股票数: %d", df_combined["ts_code"].nunique())

    return df_new


def fix_missing_data(
    start_date: str = "20250101",
    output_dir: Optional[Path] = None,
    frequency: str = "daily",
) -> bool:
    """检测并修复缺失的股票数据

    自动检测缺失的成分股和持仓股票，并获取其历史数据。
    注意：被剔除的成分股如果仍在持仓中，也需要更新行情数据。

    Args:
        start_date: 起始日期
        output_dir: 输出目录
        frequency: 数据频率 ("daily" 或 "hourly")

    Returns:
        True 如果修复成功或无需修复
    """
    from validate_data import DataValidator

    if output_dir is None:
        output_dir = Path(__file__).parent / "A_stock_data"

    logger.info("=" * 50)
    logger.info("检测缺失数据...")
    logger.info("=" * 50)

    validator = DataValidator(output_dir)
    result = validator.validate(use_api=True, frequency=frequency)

    if result.error_message:
        logger.error("验证失败: %s", result.error_message)
        return False

    if result.weight_file_outdated:
        logger.info("成分股权重文件需要更新，正在更新...")
        update_weight_file(output_dir=output_dir)

    # 收集所有缺失的股票（成分股 + 持仓股票）
    all_missing = list(set(result.missing_stocks + result.missing_held_stocks))

    if not all_missing:
        logger.info("没有缺失的股票数据")
        return True

    # 分类显示
    if result.missing_stocks:
        logger.info("发现 %d 只缺失成分股: %s", len(result.missing_stocks), result.missing_stocks)

    if result.missing_held_stocks:
        logger.info("发现 %d 只持仓缺失行情（已剔除但仍持有）: %s", len(result.missing_held_stocks), result.missing_held_stocks)

    logger.info("正在获取 %d 只缺失股票数据...", len(all_missing))
    df_new = fetch_missing_stocks(
        all_missing,
        output_dir=output_dir,
        start_date=start_date,
    )

    if not df_new.empty:
        logger.info("成功获取 %d 只股票的数据", len(all_missing))
        return True
    else:
        logger.warning("部分股票数据获取失败")
        return False


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="获取 A 股日线数据")
    parser.add_argument("--force", action="store_true", help="强制全量更新")
    parser.add_argument("--start-date", default="20251001", help="起始日期 (YYYYMMDD)")
    parser.add_argument("--fix-missing", action="store_true", help="检测并获取缺失股票数据")
    parser.add_argument("--update-weights", action="store_true", help="更新成分股权重文件")
    args = parser.parse_args()

    # 备用成分股文件路径
    fallback_path = Path(__file__).parent / "A_stock_data" / "sse_50_weight.csv"
    output_dir = Path(__file__).parent / "A_stock_data"

    # 更新权重文件
    if args.update_weights:
        logger.info("=" * 50)
        logger.info("更新成分股权重文件...")
        logger.info("=" * 50)
        update_weight_file(output_dir=output_dir)

    # 获取成分股日线数据
    logger.info("=" * 50)
    logger.info("使用 AKShare 获取 A 股数据")
    logger.info("=" * 50)

    df = get_daily_price_a_stock(
        index_code="000016.SH",
        daily_start_date=args.start_date,
        fallback_csv=fallback_path,
        force_update=args.force,
    )

    # 获取指数日线数据
    logger.info("=" * 50)
    logger.info("获取指数日线数据...")
    logger.info("=" * 50)
    df_index = get_index_daily_data(
        index_code="000016.SH",
        start_date=args.start_date,
        force_update=args.force,
    )

    # 检测并修复缺失数据
    if args.fix_missing:
        fix_missing_data(
            start_date=args.start_date,
            output_dir=output_dir,
        )
