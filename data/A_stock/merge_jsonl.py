import json
import os
from pathlib import Path
from typing import Any, Dict

import pandas as pd


def _check_data_completeness(csv_path: Path) -> None:
    """检查数据完整性并打印警告

    Args:
        csv_path: CSV 文件路径
    """
    try:
        from validate_data import DataValidator

        data_dir = csv_path.parent
        validator = DataValidator(data_dir)
        result = validator.validate(use_api=False)  # 不调用 API，使用本地文件

        if result.missing_stocks:
            print("\n" + "=" * 60)
            print(f"⚠️  警告: 发现 {len(result.missing_stocks)} 只股票数据缺失:")
            for stock in result.missing_stocks:
                print(f"    - {stock}")
            print("\n修复命令:")
            print("    python get_daily_price_akshare.py --fix-missing")
            print("=" * 60 + "\n")

    except ImportError:
        # 验证模块不可用，跳过检查
        pass
    except Exception as e:
        print(f"⚠️  数据完整性检查失败: {e}")


def convert_a_stock_to_jsonl(
    csv_path: str = "A_stock_data/daily_prices_sse_50.csv",
    output_path: str = "merged.jsonl",
    stock_name_csv: str = "A_stock_data/sse_50_weight.csv",
) -> None:
    """Convert A-share CSV data to JSONL format compatible with the trading system.

    The output format matches the Alpha Vantage format used for NASDAQ data:
    - Each line is a JSON object for one stock
    - Contains "Meta Data" and "Time Series (Daily)" fields
    - Uses "1. buy price" (open), "2. high", "3. low", "4. sell price" (close), "5. volume"
    - Includes stock name from sse_50_weight.csv for better AI understanding

    Args:
        csv_path: Path to the A-share daily price CSV file (default: A_stock_data/daily_prices_sse_50.csv)
        output_path: Path to output JSONL file (default: A_stock_data/merged.jsonl)
        stock_name_csv: Path to SSE 50 weight CSV containing stock names (default: A_stock_data/sse_50_weight.csv)
    """
    csv_path = Path(csv_path)
    output_path = Path(output_path)
    stock_name_csv = Path(stock_name_csv)

    if not csv_path.exists():
        print(f"Error: CSV file not found: {csv_path}")
        return

    # 在合并前检查数据完整性
    _check_data_completeness(csv_path)

    print(f"Reading CSV file: {csv_path}")

    # Read CSV data
    df = pd.read_csv(csv_path)

    # Read stock name mapping
    stock_name_map = {}
    if stock_name_csv.exists():
        print(f"Reading stock names from: {stock_name_csv}")
        name_df = pd.read_csv(stock_name_csv)
        # Create mapping from con_code (ts_code) to stock_name
        stock_name_map = dict(zip(name_df["con_code"], name_df["stock_name"]))
        print(f"Loaded {len(stock_name_map)} stock names")
    else:
        print(f"Warning: Stock name file not found: {stock_name_csv}")

    print(f"Total records: {len(df)}")
    print(f"Columns: {df.columns.tolist()}")

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Group by stock symbol
    grouped = df.groupby("ts_code")

    print(f"Processing {len(grouped)} stocks...")

    # Track stocks already in CSV
    csv_symbols = set(df["ts_code"].unique())
    supplemented_count = 0

    with open(output_path, "w", encoding="utf-8") as fout:
        for ts_code, group_df in grouped:
            # Sort by date ascending
            group_df = group_df.sort_values("trade_date", ascending=True)

            # Get latest date for Meta Data
            latest_date = str(group_df["trade_date"].max())
            latest_date_formatted = f"{latest_date[:4]}-{latest_date[4:6]}-{latest_date[6:]}"

            # Build Time Series (Daily) data
            time_series = {}

            for idx, row in group_df.iterrows():
                date_str = str(row["trade_date"])
                date_formatted = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"

                # For the latest date, only include buy price (to prevent future information leakage)
                if date_str == latest_date:
                    time_series[date_formatted] = {"1. buy price": str(row["open"])}
                else:
                    time_series[date_formatted] = {
                        "1. buy price": str(row["open"]),
                        "2. high": str(row["high"]),
                        "3. low": str(row["low"]),
                        "4. sell price": str(row["close"]),
                        "5. volume": (
                            str(int(row["vol"] * 100)) if pd.notna(row["vol"]) else "0"
                        ),  # Convert to shares (vol is in 手, 1手=100股)
                    }

            # Get stock name from mapping
            stock_name = stock_name_map.get(ts_code, "Unknown")

            # Build complete JSON object
            json_obj = {
                "Meta Data": {
                    "1. Information": "Daily Prices (buy price, high, low, sell price) and Volumes",
                    "2. Symbol": ts_code,
                    "2.1. Name": stock_name,
                    "3. Last Refreshed": latest_date_formatted,
                    "4. Output Size": "Full Size",
                    "5. Time Zone": "Asia/Shanghai",
                },
                "Time Series (Daily)": time_series,
            }

            # Write to JSONL file
            fout.write(json.dumps(json_obj, ensure_ascii=False) + "\n")

        # Supplement with stocks from individual JSON files not in CSV
        # This handles SSE 50 composition changes - historical stocks may have
        # price data in individual JSON files but not in the current CSV
        json_dir = csv_path.parent
        print(f"\nChecking for supplemental stock data in {json_dir}...")

        for json_file in sorted(json_dir.glob("daily_prices_*.json")):
            # Skip index files
            if "index" in json_file.name.lower() or "000016" in json_file.name:
                continue

            # Extract symbol from filename: daily_prices_600941.SHH.json -> 600941.SH
            filename = json_file.stem  # daily_prices_600941.SHH
            symbol_part = filename.replace("daily_prices_", "")  # 600941.SHH
            # Handle both .SHH and .SH formats
            if symbol_part.endswith("H"):
                symbol = symbol_part[:-1]  # Remove trailing H
            else:
                symbol = symbol_part

            # Ensure .SH suffix
            if not symbol.endswith(".SH"):
                symbol = symbol + ".SH"

            # Skip if already in CSV
            if symbol in csv_symbols:
                continue

            # Read JSON file and write to output
            try:
                with open(json_file, "r", encoding="utf-8") as jf:
                    json_data = json.load(jf)

                # Update symbol in Meta Data to match expected format
                if "Meta Data" in json_data:
                    json_data["Meta Data"]["2. Symbol"] = symbol
                    # Add stock name if available
                    if symbol in stock_name_map:
                        json_data["Meta Data"]["2.1. Name"] = stock_name_map[symbol]

                fout.write(json.dumps(json_data, ensure_ascii=False) + "\n")
                print(f"  ✅ Supplemented: {symbol} from {json_file.name}")
                supplemented_count += 1

            except Exception as e:
                print(f"  ❌ Error reading {json_file}: {e}")

    print(f"\n✅ Data conversion completed: {output_path}")
    print(f"✅ Stocks from CSV: {len(grouped)}")
    print(f"✅ Supplemented from JSON: {supplemented_count}")
    print(f"✅ Total stocks: {len(grouped) + supplemented_count}")
    print(f"✅ File size: {output_path.stat().st_size / 1024 / 1024:.2f} MB")


if __name__ == "__main__":
    # Convert A-share data to JSONL format
    print("=" * 60)
    print("A-Share Data Converter")
    print("=" * 60)
    convert_a_stock_to_jsonl()
    print("=" * 60)
