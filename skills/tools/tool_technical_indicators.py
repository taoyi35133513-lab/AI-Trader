"""技术指标计算 MCP 工具

提供 MA、MACD、RSI 等技术指标计算能力。
从本地价格数据（JSONL/DuckDB）读取历史价格进行计算。
"""

import json
import logging
from pathlib import Path
from typing import List, Optional

from fastmcp import FastMCP

logger = logging.getLogger(__name__)

mcp = FastMCP("SkillTA")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def _get_close_prices(symbol: str, end_date: str, count: int = 60) -> List[float]:
    """Get recent close prices from merged.jsonl for a symbol."""
    merged_file = PROJECT_ROOT / "data" / "A_stock" / "merged.jsonl"
    if not merged_file.exists():
        return []

    with open(merged_file, "r", encoding="utf-8") as f:
        for line in f:
            try:
                data = json.loads(line)
                if data.get("Meta Data", {}).get("2. Symbol") != symbol:
                    continue

                ts = data.get("Time Series (Daily)", {})
                # Sort dates descending, filter <= end_date
                dates = sorted(
                    [d for d in ts.keys() if d <= end_date],
                    reverse=True,
                )[:count]

                prices = []
                for d in reversed(dates):
                    close = ts[d].get("4. sell price") or ts[d].get("4. close")
                    if close:
                        prices.append(float(close))
                return prices
            except (json.JSONDecodeError, KeyError, ValueError):
                continue
    return []


@mcp.tool()
def calculate_ma(symbol: str, date: str, periods: str = "5,10,20,60") -> str:
    """计算移动平均线 (MA)

    Args:
        symbol: 股票代码，如 600519.SH
        date: 日期，如 2026-03-17
        periods: 均线周期，逗号分隔，默认 5,10,20,60
    """
    period_list = [int(p.strip()) for p in periods.split(",")]
    max_period = max(period_list)
    prices = _get_close_prices(symbol, date, max_period + 5)

    if not prices:
        return json.dumps({"error": f"No price data for {symbol} up to {date}"})

    result = {"symbol": symbol, "date": date, "close": prices[-1] if prices else None, "ma": {}}

    for period in period_list:
        if len(prices) >= period:
            ma_value = sum(prices[-period:]) / period
            result["ma"][f"MA{period}"] = round(ma_value, 2)
        else:
            result["ma"][f"MA{period}"] = None

    # Add trend signal
    if result["ma"].get("MA5") and result["ma"].get("MA20"):
        if result["ma"]["MA5"] > result["ma"]["MA20"]:
            result["signal"] = "多头排列 (MA5 > MA20)"
        else:
            result["signal"] = "空头排列 (MA5 < MA20)"

    return json.dumps(result, ensure_ascii=False)


@mcp.tool()
def calculate_macd(symbol: str, date: str) -> str:
    """计算 MACD 指标 (DIF, DEA, MACD柱)

    使用标准参数: 快线12日, 慢线26日, 信号线9日

    Args:
        symbol: 股票代码，如 600519.SH
        date: 日期，如 2026-03-17
    """
    prices = _get_close_prices(symbol, date, 60)

    if len(prices) < 26:
        return json.dumps({"error": f"Insufficient data for MACD ({len(prices)} < 26 days)"})

    # EMA calculation
    def ema(data, period):
        k = 2 / (period + 1)
        result = [data[0]]
        for i in range(1, len(data)):
            result.append(data[i] * k + result[-1] * (1 - k))
        return result

    ema12 = ema(prices, 12)
    ema26 = ema(prices, 26)

    dif_line = [ema12[i] - ema26[i] for i in range(len(prices))]
    dea_line = ema(dif_line, 9)
    macd_hist = [(dif_line[i] - dea_line[i]) * 2 for i in range(len(prices))]

    result = {
        "symbol": symbol,
        "date": date,
        "DIF": round(dif_line[-1], 3),
        "DEA": round(dea_line[-1], 3),
        "MACD": round(macd_hist[-1], 3),
    }

    # Signal
    if len(dif_line) >= 2:
        if dif_line[-2] <= dea_line[-2] and dif_line[-1] > dea_line[-1]:
            result["signal"] = "金叉 (买入信号)"
        elif dif_line[-2] >= dea_line[-2] and dif_line[-1] < dea_line[-1]:
            result["signal"] = "死叉 (卖出信号)"
        elif macd_hist[-1] > 0:
            result["signal"] = "多头 (MACD > 0)"
        else:
            result["signal"] = "空头 (MACD < 0)"

    return json.dumps(result, ensure_ascii=False)


@mcp.tool()
def calculate_rsi(symbol: str, date: str, period: int = 14) -> str:
    """计算 RSI 相对强弱指标

    Args:
        symbol: 股票代码，如 600519.SH
        date: 日期，如 2026-03-17
        period: RSI 周期，默认14日
    """
    prices = _get_close_prices(symbol, date, period + 10)

    if len(prices) < period + 1:
        return json.dumps({"error": f"Insufficient data for RSI ({len(prices)} < {period + 1} days)"})

    # Calculate price changes
    changes = [prices[i] - prices[i - 1] for i in range(1, len(prices))]

    # Separate gains and losses
    gains = [max(c, 0) for c in changes]
    losses = [abs(min(c, 0)) for c in changes]

    # Average gain and loss (SMA method)
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period

    if avg_loss == 0:
        rsi = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

    result = {
        "symbol": symbol,
        "date": date,
        "RSI": round(rsi, 2),
        "period": period,
    }

    if rsi > 70:
        result["signal"] = "超买区域 (RSI > 70)，警惕回调"
    elif rsi < 30:
        result["signal"] = "超卖区域 (RSI < 30)，关注反弹"
    else:
        result["signal"] = "中性区域"

    return json.dumps(result, ensure_ascii=False)
