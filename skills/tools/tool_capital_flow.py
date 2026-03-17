"""资金流向分析 MCP 工具

通过 akshare/tushare 获取个股资金流向数据。
"""

import json
import logging
from typing import Optional

from fastmcp import FastMCP

logger = logging.getLogger(__name__)

mcp = FastMCP("SkillFlow")


@mcp.tool()
def get_capital_flow(symbol: str, date: str) -> str:
    """获取个股主力资金净流入/流出数据

    Args:
        symbol: 股票代码，如 600519.SH
        date: 日期，如 2026-03-17

    Returns:
        JSON 格式的资金流向数据
    """
    code = symbol.split(".")[0]

    try:
        import tushare as ts
        df = ts.get_realtime_quotes(code)
        if df is not None and not df.empty:
            row = df.iloc[0]
            # 使用成交额和价格变化推断资金方向
            volume = float(row["volume"]) if row["volume"] else 0
            price = float(row["price"]) if row["price"] else 0
            pre_close = float(row["pre_close"]) if row["pre_close"] else 0
            amount = float(row["amount"]) if row["amount"] else 0

            change_pct = ((price - pre_close) / pre_close * 100) if pre_close > 0 else 0

            # 简化的资金流向判断
            if change_pct > 0 and volume > 0:
                flow_direction = "净流入"
                estimated_flow = amount * abs(change_pct) / 100
            elif change_pct < 0:
                flow_direction = "净流出"
                estimated_flow = -amount * abs(change_pct) / 100
            else:
                flow_direction = "平衡"
                estimated_flow = 0

            result = {
                "symbol": symbol,
                "date": date,
                "price": price,
                "change_pct": round(change_pct, 2),
                "volume": int(volume),
                "amount": round(amount, 2),
                "flow_direction": flow_direction,
                "estimated_net_flow": round(estimated_flow, 2),
                "signal": f"主力资金{flow_direction}，涨跌幅{change_pct:.2f}%",
            }
            return json.dumps(result, ensure_ascii=False)
    except Exception as e:
        logger.warning("Capital flow query failed for %s: %s", symbol, e)

    return json.dumps({
        "error": f"Unable to get capital flow data for {symbol}",
        "symbol": symbol,
        "date": date,
    }, ensure_ascii=False)
