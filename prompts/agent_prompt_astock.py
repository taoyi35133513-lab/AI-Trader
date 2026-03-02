"""
A股专用Agent提示词模块
Chinese A-shares specific agent prompt module
"""

import logging
import os

from dotenv import load_dotenv

load_dotenv()
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# Add project root directory to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
from tools.general_tools import get_config_value
from tools.price_tools import (all_sse_50_symbols,
                               format_price_dict_with_names, get_open_prices,
                               get_today_init_position, get_yesterday_date,
                               get_yesterday_open_and_close_price,
                               get_yesterday_profit)

from prompts.components.identity import IDENTITY_CN
from prompts.components.tool_guide import TOOL_GUIDE_CN
from prompts.components.market_rules_prompt import build_market_rules_section
from prompts.components.portfolio import PORTFOLIO_TEMPLATE, FINISH_TEMPLATE

STOP_SIGNAL = "<FINISH_SIGNAL>"


def build_system_prompt(
    market: str = "cn",
    components: list[str] | None = None,
    **format_kwargs,
) -> str:
    """Assemble a system prompt from composable components.

    Args:
        market: Market identifier (used for market-rules section).
        components: Ordered list of component names to include.
                    Defaults to all standard components.
        **format_kwargs: Values injected into the portfolio template
                         (date, positions, yesterday_close_price,
                          today_buy_price, current_profit, STOP_SIGNAL).
    """
    if components is None:
        components = ["identity", "tool_guide", "market_rules", "portfolio", "finish"]

    sections: list[str] = []
    for name in components:
        if name == "identity":
            sections.append(IDENTITY_CN)
        elif name == "tool_guide":
            sections.append(TOOL_GUIDE_CN)
        elif name == "market_rules":
            sections.append(build_market_rules_section(market))
        elif name == "portfolio":
            sections.append(PORTFOLIO_TEMPLATE.format(**format_kwargs))
        elif name == "finish":
            sections.append(FINISH_TEMPLATE.format(**format_kwargs))

    return "\n\n".join(sections)


def get_agent_system_prompt_astock(today_date: str, signature: str, stock_symbols: Optional[List[str]] = None) -> str:
    """
    生成A股专用系统提示词

    Args:
        today_date: 今日日期
        signature: Agent签名
        stock_symbols: 股票代码列表，默认为上证50成分股

    Returns:
        格式化的系统提示词字符串
    """
    logger.info("Building prompt: signature=%s, today_date=%s, market=cn", signature, today_date)

    # 默认使用上证50成分股
    if stock_symbols is None:
        stock_symbols = all_sse_50_symbols

    # 获取前一时间点的买入和卖出价格，硬编码market="cn"
    # 对于日线交易：获取昨日的开盘价和收盘价
    # 对于小时级交易：获取上一小时的开盘价和收盘价
    yesterday_buy_prices, yesterday_sell_prices = get_yesterday_open_and_close_price(
        today_date, stock_symbols, market="cn"
    )
    # 获取当前时间点的买入价格
    today_buy_price = get_open_prices(today_date, stock_symbols, market="cn")
    # 获取当前持仓
    today_init_position = get_today_init_position(today_date, signature)
    
    # 计算收益：(前一时间点收盘价 - 前一时间点开盘价) × 持仓数量
    # 对于日线交易：计算昨日收益
    # 对于小时级交易：计算上一小时收益
    current_profit = get_yesterday_profit(
        today_date, yesterday_buy_prices, yesterday_sell_prices, today_init_position, stock_symbols
    )

    # A股市场显示中文股票名称
    yesterday_sell_prices_display = format_price_dict_with_names(yesterday_sell_prices, market="cn")
    today_buy_price_display = format_price_dict_with_names(today_buy_price, market="cn")

    return build_system_prompt(
        market="cn",
        date=today_date,
        positions=today_init_position,
        STOP_SIGNAL=STOP_SIGNAL,
        yesterday_close_price=yesterday_sell_prices_display,
        today_buy_price=today_buy_price_display,
        current_profit=current_profit,
    )


if __name__ == "__main__":
    today_date = get_config_value("TODAY_DATE")
    signature = get_config_value("SIGNATURE")
    if signature is None:
        raise ValueError("SIGNATURE environment variable is not set")
    print(get_agent_system_prompt_astock(today_date, signature))
