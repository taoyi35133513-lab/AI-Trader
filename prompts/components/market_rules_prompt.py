"""Market-rules prompt component — auto-generated from MarketRules."""

from tools.market_rules import MarketRules, get_rules


def build_market_rules_section(market: str = "cn") -> str:
    """Return a prompt section describing the trading rules for *market*."""
    rules = get_rules(market)

    if market == "cn":
        return _astock_rules_cn(rules)

    # Generic fallback
    return rules.format_rules_for_prompt()


def _astock_rules_cn(rules: MarketRules) -> str:
    limits = rules.price_limits
    return f"""🇨🇳 重要 - A股交易规则（适用于所有 .SH 和 .SZ 股票代码）：
1. **股票代码格式 - 极其重要！**:
   - symbol 参数必须是字符串类型，必须包含 .SH 或 .SZ 后缀

2. **一手交易要求**: 所有买卖订单必须是{rules.lot_size}股的整数倍（1手 = {rules.lot_size}股）
   - ✅ 正确: buy("600519.SH", {rules.lot_size}), buy("600519.SH", {rules.lot_size * 3}), sell("600519.SH", {rules.lot_size * 2})
   - ❌ 错误: buy("600519.SH", 13), buy("600519.SH", 497), sell("600519.SH", 50)

3. **T+{rules.t_plus_n}结算规则**: 当天买入的股票不能当天卖出
   - 你只能卖出在今天之前购买的股票
   - 如果你今天买入{rules.lot_size}股600519.SH，必须等到明天才能卖出
   - 你仍然可以卖出之前持有的股票

4. **涨跌停限制**:
   - 普通股票：±{limits.get('normal', 0.1) * 100:.0f}%
   - ST股票：±{limits.get('st', 0.05) * 100:.0f}%
   - 科创板/创业板：±{limits.get('star_gem', 0.2) * 100:.0f}%"""
