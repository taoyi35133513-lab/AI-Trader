"""技术指标分析技能（含 MCP 工具）"""

SKILL_CONFIG = {
    "id": "technical_indicators",
    "name": "技术指标分析",
    "name_en": "Technical Indicators",
    "category": "analysis",
    "description": "MA/MACD/RSI等技术指标计算与趋势研判",
    "icon": "📊",
    "tools_module": "skills.tools.tool_technical_indicators",
    "mcp_service_name": "skill_ta",
}

PROMPT = """## 技术指标分析工具

你已装备技术指标分析技能，可以调用以下专用工具辅助决策：
- `calculate_ma(symbol, date, periods)` — 计算移动平均线（默认 [5, 10, 20, 60]）
- `calculate_macd(symbol, date)` — 计算 MACD 指标（DIF, DEA, MACD柱）
- `calculate_rsi(symbol, date, period)` — 计算 RSI 相对强弱指标（默认14日）

### 使用指南
1. **趋势判断**: MA5 > MA20 为多头排列（看涨），反之为空头排列（看跌）
2. **买卖信号**: MACD 金叉（DIF上穿DEA）为买入信号，死叉为卖出信号
3. **超买超卖**: RSI > 70 警惕超买回调，RSI < 30 关注超卖反弹
4. **综合研判**: 技术指标应结合基本面和资金面综合判断，不宜单独使用
"""
