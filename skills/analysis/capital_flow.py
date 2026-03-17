"""资金流向分析技能（含 MCP 工具）"""

SKILL_CONFIG = {
    "id": "capital_flow",
    "name": "资金流向分析",
    "name_en": "Capital Flow Analysis",
    "category": "analysis",
    "description": "分析主力资金和北向资金流向，跟踪聪明钱动向",
    "icon": "💰",
    "tools_module": "skills.tools.tool_capital_flow",
    "mcp_service_name": "skill_flow",
}

PROMPT = """## 资金流向分析工具

你已装备资金流向分析技能，可以调用以下专用工具：
- `get_capital_flow(symbol, date)` — 获取个股主力资金净流入/流出数据

### 分析框架
1. **主力资金**: 大单净流入为主力加仓信号，持续净流出需警惕
2. **量价关系**: 放量上涨+主力净流入 = 强势确认；放量下跌+主力净流出 = 弱势确认
3. **背离信号**: 价格上涨但主力资金净流出 = 顶部风险；价格下跌但主力净流入 = 底部信号

### 决策应用
- 买入时优先选择主力资金连续3日净流入的标的
- 持仓中出现连续3日主力净流出应考虑减仓
- 资金流向数据是辅助指标，需结合趋势和基本面综合判断
"""
