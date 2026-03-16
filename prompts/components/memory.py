"""记忆组件 -- 注入 Agent 历史交易记忆到系统提示词"""

MEMORY_TEMPLATE = """## 交易记忆与经验

{strategy_section}
{lessons_section}
{reflections_section}
以上记忆供你参考辅助决策，但市场在变化，不要被过去束缚。"""

STRATEGY_SECTION = """### 我的交易策略备忘
{content}
"""

LESSONS_SECTION = """### 积累的交易经验
{content}
"""

REFLECTIONS_SECTION = """### 最近交易复盘
{content}
"""

REFLECTION_GENERATE_PROMPT = """你刚刚完成了一次交易会话，请简要复盘。

## 本次交易信息
- 日期: {date}
- 操作: {actions}
- 盈亏情况: {pnl}

## 你的推理摘要
{reasoning_summary}

请用 3-5 句话完成复盘，包含：
1. **决策依据**: 我为什么这样操作？
2. **关键判断**: 我对市场做了什么假设？
3. **经验教训**: 这次有什么值得记住的？

要求简洁具体，避免空话。如果本次操作平淡无奇，只写"常规操作，无特别教训"即可。控制在 150 字以内。
"""


def build_memory_section(memories: dict) -> str:
    """Build the memory section for system prompt.

    Args:
        memories: dict from MemoryService.get_active_memories()
            {strategy: dict|None, lessons: [dict], reflections: [dict]}

    Returns:
        Formatted memory text, or empty string if no memories.
    """
    if not memories:
        return ""

    strategy = memories.get("strategy")
    lessons = memories.get("lessons", [])
    reflections = memories.get("reflections", [])

    # If no memories at all, return empty
    if not strategy and not lessons and not reflections:
        return ""

    strategy_section = ""
    if strategy:
        strategy_section = STRATEGY_SECTION.format(content=strategy["content"])

    lessons_section = ""
    if lessons:
        lesson_lines = "\n".join([f"- {l['content']}" for l in lessons])
        lessons_section = LESSONS_SECTION.format(content=lesson_lines)

    reflections_section = ""
    if reflections:
        ref_lines = "\n".join([f"[{r.get('source_dates', '')}] {r['content']}" for r in reflections])
        reflections_section = REFLECTIONS_SECTION.format(content=ref_lines)

    return MEMORY_TEMPLATE.format(
        strategy_section=strategy_section,
        lessons_section=lessons_section,
        reflections_section=reflections_section,
    )
