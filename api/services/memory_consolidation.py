"""
记忆压缩服务

L1 Reflection -> L2 Lesson 压缩
L2 Lesson -> L3 Strategy 更新
"""

import json
import logging
import os
from typing import Optional

from api.config import load_config_json
from api.services.memory_service import (
    LEVEL_LESSON,
    LEVEL_REFLECTION,
    LEVEL_STRATEGY,
    MemoryService,
)

logger = logging.getLogger(__name__)


def _resolve_env_var(value: Optional[str]) -> Optional[str]:
    if value and isinstance(value, str) and value.startswith("$"):
        return os.environ.get(value[1:])
    return value


def _get_llm_config() -> dict:
    """Get cheapest available LLM for consolidation."""
    config_data = load_config_json("config.json")
    if not config_data:
        raise ValueError("Failed to load config.json")
    for model in config_data.get("models", []):
        if model.get("enabled", False):
            base_url = _resolve_env_var(model.get("openai_base_url")) or os.environ.get("OPENAI_API_BASE", "")
            api_key = _resolve_env_var(model.get("openai_api_key")) or os.environ.get("OPENAI_API_KEY", "")
            if api_key:
                return {"model": model.get("basemodel", model["name"]), "base_url": base_url, "api_key": api_key}
    raise ValueError("No enabled model with API key found")


L1_TO_L2_PROMPT = """你是一个交易经验总结专家。以下是一个 AI 交易 Agent 最近几次交易后的自我复盘记录。

请从这些复盘中提炼出 1-3 条可复用的交易经验/规则。

要求：
- 每条经验独立一行，以"•"开头
- 具体可执行，不要空泛的建议
- 如果多条复盘指向同一个教训，合并为一条
- 如果没有值得提炼的经验，回复"无新经验"
- 总长度不超过 200 字

## 复盘记录
{reflections}
"""

L2_TO_L3_PROMPT = """你是一个交易策略顾问。以下是一个 AI 交易 Agent 积累的交易经验规则，以及它当前的策略备忘（如果有）。

请整合这些经验，更新策略备忘。策略备忘应该是该 Agent 的核心交易哲学和原则总结。

要求：
- 控制在 300 字以内
- 分为 2-3 个要点
- 保留仍然有效的旧策略内容
- 融入新经验中的重要发现
- 风格简洁有力

## 当前策略备忘
{current_strategy}

## 积累的交易经验
{lessons}
"""


async def consolidate_l1_to_l2(memory_service: MemoryService, agent_name: str, market: str):
    """Compress active L1 reflections into L2 lessons."""
    # Get ALL active reflections (not just the 2 returned for prompt injection)
    all_reflections = memory_service.conn.execute(
        "SELECT id, content, source_dates FROM agent_memory WHERE agent_name = ? AND market = ? AND level = ? AND status = 'active' ORDER BY created_at DESC",
        [agent_name, market, LEVEL_REFLECTION],
    ).fetchall()

    if len(all_reflections) < 2:
        return

    reflection_text = "\n\n".join([f"[{r[2]}] {r[1]}" for r in all_reflections])
    source_dates = ",".join([r[2] for r in all_reflections if r[2]])
    reflection_ids = [r[0] for r in all_reflections]

    try:
        from openai import AsyncOpenAI
        llm_config = _get_llm_config()
        client = AsyncOpenAI(api_key=llm_config["api_key"], base_url=llm_config["base_url"] or None)

        resp = await client.chat.completions.create(
            model=llm_config["model"],
            messages=[
                {"role": "system", "content": "你是一个交易经验总结专家，用中文回复。"},
                {"role": "user", "content": L1_TO_L2_PROMPT.format(reflections=reflection_text)},
            ],
            max_tokens=500,
            temperature=0.3,
        )

        lesson_content = resp.choices[0].message.content.strip()
        if lesson_content and "无新经验" not in lesson_content:
            memory_service.add_lesson(
                agent_name, market, lesson_content, source_dates, ",".join(str(i) for i in reflection_ids)
            )
            logger.info("L1->L2 consolidation complete for %s: archived %d reflections", agent_name, len(reflection_ids))

        # Archive consumed reflections
        memory_service.archive_memories(reflection_ids)

    except Exception as e:
        logger.error("L1->L2 consolidation failed for %s: %s", agent_name, e)


async def consolidate_l2_to_l3(memory_service: MemoryService, agent_name: str, market: str):
    """Update L3 strategy from accumulated L2 lessons."""
    memories = memory_service.get_active_memories(agent_name, market)
    current_strategy = memories.get("strategy")

    # Get ALL active lessons
    all_lessons = memory_service.conn.execute(
        "SELECT id, content, source_dates FROM agent_memory WHERE agent_name = ? AND market = ? AND level = ? AND status = 'active' ORDER BY created_at DESC",
        [agent_name, market, LEVEL_LESSON],
    ).fetchall()

    if len(all_lessons) < 2:
        return

    lesson_text = "\n".join([r[1] for r in all_lessons])
    strategy_text = current_strategy["content"] if current_strategy else "(暂无策略备忘)"
    source_dates = ",".join([r[2] for r in all_lessons if r[2]])

    try:
        from openai import AsyncOpenAI
        llm_config = _get_llm_config()
        client = AsyncOpenAI(api_key=llm_config["api_key"], base_url=llm_config["base_url"] or None)

        resp = await client.chat.completions.create(
            model=llm_config["model"],
            messages=[
                {"role": "system", "content": "你是一个交易策略顾问，用中文回复。"},
                {"role": "user", "content": L2_TO_L3_PROMPT.format(current_strategy=strategy_text, lessons=lesson_text)},
            ],
            max_tokens=600,
            temperature=0.3,
        )

        strategy_content = resp.choices[0].message.content.strip()
        if strategy_content:
            memory_service.update_strategy(agent_name, market, strategy_content, source_dates)
            logger.info("L2->L3 consolidation complete for %s", agent_name)

    except Exception as e:
        logger.error("L2->L3 consolidation failed for %s: %s", agent_name, e)
