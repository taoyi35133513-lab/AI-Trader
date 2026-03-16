"""
投资大师点评服务

汇总 Agent 交易数据，调用 LLM 生成大师风格的流式点评。
"""

import json
import logging
import os
from typing import AsyncGenerator, Optional

import duckdb

from api.config import get_database_path, load_config_json
from prompts.masters import MASTER_REGISTRY

logger = logging.getLogger(__name__)


def _resolve_env_var(value: Optional[str]) -> Optional[str]:
    """Resolve $ENV_VAR references in config values."""
    if value and isinstance(value, str) and value.startswith("$"):
        env_name = value[1:]
        resolved = os.environ.get(env_name)
        if resolved is None:
            logger.warning("Environment variable %s not set", env_name)
        return resolved
    return value


def gather_context(agent_name: str, market: str = "cn", days: int = 30) -> str:
    """汇总 Agent 交易数据为文本上下文。

    Returns:
        格式化的交易上下文字符串
    """
    from api.services.agent_service import AgentService
    from api.services.price_service import PriceService

    db_path = get_database_path()
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        agent_service = AgentService(conn)
        price_service = PriceService(conn)

        # 1. 获取资产历史
        asset_history = agent_service.get_agent_asset_history(agent_name, market)
        history = asset_history.get("history", [])

        # 2. 当前持仓（最新一条）
        positions_data = agent_service.get_agent_positions(agent_name, market)
        latest_position = positions_data[-1] if positions_data else None

        # 3. 收益率
        total_return = asset_history.get("total_return", 0)
        final_value = asset_history.get("final_value", 0)
        initial_cash = 100000

        # 4. 最近交易记录
        recent_trades = agent_service.get_recent_trades(
            market, limit=20, agents=[{"name": agent_name}]
        )

        # 5. 基准对比
        benchmark = price_service.get_benchmark_data(market)
        bench_return = 0
        if benchmark and len(benchmark) >= 2:
            bench_first = benchmark[0].get("close", 0)
            bench_last = benchmark[-1].get("close", 0)
            if bench_first > 0:
                bench_return = (bench_last - bench_first) / bench_first * 100

        # 6. Agent 推理摘录（从 DuckDB 会话表）
        reasoning_text = _get_recent_reasoning(conn, agent_name, market, limit=3)

    finally:
        conn.close()

    # 构建上下文文本
    lines = []
    lines.append(f"## Agent: {agent_name}")
    lines.append(f"- 市场: A股（上证50成分股）")
    lines.append(f"- 初始资金: ¥{initial_cash:,.0f}")
    lines.append(f"- 当前总资产: ¥{final_value:,.2f}")
    lines.append(f"- 总收益率: {total_return:.2f}%")
    lines.append(f"- 基准（上证50）收益率: {bench_return:.2f}%")
    lines.append(f"- 交易天数: {len(history)}")
    lines.append("")

    # 当前持仓
    if latest_position:
        holdings = latest_position.get("positions", {})
        cash = holdings.get("CASH", latest_position.get("cash", 0))
        lines.append("## 当前持仓")
        lines.append(f"- 现金: ¥{float(cash):,.2f}")
        for symbol, qty in holdings.items():
            if symbol == "CASH":
                continue
            lines.append(f"- {symbol}: {qty}股")
        lines.append("")

    # 最近交易
    if recent_trades:
        lines.append("## 最近交易记录")
        for trade in recent_trades[:15]:
            date_str = trade.get("date", "")
            symbol = trade.get("symbol", "")
            action = trade.get("action", "")
            qty = trade.get("quantity", 0)
            price = trade.get("price", 0)
            lines.append(f"- {date_str} | {action} {symbol} {qty}股 @ ¥{price:.2f}")
        lines.append("")

    # 资产曲线摘要（每隔几天采样）
    if history:
        lines.append("## 资产曲线（采样）")
        step = max(1, len(history) // 10)
        sampled = list(range(0, len(history), step))
        if sampled[-1] != len(history) - 1:
            sampled.append(len(history) - 1)
        for i in sampled:
            h = history[i]
            lines.append(f"- {h['date']}: ¥{h['total_value']:,.2f}")
        lines.append("")

    # 推理摘录
    if reasoning_text:
        lines.append("## Agent 近期推理摘录")
        lines.append(reasoning_text[:2000])
        lines.append("")

    return "\n".join(lines)


def _get_recent_reasoning(
    conn: duckdb.DuckDBPyConnection,
    agent_name: str,
    market: str,
    limit: int = 3,
) -> str:
    """从 DuckDB 获取最近的 Agent 推理内容"""
    try:
        rows = conn.execute(
            """
            SELECT session_date, m.content
            FROM agent_trading_sessions s
            JOIN agent_messages m ON m.session_id = s.id
            WHERE s.agent_name = ? AND s.market = ? AND m.role = 'assistant'
            ORDER BY s.session_timestamp DESC, m.id DESC
            LIMIT ?
            """,
            [agent_name, market, limit],
        ).fetchall()

        if not rows:
            return ""

        parts = []
        for row in rows:
            date_str = str(row[0])
            content = row[1][:600] if row[1] else ""
            parts.append(f"[{date_str}] {content}")

        return "\n---\n".join(parts)
    except Exception as e:
        logger.debug("Failed to get reasoning: %s", e)
        return ""


def _get_llm_config(model_name: Optional[str] = None) -> dict:
    """从 config.json 获取 LLM 配置。

    Args:
        model_name: 指定模型名称。为 None 时使用第一个 enabled 模型。
    """
    config_data = load_config_json("config.json")
    if not config_data:
        raise ValueError("Failed to load config.json")

    models = config_data.get("models", [])

    # 指定模型名时按 name 匹配
    if model_name:
        for model in models:
            if model.get("name") == model_name:
                base_url = _resolve_env_var(model.get("openai_base_url")) or os.environ.get("OPENAI_API_BASE", "")
                api_key = _resolve_env_var(model.get("openai_api_key")) or os.environ.get("OPENAI_API_KEY", "")
                if api_key:
                    return {
                        "model": model.get("basemodel", model["name"]),
                        "base_url": base_url,
                        "api_key": api_key,
                    }
        raise ValueError(f"Model '{model_name}' not found or has no API key")

    # 默认：优先使用 enabled 的模型
    for model in models:
        if model.get("enabled", False):
            base_url = _resolve_env_var(model.get("openai_base_url")) or os.environ.get("OPENAI_API_BASE", "")
            api_key = _resolve_env_var(model.get("openai_api_key")) or os.environ.get("OPENAI_API_KEY", "")
            if api_key:
                return {
                    "model": model.get("basemodel", model["name"]),
                    "base_url": base_url,
                    "api_key": api_key,
                }

    raise ValueError("No enabled model with API key found in config")


async def stream_commentary(
    agent_name: str,
    market: str = "cn",
    master_id: str = "buffett",
    days: int = 30,
    model_name: Optional[str] = None,
) -> AsyncGenerator[str, None]:
    """流式生成大师点评。

    Yields:
        SSE 格式的文本 chunks: "data: {json}\n\n"
    """
    master = MASTER_REGISTRY.get(master_id)
    if not master:
        yield f"data: {json.dumps({'error': f'Unknown master: {master_id}'})}\n\n"
        return

    # 汇总交易上下文
    try:
        context = gather_context(agent_name, market, days)
    except Exception as e:
        logger.error("Failed to gather context for %s: %s", agent_name, e)
        yield f"data: {json.dumps({'error': f'Failed to gather trading data: {e}'})}\n\n"
        return

    # 获取 LLM 配置
    try:
        llm_config = _get_llm_config(model_name)
    except Exception as e:
        logger.error("Failed to get LLM config: %s", e)
        yield f"data: {json.dumps({'error': f'LLM configuration error: {e}'})}\n\n"
        return

    # 构建消息
    system_prompt = master["prompt"]
    user_message = f"请对以下 AI 交易 Agent 的表现进行点评：\n\n{context}"

    # 使用 openai async client 流式调用
    try:
        from openai import AsyncOpenAI

        client = AsyncOpenAI(
            api_key=llm_config["api_key"],
            base_url=llm_config["base_url"] or None,
        )

        stream = await client.chat.completions.create(
            model=llm_config["model"],
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            stream=True,
            max_tokens=2000,
            temperature=0.8,
        )

        async for chunk in stream:
            if chunk.choices and chunk.choices[0].delta.content:
                content = chunk.choices[0].delta.content
                yield f"data: {json.dumps({'content': content})}\n\n"

        yield "data: [DONE]\n\n"

    except Exception as e:
        logger.error("LLM streaming error: %s", e)
        yield f"data: {json.dumps({'error': f'LLM error: {e}'})}\n\n"
