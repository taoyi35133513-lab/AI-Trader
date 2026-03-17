"""为已有交易记录补生成 L1 reflection 并触发 L2/L3 consolidation"""
import asyncio
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")


def resolve_env(val):
    if val and isinstance(val, str) and val.startswith("$"):
        return os.environ.get(val[1:])
    return val


async def main():
    import duckdb
    from openai import AsyncOpenAI
    from api.config import get_database_path
    from api.services.memory_service import MemoryService
    from api.services.memory_consolidation import consolidate_l1_to_l2, consolidate_l2_to_l3
    from prompts.components.memory import REFLECTION_GENERATE_PROMPT

    with open("configs/config.json") as f:
        config = json.load(f)

    model_map = {}
    for m in config["models"]:
        if m.get("enabled"):
            model_map[m["name"]] = {
                "basemodel": m["basemodel"],
                "base_url": resolve_env(m.get("openai_base_url")),
                "api_key": resolve_env(m.get("openai_api_key")),
            }

    print("Models:", {k: "key=set" for k, v in model_map.items() if v["api_key"]})

    agents = [
        ("deepseek-chat-v3.2", "cn",
         "data/agent_data_astock/deepseek-chat-v3.2-live/position/position.jsonl",
         "deepseek-chat-v3.2"),
        ("gemini-2.5-flash", "cn",
         "data/agent_data_astock/gemini-2.5-flash-live/position/position.jsonl",
         "gemini-2.5-flash"),
        ("deepseek-chat-v3.2-astock-hour", "cn_hour",
         "data/agent_data_astock_hour/deepseek-chat-v3.2-live-astock-hour/position/position.jsonl",
         "deepseek-chat-v3.2"),
        ("gemini-2.5-flash-astock-hour", "cn_hour",
         "data/agent_data_astock_hour/gemini-2.5-flash-live-astock-hour/position/position.jsonl",
         "gemini-2.5-flash"),
    ]

    conn = duckdb.connect(str(get_database_path()), read_only=False)
    svc = MemoryService(conn)

    existing = conn.execute(
        "SELECT agent_name, source_dates FROM agent_memory WHERE level='reflection'"
    ).fetchall()
    existing_keys = {f"{r[0]}:{r[1]}" for r in existing}
    print(f"Existing reflections: {len(existing_keys)}")

    total = 0
    for agent_name, market, pos_file, model_key in agents:
        if not os.path.exists(pos_file):
            print(f"Skip {agent_name}: no position file")
            continue
        mcfg = model_map.get(model_key)
        if not mcfg or not mcfg["api_key"]:
            print(f"Skip {agent_name}: no API key")
            continue

        positions = []
        with open(pos_file) as f:
            for line in f:
                if line.strip():
                    positions.append(json.loads(line.strip()))

        dates_done = set()
        for pos in positions:
            date = pos["date"]
            key = f"{agent_name}:{date}"
            if key in existing_keys or date in dates_done:
                continue
            dates_done.add(date)

            this_action = pos.get("this_action", {})
            if not this_action:
                continue
            action = this_action.get("action", "unknown")
            symbol = this_action.get("symbol", "")
            amount = this_action.get("amount", 0)
            cash = pos["positions"].get("CASH", 0)
            holdings = {k: v for k, v in pos["positions"].items() if k != "CASH" and v > 0}

            prompt = REFLECTION_GENERATE_PROMPT.format(
                date=date,
                actions=f"{action} {symbol} {amount}" if action != "no_trade" else "无交易",
                pnl=f"现金={cash:.0f}, 持仓={len(holdings)}只",
                reasoning_summary=f"持仓详情: {json.dumps(holdings, ensure_ascii=False)[:500]}",
            )

            try:
                client = AsyncOpenAI(
                    api_key=mcfg["api_key"],
                    base_url=mcfg["base_url"] or None,
                )
                resp = await client.chat.completions.create(
                    model=mcfg["basemodel"],
                    messages=[
                        {"role": "system", "content": "你是一个交易复盘助手，用中文简洁地总结交易经验教训，不超过100字。"},
                        {"role": "user", "content": prompt},
                    ],
                    max_tokens=200,
                    temperature=0.3,
                )
                reflection = resp.choices[0].message.content.strip()
                if reflection:
                    svc.add_reflection(agent_name, market, reflection, date)
                    total += 1
                    print(f"  [{agent_name}] {date}: {reflection[:80]}...")
            except Exception as e:
                print(f"  [{agent_name}] {date}: ERROR {e}")

        print(f"  {agent_name}: processed {len(dates_done)} new dates")

    print(f"\nTotal new reflections: {total}")

    # Trigger consolidation for all agents
    print("\n=== Triggering consolidation ===")
    for agent_name, market, _, _ in agents:
        try:
            l1_count = conn.execute(
                "SELECT COUNT(*) FROM agent_memory WHERE agent_name=? AND market=? AND level='reflection' AND status='active'",
                [agent_name, market]
            ).fetchone()[0]
            print(f"  {agent_name} ({market}): {l1_count} active L1 reflections")

            if l1_count >= 3:  # Lower threshold for initial generation
                print(f"    Consolidating L1 -> L2...")
                await consolidate_l1_to_l2(svc, agent_name, market)

            l2_count = conn.execute(
                "SELECT COUNT(*) FROM agent_memory WHERE agent_name=? AND market=? AND level='lesson' AND status='active'",
                [agent_name, market]
            ).fetchone()[0]
            if l2_count >= 1:
                print(f"    Consolidating L2 -> L3 ({l2_count} lessons)...")
                await consolidate_l2_to_l3(svc, agent_name, market)
        except Exception as e:
            print(f"    ERROR: {e}")

    # Final stats
    print("\n=== Final memory stats ===")
    rows = conn.execute("""
        SELECT agent_name, market, level, status, COUNT(*)
        FROM agent_memory
        GROUP BY agent_name, market, level, status
        ORDER BY agent_name, market, level
    """).fetchall()
    for r in rows:
        print(f"  {r[0]} | {r[1]} | {r[2]} | {r[3]} | count={r[4]}")

    conn.close()


if __name__ == "__main__":
    asyncio.run(main())
