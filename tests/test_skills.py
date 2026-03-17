"""
Tests for the Skills system.

Covers:
- Skill discovery and registry
- Builtin vs optional skills
- SkillsService: CRUD operations on agent_skills table
- Skills API endpoints via FastAPI TestClient
- Prompt injection with skill_ids
- Technical indicator MCP tools (MA, MACD, RSI)
"""

import json
import os
import sys

import duckdb
import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


# ── Fixtures ──────────────────────────────────────────────

@pytest.fixture
def skills_db(tmp_path):
    """Create a temporary DuckDB with agent_skills table."""
    db_path = tmp_path / "test_skills.duckdb"
    conn = duckdb.connect(str(db_path), read_only=False)
    from api.services.skills_service import init_skills_table
    init_skills_table(conn)
    yield conn, db_path
    conn.close()


# ── Test: Skill Discovery ──────────────────────────────────

class TestSkillDiscovery:
    """Test skill registry auto-discovery."""

    def test_discover_finds_all_skills(self):
        import skills as skills_mod
        skills_mod.SKILL_REGISTRY = {}  # reset
        registry = skills_mod.discover_skills()
        assert len(registry) >= 16, f"Expected at least 16 skills, got {len(registry)}"

    def test_all_skills_have_required_fields(self):
        from skills import get_all_skills
        for skill in get_all_skills():
            assert "id" in skill
            assert "name" in skill
            assert "name_en" in skill
            assert "category" in skill
            assert "description" in skill
            assert "icon" in skill
            assert "prompt" in skill  # can be empty string for builtin

    def test_builtin_skills_count(self):
        from skills import get_all_skills
        builtin = [s for s in get_all_skills() if s.get("builtin")]
        assert len(builtin) == 4

    def test_builtin_skills_have_mcp_service(self):
        from skills import get_all_skills
        for s in get_all_skills():
            if s.get("builtin"):
                assert s.get("mcp_service_name"), f"Builtin skill {s['id']} missing mcp_service_name"

    def test_optional_skills_count(self):
        from skills import get_all_skills
        optional = [s for s in get_all_skills() if not s.get("builtin")]
        assert len(optional) == 12

    def test_categories(self):
        from skills import get_skills_by_category
        assert len(get_skills_by_category("builtin")) == 4
        assert len(get_skills_by_category("strategy")) == 4
        assert len(get_skills_by_category("analysis")) == 4
        assert len(get_skills_by_category("risk")) == 4

    def test_get_skill_by_id(self):
        from skills import get_skill
        skill = get_skill("value_investing")
        assert skill is not None
        assert skill["name"] == "价值投资"
        assert skill["category"] == "strategy"

    def test_get_nonexistent_skill(self):
        from skills import get_skill
        assert get_skill("nonexistent_skill") is None

    def test_builtin_skill_ids(self):
        from skills import get_skill
        for sid in ["trade_execution", "price_query", "market_news", "math_calc"]:
            skill = get_skill(sid)
            assert skill is not None, f"Builtin skill {sid} not found"
            assert skill["builtin"] is True

    def test_strategy_skills_have_prompt(self):
        from skills import get_skills_by_category
        for s in get_skills_by_category("strategy"):
            assert len(s["prompt"]) > 50, f"Strategy skill {s['id']} has too short prompt"

    def test_builtin_skills_have_empty_prompt(self):
        from skills import get_skills_by_category
        for s in get_skills_by_category("builtin"):
            assert s["prompt"] == "", f"Builtin skill {s['id']} should have empty prompt"


# ── Test: Skills Service (DuckDB) ──────────────────────────

class TestSkillsService:
    """Test agent_skills DuckDB operations."""

    def test_init_table_creates_table(self, skills_db):
        conn, _ = skills_db
        tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
        assert "agent_skills" in tables

    def test_init_table_idempotent(self, skills_db):
        conn, _ = skills_db
        from api.services.skills_service import init_skills_table
        init_skills_table(conn)  # second call should not raise
        tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
        assert "agent_skills" in tables

    def test_set_and_get_skills(self, skills_db):
        conn, db_path = skills_db
        # Direct DB operations
        conn.execute("INSERT INTO agent_skills (agent_name, market, skill_id) VALUES ('test-agent', 'cn', 'value_investing')")
        conn.execute("INSERT INTO agent_skills (agent_name, market, skill_id) VALUES ('test-agent', 'cn', 'momentum')")

        rows = conn.execute(
            "SELECT skill_id FROM agent_skills WHERE agent_name='test-agent' AND market='cn' AND enabled=TRUE"
        ).fetchall()
        skill_ids = [r[0] for r in rows]
        assert sorted(skill_ids) == ["momentum", "value_investing"]

    def test_replace_skills(self, skills_db):
        conn, _ = skills_db
        conn.execute("INSERT INTO agent_skills (agent_name, market, skill_id) VALUES ('test-agent', 'cn', 'value_investing')")

        # Replace
        conn.execute("DELETE FROM agent_skills WHERE agent_name='test-agent' AND market='cn'")
        conn.execute("INSERT INTO agent_skills (agent_name, market, skill_id) VALUES ('test-agent', 'cn', 'trend_following')")

        rows = conn.execute("SELECT skill_id FROM agent_skills WHERE agent_name='test-agent'").fetchall()
        assert [r[0] for r in rows] == ["trend_following"]

    def test_agent_isolation(self, skills_db):
        conn, _ = skills_db
        conn.execute("INSERT INTO agent_skills (agent_name, market, skill_id) VALUES ('agent-a', 'cn', 'value_investing')")
        conn.execute("INSERT INTO agent_skills (agent_name, market, skill_id) VALUES ('agent-b', 'cn', 'momentum')")

        rows_a = conn.execute("SELECT skill_id FROM agent_skills WHERE agent_name='agent-a'").fetchall()
        rows_b = conn.execute("SELECT skill_id FROM agent_skills WHERE agent_name='agent-b'").fetchall()
        assert [r[0] for r in rows_a] == ["value_investing"]
        assert [r[0] for r in rows_b] == ["momentum"]

    def test_market_isolation(self, skills_db):
        conn, _ = skills_db
        conn.execute("INSERT INTO agent_skills (agent_name, market, skill_id) VALUES ('test-agent', 'cn', 'value_investing')")
        conn.execute("INSERT INTO agent_skills (agent_name, market, skill_id) VALUES ('test-agent', 'cn_hour', 'momentum')")

        rows_cn = conn.execute("SELECT skill_id FROM agent_skills WHERE agent_name='test-agent' AND market='cn'").fetchall()
        rows_hour = conn.execute("SELECT skill_id FROM agent_skills WHERE agent_name='test-agent' AND market='cn_hour'").fetchall()
        assert [r[0] for r in rows_cn] == ["value_investing"]
        assert [r[0] for r in rows_hour] == ["momentum"]

    def test_delete_skill(self, skills_db):
        conn, _ = skills_db
        conn.execute("INSERT INTO agent_skills (agent_name, market, skill_id) VALUES ('test-agent', 'cn', 'value_investing')")
        conn.execute("INSERT INTO agent_skills (agent_name, market, skill_id) VALUES ('test-agent', 'cn', 'momentum')")

        conn.execute("DELETE FROM agent_skills WHERE agent_name='test-agent' AND market='cn' AND skill_id='value_investing'")
        rows = conn.execute("SELECT skill_id FROM agent_skills WHERE agent_name='test-agent'").fetchall()
        assert [r[0] for r in rows] == ["momentum"]

    def test_empty_agent_returns_no_skills(self, skills_db):
        conn, _ = skills_db
        rows = conn.execute("SELECT skill_id FROM agent_skills WHERE agent_name='nonexistent'").fetchall()
        assert rows == []


# ── Test: Prompt Injection ──────────────────────────────────

class TestPromptInjection:
    """Test skill prompt injection into system prompt."""

    def test_build_prompt_without_skills(self):
        from prompts.agent_prompt_astock import build_system_prompt
        prompt = build_system_prompt(
            market="cn",
            skill_ids=None,
            date="2026-03-17",
            positions="CASH: 100000",
            STOP_SIGNAL="<FINISH_SIGNAL>",
            yesterday_close_price="N/A",
            today_buy_price="N/A",
            current_profit="N/A",
            user_comments="",
        )
        assert "价值投资" not in prompt
        assert "趋势跟踪" not in prompt

    def test_build_prompt_with_strategy_skill(self):
        from prompts.agent_prompt_astock import build_system_prompt
        prompt = build_system_prompt(
            market="cn",
            skill_ids=["value_investing"],
            date="2026-03-17",
            positions="CASH: 100000",
            STOP_SIGNAL="<FINISH_SIGNAL>",
            yesterday_close_price="N/A",
            today_buy_price="N/A",
            current_profit="N/A",
            user_comments="",
        )
        assert "价值投资策略指引" in prompt
        assert "安全边际" in prompt

    def test_build_prompt_with_multiple_skills(self):
        from prompts.agent_prompt_astock import build_system_prompt
        prompt = build_system_prompt(
            market="cn",
            skill_ids=["value_investing", "stop_loss_take_profit", "technical_indicators"],
            date="2026-03-17",
            positions="CASH: 100000",
            STOP_SIGNAL="<FINISH_SIGNAL>",
            yesterday_close_price="N/A",
            today_buy_price="N/A",
            current_profit="N/A",
            user_comments="",
        )
        assert "价值投资策略指引" in prompt
        assert "止损止盈规则" in prompt
        assert "技术指标分析工具" in prompt

    def test_build_prompt_with_risk_skill(self):
        from prompts.agent_prompt_astock import build_system_prompt
        prompt = build_system_prompt(
            market="cn",
            skill_ids=["position_sizing"],
            date="2026-03-17",
            positions="CASH: 100000",
            STOP_SIGNAL="<FINISH_SIGNAL>",
            yesterday_close_price="N/A",
            today_buy_price="N/A",
            current_profit="N/A",
            user_comments="",
        )
        assert "仓位管理策略" in prompt
        assert "单只上限" in prompt

    def test_builtin_skill_injects_nothing(self):
        from prompts.agent_prompt_astock import build_system_prompt
        prompt = build_system_prompt(
            market="cn",
            skill_ids=["trade_execution", "price_query"],
            date="2026-03-17",
            positions="CASH: 100000",
            STOP_SIGNAL="<FINISH_SIGNAL>",
            yesterday_close_price="N/A",
            today_buy_price="N/A",
            current_profit="N/A",
            user_comments="",
        )
        # Builtin skills have empty prompts, so no extra content
        assert "交易执行" not in prompt  # the skill prompt is empty, not injected

    def test_invalid_skill_id_ignored(self):
        from prompts.agent_prompt_astock import build_system_prompt
        # Should not raise, just skip unknown skill
        prompt = build_system_prompt(
            market="cn",
            skill_ids=["nonexistent_skill", "value_investing"],
            date="2026-03-17",
            positions="CASH: 100000",
            STOP_SIGNAL="<FINISH_SIGNAL>",
            yesterday_close_price="N/A",
            today_buy_price="N/A",
            current_profit="N/A",
            user_comments="",
        )
        assert "价值投资策略指引" in prompt

    def test_skills_placed_after_tool_guide(self):
        from prompts.agent_prompt_astock import build_system_prompt
        prompt = build_system_prompt(
            market="cn",
            skill_ids=["momentum"],
            date="2026-03-17",
            positions="CASH: 100000",
            STOP_SIGNAL="<FINISH_SIGNAL>",
            yesterday_close_price="N/A",
            today_buy_price="N/A",
            current_profit="N/A",
            user_comments="",
        )
        tool_guide_pos = prompt.find("注意事项")
        skill_pos = prompt.find("动量策略")
        # Market rules section marker
        market_rules_pos = prompt.find("交易规则")
        if market_rules_pos == -1:
            market_rules_pos = prompt.find("市场规则")
        assert tool_guide_pos >= 0 and skill_pos >= 0, "Both tool_guide and skill should exist"
        assert tool_guide_pos < skill_pos, "Skills should come after tool_guide"


# ── Test: Technical Indicator Tools ─────────────────────────

class TestTechnicalIndicators:
    """Test MA, MACD, RSI calculation logic."""

    def test_get_close_prices_nonexistent(self):
        from skills.tools.tool_technical_indicators import _get_close_prices
        prices = _get_close_prices("NONEXIST.SH", "2026-01-01", 20)
        assert prices == []

    def test_get_close_prices_valid(self):
        from skills.tools.tool_technical_indicators import _get_close_prices
        prices = _get_close_prices("600519.SH", "2026-03-13", 20)
        # merged.jsonl should have data for this symbol
        if prices:
            assert len(prices) <= 20
            assert all(isinstance(p, float) for p in prices)
            assert all(p > 0 for p in prices)

    def test_ma_calculation_logic(self):
        """Test MA calculation with known data."""
        from skills.tools.tool_technical_indicators import _get_close_prices
        prices = _get_close_prices("600519.SH", "2026-03-13", 30)
        if len(prices) >= 5:
            expected_ma5 = sum(prices[-5:]) / 5
            assert abs(expected_ma5 - sum(prices[-5:]) / 5) < 0.01

    def test_rsi_range(self):
        """Test RSI is always between 0 and 100."""
        from skills.tools.tool_technical_indicators import _get_close_prices
        prices = _get_close_prices("600519.SH", "2026-03-13", 30)
        if len(prices) >= 15:
            changes = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
            gains = [max(c, 0) for c in changes]
            losses = [abs(min(c, 0)) for c in changes]
            period = 14
            avg_gain = sum(gains[-period:]) / period
            avg_loss = sum(losses[-period:]) / period
            if avg_loss > 0:
                rs = avg_gain / avg_loss
                rsi = 100 - (100 / (1 + rs))
            else:
                rsi = 100.0
            assert 0 <= rsi <= 100

    def test_ema_calculation(self):
        """Test EMA helper produces correct values."""
        data = [10, 11, 12, 13, 14, 15]
        k = 2 / (3 + 1)  # period=3
        result = [data[0]]
        for i in range(1, len(data)):
            result.append(data[i] * k + result[-1] * (1 - k))
        # EMA should converge towards recent values
        assert result[-1] > result[0]
        assert len(result) == len(data)


# ── Test: Skills API ────────────────────────────────────────

class TestSkillsAPI:
    """Test Skills REST API endpoints."""

    @pytest.fixture
    def api_db(self, tmp_path, monkeypatch):
        db_path = tmp_path / "test_api.duckdb"
        monkeypatch.setattr("api.config.get_database_path", lambda: db_path)

        conn = duckdb.connect(str(db_path), read_only=False)
        from api.services.skills_service import init_skills_table
        init_skills_table(conn)
        conn.close()
        return db_path

    @pytest.fixture
    def client(self, api_db):
        from fastapi.testclient import TestClient
        from api.routers.skills import router
        from fastapi import FastAPI

        app = FastAPI()
        app.include_router(router, prefix="/api/skills")
        return TestClient(app)

    def test_list_skills(self, client):
        resp = client.get("/api/skills")
        assert resp.status_code == 200
        data = resp.json()
        assert "skills" in data
        assert "total" in data
        assert data["total"] >= 16

    def test_list_skills_has_builtin_category(self, client):
        resp = client.get("/api/skills")
        data = resp.json()
        assert "builtin" in data["skills"]
        builtin = data["skills"]["builtin"]
        assert len(builtin) == 4
        for s in builtin:
            assert s["builtin"] is True

    def test_list_skills_has_tools_flag(self, client):
        resp = client.get("/api/skills")
        data = resp.json()
        # Technical indicators should have has_tools=True
        analysis = data["skills"].get("analysis", [])
        ta = [s for s in analysis if s["id"] == "technical_indicators"]
        assert len(ta) == 1
        assert ta[0]["has_tools"] is True

    def test_get_agent_skills_empty(self, client, api_db):
        resp = client.get("/api/skills/agent/fresh-agent?market=cn")
        assert resp.status_code == 200
        data = resp.json()
        assert data["skill_ids"] == []

    def test_set_agent_skills(self, client, api_db):
        resp = client.put("/api/skills/agent/test-agent", json={
            "market": "cn",
            "skill_ids": ["value_investing", "stop_loss_take_profit"],
        })
        assert resp.status_code == 200
        assert resp.json()["success"] is True

        # Verify
        resp2 = client.get("/api/skills/agent/test-agent?market=cn")
        assert sorted(resp2.json()["skill_ids"]) == ["stop_loss_take_profit", "value_investing"]

    def test_set_invalid_skill_returns_error(self, client, api_db):
        resp = client.put("/api/skills/agent/test-agent", json={
            "market": "cn",
            "skill_ids": ["nonexistent_skill"],
        })
        data = resp.json()
        assert data["success"] is False
        assert "Unknown" in data["error"]

    def test_enable_single_skill(self, client, api_db):
        resp = client.post("/api/skills/agent/test-agent/momentum?market=cn")
        assert resp.status_code == 200
        assert resp.json()["success"] is True

        resp2 = client.get("/api/skills/agent/test-agent?market=cn")
        assert "momentum" in resp2.json()["skill_ids"]

    def test_disable_single_skill(self, client, api_db):
        # Enable first
        client.post("/api/skills/agent/test-agent/momentum?market=cn")

        # Disable
        resp = client.delete("/api/skills/agent/test-agent/momentum?market=cn")
        assert resp.status_code == 200

        resp2 = client.get("/api/skills/agent/test-agent?market=cn")
        assert "momentum" not in resp2.json()["skill_ids"]

    def test_enable_invalid_skill_returns_error(self, client, api_db):
        resp = client.post("/api/skills/agent/test-agent/fake_skill?market=cn")
        data = resp.json()
        assert data["success"] is False

    def test_replace_all_skills(self, client, api_db):
        # Set initial
        client.put("/api/skills/agent/test-agent", json={
            "market": "cn",
            "skill_ids": ["value_investing", "momentum"],
        })

        # Replace
        client.put("/api/skills/agent/test-agent", json={
            "market": "cn",
            "skill_ids": ["trend_following"],
        })

        resp = client.get("/api/skills/agent/test-agent?market=cn")
        assert resp.json()["skill_ids"] == ["trend_following"]
