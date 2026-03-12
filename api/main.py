"""
AI-Trader FastAPI 应用入口

Unified backend server that hosts:
- REST API endpoints for frontend
- MCP services for AI agents (Math, Trade, Search, Price)
- Agent control endpoints for starting/monitoring agents
"""

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from tools.logging_config import setup_logging

setup_logging()

logger = logging.getLogger(__name__)

from api.config import settings, load_config_json
from api.routers import agents, benchmarks, config, dashboard, prices, agent_control, live_trading, market_data, positions
from api.routers import agent_logs, agent_positions, trade_comments
from api.mcp_integration import get_mcp_apps, get_combined_lifespan


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Combined lifespan for FastAPI app and MCP services"""
    # Get MCP combined lifespan
    mcp_lifespan = get_combined_lifespan()

    # Initialize scheduler service (lazy loaded)
    from api.services.scheduler_service import get_scheduler_service
    scheduler = get_scheduler_service()

    # DB migrations
    try:
        import duckdb as _duckdb
        from api.config import get_database_path
        _conn = _duckdb.connect(str(get_database_path()))

        # Migrate: add system_prompt column if missing
        _cols = [r[1] for r in _conn.execute("PRAGMA table_info('agent_trading_sessions')").fetchall()]
        if "system_prompt" not in _cols:
            _conn.execute("ALTER TABLE agent_trading_sessions ADD COLUMN system_prompt TEXT")
            logger.info("Migrated: added system_prompt column to agent_trading_sessions")

        # Migrate: ensure trade_comments table has sequence-based auto-increment id
        _conn.execute("CREATE SEQUENCE IF NOT EXISTS trade_comments_id_seq START 1")
        _tables = [r[0] for r in _conn.execute("SHOW TABLES").fetchall()]
        if "trade_comments" not in _tables:
            _conn.execute("""
                CREATE TABLE trade_comments (
                    id INTEGER DEFAULT nextval('trade_comments_id_seq') PRIMARY KEY,
                    agent_name VARCHAR NOT NULL,
                    market VARCHAR NOT NULL DEFAULT 'cn',
                    trade_date VARCHAR(30) NOT NULL,
                    ts_code VARCHAR(20) NOT NULL,
                    action VARCHAR(10) NOT NULL,
                    comment_text TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            _conn.execute("CREATE INDEX IF NOT EXISTS idx_comments_agent ON trade_comments(agent_name)")
            _conn.execute("CREATE INDEX IF NOT EXISTS idx_comments_date ON trade_comments(trade_date)")
            _conn.execute("CREATE INDEX IF NOT EXISTS idx_comments_agent_market ON trade_comments(agent_name, market)")
            logger.info("Created trade_comments table with sequence")
        else:
            # Check if existing table has old schema (no sequence default)
            _id_col = [r for r in _conn.execute("PRAGMA table_info('trade_comments')").fetchall() if r[1] == 'id']
            if _id_col and 'nextval' not in str(_id_col[0]):
                # Old table without sequence — recreate (only if empty)
                _count = _conn.execute("SELECT COUNT(*) FROM trade_comments").fetchone()[0]
                if _count == 0:
                    _conn.execute("DROP TABLE trade_comments")
                    _conn.execute("""
                        CREATE TABLE trade_comments (
                            id INTEGER DEFAULT nextval('trade_comments_id_seq') PRIMARY KEY,
                            agent_name VARCHAR NOT NULL,
                            market VARCHAR NOT NULL DEFAULT 'cn',
                            trade_date VARCHAR(30) NOT NULL,
                            ts_code VARCHAR(20) NOT NULL,
                            action VARCHAR(10) NOT NULL,
                            comment_text TEXT NOT NULL,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    _conn.execute("CREATE INDEX IF NOT EXISTS idx_comments_agent ON trade_comments(agent_name)")
                    _conn.execute("CREATE INDEX IF NOT EXISTS idx_comments_date ON trade_comments(trade_date)")
                    _conn.execute("CREATE INDEX IF NOT EXISTS idx_comments_agent_market ON trade_comments(agent_name, market)")
                    logger.info("Recreated trade_comments table with sequence (was empty)")

        _conn.close()
    except Exception as e:
        logger.debug("DB migration check: %s", e)

    # Enter MCP lifespan
    async with mcp_lifespan(app):
        # Auto-start scheduler in live trading mode
        live_mode = os.environ.get("AI_TRADER_MODE") == "live"
        if live_mode:
            frequency = os.environ.get("AI_TRADER_FREQUENCY", "daily")
            config_data = load_config_json("config.json")
            if config_data:
                market = config_data.get("market", "cn")
                # Support compound frequency like "daily+hourly"
                frequencies = frequency.split("+")
                for freq in frequencies:
                    freq = freq.strip()
                    if freq:
                        logger.info("[Live Mode] Auto-starting scheduler (%s, %s)", freq, market)
                        await scheduler.start_scheduler(config_data, freq, market)
            else:
                logger.warning("[Live Mode] Failed to load config, scheduler not started")

        yield

    # Cleanup: stop scheduler if running
    if scheduler.is_running:
        await scheduler.stop_scheduler()


# 创建 FastAPI 应用
app = FastAPI(
    title="AI-Trader API",
    description="AI-Trader 交易系统统一后端，提供 REST API 和 MCP 服务",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# 配置 CORS - Allow all origins for development
# In production, specify exact origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for frontend separation
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global exception handler — returns consistent JSON error responses
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error("Unhandled exception on %s %s: %s", request.method, request.url.path, exc, exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"error": str(exc), "path": str(request.url.path)},
    )


# 注册 REST API 路由
app.include_router(agents.router, prefix="/api/agents", tags=["Agents"])
app.include_router(prices.router, prefix="/api/prices", tags=["Prices"])
app.include_router(benchmarks.router, prefix="/api/benchmarks", tags=["Benchmarks"])
app.include_router(dashboard.router, prefix="/api/dashboard", tags=["Dashboard"])
app.include_router(config.router, prefix="/api/config", tags=["Config"])
app.include_router(agent_control.router, prefix="/api/agent-control", tags=["Agent Control"])
app.include_router(live_trading.router, prefix="/api/live-trading", tags=["Live Trading"])
app.include_router(market_data.router, prefix="/api/market-data", tags=["Market Data"])
app.include_router(positions.router, prefix="/api/positions", tags=["Positions"])

# 新增：DuckDB 统一数据 API
app.include_router(agent_logs.router, tags=["Agent Logs"])
app.include_router(agent_positions.router, tags=["Agent Positions V2"])
app.include_router(trade_comments.router, tags=["Trade Comments"])

# 挂载 MCP 服务
# Each MCP service is mounted at /mcp/{service_name}/
# Agents connect to: http://localhost:8888/mcp/{service}/mcp
mcp_apps = get_mcp_apps()
for name, mcp_app in mcp_apps.items():
    app.mount(f"/mcp/{name}", mcp_app)


@app.get("/")
async def root():
    """根路径"""
    return {
        "name": "AI-Trader Unified Backend",
        "version": "2.3.0",
        "docs": "/docs",
        "endpoints": {
            "api": "/api/",
            "agent_control": "/api/agent-control/",
            "live_trading": "/api/live-trading/",
            "market_data": "/api/market-data/",
            "positions": "/api/positions/",
            "logs": "/api/logs/",
            "positions_v2": "/api/positions/",
            "mcp_math": "/mcp/math/mcp",
            "mcp_trade": "/mcp/trade/mcp",
            "mcp_search": "/mcp/search/mcp",
            "mcp_price": "/mcp/price/mcp",
        },
    }


@app.get("/health")
@app.get("/api/health")
async def health_check():
    """Health check with per-service MCP probing."""
    import httpx
    from api.services.scheduler_service import get_scheduler_service

    scheduler = get_scheduler_service()

    mcp_services = {
        "mcp_math": "/mcp/math/mcp",
        "mcp_trade": "/mcp/trade/mcp",
        "mcp_search": "/mcp/search/mcp",
        "mcp_price": "/mcp/price/mcp",
    }

    service_status: dict[str, str] = {"api": "ok"}

    # Probe each MCP service with a lightweight GET
    async with httpx.AsyncClient(base_url="http://127.0.0.1:8888", timeout=2.0) as client:
        for name, path in mcp_services.items():
            try:
                resp = await client.get(path)
                service_status[name] = "ok" if resp.status_code < 500 else f"error ({resp.status_code})"
            except Exception as exc:
                service_status[name] = f"unreachable ({type(exc).__name__})"

    service_status["live_scheduler"] = "running" if scheduler.is_running else "stopped"

    all_ok = all(v == "ok" for k, v in service_status.items() if k != "live_scheduler")
    return {
        "status": "healthy" if all_ok else "degraded",
        "services": service_status,
    }


if __name__ == "__main__":
    import uvicorn

    # Use port 8888 for unified backend (avoiding conflict with old MCP ports)
    uvicorn.run(
        "api.main:app",
        host=settings.api_host,
        port=8888,  # Unified port
        reload=settings.debug,
    )
