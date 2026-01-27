"""
AI-Trader FastAPI 应用入口

Unified backend server that hosts:
- REST API endpoints for frontend
- MCP services for AI agents (Math, Trade, Search, Price)
- Agent control endpoints for starting/monitoring agents
"""

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.config import settings, load_config_json
from api.routers import agents, benchmarks, config, dashboard, prices, agent_control, live_trading, market_data, positions
from api.routers import agent_logs, agent_positions
from api.mcp_integration import get_mcp_apps, get_combined_lifespan


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Combined lifespan for FastAPI app and MCP services"""
    # Get MCP combined lifespan
    mcp_lifespan = get_combined_lifespan()

    # Initialize scheduler service (lazy loaded)
    from api.services.scheduler_service import get_scheduler_service
    scheduler = get_scheduler_service()

    # Enter MCP lifespan
    async with mcp_lifespan(app):
        # Auto-start scheduler in live trading mode
        live_mode = os.environ.get("AI_TRADER_MODE") == "live"
        if live_mode:
            frequency = os.environ.get("AI_TRADER_FREQUENCY", "daily")
            config_data = load_config_json("config.json")
            if config_data:
                market = config_data.get("market", "cn")
                print(f"[Live Mode] Auto-starting scheduler ({frequency}, {market})")
                await scheduler.start_scheduler(config_data, frequency, market)
            else:
                print("[Live Mode] Warning: Failed to load config, scheduler not started")

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
    """健康检查"""
    from api.services.scheduler_service import get_scheduler_service
    scheduler = get_scheduler_service()

    return {
        "status": "healthy",
        "services": {
            "api": "running",
            "mcp_math": "running",
            "mcp_trade": "running",
            "mcp_search": "running",
            "mcp_price": "running",
            "live_scheduler": "running" if scheduler.is_running else "stopped",
        }
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
