"""
AI-Trader FastAPI 应用入口
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.config import settings
from api.routers import agents, benchmarks, dashboard, prices

# 创建 FastAPI 应用
app = FastAPI(
    title="AI-Trader API",
    description="AI-Trader 交易系统 API，提供持仓、价格、Agent 等数据接口",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# 配置 CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册路由
app.include_router(agents.router, prefix="/api/agents", tags=["Agents"])
app.include_router(prices.router, prefix="/api/prices", tags=["Prices"])
app.include_router(benchmarks.router, prefix="/api/benchmarks", tags=["Benchmarks"])
app.include_router(dashboard.router, prefix="/api/dashboard", tags=["Dashboard"])


@app.get("/")
async def root():
    """根路径"""
    return {
        "name": "AI-Trader API",
        "version": "1.0.0",
        "docs": "/docs",
    }


@app.get("/health")
@app.get("/api/health")
async def health_check():
    """健康检查"""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.debug,
    )
