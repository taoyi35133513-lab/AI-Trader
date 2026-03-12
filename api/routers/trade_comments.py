"""
交易点评 API 路由
"""

import logging
from typing import Optional

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from api.dependencies import get_db
from api.services.comment_service import CommentService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/trade-comments", tags=["Trade Comments"])


# ===== Request/Response Models =====


class CommentCreate(BaseModel):
    agent_name: str
    market: str = "cn"
    trade_date: str
    ts_code: str
    action: str
    comment_text: str


class CommentUpdate(BaseModel):
    comment_text: str


class CommentResponse(BaseModel):
    id: int
    agent_name: str
    market: str
    trade_date: str
    ts_code: str
    action: str
    comment_text: str
    created_at: str
    updated_at: Optional[str] = None


# ===== Helpers =====


def _ensure_table(conn: duckdb.DuckDBPyConnection):
    """Ensure the trade_comments table and sequence exist."""
    conn.execute("CREATE SEQUENCE IF NOT EXISTS trade_comments_id_seq START 1")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trade_comments (
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


# ===== Endpoints =====


@router.post("/", response_model=CommentResponse)
async def create_comment(body: CommentCreate, db=Depends(get_db)):
    """创建交易点评"""
    _ensure_table(db)
    service = CommentService(db)
    return service.add_comment(
        agent_name=body.agent_name,
        market=body.market,
        trade_date=body.trade_date,
        ts_code=body.ts_code,
        action=body.action,
        comment_text=body.comment_text,
    )


@router.get("/{agent_name}", response_model=list[CommentResponse])
async def get_agent_comments(
    agent_name: str,
    trade_date: Optional[str] = Query(None, description="按交易日期过滤"),
    ts_code: Optional[str] = Query(None, description="按股票代码过滤"),
    limit: int = Query(50, le=200),
    db=Depends(get_db),
):
    """获取 agent 的点评列表"""
    _ensure_table(db)
    service = CommentService(db)
    return service.get_comments_by_agent(
        agent_name=agent_name,
        trade_date=trade_date,
        ts_code=ts_code,
        limit=limit,
    )


@router.get("/{agent_name}/trade", response_model=list[CommentResponse])
async def get_trade_comments(
    agent_name: str,
    trade_date: str = Query(..., description="交易日期"),
    ts_code: str = Query(..., description="股票代码"),
    action: str = Query(..., description="交易动作 (buy/sell/hold)"),
    db=Depends(get_db),
):
    """获取特定交易的点评"""
    _ensure_table(db)
    service = CommentService(db)
    return service.get_comments_for_trade(
        agent_name=agent_name,
        trade_date=trade_date,
        ts_code=ts_code,
        action=action,
    )


@router.put("/{comment_id}", response_model=CommentResponse)
async def update_comment(comment_id: int, body: CommentUpdate, db=Depends(get_db)):
    """更新点评"""
    _ensure_table(db)
    service = CommentService(db)
    result = service.update_comment(comment_id, body.comment_text)
    if not result:
        raise HTTPException(status_code=404, detail="Comment not found")
    return result


@router.delete("/{comment_id}")
async def delete_comment(comment_id: int, db=Depends(get_db)):
    """删除点评"""
    _ensure_table(db)
    service = CommentService(db)
    if not service.delete_comment(comment_id):
        raise HTTPException(status_code=404, detail="Comment not found")
    return {"ok": True}
