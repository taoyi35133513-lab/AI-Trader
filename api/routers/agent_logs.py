"""
Agent 对话日志 API 路由
"""

from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from api.dependencies import get_db
from api.services.conversation_service import ConversationService

router = APIRouter(prefix="/api/logs", tags=["agent-logs"])


# ===== Response Models =====


class MessageResponse(BaseModel):
    role: str
    content: Optional[str] = None
    tool_call_id: Optional[str] = None
    tool_name: Optional[str] = None
    timestamp: Optional[str] = None


class ConversationResponse(BaseModel):
    session_id: int
    agent_name: str
    session_date: str
    session_time: Optional[str] = None
    session_timestamp: Optional[str] = None
    messages: List[MessageResponse]


class ConversationSummary(BaseModel):
    session_id: int
    agent_name: str
    session_date: str
    session_time: Optional[str] = None
    session_timestamp: Optional[str] = None
    message_count: int
    first_message_preview: Optional[str] = None


class SearchResult(BaseModel):
    session_id: int
    agent_name: str
    session_date: str
    session_time: Optional[str] = None
    role: str
    content: Optional[str] = None
    timestamp: Optional[str] = None


# ===== Endpoints =====


@router.get("/{agent_name}/conversations", response_model=List[ConversationSummary])
async def list_conversations(
    agent_name: str,
    market: str = Query("cn", description="市场 (cn/cn_hour)"),
    start_date: Optional[date] = Query(None, description="开始日期"),
    end_date: Optional[date] = Query(None, description="结束日期"),
    limit: int = Query(50, le=200, description="返回数量"),
    offset: int = Query(0, ge=0, description="偏移量"),
    db=Depends(get_db),
):
    """获取 Agent 的会话列表（带分页）"""
    service = ConversationService(db)

    # 如果没有指定日期范围，使用默认范围
    if not start_date:
        start_date = date(2020, 1, 1)
    if not end_date:
        end_date = date(2099, 12, 31)

    sessions = service.get_sessions_by_date_range(
        agent_name=agent_name,
        start_date=start_date,
        end_date=end_date,
        market=market,
        limit=limit,
        offset=offset,
    )

    return sessions


@router.get("/{agent_name}/conversations/date/{session_date}", response_model=ConversationResponse)
async def get_conversation_by_date(
    agent_name: str,
    session_date: str,
    market: str = Query("cn", description="市场"),
    session_time: Optional[str] = Query(None, description="会话时间 (HH:MM:SS)，小时级市场需要"),
    db=Depends(get_db),
):
    """获取指定日期的完整对话"""
    service = ConversationService(db)

    conversation = service.get_conversation_by_date(
        agent_name=agent_name,
        session_date=session_date,
        market=market,
        session_time=session_time,
    )

    if not conversation:
        raise HTTPException(status_code=404, detail=f"No conversation found for {agent_name} on {session_date}")

    return conversation


@router.get("/{agent_name}/latest", response_model=List[ConversationResponse])
async def get_latest_conversations(
    agent_name: str,
    limit: int = Query(10, le=50, description="返回数量"),
    market: str = Query("cn", description="市场"),
    db=Depends(get_db),
):
    """获取最近 N 个交易会话"""
    service = ConversationService(db)

    conversations = service.get_latest_conversations(
        agent_name=agent_name,
        limit=limit,
        market=market,
    )

    return conversations


@router.get("/{agent_name}/search", response_model=List[SearchResult])
async def search_messages(
    agent_name: str,
    keyword: str = Query(..., min_length=2, description="搜索关键词"),
    market: str = Query("cn", description="市场"),
    start_date: Optional[date] = Query(None, description="开始日期"),
    end_date: Optional[date] = Query(None, description="结束日期"),
    role: Optional[str] = Query(None, description="消息角色过滤 (user/assistant/tool)"),
    limit: int = Query(50, le=200, description="返回数量"),
    db=Depends(get_db),
):
    """搜索对话消息"""
    service = ConversationService(db)

    results = service.search_conversations(
        agent_name=agent_name,
        keyword=keyword,
        start_date=start_date,
        end_date=end_date,
        role=role,
        market=market,
        limit=limit,
    )

    return results


@router.get("/all/sessions", response_model=List[ConversationSummary])
async def get_all_sessions(
    market: str = Query("cn", description="市场"),
    limit: int = Query(200, le=500, description="返回数量"),
    db=Depends(get_db),
):
    """获取所有 Agent 的会话（用于 dashboard）"""
    service = ConversationService(db)

    sessions = service.get_all_sessions(market=market, limit=limit)

    return sessions
