"""
投资大师点评 API 路由
"""

from typing import Optional

from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from api.services.master_commentary_service import stream_commentary
from prompts.masters import MASTER_REGISTRY

router = APIRouter()


class CommentaryRequest(BaseModel):
    agent_name: str
    market: str = "cn"
    master_id: str = Field(default="buffett")
    days: int = Field(default=30, ge=1, le=365)
    model_name: Optional[str] = None


@router.get("/masters")
async def list_masters():
    """返回可用的投资大师列表"""
    masters = []
    for mid, info in MASTER_REGISTRY.items():
        masters.append({
            "id": info["id"],
            "name": info["name"],
            "name_en": info["name_en"],
            "avatar": info["avatar"],
            "description": info["description"],
        })
    return {"masters": masters}


@router.post("/stream")
async def stream_master_commentary(req: CommentaryRequest):
    """流式生成大师点评 (SSE)"""
    return StreamingResponse(
        stream_commentary(
            agent_name=req.agent_name,
            market=req.market,
            master_id=req.master_id,
            days=req.days,
            model_name=req.model_name,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
