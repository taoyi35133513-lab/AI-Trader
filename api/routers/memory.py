"""
Agent 记忆管理 API
"""

from typing import Optional

import duckdb
from fastapi import APIRouter
from pydantic import BaseModel, Field

from api.config import get_database_path
from api.services.memory_service import MemoryService, init_memory_table

router = APIRouter()


def _get_memory_service() -> MemoryService:
    db_path = get_database_path()
    conn = duckdb.connect(str(db_path), read_only=False)
    return MemoryService(conn)


class MemoryCreate(BaseModel):
    agent_name: str
    market: str = "cn"
    level: str = Field(description="reflection | lesson | strategy")
    content: str
    source_dates: Optional[str] = None
    tags: Optional[str] = None


@router.get("/list")
async def list_memories(agent_name: str, market: str = "cn", include_archived: bool = False):
    """List all memories for an agent."""
    svc = _get_memory_service()
    try:
        memories = svc.get_all_memories(agent_name, market, include_archived)
        return {"memories": memories}
    finally:
        svc.conn.close()


@router.get("/active")
async def get_active_memories(agent_name: str, market: str = "cn"):
    """Get active memories grouped by level (what gets injected into prompt)."""
    svc = _get_memory_service()
    try:
        memories = svc.get_active_memories(agent_name, market)
        return {"memories": memories}
    finally:
        svc.conn.close()


@router.get("/stats")
async def get_memory_stats(agent_name: str, market: str = "cn"):
    """Get memory statistics for an agent."""
    svc = _get_memory_service()
    try:
        stats = svc.get_memory_stats(agent_name, market)
        return {"stats": stats}
    finally:
        svc.conn.close()


@router.post("/add")
async def add_memory(req: MemoryCreate):
    """Manually add a memory."""
    svc = _get_memory_service()
    try:
        if req.level == "reflection":
            mid = svc.add_reflection(req.agent_name, req.market, req.content, req.source_dates or "", tags=req.tags)
        elif req.level == "lesson":
            mid = svc.add_lesson(req.agent_name, req.market, req.content, req.source_dates or "", "")
        elif req.level == "strategy":
            mid = svc.update_strategy(req.agent_name, req.market, req.content, req.source_dates or "")
        else:
            return {"error": f"Invalid level: {req.level}"}
        return {"id": mid, "status": "created"}
    finally:
        svc.conn.close()


@router.post("/archive/{memory_id}")
async def archive_memory(memory_id: int):
    """Archive a specific memory."""
    svc = _get_memory_service()
    try:
        svc.archive_memories([memory_id])
        return {"status": "archived"}
    finally:
        svc.conn.close()


@router.delete("/{memory_id}")
async def delete_memory(memory_id: int):
    """Delete a specific memory."""
    svc = _get_memory_service()
    try:
        svc.delete_memory(memory_id)
        return {"status": "deleted"}
    finally:
        svc.conn.close()


@router.post("/consolidate")
async def trigger_consolidation(agent_name: str, market: str = "cn"):
    """Manually trigger memory consolidation."""
    from api.services.memory_consolidation import consolidate_l1_to_l2, consolidate_l2_to_l3

    svc = _get_memory_service()
    try:
        results = {}
        if svc.should_consolidate_l1(agent_name, market):
            await consolidate_l1_to_l2(svc, agent_name, market)
            results["l1_to_l2"] = "triggered"
        else:
            results["l1_to_l2"] = "not_needed"

        if svc.should_consolidate_l2(agent_name, market):
            await consolidate_l2_to_l3(svc, agent_name, market)
            results["l2_to_l3"] = "triggered"
        else:
            results["l2_to_l3"] = "not_needed"

        return {"status": "ok", "results": results}
    finally:
        svc.conn.close()
