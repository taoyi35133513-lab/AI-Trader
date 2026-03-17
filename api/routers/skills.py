"""Skills API 路由

提供技能列表查询和 agent 技能管理接口。
"""

from typing import List, Optional

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()


class SkillInfo(BaseModel):
    id: str
    name: str
    name_en: str
    category: str
    description: str
    icon: str
    has_tools: bool = False


class AgentSkillsRequest(BaseModel):
    market: str = "cn"
    skill_ids: List[str]


@router.get("")
async def list_skills():
    """列出所有可用技能，按类别分组。"""
    from skills import get_all_skills

    all_skills = get_all_skills()
    by_category = {}
    for s in all_skills:
        cat = s["category"]
        if cat not in by_category:
            by_category[cat] = []
        by_category[cat].append(SkillInfo(
            id=s["id"],
            name=s["name"],
            name_en=s["name_en"],
            category=s["category"],
            description=s["description"],
            icon=s["icon"],
            has_tools=bool(s.get("tools_module")),
        ))

    return {"skills": by_category, "total": len(all_skills)}


@router.get("/agent/{agent_name}")
async def get_agent_skills(agent_name: str, market: str = "cn"):
    """获取 agent 当前激活的技能列表。"""
    from api.services.skills_service import get_agent_skills as _get

    skill_ids = _get(agent_name, market)
    return {"agent_name": agent_name, "market": market, "skill_ids": skill_ids}


@router.put("/agent/{agent_name}")
async def set_agent_skills(agent_name: str, request: AgentSkillsRequest):
    """设置 agent 的技能列表（替换所有现有技能）。"""
    from skills import get_skill
    from api.services.skills_service import set_agent_skills as _set

    # Validate skill IDs
    invalid = [sid for sid in request.skill_ids if not get_skill(sid)]
    if invalid:
        return {"success": False, "error": f"Unknown skill IDs: {invalid}"}

    _set(agent_name, request.market, request.skill_ids)
    return {"success": True, "agent_name": agent_name, "skill_ids": request.skill_ids}


@router.post("/agent/{agent_name}/{skill_id}")
async def enable_skill(agent_name: str, skill_id: str, market: str = "cn"):
    """为 agent 启用单个技能。"""
    from skills import get_skill
    from api.services.skills_service import add_agent_skill

    if not get_skill(skill_id):
        return {"success": False, "error": f"Unknown skill: {skill_id}"}

    add_agent_skill(agent_name, market, skill_id)
    return {"success": True}


@router.delete("/agent/{agent_name}/{skill_id}")
async def disable_skill(agent_name: str, skill_id: str, market: str = "cn"):
    """为 agent 禁用单个技能。"""
    from api.services.skills_service import remove_agent_skill

    remove_agent_skill(agent_name, market, skill_id)
    return {"success": True}
