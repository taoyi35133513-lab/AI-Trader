"""
Agent Control Router

API endpoints for starting, monitoring, and controlling trading agents.
"""

from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from api.config import load_config_json
from api.services.agent_runner import AgentStatus, get_agent_runner

router = APIRouter()


class StartAgentRequest(BaseModel):
    """Request body for starting agents"""
    frequency: str = "daily"
    model_names: Optional[List[str]] = None  # If None, start all enabled


class StartAgentResponse(BaseModel):
    """Response for start agent request"""
    run_id: str
    model_name: str
    signature: str
    frequency: str
    status: str


class AgentRunResponse(BaseModel):
    """Response for agent run status"""
    run_id: str
    model_name: str
    signature: str
    frequency: str
    status: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error_message: Optional[str] = None
    progress: dict = {}


@router.post("/start", response_model=List[StartAgentResponse])
async def start_agents(request: StartAgentRequest):
    """
    Start trading agents.

    If model_names is provided, only start those models.
    Otherwise, start all enabled models from config.

    Args:
        request: StartAgentRequest with frequency and optional model names

    Returns:
        List of started agent runs
    """
    config = load_config_json("config.json")

    if not config:
        raise HTTPException(status_code=500, detail="Failed to load config")

    # Filter models if specific names requested
    if request.model_names:
        models = [
            m for m in config.get("models", [])
            if m.get("name") in request.model_names
        ]
        if not models:
            raise HTTPException(
                status_code=404,
                detail=f"No models found matching: {request.model_names}"
            )
        # Temporarily enable these models for this run
        for m in models:
            m["enabled"] = True
    else:
        models = [m for m in config.get("models", []) if m.get("enabled", False)]

    if not models:
        raise HTTPException(status_code=400, detail="No enabled models to start")

    # Validate frequency
    frequency = request.frequency
    if frequency not in ("daily", "hourly"):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid frequency: {frequency}. Must be 'daily' or 'hourly'"
        )

    runner = get_agent_runner()

    # Build a temporary config with selected models
    run_config = {
        "models": models,
        "date_range": config.get("date_range", {}),
        "market": config.get("market", "cn"),
    }

    runs = await runner.start_all_enabled_agents(run_config, frequency)

    return [
        StartAgentResponse(
            run_id=run.run_id,
            model_name=run.model_name,
            signature=run.signature,
            frequency=run.frequency,
            status=run.status.value,
        )
        for run in runs
    ]


@router.get("/runs", response_model=List[AgentRunResponse])
async def list_agent_runs():
    """
    List all agent runs (current and historical).

    Returns:
        List of all agent runs with their status
    """
    runner = get_agent_runner()
    runs = await runner.get_all_runs()

    return [
        AgentRunResponse(**run.to_dict())
        for run in runs
    ]


@router.get("/runs/{run_id}", response_model=AgentRunResponse)
async def get_agent_run(run_id: str):
    """
    Get status of a specific agent run.

    Args:
        run_id: The run ID to query

    Returns:
        Agent run details
    """
    runner = get_agent_runner()
    run = await runner.get_run(run_id)

    if not run:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    return AgentRunResponse(**run.to_dict())


@router.post("/runs/{run_id}/cancel")
async def cancel_agent_run(run_id: str):
    """
    Cancel a running agent.

    Args:
        run_id: The run ID to cancel

    Returns:
        Success status
    """
    runner = get_agent_runner()
    success = await runner.cancel_run(run_id)

    if not success:
        run = await runner.get_run(run_id)
        if not run:
            raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
        raise HTTPException(
            status_code=400,
            detail=f"Cannot cancel run with status: {run.status.value}"
        )

    return {"status": "cancelled", "run_id": run_id}


@router.get("/status")
async def get_agent_status():
    """
    Get overall agent system status.

    Returns:
        Summary of running, completed, and failed agents
    """
    runner = get_agent_runner()
    runs = await runner.get_all_runs()

    status_counts = {
        "pending": 0,
        "running": 0,
        "completed": 0,
        "failed": 0,
        "cancelled": 0,
    }

    for run in runs:
        status_counts[run.status.value] += 1

    return {
        "total_runs": len(runs),
        "status_counts": status_counts,
        "active_runs": [
            run.to_dict() for run in runs
            if run.status in (AgentStatus.PENDING, AgentStatus.RUNNING)
        ],
    }
