"""
Live Trading Router

API endpoints for controlling live trading scheduler.
"""

from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from api.config import load_config_json
from api.services.scheduler_service import get_scheduler_service

router = APIRouter()


# Request/Response Models

class StartSchedulerRequest(BaseModel):
    """Request body for starting scheduler"""
    frequency: str = "daily"
    model_names: Optional[List[str]] = None  # If None, use all enabled models


class SchedulerJobInfo(BaseModel):
    """Job information"""
    id: str
    name: str
    trigger: Optional[str] = None
    next_run: Optional[str] = None


class SchedulerStatusResponse(BaseModel):
    """Response for scheduler status"""
    running: bool
    frequency: Optional[str] = None
    market: str = "cn"
    started_at: Optional[str] = None
    jobs: List[SchedulerJobInfo] = []
    next_runs: List[SchedulerJobInfo] = []
    last_execution: Optional[dict] = None
    error_message: Optional[str] = None


class TriggerResponse(BaseModel):
    """Response for manual trigger"""
    success: bool
    message: Optional[str] = None
    error: Optional[str] = None


# Endpoints

@router.post("/start", response_model=SchedulerStatusResponse)
async def start_scheduler(request: StartSchedulerRequest):
    """
    Start live trading scheduler.

    This will schedule trading sessions based on the specified frequency:
    - daily: Runs at 09:35 on weekdays (5 min after market open)
    - hourly: Runs at 10:35, 11:35, 14:05, 15:05 on weekdays

    Args:
        request: StartSchedulerRequest with frequency and optional model names

    Returns:
        Current scheduler status
    """
    # Load config
    config = load_config_json("config.json")
    if not config:
        raise HTTPException(status_code=500, detail="Failed to load config")

    # Filter models if specific names requested
    if request.model_names:
        filtered_models = [
            m for m in config.get("models", [])
            if m.get("name") in request.model_names
        ]
        if not filtered_models:
            raise HTTPException(
                status_code=404,
                detail=f"No models found matching: {request.model_names}"
            )
        # Temporarily enable these models
        for m in filtered_models:
            m["enabled"] = True
        config["models"] = filtered_models

    # Validate frequency
    if request.frequency not in ("daily", "hourly"):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid frequency: {request.frequency}. Must be 'daily' or 'hourly'"
        )

    # Check for enabled models
    enabled_models = [m for m in config.get("models", []) if m.get("enabled", False)]
    if not enabled_models:
        raise HTTPException(status_code=400, detail="No enabled models in config")

    # Start scheduler
    scheduler = get_scheduler_service()
    market = config.get("market", "cn")
    status = await scheduler.start_scheduler(config, request.frequency, market)

    if status.error_message:
        raise HTTPException(status_code=400, detail=status.error_message)

    return _status_to_response(status)


@router.post("/stop", response_model=SchedulerStatusResponse)
async def stop_scheduler():
    """
    Stop live trading scheduler.

    This will gracefully stop all scheduled trading jobs.

    Returns:
        Current scheduler status
    """
    scheduler = get_scheduler_service()
    status = await scheduler.stop_scheduler()

    return _status_to_response(status)


@router.get("/status", response_model=SchedulerStatusResponse)
async def get_scheduler_status():
    """
    Get current scheduler status.

    Returns:
        Current scheduler status including jobs and next run times
    """
    scheduler = get_scheduler_service()
    status = await scheduler.get_status()

    return _status_to_response(status)


@router.get("/schedule", response_model=List[SchedulerJobInfo])
async def get_schedule():
    """
    Get upcoming scheduled execution times.

    Returns:
        List of scheduled jobs with next run times
    """
    scheduler = get_scheduler_service()
    status = await scheduler.get_status()

    return [
        SchedulerJobInfo(
            id=job.get("id", ""),
            name=job.get("name", ""),
            next_run=job.get("next_run"),
        )
        for job in status.next_runs
    ]


@router.post("/trigger", response_model=TriggerResponse)
async def trigger_trading_session():
    """
    Manually trigger a trading session immediately.

    This is useful for testing. The scheduler must be started first
    (to load configuration), or you can use the backtest mode instead.

    Returns:
        Execution result
    """
    scheduler = get_scheduler_service()
    result = await scheduler.trigger_now()

    return TriggerResponse(
        success=result.get("success", False),
        message=result.get("message"),
        error=result.get("error"),
    )


def _status_to_response(status) -> SchedulerStatusResponse:
    """Convert SchedulerStatus to SchedulerStatusResponse"""
    return SchedulerStatusResponse(
        running=status.running,
        frequency=status.frequency,
        market=status.market,
        started_at=status.started_at.isoformat() if status.started_at else None,
        jobs=[
            SchedulerJobInfo(
                id=job.get("id", ""),
                name=job.get("name", ""),
                trigger=job.get("trigger"),
            )
            for job in status.jobs
        ],
        next_runs=[
            SchedulerJobInfo(
                id=job.get("id", ""),
                name=job.get("name", ""),
                next_run=job.get("next_run"),
            )
            for job in status.next_runs
        ],
        last_execution=status.last_execution,
        error_message=status.error_message,
    )
