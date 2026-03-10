"""
Configuration API endpoint for frontend consumption.
Provides agent and market configuration without requiring YAML file generation.
"""

import logging
from typing import List, Optional

from fastapi import APIRouter

from api.config import get_data_dir, load_config_json
from api.utils.model_display import (
    PROVIDER_COLORS,
    PROVIDER_ICONS,
    display_name as _display_name,
    get_provider as _get_provider,
    iter_valid_models as _iter_valid_models,
    resolve_model_name as _resolve_model_name,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("")
async def get_config(frequency: str = "daily"):
    """
    Get frontend configuration.

    Args:
        frequency: Trading frequency ("daily" or "hourly")

    Returns:
        Configuration object with market info, agents, and UI settings
    """
    config = load_config_json("config.json")

    # Derive data directory from frequency
    suffix = "_hour" if frequency == "hourly" else ""
    data_dir = f"agent_data_astock{suffix}"

    # Build agents list by scanning data directories + config
    is_hourly = frequency == "hourly"
    market_key = "cn_hour" if is_hourly else "cn"

    # Config metadata lookup
    config_meta = {}
    for model, name in _iter_valid_models(config):
        folder = f"{name}-astock-hour" if is_hourly else name
        config_meta[folder] = (model, name)

    discovered_folders: set = set()

    # Scan data directory
    scan_dir = get_data_dir(market_key)
    if scan_dir.exists():
        for child in scan_dir.iterdir():
            if child.is_dir():
                position_file = child / "position" / "position.jsonl"
                if position_file.exists():
                    folder_name = child.name
                    if is_hourly and not folder_name.endswith("-astock-hour"):
                        continue
                    if not is_hourly and folder_name.endswith("-astock-hour"):
                        continue
                    discovered_folders.add(folder_name)

    # Also include config models
    for folder_name in config_meta:
        discovered_folders.add(folder_name)

    agents = []
    for folder_name in sorted(discovered_folders):
        base_name = folder_name
        if is_hourly and base_name.endswith("-astock-hour"):
            base_name = base_name[: -len("-astock-hour")]

        cfg_entry = config_meta.get(folder_name)
        display = _display_name(cfg_entry[1] if cfg_entry else base_name)

        provider = _get_provider(base_name)
        agents.append({
            "name": base_name,
            "display_name": display,
            "folder": folder_name,
            "icon": PROVIDER_ICONS.get(provider, "./figs/stock.svg"),
            "color": PROVIDER_COLORS.get(provider, "#999999"),
            "enabled": cfg_entry[0].get("enabled", False) if cfg_entry else False,
        })

    # Market info based on frequency
    market = config.get("market", "cn")
    time_granularity = "hourly" if frequency == "hourly" else "daily"

    if market == "cn":
        market_info = {
            "name": "A-Shares (SSE 50)" if frequency == "daily" else "A-Shares (Hourly)",
            "currency": "CNY",
            "benchmark": "SSE 50",
            "benchmark_display_name": "SSE 50 Index",
            "time_granularity": time_granularity,
            "icon": "\U0001F1E8\U0001F1F3",  # China flag emoji
        }
    else:
        market_info = {
            "name": "US Market",
            "currency": "USD",
            "benchmark": "QQQ",
            "benchmark_display_name": "QQQ Invesco",
            "time_granularity": time_granularity,
            "icon": "\U0001F1FA\U0001F1F8",  # US flag emoji
        }

    return {
        "market": market,
        "frequency": frequency,
        "data_dir": data_dir,
        "agents": agents,
        "market_info": market_info,
        "date_range": config.get("date_range", {}),
        "ui_settings": {
            "initial_value": 100000,
            "max_recent_trades": 20,
            "date_formats": {
                "hourly": "MM/DD HH:mm",
                "daily": "YYYY-MM-DD"
            }
        },
        "chart_settings": {
            "default_scale": "linear",
            "max_ticks": 15,
            "point_radius": 0,
            "point_hover_radius": 7,
            "border_width": 3,
            "tension": 0.42
        }
    }


@router.get("/full")
async def get_full_config():
    """
    Get full frontend configuration in YAML-compatible format.

    This returns the complete configuration structure that matches the
    legacy config.yaml format, allowing the frontend to work without
    loading a static YAML file.
    """
    config = load_config_json("config.json")

    # Build agents list for each market/frequency by scanning data directories
    def build_agents(frequency: str, enabled_only: bool = False):
        is_hourly = frequency == "hourly"
        market = "cn_hour" if is_hourly else "cn"

        # Build config metadata lookup keyed by folder name
        config_meta = {}
        for model, name in _iter_valid_models(config):
            folder = f"{name}-astock-hour" if is_hourly else name
            config_meta[folder] = (model, name)

        discovered_folders: set = set()

        # Source 1: Scan data directory for agent folders with position data
        data_dir = get_data_dir(market)
        if data_dir.exists():
            for child in data_dir.iterdir():
                if child.is_dir():
                    position_file = child / "position" / "position.jsonl"
                    if position_file.exists():
                        folder_name = child.name
                        if is_hourly and not folder_name.endswith("-astock-hour"):
                            continue
                        if not is_hourly and folder_name.endswith("-astock-hour"):
                            continue
                        discovered_folders.add(folder_name)

        # Source 2: Config models (add any not yet discovered)
        for folder_name in config_meta:
            discovered_folders.add(folder_name)

        agents = []
        for folder_name in sorted(discovered_folders):
            if enabled_only:
                model_cfg = config_meta.get(folder_name)
                if not model_cfg or not model_cfg[0].get("enabled", False):
                    continue

            # Determine base name for display/provider
            base_name = folder_name
            if is_hourly and base_name.endswith("-astock-hour"):
                base_name = base_name[: -len("-astock-hour")]

            # Use config name if available
            cfg_entry = config_meta.get(folder_name)
            display = _display_name(cfg_entry[1] if cfg_entry else base_name)
            is_enabled = cfg_entry[0].get("enabled", False) if cfg_entry else False

            provider = _get_provider(base_name)
            agents.append({
                "folder": folder_name,
                "display_name": display,
                "icon": PROVIDER_ICONS.get(provider, "./figs/stock.svg"),
                "color": PROVIDER_COLORS.get(provider, "#999999"),
                "enabled": is_enabled
            })
        return agents

    return {
        "markets": {
            "cn": {
                "name": "A-Shares (SSE 50)",
                "subtitle": "Track how different AI models perform in SSE 50 A-share stock trading",
                "data_dir": "agent_data_astock",
                "benchmark_file": "A_stock/index_daily_sse_50.json",
                "benchmark_name": "SSE 50",
                "benchmark_display_name": "SSE 50 Index",
                "currency": "CNY",
                "icon": "🇨🇳",
                "price_data_type": "merged",
                "price_data_file": "A_stock/merged.jsonl",
                "time_granularity": "daily",
                "enabled": True,
                "agents": build_agents("daily")
            },
            "cn_hour": {
                "name": "A-Shares (Hourly)",
                "subtitle": "Track how different AI models perform in SSE 50 A-share stock trading (Hourly)",
                "data_dir": "agent_data_astock_hour",
                "benchmark_file": "A_stock/index_daily_sse_50.json",
                "benchmark_name": "SSE 50",
                "benchmark_display_name": "SSE 50 Index",
                "currency": "CNY",
                "icon": "🇨🇳",
                "price_data_type": "merged",
                "price_data_file": "A_stock/merged_hourly.jsonl",
                "time_granularity": "hourly",
                "enabled": False,  # Hidden from main selector, toggled via JS
                "agents": build_agents("hourly")
            }
        },
        "data": {
            "base_path": "./data",
            "price_file_prefix": "daily_prices_",
            "benchmark_file": "Adaily_prices_QQQ.json"
        },
        "benchmark": {
            "folder": "QQQ",
            "display_name": "QQQ Invesco",
            "icon": "./figs/stock.svg",
            "color": "#ff6b00",
            "enabled": True
        },
        "chart": {
            "default_scale": "linear",
            "max_ticks": 15,
            "point_radius": 0,
            "point_hover_radius": 7,
            "border_width": 3,
            "tension": 0.42
        },
        "ui": {
            "initial_value": 100000,
            "max_recent_trades": 20,
            "date_formats": {
                "hourly": "MM/DD HH:mm",
                "daily": "YYYY-MM-DD"
            }
        },
        "api": {
            "enabled": True,
            "base_url": "",
            "fallback_to_files": False
        }
    }


@router.get("/models")
async def get_models():
    """Get all available models (enabled and disabled)"""
    config = load_config_json("config.json")

    models = []
    for model, name in _iter_valid_models(config):
        provider = _get_provider(name)
        models.append({
            "name": name,
            "basemodel": model.get("basemodel"),
            "display_name": _display_name(name),
            "enabled": model.get("enabled", False),
            "provider": provider,
            "icon": PROVIDER_ICONS.get(provider, "./figs/stock.svg"),
            "color": PROVIDER_COLORS.get(provider, "#999999"),
        })

    return {"models": models}
