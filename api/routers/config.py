"""
Configuration API endpoint for frontend consumption.
Provides agent and market configuration without requiring YAML file generation.
"""

from typing import List

from fastapi import APIRouter

from api.config import load_config_json

router = APIRouter()

# Provider to icon mapping
PROVIDER_ICONS = {
    "google": "./figs/google.svg",
    "openai": "./figs/openai.svg",
    "anthropic": "./figs/claude-color.svg",
    "deepseek": "./figs/deepseek.svg",
    "qwen": "./figs/qwen.svg",
    "minimax": "./figs/minimax.svg",
    "zhipu": "./figs/zhipu-color.svg",
}

# Provider to color mapping
PROVIDER_COLORS = {
    "google": "#00d4ff",
    "openai": "#ffbe0b",
    "anthropic": "#8338ec",
    "deepseek": "#ff006e",
    "qwen": "#00ffcc",
    "minimax": "#3a86ff",
    "zhipu": "#6610f2",
}


def _get_provider(model_name: str) -> str:
    """Determine provider from model name"""
    name_lower = model_name.lower()
    if name_lower.startswith("gemini"):
        return "google"
    if name_lower.startswith("gpt"):
        return "openai"
    if name_lower.startswith("claude"):
        return "anthropic"
    if name_lower.startswith("deepseek"):
        return "deepseek"
    if name_lower.startswith("qwen"):
        return "qwen"
    if name_lower.startswith("minimax"):
        return "minimax"
    if name_lower.startswith("glm"):
        return "zhipu"
    return "default"


def _display_name(model_name: str) -> str:
    """Generate display-friendly name from model name"""
    name_lower = model_name.lower()
    if name_lower.startswith("gemini-"):
        parts = model_name.split("-")[1:]
        formatted = []
        for p in parts:
            if p.replace(".", "").isdigit():
                formatted.append(p)
            else:
                formatted.append(p.capitalize())
        return "Gemini " + " ".join(formatted)
    if name_lower.startswith("gpt-"):
        return model_name.replace("gpt-", "GPT-")
    if name_lower.startswith("claude-"):
        return model_name.replace("claude-", "Claude ")
    if name_lower.startswith("deepseek-"):
        return model_name.replace("deepseek-", "DeepSeek ")
    if name_lower.startswith("qwen"):
        return model_name.replace("qwen", "Qwen")
    if name_lower.startswith("minimax"):
        return model_name.replace("minimax", "MiniMax")
    if name_lower.startswith("glm"):
        return model_name.replace("glm", "GLM")
    return model_name


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

    # Build agents list from enabled models
    agents = []
    for model in config.get("models", []):
        if model.get("enabled", False):
            name = model["name"]
            provider = _get_provider(name)
            # Derive folder/signature based on frequency
            folder = f"{name}-astock-hour" if frequency == "hourly" else name
            agents.append({
                "name": name,
                "display_name": _display_name(name),
                "folder": folder,
                "icon": PROVIDER_ICONS.get(provider, "./figs/stock.svg"),
                "color": PROVIDER_COLORS.get(provider, "#999999"),
                "enabled": True
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

    # Build agents list for each market/frequency
    def build_agents(frequency: str, enabled_only: bool = False):
        agents = []
        for model in config.get("models", []):
            if enabled_only and not model.get("enabled", False):
                continue
            name = model["name"]
            provider = _get_provider(name)
            folder = f"{name}-astock-hour" if frequency == "hourly" else name
            agents.append({
                "folder": folder,
                "display_name": _display_name(name),
                "icon": PROVIDER_ICONS.get(provider, "./figs/stock.svg"),
                "color": PROVIDER_COLORS.get(provider, "#999999"),
                "enabled": model.get("enabled", False)
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
            "base_url": "http://localhost:8888",
            "fallback_to_files": False
        }
    }


@router.get("/models")
async def get_models():
    """Get all available models (enabled and disabled)"""
    config = load_config_json("config.json")

    models = []
    for model in config.get("models", []):
        name = model["name"]
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
