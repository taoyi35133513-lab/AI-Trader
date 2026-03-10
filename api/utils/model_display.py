"""
Model display utilities.

Shared by both routers and services to avoid circular imports.
"""

from typing import Optional

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


def get_provider(model_name: str) -> str:
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


def display_name(model_name: str) -> str:
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


def resolve_model_name(model: dict) -> Optional[str]:
    """Resolve a stable model name from flexible config formats."""
    if not isinstance(model, dict):
        return None

    name = model.get("name") or model.get("signature")
    if isinstance(name, str) and name.strip():
        return name.strip()

    # Fallback: derive from basemodel (e.g. "openai/gpt-5" -> "gpt-5")
    basemodel = model.get("basemodel")
    if isinstance(basemodel, str) and basemodel.strip():
        return basemodel.split("/")[-1].strip()

    return None


def iter_valid_models(config: dict):
    """Yield normalized model entries and skip invalid items safely."""
    for raw in config.get("models", []):
        if not isinstance(raw, dict):
            continue
        name = resolve_model_name(raw)
        if not name:
            continue
        yield raw, name
