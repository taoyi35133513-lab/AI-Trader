import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


def _log(msg: str, level: str = "INFO") -> None:
    """Print log message to stderr."""
    print(f"[{level}] {msg}", file=sys.stderr)


def _load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Invalid JSON config: {path}")
    return data


def _load_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Invalid YAML config: {path}")
    return data


def _market_id_from_backend_config(cfg: Dict[str, Any]) -> str:
    agent_type = str(cfg.get("agent_type", "") or "")
    market = str(cfg.get("market", "us") or "us")
    if agent_type == "BaseAgentAStock_Hour":
        return "cn_hour"
    if agent_type == "BaseAgentCrypto":
        return "crypto"
    if market in {"cn", "us"}:
        return market
    return "us"


def _provider_key(model_name: str) -> str:
    normalized = model_name.lower()
    if normalized.startswith("gemini"):
        return "google"
    if normalized.startswith("gpt"):
        return "openai"
    if normalized.startswith("claude"):
        return "anthropic"
    if normalized.startswith("deepseek"):
        return "deepseek"
    if normalized.startswith("qwen"):
        return "qwen"
    if normalized.startswith("minimax"):
        return "minimax"
    if normalized.startswith("glm"):
        return "zhipu"
    return "default"


def _default_icon(provider: str) -> str:
    icons = {
        "google": "./figs/google.svg",
        "openai": "./figs/openai.svg",
        "anthropic": "./figs/claude-color.svg",
        "deepseek": "./figs/deepseek.svg",
        "qwen": "./figs/qwen.svg",
        "minimax": "./figs/minimax.svg",
        "zhipu": "./figs/zhipu-color.svg",
        "default": "./figs/stock.svg",
    }
    return icons.get(provider, "./figs/stock.svg")


def _default_color(provider: str) -> str:
    colors = {
        "google": "#00d4ff",
        "qwen": "#00ffcc",
        "deepseek": "#ff006e",
        "openai": "#ffbe0b",
        "anthropic": "#8338ec",
        "minimax": "#3a86ff",
        "zhipu": "#6610f2",
        "default": "#999999",
    }
    return colors.get(provider, "#999999")


def _display_name(model_name: str) -> str:
    raw = str(model_name or "")
    normalized = raw.lower()
    if normalized.startswith("gemini-"):
        parts = raw.split("-")[1:]
        out = []
        for p in parts:
            if not p:
                continue
            if p.replace(".", "").isdigit():
                out.append(p)
            else:
                out.append(p[0].upper() + p[1:])
        return ("Gemini " + " ".join(out)).strip()
    if normalized.startswith("gpt-"):
        # GPT-4-turbo -> GPT-4 Turbo, GPT-4.1 -> GPT-4.1
        parts = raw.split("-")
        parts[0] = "GPT"
        for i in range(1, len(parts)):
            p = parts[i]
            # Keep version numbers as-is, capitalize words
            if not p.replace(".", "").isdigit():
                parts[i] = p[0].upper() + p[1:].lower() if p else p
        # Join with '-' then replace '-' before capital letters with space
        return re.sub(r"-(?=[A-Z])", " ", "-".join(parts))
    if normalized.startswith("claude-"):
        return raw.replace("claude-", "Claude ", 1)
    if normalized.startswith("deepseek-"):
        return raw.replace("deepseek-", "DeepSeek ", 1)
    if normalized.startswith("qwen"):
        return raw.replace("qwen", "Qwen", 1)
    if normalized.startswith("minimax"):
        return raw.replace("minimax", "MiniMax", 1)
    if normalized.startswith("glm"):
        return raw.replace("glm", "GLM", 1)
    return raw


def _agents_from_backend_config(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    models = cfg.get("models", [])
    if not isinstance(models, list):
        return []
    enabled_models = [m for m in models if isinstance(m, dict) and m.get("enabled", True)]
    agents: List[Dict[str, Any]] = []
    for m in enabled_models:
        model_name = str(m.get("name") or m.get("signature") or "unknown")
        signature = str(m.get("signature") or m.get("name") or model_name)
        provider = _provider_key(model_name)
        agents.append(
            {
                "folder": signature,
                "display_name": _display_name(model_name),
                "icon": _default_icon(provider),
                "color": _default_color(provider),
                "enabled": True,
            }
        )
    return agents


def _data_dir_from_log_path(cfg: Dict[str, Any]) -> Optional[str]:
    log_path = (cfg.get("log_config") or {}).get("log_path")
    if not log_path:
        return None
    return Path(str(log_path)).name


def sync(
    backend_configs: List[Path],
    template_config: Path,
    output_config: Path,
) -> None:
    base = _load_yaml(template_config)
    base_markets = base.get("markets")
    if not isinstance(base_markets, dict):
        raise ValueError(f"Missing markets in {template_config}")

    for cfg_path in backend_configs:
        cfg = _load_json(cfg_path)
        market_id = _market_id_from_backend_config(cfg)
        if market_id not in base_markets:
            continue

        agents = _agents_from_backend_config(cfg)
        base_markets[market_id]["agents"] = agents

        data_dir = _data_dir_from_log_path(cfg)
        if data_dir:
            base_markets[market_id]["data_dir"] = data_dir

        # Determine enabled status: respect backend config, fallback to agent presence
        backend_enabled = cfg.get("enabled")
        if backend_enabled is not None:
            base_markets[market_id]["enabled"] = bool(backend_enabled)
        elif len(agents) == 0:
            base_markets[market_id]["enabled"] = False
        elif len(agents) > 0 and base_markets[market_id].get("enabled") is None:
            base_markets[market_id]["enabled"] = True

    output_config.parent.mkdir(parents=True, exist_ok=True)
    with output_config.open("w", encoding="utf-8") as f:
        yaml.safe_dump(base, f, sort_keys=False, allow_unicode=True, width=4096)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--backend-config",
        action="append",
        dest="backend_configs",
        default=[],
        help="Path to backend JSON config (repeatable).",
    )
    parser.add_argument(
        "--template",
        default=str(Path(__file__).parent.parent / "docs" / "config.yaml"),
    )
    parser.add_argument(
        "--output",
        default=str(Path(__file__).parent.parent / "docs" / "config.generated.yaml"),
    )
    args = parser.parse_args()

    repo_root = Path(__file__).parent.parent
    backend_configs = [Path(p) for p in args.backend_configs]
    if not backend_configs:
        backend_configs = [
            repo_root / "configs" / "default_config.json",
            repo_root / "configs" / "astock_config.json",
            repo_root / "configs" / "astock_hour_config.json",
        ]

    # Check for missing config files and warn
    existing_configs = []
    for cfg_path in backend_configs:
        if cfg_path.exists():
            existing_configs.append(cfg_path)
        else:
            _log(f"Config file not found, skipping: {cfg_path}", "WARN")

    if not existing_configs:
        _log("No backend config files found. Output will use template defaults.", "WARN")

    sync(
        backend_configs=existing_configs,
        template_config=Path(args.template),
        output_config=Path(args.output),
    )

    _log(f"Generated config: {args.output}")


if __name__ == "__main__":
    main()
