"""
Skills 技能注册表

自动发现并注册 strategies/、analysis/、risk/ 下所有技能模块。
每个技能模块导出 SKILL_CONFIG (dict) 和 PROMPT (str)。
"""

import importlib
import logging
import pkgutil
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

SKILL_REGISTRY: Dict[str, dict] = {}

_SUBDIRS = ["strategies", "analysis", "risk"]


def discover_skills() -> Dict[str, dict]:
    """Auto-discover skill modules and populate SKILL_REGISTRY."""
    global SKILL_REGISTRY

    if SKILL_REGISTRY:
        return SKILL_REGISTRY

    base_dir = Path(__file__).parent
    for subdir in _SUBDIRS:
        pkg_path = base_dir / subdir
        if not pkg_path.is_dir():
            continue
        pkg_name = f"skills.{subdir}"
        try:
            pkg = importlib.import_module(pkg_name)
        except ImportError:
            continue

        for _importer, mod_name, _ispkg in pkgutil.iter_modules([str(pkg_path)]):
            if mod_name.startswith("_"):
                continue
            full_name = f"{pkg_name}.{mod_name}"
            try:
                mod = importlib.import_module(full_name)
                config = getattr(mod, "SKILL_CONFIG", None)
                prompt = getattr(mod, "PROMPT", None)
                if config and prompt:
                    skill_id = config["id"]
                    SKILL_REGISTRY[skill_id] = {**config, "prompt": prompt}
                    logger.debug("Registered skill: %s (%s)", skill_id, config["name"])
            except Exception as e:
                logger.warning("Failed to load skill %s: %s", full_name, e)

    logger.info("Discovered %d skills", len(SKILL_REGISTRY))
    return SKILL_REGISTRY


def get_skill(skill_id: str) -> Optional[dict]:
    """Get a skill by ID."""
    if not SKILL_REGISTRY:
        discover_skills()
    return SKILL_REGISTRY.get(skill_id)


def get_skills_by_category(category: str) -> List[dict]:
    """Get all skills in a category."""
    if not SKILL_REGISTRY:
        discover_skills()
    return [s for s in SKILL_REGISTRY.values() if s["category"] == category]


def get_all_skills() -> List[dict]:
    """Get all registered skills."""
    if not SKILL_REGISTRY:
        discover_skills()
    return list(SKILL_REGISTRY.values())
