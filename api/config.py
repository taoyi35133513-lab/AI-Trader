"""
API 配置管理
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """API 配置"""

    # 服务配置
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    debug: bool = True

    # CORS 配置
    cors_origins: List[str] = ["http://localhost:8888", "http://127.0.0.1:8888"]

    # 数据库配置
    database_path: str = "data/database/ai_trader.duckdb"

    # 项目路径
    project_root: Path = Path(__file__).parent.parent

    # 数据目录配置
    data_dirs: Dict[str, str] = {
        "cn": "agent_data_astock",
        "cn_hour": "agent_data_astock_hour",
        "us": "agent_data",
    }

    class Config:
        env_prefix = "API_"


settings = Settings()


def get_project_root() -> Path:
    """获取项目根目录"""
    return settings.project_root


def get_database_path() -> Path:
    """获取数据库路径"""
    return settings.project_root / settings.database_path


def get_data_dir(market: str) -> Path:
    """获取市场对应的数据目录"""
    data_dir = settings.data_dirs.get(market, "agent_data")
    return settings.project_root / "data" / data_dir


def load_config_json(config_name: str = "config.json") -> dict:
    """加载配置文件"""
    config_path = settings.project_root / "configs" / config_name
    if config_path.exists():
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)
    print(f"Warning: Config file not found: {config_path}")
    return {}
