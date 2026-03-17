"""
Pydantic models for validating configs/config.json.

Usage:
    from configs.schema import TradingConfig

    raw = json.load(open("configs/config.json"))
    config = TradingConfig(**raw)          # raises ValidationError on bad data
    config.enabled_models()                # convenience property
"""

from typing import Dict, Literal, Optional

from pydantic import BaseModel


class ModelConfig(BaseModel):
    name: str
    basemodel: str
    enabled: bool = False
    openai_base_url: Optional[str] = None
    openai_api_key: Optional[str] = None
    llm_class: Optional[str] = None  # explicit LLM registry override
    skills: Optional[list[str]] = None  # default skill IDs for this model


class AgentTypeConfig(BaseModel):
    module: str
    class_: str  # "class" is reserved in Python
    frequency: str

    model_config = {"populate_by_name": True}

    def __init__(self, **data):
        # Support "class" key from JSON
        if "class" in data and "class_" not in data:
            data["class_"] = data.pop("class")
        super().__init__(**data)


class TradingConfig(BaseModel):
    frequency: Literal["daily", "hourly"] = "daily"
    market: str = "cn"
    models: list[ModelConfig]
    agent_types: Optional[Dict[str, AgentTypeConfig]] = None

    def enabled_models(self) -> list[ModelConfig]:
        return [m for m in self.models if m.enabled]
