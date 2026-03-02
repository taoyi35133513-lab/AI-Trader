"""
LLM Factory — registry-based model instantiation.

Provides a single `create_llm()` entry-point that picks the right ChatModel
class based on the model name (auto-detection) or an explicit `llm_class`
override in config.

Adding a new model family only requires registering it in LLM_REGISTRY.
"""

import json
import logging
from typing import Optional

from langchain_openai import ChatOpenAI

logger = logging.getLogger(__name__)


class DeepSeekChatOpenAI(ChatOpenAI):
    """
    Custom ChatOpenAI wrapper for DeepSeek API compatibility.

    DeepSeek sometimes returns tool_calls.args as JSON strings instead of dicts.
    This subclass transparently fixes that in both sync and async code paths.
    """

    def _generate(self, messages: list, stop: Optional[list] = None, **kwargs):
        result = super()._generate(messages, stop, **kwargs)
        _fix_tool_calls(result)
        return result

    async def _agenerate(self, messages: list, stop: Optional[list] = None, **kwargs):
        result = await super()._agenerate(messages, stop, **kwargs)
        _fix_tool_calls(result)
        return result


def _fix_tool_calls(result) -> None:
    """Ensure tool_call arguments are dicts, not JSON strings."""
    for generation in result.generations:
        for gen in generation:
            if not (hasattr(gen, "message") and hasattr(gen.message, "additional_kwargs")):
                continue
            tool_calls = gen.message.additional_kwargs.get("tool_calls")
            if not tool_calls:
                continue
            for tool_call in tool_calls:
                func = tool_call.get("function", {})
                args = func.get("arguments")
                if isinstance(args, str):
                    try:
                        func["arguments"] = json.loads(args)
                    except json.JSONDecodeError:
                        pass


# ---------------------------------------------------------------------------
# Registry: maps a keyword prefix to the ChatModel class that should be used.
# The "default" key is the fallback when no prefix matches.
# ---------------------------------------------------------------------------
LLM_REGISTRY: dict[str, type] = {
    "deepseek": DeepSeekChatOpenAI,
    "default": ChatOpenAI,
}


def create_llm(
    basemodel: str,
    *,
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
    llm_class: Optional[str] = None,
    max_retries: int = 3,
    timeout: int = 30,
    **extra_kwargs,
) -> ChatOpenAI:
    """Create an LLM instance using the registry.

    Resolution order:
    1. If ``llm_class`` is provided (e.g. ``"deepseek"``), look it up directly.
    2. Otherwise, auto-detect by checking if any registry key is a substring of
       ``basemodel`` (case-insensitive).
    3. Fall back to ``LLM_REGISTRY["default"]``.

    Args:
        basemodel: Model identifier passed to the API (e.g. ``"deepseek-chat-v3.1"``).
        base_url: OpenAI-compatible base URL.
        api_key: API key.
        llm_class: Explicit registry key override.
        max_retries: Number of retries on transient errors.
        timeout: Request timeout in seconds.
        **extra_kwargs: Forwarded to the ChatModel constructor.

    Returns:
        An instance of the resolved ChatModel class.
    """
    cls = None

    # 1. Explicit override
    if llm_class:
        cls = LLM_REGISTRY.get(llm_class)
        if cls is None:
            logger.warning(
                "llm_class=%r not found in registry, falling back to auto-detect",
                llm_class,
            )

    # 2. Auto-detect from model name
    if cls is None:
        model_lower = basemodel.lower()
        for prefix, candidate_cls in LLM_REGISTRY.items():
            if prefix != "default" and prefix in model_lower:
                cls = candidate_cls
                break

    # 3. Default
    if cls is None:
        cls = LLM_REGISTRY["default"]

    logger.info("Creating LLM: model=%s, class=%s", basemodel, cls.__name__)

    return cls(
        model=basemodel,
        base_url=base_url,
        api_key=api_key,
        max_retries=max_retries,
        timeout=timeout,
        **extra_kwargs,
    )
