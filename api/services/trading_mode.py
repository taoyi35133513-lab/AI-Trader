"""
Trading Mode Abstraction Layer

Provides clean separation between backtest and live trading modes
with strategy pattern for signature generation.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Optional


class TradingMode(str, Enum):
    """Trading mode enumeration"""
    BACKTEST = "backtest"
    LIVE = "live"


class SignatureStrategy(ABC):
    """Abstract base class for signature generation strategies"""

    @abstractmethod
    def generate(self, model_name: str, frequency: str, date: Optional[str] = None) -> str:
        """
        Generate a signature for the agent.

        Args:
            model_name: Name of the model (e.g., "gemini-2.5-flash")
            frequency: Trading frequency ("daily" or "hourly")
            date: Optional date for live mode (YYYY-MM-DD format)

        Returns:
            Generated signature string
        """
        pass

    def _get_frequency_suffix(self, frequency: str) -> str:
        """Get frequency-based suffix"""
        return "-astock-hour" if frequency == "hourly" else ""


class BacktestSignatureStrategy(SignatureStrategy):
    """
    Signature strategy for backtest mode.

    Format: {model_name} or {model_name}-astock-hour
    Example: gemini-2.5-flash or gemini-2.5-flash-astock-hour
    """

    def generate(self, model_name: str, frequency: str, date: Optional[str] = None) -> str:
        freq_suffix = self._get_frequency_suffix(frequency)
        return f"{model_name}{freq_suffix}"


class LiveSignatureStrategy(SignatureStrategy):
    """
    Signature strategy for live trading mode.

    Format: {model_name}-live or {model_name}-live-astock-hour
    Example: gemini-2.5-flash-live or gemini-2.5-flash-live-astock-hour

    Note: We use a fixed "-live" suffix instead of date-based suffix to maintain
    continuous position tracking across trading days.
    """

    def generate(self, model_name: str, frequency: str, date: Optional[str] = None) -> str:
        freq_suffix = self._get_frequency_suffix(frequency)
        return f"{model_name}-live{freq_suffix}"


# Strategy registry
_SIGNATURE_STRATEGIES = {
    TradingMode.BACKTEST: BacktestSignatureStrategy(),
    TradingMode.LIVE: LiveSignatureStrategy(),
}


def get_signature_strategy(mode: TradingMode) -> SignatureStrategy:
    """
    Get the signature strategy for the given trading mode.

    Args:
        mode: Trading mode (BACKTEST or LIVE)

    Returns:
        SignatureStrategy instance for the mode
    """
    return _SIGNATURE_STRATEGIES[mode]


def generate_signature(
    model_name: str,
    frequency: str,
    mode: TradingMode = TradingMode.BACKTEST,
    date: Optional[str] = None
) -> str:
    """
    Convenience function to generate a signature.

    Args:
        model_name: Name of the model
        frequency: Trading frequency
        mode: Trading mode (default: BACKTEST)
        date: Optional date for live mode

    Returns:
        Generated signature string
    """
    strategy = get_signature_strategy(mode)
    return strategy.generate(model_name, frequency, date)


def derive_log_path(frequency: str) -> str:
    """
    Derive log path from frequency.

    Args:
        frequency: Trading frequency ("daily" or "hourly")

    Returns:
        Log path string
    """
    suffix = "_hour" if frequency == "hourly" else ""
    return f"./data/agent_data_astock{suffix}"


def derive_agent_type(frequency: str) -> str:
    """
    Derive agent type from frequency.

    Args:
        frequency: Trading frequency ("daily" or "hourly")

    Returns:
        Agent type string
    """
    return "BaseAgentAStock_Hour" if frequency == "hourly" else "BaseAgentAStock"
