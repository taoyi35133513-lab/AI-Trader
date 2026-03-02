"""
Market trading rules abstraction.

Centralises all market-specific constraints (lot size, T+N settlement,
price limits, trading hours, currency) so that both tool-side validation
and prompt generation read from the same source of truth.

Usage:
    from tools.market_rules import get_rules

    rules = get_rules("cn")
    rules.validate_lot_size(150)   # raises ValueError for A-shares
    rules.lot_size                 # 100
    rules.t_plus_n                 # 1
"""

from dataclasses import dataclass, field
from typing import Dict, List


@dataclass(frozen=True)
class MarketRules:
    """Base market rules — subclass or instantiate for each market."""

    market: str
    currency: str
    lot_size: int
    t_plus_n: int
    price_limits: Dict[str, float]
    trading_hours: List[str]
    hourly_trading_hours: List[str] = field(default_factory=list)

    def validate_lot_size(self, amount: int) -> None:
        """Raise ValueError if *amount* is not a valid lot."""
        if amount % self.lot_size != 0:
            raise ValueError(
                f"{self.market} market requires multiples of {self.lot_size} shares. "
                f"Got {amount}."
            )

    def is_t_plus_n_restricted(self) -> bool:
        return self.t_plus_n > 0

    def format_rules_for_prompt(self) -> str:
        """Return a human-readable summary suitable for inclusion in a system prompt."""
        lines = [
            f"Market: {self.market.upper()}",
            f"Currency: {self.currency}",
            f"Lot size: {self.lot_size} shares",
            f"Settlement: T+{self.t_plus_n}",
        ]
        if self.price_limits:
            limits_str = ", ".join(f"{k}: ±{v*100:.0f}%" for k, v in self.price_limits.items())
            lines.append(f"Price limits: {limits_str}")
        if self.trading_hours:
            lines.append(f"Trading hours: {', '.join(self.trading_hours)}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Pre-defined market rules
# ---------------------------------------------------------------------------

ASTOCK_RULES = MarketRules(
    market="cn",
    currency="CNY",
    lot_size=100,
    t_plus_n=1,
    price_limits={
        "normal": 0.10,
        "st": 0.05,
        "star_gem": 0.20,   # 科创板 / 创业板
    },
    trading_hours=["09:30-11:30", "13:00-15:00"],
    hourly_trading_hours=["10:30:00", "11:30:00", "14:00:00", "15:00:00"],
)

US_STOCK_RULES = MarketRules(
    market="us",
    currency="USD",
    lot_size=1,
    t_plus_n=0,
    price_limits={},
    trading_hours=["09:30-16:00"],
)

_REGISTRY: Dict[str, MarketRules] = {
    "cn": ASTOCK_RULES,
    "us": US_STOCK_RULES,
}


def get_rules(market: str = "cn") -> MarketRules:
    """Return rules for *market*, defaulting to A-shares."""
    return _REGISTRY.get(market, ASTOCK_RULES)
