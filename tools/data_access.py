"""
Unified data access layer with DuckDB-first strategy and JSONL fallback.

This module provides data access classes that:
1. Prioritize DuckDB for all price and position queries
2. Fall back to JSONL on any DuckDB error (missing table, connection failure)
3. Maintain backward compatibility with existing function signatures

Usage:
    from tools.data_access import PriceDataAccess, PositionDataAccess

    price_access = PriceDataAccess()
    prices = price_access.get_open_prices("2025-10-30", ["600519.SH"])

    position_access = PositionDataAccess()
    pos, action_id = position_access.get_latest_position("2025-10-30", "gpt-5")
"""

import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Ensure project root is on path
project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

logger = logging.getLogger(__name__)


def _is_duckdb_enabled() -> bool:
    """Check if DuckDB is enabled via environment variable."""
    return os.getenv("USE_DUCKDB", "true").lower() == "true"


def _is_fallback_enabled() -> bool:
    """Check if JSONL fallback is enabled."""
    return os.getenv("DUCKDB_FALLBACK_ENABLED", "true").lower() == "true"


class PriceDataAccess:
    """Unified price data access with DuckDB-first strategy.

    Provides methods to query price data with automatic fallback from
    DuckDB to JSONL files when database queries fail.
    """

    def __init__(self, prefer_duckdb: Optional[bool] = None, market: str = "cn"):
        """Initialize price data access.

        Args:
            prefer_duckdb: If True, try DuckDB first; if False, use JSONL only.
                          If None, use USE_DUCKDB environment variable.
            market: Market identifier (default: "cn" for A-shares)
        """
        if prefer_duckdb is None:
            prefer_duckdb = _is_duckdb_enabled()
        self.prefer_duckdb = prefer_duckdb
        self.fallback_enabled = _is_fallback_enabled()
        self.market = market

    def _get_db_manager(self, read_only: bool = True):
        """Get DatabaseManager instance (lazy import to avoid circular dependency).

        Args:
            read_only: Use read-only mode for concurrent access (default: True)
        """
        from data.database.connection import DatabaseManager
        return DatabaseManager(read_only=read_only)

    def get_open_prices(
        self, today_date: str, symbols: List[str], merged_path: Optional[str] = None
    ) -> Dict[str, Optional[float]]:
        """Get opening prices with DuckDB-first strategy.

        Args:
            today_date: Date string in YYYY-MM-DD or YYYY-MM-DD HH:MM:SS format
            symbols: List of stock symbols
            merged_path: Optional custom JSONL path (for backward compatibility)

        Returns:
            Dictionary in format {"{symbol}_price": float or None}
        """
        if self.prefer_duckdb:
            try:
                from tools import duckdb_queries as dq

                with self._get_db_manager() as db:
                    is_hourly = " " in today_date
                    if is_hourly:
                        result = dq.query_hourly_open_prices(db, symbols, today_date)
                    else:
                        result = dq.query_daily_open_prices(db, symbols, today_date, self.market)

                    if result:
                        logger.debug(f"DuckDB: Retrieved {len(result)} open prices for {today_date}")
                        return result
                    else:
                        logger.warning(f"DuckDB returned no data for {today_date}, trying JSONL fallback")

            except Exception as e:
                logger.warning(f"DuckDB query failed: {e}")
                if not self.fallback_enabled:
                    raise

        # Fallback to JSONL
        return self._get_open_prices_jsonl(today_date, symbols, merged_path)

    def _get_open_prices_jsonl(
        self, today_date: str, symbols: List[str], merged_path: Optional[str] = None
    ) -> Dict[str, Optional[float]]:
        """JSONL fallback implementation for get_open_prices."""
        # Import the original JSONL implementation
        from tools import price_tools_jsonl as jsonl

        result = jsonl.get_open_prices_jsonl(today_date, symbols, merged_path, self.market)
        logger.debug(f"JSONL: Retrieved {len(result)} open prices for {today_date}")
        return result

    def get_ohlcv(
        self, symbol: str, date: str
    ) -> Dict[str, Any]:
        """Get OHLCV data for a single symbol with DuckDB-first strategy.

        Args:
            symbol: Stock symbol
            date: Date string

        Returns:
            Dictionary with symbol, date, and ohlcv fields
        """
        if self.prefer_duckdb:
            try:
                from tools import duckdb_queries as dq

                with self._get_db_manager() as db:
                    is_hourly = " " in date
                    if is_hourly:
                        result = dq.query_hourly_ohlcv(db, symbol, date)
                    else:
                        result = dq.query_daily_ohlcv(db, symbol, date, self.market)

                    if "error" not in result:
                        logger.debug(f"DuckDB: Retrieved OHLCV for {symbol} on {date}")
                        return result
                    else:
                        logger.warning(f"DuckDB: {result['error']}, trying JSONL fallback")

            except Exception as e:
                logger.warning(f"DuckDB OHLCV query failed: {e}")
                if not self.fallback_enabled:
                    raise

        # Fallback to JSONL
        return self._get_ohlcv_jsonl(symbol, date)

    def _get_ohlcv_jsonl(self, symbol: str, date: str) -> Dict[str, Any]:
        """JSONL fallback implementation for get_ohlcv."""
        from tools import price_tools_jsonl as jsonl

        result = jsonl.get_ohlcv_jsonl(symbol, date, self.market)
        logger.debug(f"JSONL: Retrieved OHLCV for {symbol} on {date}")
        return result

    def get_yesterday_date(
        self, today_date: str, merged_path: Optional[str] = None
    ) -> str:
        """Get previous trading day with DuckDB-first strategy.

        Args:
            today_date: Today's date string
            merged_path: Optional custom JSONL path

        Returns:
            Previous trading day string
        """
        if self.prefer_duckdb:
            try:
                from tools import duckdb_queries as dq

                with self._get_db_manager() as db:
                    result = dq.query_previous_trading_day(db, today_date, self.market)
                    if result:
                        logger.debug(f"DuckDB: Previous trading day for {today_date} is {result}")
                        return result
                    else:
                        logger.warning(f"DuckDB: No previous trading day found, trying JSONL")

            except Exception as e:
                logger.warning(f"DuckDB yesterday_date query failed: {e}")
                if not self.fallback_enabled:
                    raise

        # Fallback to JSONL
        return self._get_yesterday_date_jsonl(today_date, merged_path)

    def _get_yesterday_date_jsonl(
        self, today_date: str, merged_path: Optional[str] = None
    ) -> str:
        """JSONL fallback implementation for get_yesterday_date."""
        from tools import price_tools_jsonl as jsonl

        return jsonl.get_yesterday_date_jsonl(today_date, merged_path, self.market)

    def get_yesterday_open_and_close_price(
        self, today_date: str, symbols: List[str], merged_path: Optional[str] = None
    ) -> Tuple[Dict[str, Optional[float]], Dict[str, Optional[float]]]:
        """Get yesterday's open and close prices with DuckDB-first strategy.

        Args:
            today_date: Today's date string
            symbols: List of stock symbols
            merged_path: Optional custom JSONL path

        Returns:
            Tuple of (buy_prices, sell_prices) dictionaries
        """
        # First get yesterday's date
        yesterday = self.get_yesterday_date(today_date, merged_path)

        if self.prefer_duckdb:
            try:
                from tools import duckdb_queries as dq

                with self._get_db_manager() as db:
                    buy_results, sell_results = dq.query_yesterday_prices(
                        db, symbols, yesterday, self.market
                    )
                    if buy_results or sell_results:
                        logger.debug(f"DuckDB: Retrieved yesterday prices for {len(symbols)} symbols")
                        return buy_results, sell_results
                    else:
                        logger.warning("DuckDB: No yesterday prices found, trying JSONL")

            except Exception as e:
                logger.warning(f"DuckDB yesterday prices query failed: {e}")
                if not self.fallback_enabled:
                    raise

        # Fallback to JSONL
        return self._get_yesterday_prices_jsonl(today_date, symbols, merged_path)

    def _get_yesterday_prices_jsonl(
        self, today_date: str, symbols: List[str], merged_path: Optional[str] = None
    ) -> Tuple[Dict[str, Optional[float]], Dict[str, Optional[float]]]:
        """JSONL fallback implementation for get_yesterday_open_and_close_price."""
        from tools import price_tools_jsonl as jsonl

        return jsonl.get_yesterday_open_and_close_price_jsonl(
            today_date, symbols, merged_path, self.market
        )

    def is_trading_day(self, date: str) -> bool:
        """Check if date is a trading day with DuckDB-first strategy."""
        if self.prefer_duckdb:
            try:
                from tools import duckdb_queries as dq

                with self._get_db_manager() as db:
                    result = dq.query_is_trading_day(db, date, self.market)
                    logger.debug(f"DuckDB: is_trading_day({date}) = {result}")
                    return result

            except Exception as e:
                logger.warning(f"DuckDB is_trading_day query failed: {e}")
                if not self.fallback_enabled:
                    raise

        # Fallback to JSONL
        from tools import price_tools_jsonl as jsonl
        return jsonl.is_trading_day_jsonl(date, self.market)

    def get_all_trading_days(self) -> List[str]:
        """Get all trading days with DuckDB-first strategy."""
        if self.prefer_duckdb:
            try:
                from tools import duckdb_queries as dq

                with self._get_db_manager() as db:
                    result = dq.query_all_trading_days(db, self.market)
                    if result:
                        logger.debug(f"DuckDB: Retrieved {len(result)} trading days")
                        return result
                    else:
                        logger.warning("DuckDB: No trading days found, trying JSONL")

            except Exception as e:
                logger.warning(f"DuckDB get_all_trading_days query failed: {e}")
                if not self.fallback_enabled:
                    raise

        # Fallback to JSONL
        from tools import price_tools_jsonl as jsonl
        return jsonl.get_all_trading_days_jsonl(self.market)


class PositionDataAccess:
    """Unified position data access with DuckDB-first strategy.

    Provides methods to query and write position data with automatic fallback
    from DuckDB to JSONL files.

    Note: Write operations write to BOTH DuckDB and JSONL for redundancy.
    """

    def __init__(self, prefer_duckdb: Optional[bool] = None):
        """Initialize position data access.

        Args:
            prefer_duckdb: If True, try DuckDB first; if False, use JSONL only.
        """
        if prefer_duckdb is None:
            prefer_duckdb = _is_duckdb_enabled()
        self.prefer_duckdb = prefer_duckdb
        self.fallback_enabled = _is_fallback_enabled()

    def _get_db_manager(self, read_only: bool = False):
        """Get DatabaseManager instance.

        Args:
            read_only: Use read-only mode (default: False for position writes)
        """
        from data.database.connection import DatabaseManager
        return DatabaseManager(read_only=read_only)

    def get_latest_position(
        self, today_date: str, signature: str
    ) -> Tuple[Dict[str, float], int]:
        """Get latest position with DuckDB-first strategy.

        Args:
            today_date: Today's date string
            signature: Agent signature/name

        Returns:
            Tuple of (positions dict, max action_id)
        """
        if self.prefer_duckdb:
            try:
                from tools import duckdb_queries as dq

                with self._get_db_manager() as db:
                    positions, action_id = dq.query_latest_position(db, signature, today_date)
                    if positions or action_id >= 0:
                        logger.debug(f"DuckDB: Retrieved latest position for {signature}")
                        return positions, action_id
                    else:
                        logger.warning(f"DuckDB: No position found for {signature}, trying JSONL")

            except Exception as e:
                logger.warning(f"DuckDB get_latest_position failed: {e}")
                if not self.fallback_enabled:
                    raise

        # Fallback to JSONL
        return self._get_latest_position_jsonl(today_date, signature)

    def _get_latest_position_jsonl(
        self, today_date: str, signature: str
    ) -> Tuple[Dict[str, float], int]:
        """JSONL fallback implementation for get_latest_position."""
        from tools import price_tools_jsonl as jsonl
        return jsonl.get_latest_position_jsonl(today_date, signature)

    def get_today_init_position(
        self, today_date: str, signature: str
    ) -> Dict[str, float]:
        """Get opening position for today with DuckDB-first strategy.

        Args:
            today_date: Today's date string
            signature: Agent signature/name

        Returns:
            Position dictionary {symbol: quantity, "CASH": cash_amount}
        """
        if self.prefer_duckdb:
            try:
                from tools import duckdb_queries as dq

                with self._get_db_manager() as db:
                    positions = dq.query_today_init_position(db, today_date, signature)
                    if positions:
                        logger.debug(f"DuckDB: Retrieved init position for {signature}")
                        return positions
                    else:
                        logger.warning(f"DuckDB: No init position for {signature}, trying JSONL")

            except Exception as e:
                logger.warning(f"DuckDB get_today_init_position failed: {e}")
                if not self.fallback_enabled:
                    raise

        # Fallback to JSONL
        return self._get_today_init_position_jsonl(today_date, signature)

    def _get_today_init_position_jsonl(
        self, today_date: str, signature: str
    ) -> Dict[str, float]:
        """JSONL fallback implementation for get_today_init_position."""
        from tools import price_tools_jsonl as jsonl
        return jsonl.get_today_init_position_jsonl(today_date, signature)

    def add_position_record(
        self, date: str, signature: str, action: dict, positions: dict
    ) -> None:
        """Add position record (writes to BOTH DuckDB and JSONL for redundancy).

        Args:
            date: Trade date
            signature: Agent signature/name
            action: Action dictionary {action, symbol, amount}
            positions: Position dictionary {symbol: quantity, CASH: amount}
        """
        errors = []

        # Write to DuckDB
        if self.prefer_duckdb:
            try:
                from tools import duckdb_queries as dq

                with self._get_db_manager() as db:
                    dq.insert_position_record(db, signature, date, action, positions)
                    logger.info(f"DuckDB: Saved position for {signature} on {date}")

            except Exception as e:
                logger.error(f"DuckDB add_position_record failed: {e}")
                errors.append(("DuckDB", e))

        # Always write to JSONL (backup)
        try:
            from tools import price_tools_jsonl as jsonl
            jsonl.add_position_record_jsonl(date, signature, action, positions)
            logger.info(f"JSONL: Saved position for {signature} on {date}")

        except Exception as e:
            logger.error(f"JSONL add_position_record failed: {e}")
            errors.append(("JSONL", e))

        if len(errors) == 2:
            raise RuntimeError(f"Failed to write position to any source: {errors}")

    def add_no_trade_record(self, today_date: str, signature: str) -> None:
        """Add no-trade record for the day.

        Args:
            today_date: Trade date
            signature: Agent signature/name
        """
        # Get current position
        current_position, _ = self.get_latest_position(today_date, signature)

        action = {"action": "no_trade", "symbol": "", "amount": 0}
        self.add_position_record(today_date, signature, action, current_position)
