"""
Abstract provider interfaces for data and execution.

This module defines the base interfaces that data and execution providers must implement.
TqSdk is the current implementation (in src/ghtrader/tq/), but this abstraction allows
for future extensibility with alternative providers.

PRD Section 5.1.3 specifies these interfaces.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from typing import Any, AsyncIterator, Iterator


@dataclass(frozen=True)
class Tick:
    """A single tick data point."""

    symbol: str
    datetime_ns: int
    last_price: float
    volume: int
    open_interest: int
    bid_price1: float
    bid_volume1: int
    ask_price1: float
    ask_volume1: int
    # L2-L5 fields (optional, may be None for L1-only sources)
    bid_price2: float | None = None
    bid_volume2: int | None = None
    ask_price2: float | None = None
    ask_volume2: int | None = None
    bid_price3: float | None = None
    bid_volume3: int | None = None
    ask_price3: float | None = None
    ask_volume3: int | None = None
    bid_price4: float | None = None
    bid_volume4: int | None = None
    ask_price4: float | None = None
    ask_volume4: int | None = None
    bid_price5: float | None = None
    bid_volume5: int | None = None
    ask_price5: float | None = None
    ask_volume5: int | None = None


@dataclass(frozen=True)
class TickBatch:
    """A batch of ticks for a trading day."""

    symbol: str
    trading_day: date
    ticks: list[Tick]


@dataclass(frozen=True)
class ContractMetadata:
    """Metadata about a futures contract."""

    symbol: str
    exchange: str
    variety: str
    expired: bool
    expire_datetime: str | None = None
    open_date: str | None = None
    delivery_year: int | None = None
    delivery_month: int | None = None


@dataclass(frozen=True)
class Position:
    """A position in a symbol."""

    symbol: str
    volume_long: int
    volume_short: int
    volume_long_today: int
    volume_short_today: int
    float_profit: float
    position_profit: float


@dataclass(frozen=True)
class AccountSnapshot:
    """A snapshot of account state."""

    balance: float
    available: float
    margin: float
    float_profit: float
    position_profit: float
    risk_ratio: float


@dataclass(frozen=True)
class OrderId:
    """Unique identifier for an order."""

    id: str


class DataProvider(ABC):
    """
    Abstract base class for tick data providers.

    Implementations must provide methods for:
    - Historical tick download
    - Live tick subscription
    - Trading calendar retrieval
    - Contract metadata lookup
    """

    @abstractmethod
    def download_historical(
        self,
        symbol: str,
        start: date,
        end: date,
    ) -> Iterator[TickBatch]:
        """
        Download historical tick data for a symbol.

        Args:
            symbol: Instrument code (e.g., "SHFE.cu2502")
            start: Start date (trading day, inclusive)
            end: End date (trading day, inclusive)

        Yields:
            TickBatch objects, one per trading day
        """
        ...

    @abstractmethod
    async def subscribe_live(
        self,
        symbols: list[str],
    ) -> AsyncIterator[Tick]:
        """
        Subscribe to live tick data for multiple symbols.

        Args:
            symbols: List of instrument codes

        Yields:
            Tick objects as they arrive
        """
        ...

    @abstractmethod
    def get_trading_calendar(
        self,
        exchange: str,
    ) -> list[date]:
        """
        Get the trading calendar for an exchange.

        Args:
            exchange: Exchange code (e.g., "SHFE")

        Returns:
            List of trading days (sorted ascending)
        """
        ...

    @abstractmethod
    def get_contract_info(
        self,
        symbol: str,
    ) -> ContractMetadata:
        """
        Get metadata for a contract.

        Args:
            symbol: Instrument code (e.g., "SHFE.cu2502")

        Returns:
            ContractMetadata object
        """
        ...


class ExecutionProvider(ABC):
    """
    Abstract base class for order execution providers.

    Implementations must provide methods for:
    - Order submission and cancellation
    - Position queries
    - Account state queries
    """

    @abstractmethod
    def submit_order(
        self,
        symbol: str,
        side: str,
        quantity: int,
        price: float | None = None,
    ) -> OrderId:
        """
        Submit an order.

        Args:
            symbol: Instrument code
            side: "buy" or "sell"
            quantity: Order quantity (positive integer)
            price: Limit price (None for market order)

        Returns:
            OrderId for tracking
        """
        ...

    @abstractmethod
    def cancel_order(
        self,
        order_id: OrderId,
    ) -> bool:
        """
        Cancel an order.

        Args:
            order_id: The order to cancel

        Returns:
            True if cancellation was successful
        """
        ...

    @abstractmethod
    def get_positions(self) -> dict[str, Position]:
        """
        Get current positions.

        Returns:
            Dict mapping symbol to Position
        """
        ...

    @abstractmethod
    def get_account(self) -> AccountSnapshot:
        """
        Get current account state.

        Returns:
            AccountSnapshot object
        """
        ...


__all__ = [
    "Tick",
    "TickBatch",
    "ContractMetadata",
    "Position",
    "AccountSnapshot",
    "OrderId",
    "DataProvider",
    "ExecutionProvider",
]
