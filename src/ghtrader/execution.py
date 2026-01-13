from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Literal


Direction = Literal["BUY", "SELL"]
Offset = Literal["OPEN", "CLOSE", "CLOSETODAY"]


@dataclass(frozen=True)
class OrderIntent:
    """
    A minimal, broker-agnostic order instruction used by the direct-order executor.
    """

    direction: Direction
    offset: Offset
    volume: int


@dataclass(frozen=True)
class RiskLimits:
    """
    Minimal risk limits enforced by ghTrader (must work without TqSdk pro features).
    """

    max_abs_position: int = 1
    max_order_size: int = 1
    max_ops_per_sec: int = 10
    max_daily_loss: float | None = None  # absolute currency loss from start balance
    enforce_trading_time: bool = True


class OrderRateLimiter:
    """
    Simple token-bucket-ish limiter for order operations (insert/cancel).
    """

    def __init__(self, max_ops_per_sec: int):
        if max_ops_per_sec <= 0:
            raise ValueError("max_ops_per_sec must be > 0")
        self.max_ops_per_sec = int(max_ops_per_sec)
        self._events: list[float] = []

    def allow(self) -> bool:
        now = time.monotonic()
        window_start = now - 1.0
        self._events = [t for t in self._events if t >= window_start]
        if len(self._events) >= self.max_ops_per_sec:
            return False
        self._events.append(now)
        return True


def _split_volume(volume: int, max_order_size: int) -> list[int]:
    if volume <= 0:
        return []
    if max_order_size <= 0:
        raise ValueError("max_order_size must be > 0")
    out: list[int] = []
    left = int(volume)
    while left > 0:
        v = min(left, max_order_size)
        out.append(v)
        left -= v
    return out


def _is_shfe_like(exchange: str) -> bool:
    return exchange.upper() in {"SHFE", "INE"}


def _plan_close_intents(
    *,
    exchange: str,
    direction: Direction,
    close_volume: int,
    pos_today: int,
    max_order_size: int,
) -> list[OrderIntent]:
    """
    Plan close intents, splitting CLOSETODAY/CLOSE for SHFE-like exchanges.

    Args:
        direction: BUY when closing shorts; SELL when closing longs.
        pos_today: today's position available to CLOSETODAY (volume_*_today).
    """
    if close_volume <= 0:
        return []

    intents: list[OrderIntent] = []
    if _is_shfe_like(exchange):
        vol_today = min(int(close_volume), int(pos_today))
        vol_his = int(close_volume) - int(vol_today)
        for v in _split_volume(vol_today, max_order_size):
            intents.append(OrderIntent(direction=direction, offset="CLOSETODAY", volume=v))
        for v in _split_volume(vol_his, max_order_size):
            intents.append(OrderIntent(direction=direction, offset="CLOSE", volume=v))
    else:
        for v in _split_volume(int(close_volume), max_order_size):
            intents.append(OrderIntent(direction=direction, offset="CLOSE", volume=v))
    return intents


def plan_direct_orders_to_target(
    *,
    exchange: str,
    current_long_today: int,
    current_long_his: int,
    current_short_today: int,
    current_short_his: int,
    target_net: int,
    max_order_size: int,
) -> list[OrderIntent]:
    """
    Compute a sequence of direct order intents to move from current position to a target net position.

    Notes:
    - This plans **aggressive** adjustment: close opposing position first, then open new exposure.
    - SHFE/INE require splitting close orders into CLOSETODAY then CLOSE based on today-position.
    """
    for name, v in [
        ("current_long_today", current_long_today),
        ("current_long_his", current_long_his),
        ("current_short_today", current_short_today),
        ("current_short_his", current_short_his),
    ]:
        if int(v) < 0:
            raise ValueError(f"{name} must be >= 0")

    long_total = int(current_long_today) + int(current_long_his)
    short_total = int(current_short_today) + int(current_short_his)
    current_net = long_total - short_total
    delta = int(target_net) - int(current_net)

    if delta == 0:
        return []

    intents: list[OrderIntent] = []

    if delta > 0:
        # Need to BUY delta: close shorts first, then open longs.
        close_short = min(short_total, delta)
        open_long = delta - close_short
        intents += _plan_close_intents(
            exchange=exchange,
            direction="BUY",
            close_volume=close_short,
            pos_today=int(current_short_today),
            max_order_size=max_order_size,
        )
        for v in _split_volume(open_long, max_order_size):
            intents.append(OrderIntent(direction="BUY", offset="OPEN", volume=v))
    else:
        # Need to SELL -delta: close longs first, then open shorts.
        sell_vol = -delta
        close_long = min(long_total, sell_vol)
        open_short = sell_vol - close_long
        intents += _plan_close_intents(
            exchange=exchange,
            direction="SELL",
            close_volume=close_long,
            pos_today=int(current_long_today),
            max_order_size=max_order_size,
        )
        for v in _split_volume(open_short, max_order_size):
            intents.append(OrderIntent(direction="SELL", offset="OPEN", volume=v))

    return intents


def clamp_target_position(target: int, *, max_abs_position: int) -> int:
    if max_abs_position < 0:
        raise ValueError("max_abs_position must be >= 0")
    if target > max_abs_position:
        return max_abs_position
    if target < -max_abs_position:
        return -max_abs_position
    return target


class TargetPosExecutor:
    """
    Wrapper around TqSdk TargetPosTask (target net position).

    NOTE: TqSdk warns not to mix TargetPosTask with direct insert_order for the same (account,symbol).
    """

    def __init__(
        self,
        *,
        api: object,
        symbols: list[str],
        account: object | None = None,
        price: str = "ACTIVE",
        offset_priority: str = "今昨,开",
        min_volume: int | None = None,
        max_volume: int | None = None,
    ) -> None:
        from tqsdk import TargetPosTask

        self.api = api
        self.account = account
        self.price = price
        self.offset_priority = offset_priority
        self.min_volume = min_volume
        self.max_volume = max_volume
        self.symbols = list(symbols)
        self._TargetPosTask = TargetPosTask
        self.tasks = {s: self._new_task(s) for s in self.symbols}

    def _new_task(self, symbol: str):
        return self._TargetPosTask(
            self.api,
            symbol,
            price=self.price,
            offset_priority=self.offset_priority,
            min_volume=self.min_volume,
            max_volume=self.max_volume,
            account=self.account,
        )

    def _ensure_task(self, symbol: str) -> None:
        if symbol in self.tasks:
            return
        self.symbols.append(symbol)
        self.tasks[symbol] = self._new_task(symbol)

    def set_target(self, symbol: str, target_net: int) -> None:
        self._ensure_task(symbol)
        self.tasks[symbol].set_target_volume(int(target_net))

    def cancel_all(self) -> None:
        for t in self.tasks.values():
            try:
                t.cancel()
            except Exception:
                continue


class DirectOrderExecutor:
    """
    Direct order execution via insert_order/cancel_order.
    """

    def __init__(
        self,
        *,
        api: object,
        account: object | None = None,
        limits: RiskLimits | None = None,
        price_mode: Literal["ACTIVE", "PASSIVE"] = "ACTIVE",
        advanced: str | None = None,
        rate_limiter: OrderRateLimiter | None = None,
    ) -> None:
        self.api = api
        self.account = account
        self.limits = limits or RiskLimits()
        self.price_mode = price_mode
        self.advanced = advanced
        self.rate_limiter = rate_limiter or OrderRateLimiter(self.limits.max_ops_per_sec)

    def _price_for(self, *, quote: object, direction: Direction) -> float | None:
        # Best-effort: if price is NaN/None, return None (market/FAK depending on exchange support).
        def _get(name: str) -> float | None:
            try:
                v = float(getattr(quote, name))
                return None if v != v else v
            except Exception:
                return None

        if self.price_mode == "PASSIVE":
            return _get("bid_price1") if direction == "BUY" else _get("ask_price1")
        return _get("ask_price1") if direction == "BUY" else _get("bid_price1")

    def cancel_all_alive(self) -> int:
        """
        Cancel all ALIVE orders for the current account (best-effort).
        """
        try:
            orders = self.api.get_order(account=self.account)
        except Exception:
            return 0
        n = 0
        for _, o in orders.items():
            try:
                if getattr(o, "status", "") != "ALIVE":
                    continue
                if not self.rate_limiter.allow():
                    break
                self.api.cancel_order(o, account=self.account)
                n += 1
            except Exception:
                continue
        return n

    def set_target(self, *, symbol: str, target_net: int, position: object, quote: object) -> list[object]:
        """
        Plan and submit orders to reach a target net position (best-effort, aggressive).
        Returns list of order references returned by TqSdk.
        """
        exchange = symbol.split(".", 1)[0]
        intents = plan_direct_orders_to_target(
            exchange=exchange,
            current_long_today=int(getattr(position, "volume_long_today", 0) or 0),
            current_long_his=int(getattr(position, "volume_long_his", 0) or 0),
            current_short_today=int(getattr(position, "volume_short_today", 0) or 0),
            current_short_his=int(getattr(position, "volume_short_his", 0) or 0),
            target_net=int(target_net),
            max_order_size=int(self.limits.max_order_size),
        )
        orders: list[object] = []
        for intent in intents:
            if not self.rate_limiter.allow():
                break
            price = self._price_for(quote=quote, direction=intent.direction)
            try:
                o = self.api.insert_order(
                    symbol=symbol,
                    direction=intent.direction,
                    offset=intent.offset,
                    volume=intent.volume,
                    limit_price=price,
                    advanced=self.advanced,
                    account=self.account,
                )
                orders.append(o)
            except Exception:
                continue
        return orders


