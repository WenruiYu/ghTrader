"""
TqSdk execution adapters (PRD 5.12).

This module holds TqSdk-specific execution mechanisms (TargetPosTask and direct insert/cancel)
so that non-`ghtrader.tq.*` modules remain free of direct `tqsdk` imports (PRD 5.1.3).
"""

from __future__ import annotations

from typing import Literal

from ghtrader.trading.execution import Direction, OrderRateLimiter, RiskLimits, plan_direct_orders_to_target


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
        from tqsdk import TargetPosTask  # type: ignore

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

