from __future__ import annotations

import json
import os
import signal
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Literal

import structlog

from ghtrader.config import get_runs_dir, get_tqsdk_auth, is_live_enabled

log = structlog.get_logger()


TradingMode = Literal["paper", "sim", "live"]
SimAccount = Literal["tqsim", "tqkq"]


def _ensure_tqsdk_on_path() -> None:
    # Keep consistent with other ghtrader modules that vendor tqsdk-python.
    tqsdk_path = Path(__file__).parent.parent.parent.parent / "tqsdk-python"
    if tqsdk_path.exists() and str(tqsdk_path) not in sys.path:
        sys.path.insert(0, str(tqsdk_path))


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def trading_day_from_ts_ns(ts_ns: int, *, data_dir: Path | None = None) -> date:
    """
    Best-effort mapping from a tick timestamp (epoch-nanoseconds) to a **trading day** date.

    Priority:
    1) Use TqSdk internals when available (handles night-session boundary).
    2) Fallback: apply an 18:00 local-day boundary; when a trading calendar cache exists,
       use the next trading day after the calendar date.

    Notes:
    - This is intentionally defensive because unit tests/CI may not have TqSdk installed.
    - Callers that require strict holiday correctness should ensure the trading calendar cache
      exists under data_dir (see ghtrader.trading_calendar).
    """
    from datetime import timedelta

    # Try TqSdk first (if available).
    try:
        _ensure_tqsdk_on_path()
        from tqsdk.datetime import _get_trading_day_from_timestamp, _timestamp_nano_to_datetime  # type: ignore

        td_ns = _get_trading_day_from_timestamp(int(ts_ns))
        return _timestamp_nano_to_datetime(td_ns).date()
    except Exception:
        pass

    # Fallback: interpret timestamp as a wall-clock datetime and apply the 18:00 boundary.
    try:
        from zoneinfo import ZoneInfo

        # Treat ts_ns as UNIX epoch in UTC; convert to Asia/Shanghai for the boundary check.
        dt = datetime.fromtimestamp(int(ts_ns) / 1_000_000_000, tz=timezone.utc).astimezone(ZoneInfo("Asia/Shanghai"))
    except Exception:
        # Last resort: UTC datetime without tz conversion.
        dt = datetime.utcfromtimestamp(int(ts_ns) / 1_000_000_000).replace(tzinfo=timezone.utc)

    d0: date = dt.date()
    if int(dt.hour) < 18:
        return d0

    # After 18:00 local, map to next trading day if possible.
    if data_dir is not None:
        try:
            from bisect import bisect_right

            from ghtrader.trading_calendar import get_trading_calendar

            cal = get_trading_calendar(data_dir=data_dir, refresh=False)
            if cal:
                j = bisect_right(cal, d0)
                if j < len(cal):
                    return cal[j]
        except Exception:
            pass

    return d0 + timedelta(days=1)


@dataclass(frozen=True)
class TradeAccountConfig:
    broker_id: str
    account_id: str
    password: str


def load_trade_account_config_from_env() -> TradeAccountConfig:
    broker = os.environ.get("TQ_BROKER_ID", "").strip()
    acc = os.environ.get("TQ_ACCOUNT_ID", "").strip()
    pwd = os.environ.get("TQ_ACCOUNT_PASSWORD", "").strip()
    if not broker or not acc or not pwd:
        raise RuntimeError("Missing live trading credentials: TQ_BROKER_ID/TQ_ACCOUNT_ID/TQ_ACCOUNT_PASSWORD")
    return TradeAccountConfig(broker_id=broker, account_id=acc, password=pwd)


def create_tq_account(*, mode: TradingMode, sim_account: SimAccount = "tqsim") -> Any:
    _ensure_tqsdk_on_path()
    from tqsdk import TqAccount, TqKq, TqSim

    if mode in {"paper", "sim"}:
        if sim_account == "tqkq":
            return TqKq()
        return TqSim()

    # live
    if not is_live_enabled():
        raise RuntimeError("Live trading is disabled. Set GHTRADER_LIVE_ENABLED=true in .env to enable (dangerous).")
    cfg = load_trade_account_config_from_env()
    return TqAccount(cfg.broker_id, cfg.account_id, cfg.password)


def create_tq_api(*, account: Any, web_gui: bool | str = False) -> Any:
    _ensure_tqsdk_on_path()
    from tqsdk import TqApi

    auth = get_tqsdk_auth()
    return TqApi(account=account, auth=auth, web_gui=web_gui)


def _safe_float(x: Any) -> float | None:
    try:
        v = float(x)
        if v != v:  # NaN
            return None
        return v
    except Exception:
        return None


def snapshot_account_state(*, api: Any, symbols: list[str], account: Any | None = None) -> dict[str, Any]:
    """
    Create a JSON-serializable snapshot of account/order/position state (best-effort).
    """
    snap: dict[str, Any] = {"ts": now_utc_iso(), "symbols": list(symbols)}

    # Account
    try:
        acc = api.get_account(account=account)
        snap["account"] = {
            "balance": _safe_float(getattr(acc, "balance", None)),
            "available": _safe_float(getattr(acc, "available", None)),
            "margin": _safe_float(getattr(acc, "margin", None)),
            "float_profit": _safe_float(getattr(acc, "float_profit", None)),
            "position_profit": _safe_float(getattr(acc, "position_profit", None)),
            "risk_ratio": _safe_float(getattr(acc, "risk_ratio", None)),
        }
    except Exception as e:
        snap["account_error"] = str(e)

    # Positions
    pos_out: dict[str, Any] = {}
    for s in symbols:
        try:
            p = api.get_position(s, account=account)
            pos_out[s] = {
                "volume_long": int(getattr(p, "volume_long", 0) or 0),
                "volume_short": int(getattr(p, "volume_short", 0) or 0),
                "volume_long_today": int(getattr(p, "volume_long_today", 0) or 0),
                "volume_short_today": int(getattr(p, "volume_short_today", 0) or 0),
                "volume_long_his": int(getattr(p, "volume_long_his", 0) or 0),
                "volume_short_his": int(getattr(p, "volume_short_his", 0) or 0),
                "float_profit_long": _safe_float(getattr(p, "float_profit_long", None)),
                "float_profit_short": _safe_float(getattr(p, "float_profit_short", None)),
            }
        except Exception as e:
            pos_out[s] = {"error": str(e)}
    snap["positions"] = pos_out

    # Orders (alive only, best-effort)
    try:
        orders = api.get_order(account=account)
        alive: list[dict[str, Any]] = []
        for _, o in orders.items():  # Entity-like
            try:
                if getattr(o, "status", "") != "ALIVE":
                    continue
                sym = f"{getattr(o, 'exchange_id', '')}.{getattr(o, 'instrument_id', '')}"
                alive.append(
                    {
                        "order_id": str(getattr(o, "order_id", "")),
                        "symbol": sym,
                        "direction": str(getattr(o, "direction", "")),
                        "offset": str(getattr(o, "offset", "")),
                        "price_type": str(getattr(o, "price_type", "")),
                        "limit_price": _safe_float(getattr(o, "limit_price", None)),
                        "volume_orign": int(getattr(o, "volume_orign", 0) or 0),
                        "volume_left": int(getattr(o, "volume_left", 0) or 0),
                        "last_msg": str(getattr(o, "last_msg", "")),
                    }
                )
            except Exception:
                continue
        snap["orders_alive"] = alive
    except Exception as e:
        snap["orders_error"] = str(e)

    return snap


class TradeRunWriter:
    """
    Simple run directory writer for trading jobs.

    Persists:
    - run_config.json
    - snapshots.jsonl
    - events.jsonl
    """

    def __init__(self, run_id: str, runs_dir: Path | None = None):
        self.run_id = run_id
        self.runs_dir = runs_dir or get_runs_dir()
        self.root = self.runs_dir / "trading" / self.run_id
        self.root.mkdir(parents=True, exist_ok=True)
        self._snap_path = self.root / "snapshots.jsonl"
        self._evt_path = self.root / "events.jsonl"

    def write_config(self, cfg: dict[str, Any]) -> None:
        p = self.root / "run_config.json"
        payload = {"created_at": now_utc_iso(), **cfg}
        p.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")

    def append_snapshot(self, snap: dict[str, Any]) -> None:
        with open(self._snap_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(snap, default=str) + "\n")

    def append_event(self, evt: dict[str, Any]) -> None:
        with open(self._evt_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(evt, default=str) + "\n")


class GracefulShutdown:
    """
    Cooperative shutdown flag for SIGTERM/SIGINT.
    """

    def __init__(self):
        self.requested = False

    def install(self) -> None:
        def _handler(signum: int, _frame: Any) -> None:
            log.warning("trade.shutdown_signal", signum=signum)
            self.requested = True

        signal.signal(signal.SIGTERM, _handler)
        signal.signal(signal.SIGINT, _handler)

