from __future__ import annotations

import json
import os
import signal
import time
from collections import deque
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Literal

import structlog

from ghtrader.config import get_env, get_runs_dir, get_tqsdk_auth, is_live_enabled, load_config

log = structlog.get_logger()


TradingMode = Literal["paper", "sim", "live"]
SimAccount = Literal["tqsim", "tqkq"]


from ghtrader.util.time import now_iso as now_utc_iso


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

            from ghtrader.data.trading_calendar import get_trading_calendar

            cal = get_trading_calendar(data_dir=data_dir, refresh=False)
            if cal:
                j = bisect_right(cal, d0)
                if j < len(cal):
                    return cal[j]
        except Exception:
            pass

    return d0 + timedelta(days=1)


def is_in_trading_time(quote: Any, quote_datetime: Any, local_time_record: Any) -> bool:
    """
    Best-effort wrapper for TqSdk trading-session detection.

    Returns True if TqSdk is unavailable or the check fails (never block trading on guard failure).
    """
    try:
        from tqsdk.datetime import _is_in_trading_time  # type: ignore

        return bool(_is_in_trading_time(quote, quote_datetime, local_time_record))
    except Exception:
        return True


@dataclass(frozen=True)
class TradeAccountConfig:
    broker_id: str
    account_id: str
    password: str


def _canonical_account_profile(profile: str) -> str:
    """
    Canonicalize an account profile identifier.

    - used for locks, run_config.json, and dashboard keys
    - allows only [a-z0-9_]
    """
    p = str(profile or "").strip()
    if not p:
        return "default"
    p = p.lower()
    out: list[str] = []
    prev_us = False
    for ch in p:
        if ch.isalnum():
            out.append(ch)
            prev_us = False
        else:
            if not prev_us:
                out.append("_")
                prev_us = True
    s = "".join(out).strip("_")
    return s or "default"


def canonical_account_profile(profile: str) -> str:
    """Public wrapper for consistent profile normalization across modules."""
    return _canonical_account_profile(profile)


def _env_profile_suffixes(profile: str) -> list[str]:
    """
    Return env var suffixes to try for a profile.

    We primarily use an uppercase suffix (`MAIN`) but also accept lowercase (`main`)
    for convenience.
    """
    p = _canonical_account_profile(profile)
    if p == "default":
        return [""]
    up = "".join([ch.upper() if ch.isalnum() else "_" for ch in p]).strip("_")
    lo = "".join([ch.lower() if ch.isalnum() else "_" for ch in p]).strip("_")
    out: list[str] = []
    for s in [up, lo]:
        if s and s not in out:
            out.append(s)
    return out


def list_account_profiles_from_env() -> list[str]:
    """
    List configured broker account profiles from env.

    - always includes `default` for backwards compatibility
    - additional profiles are taken from `GHTRADER_TQ_ACCOUNT_PROFILES=main,alt,...`
    """
    load_config()
    raw = str(get_env("GHTRADER_TQ_ACCOUNT_PROFILES", "") or "").strip()
    profiles: list[str] = ["default"]
    if not raw:
        return profiles
    for part in raw.split(","):
        p = _canonical_account_profile(part)
        if p and p not in profiles:
            profiles.append(p)
    return profiles


def load_trade_account_config_from_env(*, profile: str = "default") -> TradeAccountConfig:
    load_config()
    p = _canonical_account_profile(profile)
    if p == "default":
        broker = os.environ.get("TQ_BROKER_ID", "").strip()
        acc = os.environ.get("TQ_ACCOUNT_ID", "").strip()
        pwd = os.environ.get("TQ_ACCOUNT_PASSWORD", "").strip()
        if not broker or not acc or not pwd:
            raise RuntimeError("Missing live trading credentials: TQ_BROKER_ID/TQ_ACCOUNT_ID/TQ_ACCOUNT_PASSWORD")
        return TradeAccountConfig(broker_id=broker, account_id=acc, password=pwd)

    suffixes = _env_profile_suffixes(p)
    last_err = ""
    for suf in suffixes:
        broker = os.environ.get(f"TQ_BROKER_ID_{suf}", "").strip()
        acc = os.environ.get(f"TQ_ACCOUNT_ID_{suf}", "").strip()
        pwd = os.environ.get(f"TQ_ACCOUNT_PASSWORD_{suf}", "").strip()
        if broker and acc and pwd:
            return TradeAccountConfig(broker_id=broker, account_id=acc, password=pwd)
        last_err = f"Missing live trading credentials for profile {p!r}: TQ_BROKER_ID_{suf}/TQ_ACCOUNT_ID_{suf}/TQ_ACCOUNT_PASSWORD_{suf}"
    raise RuntimeError(last_err or f"Missing live trading credentials for profile {p!r}")


def is_trade_account_configured(*, profile: str = "default") -> bool:
    try:
        _ = load_trade_account_config_from_env(profile=profile)
        return True
    except Exception:
        return False


def create_tq_account(
    *, mode: TradingMode, sim_account: SimAccount = "tqsim", monitor_only: bool = False, account_profile: str = "default"
) -> Any:
    try:
        from tqsdk import TqAccount, TqKq, TqSim  # type: ignore
    except Exception as e:
        raise RuntimeError("tqsdk not installed. Install with: pip install tqsdk") from e

    if mode in {"paper", "sim"}:
        if sim_account == "tqkq":
            return TqKq()
        return TqSim()

    # live
    if (not monitor_only) and (not is_live_enabled()):
        raise RuntimeError("Live trading is disabled. Set GHTRADER_LIVE_ENABLED=true in .env to enable (dangerous).")
    cfg = load_trade_account_config_from_env(profile=account_profile)
    return TqAccount(cfg.broker_id, cfg.account_id, cfg.password)


def create_tq_api(*, account: Any, web_gui: bool | str = False) -> Any:
    try:
        from tqsdk import TqApi  # type: ignore
    except Exception as e:
        raise RuntimeError("tqsdk not installed. Install with: pip install tqsdk") from e

    auth = get_tqsdk_auth()
    return TqApi(account=account, auth=auth, web_gui=web_gui)


def create_tq_data_api(*, disable_print: bool = True) -> Any:
    """
    Create a TqApi instance for data probing (no account/session binding).

    Direct `tqsdk` imports stay isolated in `ghtrader.tq.*` per PRD §5.1.3.
    """
    try:
        from tqsdk import TqApi  # type: ignore
    except Exception as e:
        raise RuntimeError("tqsdk not installed. Install with: pip install tqsdk") from e

    auth = get_tqsdk_auth()
    return TqApi(auth=auth, disable_print=bool(disable_print))


from ghtrader.util.safe_parse import safe_float as _safe_float


def snapshot_account_state(
    *,
    api: Any,
    symbols: list[str],
    account: Any | None = None,
    account_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Create a JSON-serializable snapshot of account/order/position state (best-effort).
    """
    snap: dict[str, Any] = {"schema_version": 2, "ts": now_utc_iso(), "symbols": list(symbols)}
    if account_meta:
        # Non-secret hints for dashboards/log consumers.
        snap["account_meta"] = dict(account_meta)

    # Account
    try:
        acc = api.get_account(account=account)
        bal = _safe_float(getattr(acc, "balance", None))
        fp = _safe_float(getattr(acc, "float_profit", None))
        equity = (float(bal) + float(fp)) if (isinstance(bal, (int, float)) and isinstance(fp, (int, float))) else None
        snap["account"] = {
            "balance": bal,
            "available": _safe_float(getattr(acc, "available", None)),
            "margin": _safe_float(getattr(acc, "margin", None)),
            "float_profit": fp,
            "position_profit": _safe_float(getattr(acc, "position_profit", None)),
            "risk_ratio": _safe_float(getattr(acc, "risk_ratio", None)),
            "equity": equity,
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
                # Best-effort pricing fields (may be missing depending on account/provider).
                "open_price_long": _safe_float(getattr(p, "open_price_long", None)),
                "open_price_short": _safe_float(getattr(p, "open_price_short", None)),
                "position_price_long": _safe_float(getattr(p, "position_price_long", None)),
                "position_price_short": _safe_float(getattr(p, "position_price_short", None)),
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

