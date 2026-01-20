"""
TqSdk-based main schedule builder.

Builds a main-contract change-event list for a continuous alias like:
  KQ.m@SHFE.cu

The event list is later expanded into a daily schedule and persisted to QuestDB.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
import os
import time
from pathlib import Path
from typing import Any

import structlog

from ghtrader.config import get_tqsdk_auth
from ghtrader.data.trading_sessions import normalize_trading_sessions, write_trading_sessions_cache
from ghtrader.tq.runtime import trading_day_from_ts_ns

log = structlog.get_logger()


def _quote_value(quote: Any, key: str) -> Any:
    if isinstance(quote, dict):
        return quote.get(key)
    return getattr(quote, key, None)


def _quote_datetime_to_ts_ns(value: Any) -> int | None:
    """
    Best-effort conversion of TqSdk quote datetime to epoch-nanoseconds.
    """
    if value in (None, ""):
        return None
    try:
        if isinstance(value, (int, float)):
            v = float(value)
            if v > 1e14:
                return int(v)
            if v > 1e12:
                return int(v * 1_000_000.0)
            if v > 1e9:
                return int(v * 1_000_000_000.0)
            return int(v * 1_000_000_000.0)
    except Exception:
        pass
    try:
        s = str(value).strip()
    except Exception:
        return None
    if not s:
        return None
    dt: datetime | None = None
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        try:
            dt = datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
        except Exception:
            dt = None
    if dt is None:
        try:
            d0 = date.fromisoformat(s[:10])
            dt = datetime(d0.year, d0.month, d0.day)
        except Exception:
            return None
    if dt.tzinfo is None:
        try:
            from zoneinfo import ZoneInfo

            dt = dt.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
        except Exception:
            dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000_000)


def _event_trading_day(*, ts_ns: int | None, quote_datetime: Any, data_dir: Path) -> date | None:
    if ts_ns is None:
        ts_ns = _quote_datetime_to_ts_ns(quote_datetime)
    if ts_ns is not None:
        return trading_day_from_ts_ns(int(ts_ns), data_dir=data_dir)
    try:
        s = str(quote_datetime).strip()
        if s:
            return date.fromisoformat(s[:10])
    except Exception:
        pass
    return None


def extract_main_schedule_events(
    *,
    exchange: str,
    variety: str,
    start: date,
    end: date,
    data_dir: Path,
) -> list[dict[str, Any]]:
    """
    Extract (trading_day -> underlying_symbol) change events for KQ.m@EXCHANGE.var.

    Returns a list of dicts with:
      - trading_day (date)
      - underlying_symbol (str)
      - event_ts_ns (int | None)
      - source ("initial" | "change")
    """
    from tqsdk import BacktestFinished, TqApi, TqBacktest  # type: ignore

    ex = str(exchange).upper().strip()
    var = str(variety).lower().strip()
    symbol = f"KQ.m@{ex}.{var}"

    api = None
    events: list[dict[str, Any]] = []
    last_underlying: str | None = None
    last_event_day: date | None = None
    sessions_written = False
    updates = 0
    last_progress_ts = time.time()
    last_seen_td: date | None = None
    last_seen_underlying: str | None = None
    try:
        progress_every_s = float(os.environ.get("GHTRADER_PROGRESS_EVERY_S", "15") or "15")
    except Exception:
        progress_every_s = 15.0
    progress_every_s = max(5.0, float(progress_every_s))
    log.info(
        "tq_main_schedule.extract_start",
        symbol=symbol,
        start=start.isoformat(),
        end=end.isoformat(),
        progress_every_s=progress_every_s,
    )
    try:
        auth = get_tqsdk_auth()
        api = TqApi(backtest=TqBacktest(start_dt=start, end_dt=end), auth=auth, disable_print=True)
        quote = api.get_quote(symbol)
        while True:
            try:
                if not api.wait_update():
                    break
            except BacktestFinished:
                break
            updates += 1

            underlying = str(_quote_value(quote, "underlying_symbol") or "").strip()
            if not underlying:
                continue

            dt_raw = _quote_value(quote, "datetime")
            ts_ns = _quote_datetime_to_ts_ns(dt_raw)
            td = _event_trading_day(ts_ns=ts_ns, quote_datetime=dt_raw, data_dir=data_dir)
            if td is None:
                continue
            last_seen_td = td
            last_seen_underlying = underlying

            if (time.time() - last_progress_ts) >= progress_every_s:
                log.info(
                    "tq_main_schedule.progress",
                    symbol=symbol,
                    updates=int(updates),
                    last_trading_day=str(last_seen_td),
                    last_underlying=str(last_seen_underlying),
                    events=int(len(events)),
                )
                last_progress_ts = time.time()

            if not sessions_written:
                try:
                    trading_time = _quote_value(quote, "trading_time")
                    if trading_time not in (None, ""):
                        payload = normalize_trading_sessions(
                            trading_time=trading_time, exchange=ex, variety=var
                        )
                        write_trading_sessions_cache(data_dir=data_dir, exchange=ex, variety=var, payload=payload)
                        sessions_written = True
                except Exception as e:
                    log.debug("tq_main_schedule.trading_sessions_cache_failed", error=str(e))

            if last_underlying is None:
                events.append(
                    {
                        "trading_day": td,
                        "underlying_symbol": underlying,
                        "event_ts_ns": ts_ns,
                        "source": "initial",
                    }
                )
                log.debug(
                    "tq_main_schedule.event_initial",
                    symbol=symbol,
                    trading_day=td.isoformat(),
                    underlying_symbol=underlying,
                )
                last_underlying = underlying
                last_event_day = td
                continue

            if api.is_changing(quote, "underlying_symbol"):
                if underlying != last_underlying or td != last_event_day:
                    events.append(
                        {
                            "trading_day": td,
                            "underlying_symbol": underlying,
                            "event_ts_ns": ts_ns,
                            "source": "change",
                        }
                    )
                    log.debug(
                        "tq_main_schedule.event_change",
                        symbol=symbol,
                        trading_day=td.isoformat(),
                        underlying_symbol=underlying,
                    )
                    last_underlying = underlying
                    last_event_day = td
    finally:
        try:
            if api is not None:
                api.close()
        except Exception as e:
            log.debug("tq_main_schedule.api_close_failed", error=str(e))

    # Sort events by trading day and keep the latest event per day.
    dedup: dict[date, dict[str, Any]] = {}
    for ev in events:
        td = ev.get("trading_day")
        if isinstance(td, date):
            dedup[td] = ev
    out = [dedup[k] for k in sorted(dedup.keys())]
    log.info(
        "tq_main_schedule.extract_done",
        symbol=symbol,
        updates=int(updates),
        events=int(len(out)),
        last_trading_day=str(last_seen_td or ""),
        last_underlying=str(last_seen_underlying or ""),
    )
    return out
