from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from ghtrader.util.json_io import read_json

EVENT_ACTIONS = {"skip", "shift_start", "shift_end"}


@dataclass(frozen=True)
class ExchangeEvent:
    exchange: str
    variety: str | None
    session: str
    action: str
    start_day: date
    end_day: date
    start_time: str | None
    end_time: str | None
    reason: str
    source_url: str | None


def _parse_day(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    s = str(value or "").strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except Exception:
        return None


def _normalize_event(raw: dict[str, Any]) -> ExchangeEvent | None:
    ex = str(raw.get("exchange") or "").upper().strip()
    if not ex:
        return None
    var_raw = str(raw.get("variety") or "").strip().lower()
    var = var_raw or None
    sess = str(raw.get("session") or "day").strip().lower()
    action = str(raw.get("action") or "").strip().lower()
    if action not in EVENT_ACTIONS:
        return None

    trading_day = _parse_day(raw.get("trading_day"))
    start_day = _parse_day(raw.get("start_day"))
    end_day = _parse_day(raw.get("end_day"))
    if trading_day is not None:
        start_day = trading_day
        end_day = trading_day
    if start_day is None or end_day is None:
        return None
    if start_day > end_day:
        start_day, end_day = end_day, start_day

    start_time = str(raw.get("start_time") or raw.get("start") or "").strip() or None
    end_time = str(raw.get("end_time") or raw.get("end") or "").strip() or None
    reason = str(raw.get("reason") or "").strip() or "unspecified"
    source_url = str(raw.get("source_url") or "").strip() or None

    return ExchangeEvent(
        exchange=ex,
        variety=var,
        session=sess,
        action=action,
        start_day=start_day,
        end_day=end_day,
        start_time=start_time,
        end_time=end_time,
        reason=reason,
        source_url=source_url,
    )


def _default_events() -> list[ExchangeEvent]:
    defaults: list[dict[str, Any]] = [
        {
            "exchange": "SHFE",
            "session": "night",
            "action": "shift_start",
            "trading_day": "2019-12-26",
            "start_time": "22:30:00",
            "reason": "shfe_2019_12_25_delay_open",
            "source_url": "https://www.shfe.com.cn/publicnotice/notice/201912/t20191225_795464.html",
        },
        {
            "exchange": "SHFE",
            "session": "night",
            "action": "skip",
            "start_day": "2020-02-03",
            "end_day": "2020-05-06",
            "reason": "shfe_covid_night_suspend",
        },
    ]
    out: list[ExchangeEvent] = []
    for raw in defaults:
        ev = _normalize_event(raw)
        if ev is not None:
            out.append(ev)
    return out


def load_exchange_events(
    *,
    data_dir: Path | None,
    exchange: str | None = None,
    variety: str | None = None,
) -> list[ExchangeEvent]:
    events: list[ExchangeEvent] = []
    events.extend(_default_events())

    path = None
    if data_dir is not None:
        path = Path(data_dir) / "calendars" / "exchange_events.json"
    if path and path.exists():
        obj = read_json(path)
        if isinstance(obj, list):
            for raw in obj:
                if isinstance(raw, dict):
                    ev = _normalize_event(raw)
                    if ev is not None:
                        events.append(ev)

    ex = str(exchange or "").upper().strip() or None
    var = str(variety or "").lower().strip() or None
    filtered: list[ExchangeEvent] = []
    for ev in events:
        if ex and ev.exchange != ex:
            continue
        if var and ev.variety and ev.variety != var:
            continue
        filtered.append(ev)
    return filtered


def events_for_day(events: list[ExchangeEvent], day: date) -> list[ExchangeEvent]:
    out: list[ExchangeEvent] = []
    for ev in events:
        if ev.start_day <= day <= ev.end_day:
            out.append(ev)
    return out
