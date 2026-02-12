from __future__ import annotations

import json
import os
import urllib.request
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import structlog

from ghtrader.util.json_io import read_json, write_json_atomic

log = structlog.get_logger()

DEFAULT_SHINNY_HOLIDAY_URL = "https://files.shinnytech.com/shinny_chinese_holiday.json"


def holiday_url() -> str:
    """
    Return the holiday-list URL used to compute trading days.

    This matches the intent of TqSdk’s holiday list source but is implemented
    without akshare.
    """
    u = str(os.environ.get("TQ_CHINESE_HOLIDAY_URL") or DEFAULT_SHINNY_HOLIDAY_URL).strip()
    return u or DEFAULT_SHINNY_HOLIDAY_URL


def holidays_cache_path(data_dir: Path) -> Path:
    return data_dir / "trading_calendar" / "shinny_chinese_holiday.json"


def calendar_cache_path(data_dir: Path) -> Path:
    return data_dir / "trading_calendar" / "calendar.json"


def _parse_date_any(s: str) -> date | None:
    ss = str(s).strip()
    if not ss:
        return None
    try:
        if "-" in ss:
            return date.fromisoformat(ss)
    except Exception:
        pass
    try:
        if len(ss) == 8 and ss.isdigit():
            return date(int(ss[:4]), int(ss[4:6]), int(ss[6:8]))
    except Exception:
        pass
    return None


def _extract_holiday_strings(obj: Any) -> list[str]:
    """
    Best-effort extractor for a holiday JSON payload.

    We accept a few possible formats:
    - ["2026-01-01", ...]
    - {"holidays": [...]} or {"data":[...]}
    - {"2026": [...], "2025":[...], ...}
    """
    if isinstance(obj, list):
        return [str(x) for x in obj]
    if isinstance(obj, dict):
        for k in ("holidays", "holiday", "data"):
            v = obj.get(k)
            if isinstance(v, list):
                return [str(x) for x in v]
        out: list[str] = []
        for v in obj.values():
            if isinstance(v, list):
                out.extend([str(x) for x in v])
        if out:
            return out
    return []


def _fetch_holidays_raw(url: str, *, timeout_s: float = 15.0) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "ghtrader/0.1"})
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        raw = resp.read()
    return raw.decode("utf-8", errors="replace")


def _load_holidays_from_json_text(text: str) -> set[date]:
    try:
        obj = json.loads(text)
    except Exception:
        return set()
    items = _extract_holiday_strings(obj)
    out: set[date] = set()
    for it in items:
        d = _parse_date_any(it)
        if d is not None:
            out.add(d)
    return out


def get_holidays(*, data_dir: Path, refresh: bool = False, allow_download: bool = True) -> set[date]:
    """
    Return a cached set of holiday dates.

    If the holiday file cannot be downloaded/parsed, returns an empty set
    (callers should fall back to weekday-only behavior).
    """
    cache = holidays_cache_path(data_dir)
    if cache.exists() and not refresh:
        try:
            text = cache.read_text(encoding="utf-8")
            days = _load_holidays_from_json_text(text)
            if days:
                return days
        except Exception as e:
            log.warning("calendar.holidays_cache_read_failed", path=str(cache), error=str(e))

    # Some dashboard/UI paths must be cache-only (no network).
    if not bool(allow_download):
        return set()

    try:
        url = holiday_url()
        text = _fetch_holidays_raw(url)
        days = _load_holidays_from_json_text(text)
        if days:
            cache.parent.mkdir(parents=True, exist_ok=True)
            cache.write_text(text, encoding="utf-8")
            return days
        log.warning("calendar.holidays_download_empty", url=url)
    except Exception as e:
        log.warning("calendar.holidays_download_failed", url=holiday_url(), error=str(e))
    return set()


def _weekday_days_between(start: date, end: date) -> list[date]:
    out: list[date] = []
    cur = start
    while cur <= end:
        if cur.weekday() < 5:
            out.append(cur)
        cur += timedelta(days=1)
    return out


def _compute_trading_days(*, start: date, end: date, holidays: set[date]) -> list[date]:
    out: list[date] = []
    cur = start
    while cur <= end:
        if cur.weekday() < 5 and cur not in holidays:
            out.append(cur)
        cur += timedelta(days=1)
    return out


def _load_calendar_from_cache(path: Path) -> list[date] | None:
    try:
        if not path.exists():
            return None
        obj = read_json(path)
        if not isinstance(obj, list):
            return None
        out: set[date] = set()
        for it in obj:
            d = _parse_date_any(str(it))
            if d is not None:
                out.add(d)
        return sorted(out)
    except Exception as e:
        log.warning("calendar.cache_read_failed", path=str(path), error=str(e))
        return None


def _write_calendar_cache(path: Path, days: list[date]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json_atomic(path, [d.isoformat() for d in days])


def get_trading_calendar(*, data_dir: Path, refresh: bool = False, allow_download: bool = True) -> list[date]:
    """
    Return a cached list of trading days (weekday minus Shinny holiday list).

    This is a global China futures trading-day approximation (sufficient for
    ingest bookkeeping and night-session trading-day mapping). If holidays are
    unavailable, returns an empty list (callers may fall back to weekday-only).
    """
    cache = calendar_cache_path(data_dir)
    if not refresh:
        cached = _load_calendar_from_cache(cache)
        if cached:
            return cached

    # Compute a practical calendar window and cache it.
    holidays = get_holidays(data_dir=data_dir, refresh=refresh, allow_download=allow_download)
    if not holidays:
        return []

    start = date(2000, 1, 1)
    end = date.today() + timedelta(days=366)
    days = _compute_trading_days(start=start, end=end, holidays=holidays)
    try:
        _write_calendar_cache(cache, days)
    except Exception as e:
        log.warning("calendar.cache_write_failed", path=str(cache), error=str(e))
    return days


def is_trading_day(*, day: date, data_dir: Path, refresh: bool = False) -> bool:
    holidays = get_holidays(data_dir=data_dir, refresh=refresh)
    if not holidays:
        return day.weekday() < 5
    return day.weekday() < 5 and day not in holidays


def next_trading_day(*, day: date, data_dir: Path, refresh: bool = False) -> date:
    """
    Return the next trading day strictly after `day`.
    """
    d = day + timedelta(days=1)
    for _ in range(14):  # bounded: long holiday weeks exist
        if is_trading_day(day=d, data_dir=data_dir, refresh=refresh):
            return d
        d += timedelta(days=1)
    return d


def get_trading_days(
    *,
    market: str | None,  # reserved for future per-exchange calendars
    start: date,
    end: date,
    data_dir: Path,
    refresh: bool = False,
) -> list[date]:
    _ = market
    if end < start:
        return []

    holidays = get_holidays(data_dir=data_dir, refresh=refresh)
    if not holidays:
        return _weekday_days_between(start, end)
    return _compute_trading_days(start=start, end=end, holidays=holidays)


def last_trading_day_leq(cal: list[date], d: date) -> date | None:
    """
    Return the latest trading day in calendar <= d, or None if not found.

    If cal is empty, returns d if it's a weekday, else None.
    """
    from bisect import bisect_right

    if not cal:
        return d if d.weekday() < 5 else None
    i = bisect_right(cal, d)
    return cal[i - 1] if i > 0 else None


def latest_trading_day(*, data_dir: Path, refresh: bool = False, allow_download: bool = True) -> date:
    """
    Return the latest trading day <= today.

    Uses the cached trading calendar when available; otherwise falls back to the
    most recent weekday.
    """
    cal = get_trading_calendar(data_dir=data_dir, refresh=refresh, allow_download=allow_download)
    today = date.today()
    ltd = last_trading_day_leq(cal, today)
    if ltd is not None:
        return ltd
    d = today
    for _ in range(7):
        if d.weekday() < 5:
            return d
        d -= timedelta(days=1)
    return today
