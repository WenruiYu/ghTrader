from __future__ import annotations

import importlib
import json
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import structlog

log = structlog.get_logger()


def _repo_root() -> Path:
    # src/ghtrader/trading_calendar.py -> repo root
    return Path(__file__).resolve().parent.parent.parent


def _ensure_akshare_importable() -> None:
    """
    Ensure `import akshare` resolves to the real package (not a namespace stub).

    Duplicates the guard used in `ghtrader.akshare_daily` to avoid import cycles.
    """
    try:
        import akshare  # type: ignore

        if getattr(akshare, "__file__", None):
            return
        raise ModuleNotFoundError("akshare resolves to a namespace package (not installed)")
    except Exception:
        akshare_repo = _repo_root() / "akshare"
        if akshare_repo.exists() and str(akshare_repo) not in sys.path:
            sys.modules.pop("akshare", None)
            sys.path.insert(0, str(akshare_repo))
            importlib.invalidate_caches()
            try:
                import akshare  # type: ignore

                if getattr(akshare, "__file__", None):
                    return
            except Exception:
                pass
        raise RuntimeError("Akshare is required for trading calendar. Install with: pip install -e ./akshare")


def _weekday_days_between(start: date, end: date) -> list[date]:
    out: list[date] = []
    cur = start
    while cur <= end:
        if cur.weekday() < 5:
            out.append(cur)
        cur += timedelta(days=1)
    return out


def calendar_cache_path(data_dir: Path) -> Path:
    return data_dir / "akshare" / "calendar" / "calendar.parquet"


def _load_calendar_from_cache(path: Path) -> list[date] | None:
    try:
        if not path.exists():
            return None
        df = pd.read_parquet(path)
        if "date" not in df.columns:
            return None
        d = pd.to_datetime(df["date"], errors="coerce").dt.date.dropna()
        return sorted(set(d.tolist()))
    except Exception as e:
        log.warning("calendar.cache_read_failed", path=str(path), error=str(e))
        return None


def _write_calendar_cache(path: Path, days: list[date]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame({"date": pd.to_datetime([d.isoformat() for d in days])})
    df.to_parquet(path, index=False)


def _load_calendar_from_akshare() -> list[date]:
    _ensure_akshare_importable()
    from akshare.futures import cons  # type: ignore

    raw = cons.get_calendar()
    out: list[date] = []
    for s in raw:
        try:
            out.append(date.fromisoformat(f"{s[:4]}-{s[4:6]}-{s[6:8]}"))
        except Exception:
            continue
    if not out:
        raise RuntimeError("akshare futures calendar is empty")
    return sorted(set(out))


def get_trading_calendar(
    *,
    data_dir: Path,
    refresh: bool = False,
) -> list[date]:
    """
    Return the global China trading calendar used by akshare futures functions.

    If akshare is unavailable, falls back to a weekday-only approximation.
    """
    cache = calendar_cache_path(data_dir)
    if not refresh:
        cached = _load_calendar_from_cache(cache)
        if cached:
            return cached

    try:
        days = _load_calendar_from_akshare()
        _write_calendar_cache(cache, days)
        return days
    except Exception as e:
        log.warning("calendar.akshare_unavailable", error=str(e))
        # Fallback is only used when akshare isn't installed; we do not cache fallback.
        # Callers should treat this as best-effort.
        return []


def is_trading_day(
    *,
    day: date,
    data_dir: Path,
    refresh: bool = False,
) -> bool:
    days = get_trading_calendar(data_dir=data_dir, refresh=refresh)
    if not days:
        # fallback approximation
        return day.weekday() < 5
    return day in set(days)


def get_trading_days(
    *,
    market: str | None,  # reserved for future per-exchange calendars
    start: date,
    end: date,
    data_dir: Path,
    refresh: bool = False,
) -> list[date]:
    if end < start:
        return []

    days = get_trading_calendar(data_dir=data_dir, refresh=refresh)
    if not days:
        return _weekday_days_between(start, end)

    # calendar is globally sorted; filter by range
    return [d for d in days if start <= d <= end]

