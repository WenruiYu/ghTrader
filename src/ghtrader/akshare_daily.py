"""
Akshare adapter: daily futures data fetch + caching + symbol normalization.

We use akshare primarily to obtain *daily* open interest (OI) per contract, which
is required to build an SHFE-style main-contract roll schedule.

Notes:
- The vendored akshare repo lives at ./akshare. Users should install it via:
    pip install -e ./akshare
- This module keeps akshare imports local to avoid heavy import costs at ghtrader
  CLI startup.
"""

from __future__ import annotations

import importlib
import os
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import structlog

log = structlog.get_logger()

EXPECTED_FUTURES_DAILY_COLUMNS = [
    "symbol",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "open_interest",
    "turnover",
    "settle",
    "pre_settle",
    "variety",
]


def _repo_root() -> Path:
    # src/ghtrader/akshare_daily.py -> repo root
    return Path(__file__).resolve().parent.parent.parent


def _ensure_akshare_importable() -> None:
    """
    Ensure `import akshare` resolves to the real package (not a namespace stub).

    In this repo layout, `./akshare` (no __init__.py) can be treated as a namespace
    package when akshare isn't installed, causing confusing import errors.
    """
    try:
        import akshare  # type: ignore

        # Namespace packages have no __file__.
        if getattr(akshare, "__file__", None):
            return

        raise ModuleNotFoundError("akshare resolves to a namespace package (not installed)")
    except Exception:
        # Try to make the vendored repo importable without requiring the user to
        # install it, but prefer `pip install -e ./akshare` for correctness.
        akshare_repo = _repo_root() / "akshare"
        if akshare_repo.exists() and str(akshare_repo) not in sys.path:
            # If a namespace package was already loaded, drop it before retry.
            sys.modules.pop("akshare", None)
            sys.path.insert(0, str(akshare_repo))
            importlib.invalidate_caches()
            try:
                import akshare  # type: ignore

                if getattr(akshare, "__file__", None):
                    return
            except Exception:
                pass

        raise RuntimeError(
            "Akshare is required for daily OI data. Install the vendored repo with: "
            "pip install -e ./akshare"
        )


def normalize_akshare_symbol_to_tqsdk(raw_symbol: str, exchange: str = "SHFE") -> str:
    """
    Convert akshare daily symbol like 'CU2602' -> 'SHFE.cu2602'.

    This is a best-effort normalizer for SHFE metals contracts used by ghTrader.
    """
    s = str(raw_symbol).strip()
    ex = exchange.upper().strip()

    # Already looks like TqSdk format.
    if "." in s:
        parts = s.split(".", 1)
        if len(parts) == 2:
            ex_in, rest = parts[0].upper(), parts[1]
            m = re.match(r"^([A-Za-z]+)(\d+)$", rest.strip())
            if m:
                var, ym = m.group(1).lower(), m.group(2)
                return f"{ex_in}.{var}{ym}"
        return s

    m = re.match(r"^([A-Za-z]+)(\d+)$", s)
    if not m:
        raise ValueError(f"Cannot normalize akshare symbol: {raw_symbol!r}")

    variety, ym = m.group(1).lower(), m.group(2)
    return f"{ex}.{variety}{ym}"


def _cache_root(data_dir: Path) -> Path:
    return data_dir / "akshare" / "futures_daily"


def _cache_path(data_dir: Path, market: str, day: date) -> Path:
    return _cache_root(data_dir) / f"market={market.upper()}" / f"date={day.isoformat()}" / "daily.parquet"


def fetch_futures_daily_for_date(
    *,
    data_dir: Path,
    market: str,
    day: date,
    refresh: bool = False,
    max_retries: int = 3,
) -> pd.DataFrame:
    """
    Fetch exchange daily futures data for a single trading day, with local caching.

    Returns a DataFrame with (at least) columns:
      ['symbol','date','open','high','low','close','volume','open_interest','turnover','settle','pre_settle','variety']
    """
    # Avoid caching non-trading day empties: consult the trading calendar first.
    from ghtrader.trading_calendar import is_trading_day

    if not is_trading_day(day=day, data_dir=data_dir):
        return pd.DataFrame(columns=EXPECTED_FUTURES_DAILY_COLUMNS)

    cache_path = _cache_path(data_dir, market, day)
    if cache_path.exists() and not refresh:
        return pd.read_parquet(cache_path)

    _ensure_akshare_importable()
    from akshare.futures.futures_daily_bar import get_futures_daily  # type: ignore

    last_err: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            df = get_futures_daily(
                start_date=day.strftime("%Y%m%d"),
                end_date=day.strftime("%Y%m%d"),
                market=market.upper(),
            )
            last_err = None
            break
        except Exception as e:
            last_err = e
            if attempt >= max_retries:
                break
            sleep_s = 0.5 * (2**attempt)
            log.warning(
                "akshare.daily_fetch_retry",
                market=market.upper(),
                day=day.isoformat(),
                attempt=attempt + 1,
                max_retries=max_retries,
                sleep_s=sleep_s,
                error=str(e),
            )
            time.sleep(sleep_s)

    if last_err is not None:
        # Do not cache failures as empty; surface failure to caller.
        raise RuntimeError(
            f"akshare daily fetch failed after retries: market={market.upper()} day={day.isoformat()} err={last_err}"
        ) from last_err
    if df is None or df.empty:
        # Persist an empty file as a negative cache to avoid repeated calls.
        # Important: Parquet writers require a schema; keep a stable empty schema.
        df = pd.DataFrame(columns=EXPECTED_FUTURES_DAILY_COLUMNS)
    else:
        # Akshare sometimes returns numeric columns as mixed object dtype with empty strings.
        # Coerce to numeric before Parquet write to avoid ArrowInvalid.
        df = df.copy()
        for col in EXPECTED_FUTURES_DAILY_COLUMNS:
            if col not in df.columns:
                df[col] = pd.NA

        df = df[EXPECTED_FUTURES_DAILY_COLUMNS]
        df["symbol"] = df["symbol"].astype(str)
        df["variety"] = df["variety"].astype(str)

        numeric_cols = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "open_interest",
            "turnover",
            "settle",
            "pre_settle",
        ]
        for c in numeric_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(cache_path, index=False)
    log.debug("akshare.daily_cached", market=market.upper(), day=day.isoformat(), path=str(cache_path))
    return df


def iter_futures_daily_range(
    *,
    data_dir: Path,
    market: str,
    start: date,
    end: date,
    refresh: bool = False,
    skip_errors: bool = True,
) -> "object":
    """
    Yield per-day futures daily data for trading days only.

    This is used by long-running backfills so we can stream-process and avoid
    building a huge in-memory DataFrame.
    """
    from ghtrader.trading_calendar import get_trading_days

    if end < start:
        return iter(())

    days = get_trading_days(market=market.upper(), start=start, end=end, data_dir=data_dir, refresh=refresh)

    def _gen():
        for d in days:
            try:
                df_day = fetch_futures_daily_for_date(
                    data_dir=data_dir, market=market, day=d, refresh=refresh, max_retries=3
                )
            except Exception as e:
                log.warning("akshare.daily_range_failed", market=market.upper(), day=d.isoformat(), error=str(e))
                if skip_errors:
                    continue
                raise
            if df_day is not None and not df_day.empty:
                yield df_day

    return _gen()


def fetch_futures_daily_range(
    *,
    data_dir: Path,
    market: str,
    start: date,
    end: date,
    refresh: bool = False,
) -> pd.DataFrame:
    """
    Fetch a date range by stitching cached per-day files (and downloading missing days).

    This is intentionally simple and deterministic; performance improvements (batching)
    can be added later once the schedule builder is finalized.
    """
    if end < start:
        raise ValueError("end must be >= start")

    out: list[pd.DataFrame] = list(
        iter_futures_daily_range(
            data_dir=data_dir,
            market=market,
            start=start,
            end=end,
            refresh=refresh,
            skip_errors=True,
        )
    )

    if not out:
        return pd.DataFrame()
    df = pd.concat(out, ignore_index=True)

    # Normalize the `date` column if present.
    if "date" in df.columns:
        try:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
        except Exception:
            pass

    return df

