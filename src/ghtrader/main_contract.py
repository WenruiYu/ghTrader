"""
Main-contract schedule builder (SHFE-style OI rule).

We implement the SHFE rule as described in PRD.md:
- Main contract for day T+1 is determined using end-of-day OI on day T.
- Switch trigger: some other contract's OI > current_main_OI * 1.1
- No intraday switching.

This module produces a deterministic (date -> main underlying contract) schedule that
can be used to materialize a "main-with-depth" continuous dataset from specific-contract
L5 ticks.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import structlog

from ghtrader.akshare_daily import fetch_futures_daily_range, normalize_akshare_symbol_to_tqsdk
from ghtrader.config import get_data_dir

log = structlog.get_logger()


def _to_date_series(s: pd.Series) -> pd.Series:
    dt = pd.to_datetime(s, errors="coerce")
    return dt.dt.date


def _stable_hash_df(df: pd.DataFrame) -> str:
    # Stable across runs (column order + values).
    payload = df.to_csv(index=False).encode()
    return hashlib.sha256(payload).hexdigest()[:16]


def _rolls_dir(data_dir: Path) -> Path:
    return data_dir / "rolls" / "shfe_main_schedule"


def _schedule_dir(data_dir: Path, var: str) -> Path:
    return _rolls_dir(data_dir) / f"var={var.lower()}"


@dataclass(frozen=True)
class MainScheduleResult:
    schedule: pd.DataFrame
    schedule_hash: str
    schedule_path: Path
    manifest_path: Path


def compute_shfe_main_schedule_from_daily(
    daily: pd.DataFrame,
    *,
    var: str,
    rule_threshold: float = 1.1,
    market: str = "SHFE",
) -> pd.DataFrame:
    """
    Pure schedule computation (no IO, no network).

    Args:
        daily: akshare-style daily futures DataFrame with columns:
          - date, symbol, open_interest (and optionally variety)
        var: Variety code (e.g. 'cu', 'au', 'ag')
        rule_threshold: Switch threshold (default 1.1)
        market: Exchange/market tag for TqSdk normalization (default SHFE)
    """
    if daily is None or daily.empty:
        raise ValueError("daily data is empty")

    var_u = var.upper()

    # Normalize types
    if "date" not in daily.columns or "symbol" not in daily.columns or "open_interest" not in daily.columns:
        raise ValueError(f"Unexpected daily schema: columns={list(daily.columns)}")

    df = daily.copy()
    df["date"] = _to_date_series(df["date"])
    df["open_interest"] = pd.to_numeric(df["open_interest"], errors="coerce")

    # Filter to target variety only
    if "variety" in df.columns:
        df = df[df["variety"].astype(str).str.upper() == var_u]
    else:
        df = df[df["symbol"].astype(str).str.upper().str.startswith(var_u)]

    df = df.dropna(subset=["date", "symbol", "open_interest"])
    if df.empty:
        raise ValueError(f"No daily rows after filtering for variety={var_u}")

    # Group by trading day
    by_day: dict[date, pd.DataFrame] = {}
    for d, g in df.groupby("date"):
        g2 = g[["symbol", "open_interest"]].copy()
        # If duplicate symbols exist, keep the max OI
        g2 = g2.groupby("symbol", as_index=False)["open_interest"].max()
        by_day[d] = g2.sort_values("open_interest", ascending=False).reset_index(drop=True)

    days = sorted(by_day.keys())
    if not days:
        raise ValueError("No trading days found")

    rows: list[dict[str, Any]] = []
    current_main_raw: str | None = None

    for i, day in enumerate(days):
        # Reference OI day for decision: yesterday, except for the first day where we have no prior info.
        ref_day = days[i - 1] if i > 0 else day
        ref = by_day.get(ref_day)
        if ref is None or ref.empty:
            continue

        top_raw = str(ref.iloc[0]["symbol"])
        top_oi = float(ref.iloc[0]["open_interest"])

        prev_main_raw = current_main_raw
        prev_main_oi = float("nan")
        if prev_main_raw is not None:
            match = ref[ref["symbol"].astype(str) == prev_main_raw]
            if not match.empty:
                prev_main_oi = float(match["open_interest"].iloc[0])

        # Next-best = best contract excluding prev_main (for diagnostics)
        if prev_main_raw is not None:
            alt = ref[ref["symbol"].astype(str) != prev_main_raw]
            if not alt.empty:
                next_best_raw = str(alt.iloc[0]["symbol"])
                next_best_oi = float(alt.iloc[0]["open_interest"])
            else:
                next_best_raw = ""
                next_best_oi = float("nan")
        else:
            next_best_raw = ""
            next_best_oi = float("nan")

        # Apply rule to compute main effective on `day`
        switch_flag = False
        if prev_main_raw is None:
            current_main_raw = top_raw
        elif not np.isfinite(prev_main_oi):
            # If prior main isn't present in the ref data, reset to max.
            switch_flag = True
            current_main_raw = top_raw
        elif top_raw != prev_main_raw and top_oi > prev_main_oi * float(rule_threshold):
            switch_flag = True
            current_main_raw = top_raw
        else:
            current_main_raw = prev_main_raw

        # Main OI as measured on ref_day
        main_match = ref[ref["symbol"].astype(str) == current_main_raw]
        main_oi = float(main_match["open_interest"].iloc[0]) if not main_match.empty else float("nan")

        rows.append(
            {
                "date": day,
                "main_contract": normalize_akshare_symbol_to_tqsdk(current_main_raw, exchange=market),
                "main_oi": main_oi,
                "next_best_contract": (
                    normalize_akshare_symbol_to_tqsdk(next_best_raw, exchange=market) if next_best_raw else ""
                ),
                "next_best_oi": next_best_oi,
                "switch_flag": bool(switch_flag),
            }
        )

    schedule = pd.DataFrame(rows)
    if schedule.empty:
        raise ValueError("Computed empty schedule")

    # Segment id: increments whenever the underlying main_contract changes.
    # This is used downstream to prevent cross-roll leakage in sequence modeling.
    schedule = schedule.sort_values("date").reset_index(drop=True)
    seg_ids: list[int] = []
    seg = 0
    prev: str | None = None
    for mc in schedule["main_contract"].astype(str).tolist():
        if prev is None:
            seg = 0
        elif mc != prev:
            seg += 1
        seg_ids.append(int(seg))
        prev = mc
    schedule["segment_id"] = seg_ids
    return schedule


def build_shfe_main_schedule(
    *,
    var: str,
    start: date,
    end: date,
    rule_threshold: float = 1.1,
    data_dir: Path | None = None,
    refresh_akshare: bool = False,
) -> MainScheduleResult:
    """
    Build and persist an SHFE-style main contract schedule for a given variety.

    Args:
        var: Variety code (e.g. 'cu', 'au', 'ag')
        start/end: Date range to build (inclusive)
        rule_threshold: Switch threshold (default 1.1)
        data_dir: ghTrader data dir (defaults to config)
        refresh_akshare: Force re-download of cached daily data
    """
    if data_dir is None:
        data_dir = get_data_dir()

    if end < start:
        raise ValueError("end must be >= start")

    market = "SHFE"
    var_u = var.upper()

    daily = fetch_futures_daily_range(
        data_dir=data_dir,
        market=market,
        start=start,
        end=end,
        refresh=refresh_akshare,
    )
    if daily is None or daily.empty:
        raise RuntimeError(f"No daily data returned for {market} {start}..{end}")
    schedule = compute_shfe_main_schedule_from_daily(
        daily,
        var=var,
        rule_threshold=rule_threshold,
        market=market,
    )

    # Persist
    out_dir = _schedule_dir(data_dir, var)
    out_dir.mkdir(parents=True, exist_ok=True)
    schedule_path = out_dir / "schedule.parquet"
    schedule.to_parquet(schedule_path, index=False)

    schedule_hash = _stable_hash_df(schedule)
    manifest = {
        "created_at": datetime.now().isoformat(),
        "market": market,
        "variety": var.lower(),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "rule_threshold": float(rule_threshold),
        "rows": int(len(schedule)),
        "schedule_hash": schedule_hash,
        "schedule_path": str(schedule_path),
        "distinct_main_contracts": sorted([c for c in schedule["main_contract"].unique().tolist() if c]),
        "source": "akshare",
    }
    manifest_path = out_dir / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2, default=str)

    log.info(
        "main_schedule.built",
        var=var.lower(),
        start=start.isoformat(),
        end=end.isoformat(),
        threshold=float(rule_threshold),
        rows=len(schedule),
        schedule_hash=schedule_hash,
        path=str(schedule_path),
    )

    return MainScheduleResult(
        schedule=schedule,
        schedule_hash=schedule_hash,
        schedule_path=schedule_path,
        manifest_path=manifest_path,
    )

