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


def normalize_contract_symbol(raw_symbol: str, *, exchange: str = "SHFE") -> str:
    """
    Normalize a contract identifier into TqSdk symbol form.

    Examples:
    - "SHFE.cu2602" -> "SHFE.cu2602" (no-op)
    - "CU2602"      -> "SHFE.cu2602"
    """
    s = str(raw_symbol).strip()
    ex = str(exchange).upper().strip()

    # Already in TqSdk dotted form: normalize case, but avoid mangling continuous aliases like KQ.m@...
    if "." in s:
        if s.startswith("KQ.") or s.startswith("KQ.m@"):
            return s
        head, tail = s.split(".", 1)
        # Only normalize obvious exchange-prefixed symbols (e.g. SHFE.cu2602).
        if head.isalpha() and 2 <= len(head) <= 6:
            return f"{head.upper()}.{tail.lower()}"
        return s
    # Letters + digits (best-effort).
    letters = ""
    digits = ""
    for ch in s:
        if ch.isalpha() and not digits:
            letters += ch
        elif ch.isdigit():
            digits += ch
    letters = letters.lower().strip()
    digits = digits.strip()
    if letters and digits:
        return f"{ex}.{letters}{digits}"
    return s


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
        daily: daily OI DataFrame with columns:
          - date, symbol, open_interest (and optionally variety)
        Symbols may be either:
          - TqSdk symbols (e.g. SHFE.cu2602), or
          - raw contract codes (e.g. CU2602) which will be normalized.
        var: Variety code (e.g. 'cu', 'au', 'ag')
        rule_threshold: Switch threshold (default 1.1)
        market: Exchange/market tag (default SHFE)
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

    # Normalize symbols into TqSdk form, then filter to target variety only.
    df["symbol"] = df["symbol"].astype(str).str.strip().map(lambda s: normalize_contract_symbol(s, exchange=market))
    if "variety" in df.columns:
        df = df[df["variety"].astype(str).str.upper() == var_u]
    else:
        tail = df["symbol"].astype(str).str.split(".", n=1).str[-1]
        df = df[tail.str.upper().str.startswith(var_u)]

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
    current_main: str | None = None

    for i, day in enumerate(days):
        # Reference OI day for decision: yesterday, except for the first day where we have no prior info.
        ref_day = days[i - 1] if i > 0 else day
        ref = by_day.get(ref_day)
        if ref is None or ref.empty:
            continue

        top_sym = str(ref.iloc[0]["symbol"])
        top_oi = float(ref.iloc[0]["open_interest"])

        prev_main = current_main
        prev_main_oi = float("nan")
        if prev_main is not None:
            match = ref[ref["symbol"].astype(str) == prev_main]
            if not match.empty:
                prev_main_oi = float(match["open_interest"].iloc[0])

        # Next-best = best contract excluding prev_main (for diagnostics)
        if prev_main is not None:
            alt = ref[ref["symbol"].astype(str) != prev_main]
            if not alt.empty:
                next_best = str(alt.iloc[0]["symbol"])
                next_best_oi = float(alt.iloc[0]["open_interest"])
            else:
                next_best = ""
                next_best_oi = float("nan")
        else:
            next_best = ""
            next_best_oi = float("nan")

        # Apply rule to compute main effective on `day`
        switch_flag = False
        if prev_main is None:
            current_main = top_sym
        elif not np.isfinite(prev_main_oi):
            # If prior main isn't present in the ref data, reset to max.
            switch_flag = True
            current_main = top_sym
        elif top_sym != prev_main and top_oi > prev_main_oi * float(rule_threshold):
            switch_flag = True
            current_main = top_sym
        else:
            current_main = prev_main

        # Main OI as measured on ref_day
        main_match = ref[ref["symbol"].astype(str) == current_main]
        main_oi = float(main_match["open_interest"].iloc[0]) if not main_match.empty else float("nan")

        rows.append(
            {
                "date": day,
                "main_contract": str(current_main or ""),
                "main_oi": main_oi,
                "next_best_contract": (
                    str(next_best or "") if next_best else ""
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
) -> MainScheduleResult:
    """
    Build and persist an SHFE-style main contract schedule for a given variety.

    Args:
        var: Variety code (e.g. 'cu', 'au', 'ag')
        start/end: Date range to build (inclusive)
        rule_threshold: Switch threshold (default 1.1)
        data_dir: ghTrader data dir (defaults to config)
    """
    # NOTE: akshare has been removed. Schedule building is now QuestDB-backed.
    from ghtrader.main_schedule_db import build_shfe_main_schedule_from_questdb

    return build_shfe_main_schedule_from_questdb(
        var=var,
        start=start,
        end=end,
        rule_threshold=rule_threshold,
        data_dir=data_dir,
        lake_version="v2",
        exchange="SHFE",
    )

