from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from ghtrader.config import get_lake_version


def is_continuous_alias(symbol: str) -> bool:
    # ghTrader uses TqSdk continuous main alias naming like: KQ.m@SHFE.cu
    return symbol.startswith("KQ.m@") and "." in symbol


def _extract_variety(symbol: str) -> str:
    # KQ.m@SHFE.cu -> cu
    return symbol.rsplit(".", 1)[1].strip().lower()


def _candidate_schedule_paths(*, symbol: str, data_dir: Path) -> list[Path]:
    var = _extract_variety(symbol)
    # Preferred: roll schedule built by `main-schedule`
    p_rolls = data_dir / "rolls" / "shfe_main_schedule" / f"var={var}" / "schedule.parquet"
    # Fallback: schedule copied into derived main_l5 ticks root by `main-depth`
    p_derived_v1 = data_dir / "lake" / "main_l5" / "ticks" / f"symbol={symbol}" / "schedule.parquet"
    p_derived_v2 = data_dir / "lake_v2" / "main_l5" / "ticks" / f"symbol={symbol}" / "schedule.parquet"

    lv = "v1"
    try:
        lv = get_lake_version()
    except Exception:
        lv = "v1"

    if lv == "v2":
        return [p_rolls, p_derived_v2, p_derived_v1]
    return [p_rolls, p_derived_v1, p_derived_v2]


def resolve_trading_symbol(*, symbol: str, data_dir: Path, trading_day: date | None = None) -> str:
    """
    Resolve a user-facing trading symbol to an actual underlying contract symbol.

    - Specific contracts pass through (e.g. SHFE.cu2602)
    - Continuous aliases (e.g. KQ.m@SHFE.cu) resolve using the persisted roll schedule
    """
    if not is_continuous_alias(symbol):
        return symbol

    if trading_day is None:
        # Best-effort trading-day determination (handles night session via 18:00 boundary).
        # We intentionally avoid full holiday logic here; schedule lookup will use <= and work well enough.
        from datetime import datetime, timezone

        # Ensure vendored tqsdk is importable if installed locally.
        # (Other modules add it to sys.path; keep resolver self-contained.)
        import sys

        tqsdk_path = Path(__file__).parent.parent.parent.parent / "tqsdk-python"
        if tqsdk_path.exists() and str(tqsdk_path) not in sys.path:
            sys.path.insert(0, str(tqsdk_path))

        from tqsdk.datetime import _get_trading_day_from_timestamp, _timestamp_nano_to_datetime

        # Use CST-like trading day boundary (tqsdk uses its own 1990-based origin, but timestamp unit is ns).
        now = datetime.now(timezone.utc).astimezone(timezone.utc)
        now_ns = int(now.timestamp() * 1_000_000_000)
        td_ns = _get_trading_day_from_timestamp(now_ns)
        trading_day = _timestamp_nano_to_datetime(td_ns).date()

    last_err: Exception | None = None
    for p in _candidate_schedule_paths(symbol=symbol, data_dir=data_dir):
        if not p.exists():
            continue
        try:
            df = pd.read_parquet(p)
            if df is None or df.empty:
                continue
            if "date" not in df.columns or "main_contract" not in df.columns:
                continue
            df = df.copy()
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
            df = df.dropna(subset=["date", "main_contract"])
            if df.empty:
                continue
            eligible = df[df["date"] <= trading_day]
            if eligible.empty:
                # If schedule doesn't start yet, pick earliest available (best-effort)
                row = df.sort_values("date").iloc[0]
            else:
                row = eligible.sort_values("date").iloc[-1]
            return str(row["main_contract"])
        except Exception as e:
            last_err = e
            continue

    if last_err:
        raise RuntimeError(f"Failed to resolve continuous symbol {symbol}: {last_err}") from last_err
    raise FileNotFoundError(f"No schedule.parquet found to resolve continuous symbol {symbol} under {data_dir}")

