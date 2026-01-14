"""
FactorEngine: registry-based feature computation with incremental updates.

Supports two modes:
- Offline batch: compute full factor matrix from Parquet lake
- Online RT: compute a thin subset for real-time signals (using ring buffers)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Callable
import shutil
import uuid

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from ghtrader.lake import LakeVersion, TicksLake, list_available_dates, list_available_dates_in_lake, read_ticks_for_symbol, ticks_symbol_dir

log = structlog.get_logger()

def _stable_hash_df(df: pd.DataFrame) -> str:
    import hashlib

    payload = df.to_csv(index=False).encode()
    return hashlib.sha256(payload).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Factor base class and registry
# ---------------------------------------------------------------------------

class Factor(ABC):
    """Base class for all factors."""
    
    name: str
    
    @abstractmethod
    def compute_batch(self, df: pd.DataFrame) -> pd.Series:
        """Compute factor for entire DataFrame (offline mode)."""
        pass
    
    @abstractmethod
    def compute_incremental(self, state: dict[str, Any], tick: dict[str, Any]) -> float:
        """Compute factor incrementally (online mode). Returns NaN if not enough data."""
        pass
    
    @abstractmethod
    def init_state(self) -> dict[str, Any]:
        """Initialize state for incremental computation."""
        pass


# Global factor registry
_FACTOR_REGISTRY: dict[str, type[Factor]] = {}


def register_factor(cls: type[Factor]) -> type[Factor]:
    """Decorator to register a factor class."""
    _FACTOR_REGISTRY[cls.name] = cls
    return cls


def get_factor(name: str) -> Factor:
    """Get a factor instance by name."""
    if name not in _FACTOR_REGISTRY:
        raise ValueError(f"Unknown factor: {name}")
    return _FACTOR_REGISTRY[name]()


def list_factors() -> list[str]:
    """List all registered factor names."""
    return list(_FACTOR_REGISTRY.keys())


# ---------------------------------------------------------------------------
# Book shape factors
# ---------------------------------------------------------------------------

@register_factor
class SpreadFactor(Factor):
    """Bid-ask spread (ask1 - bid1)."""
    
    name = "spread"
    
    def compute_batch(self, df: pd.DataFrame) -> pd.Series:
        return df["ask_price1"] - df["bid_price1"]
    
    def compute_incremental(self, state: dict[str, Any], tick: dict[str, Any]) -> float:
        return tick["ask_price1"] - tick["bid_price1"]
    
    def init_state(self) -> dict[str, Any]:
        return {}


@register_factor
class MidPriceFactor(Factor):
    """Mid price (bid1 + ask1) / 2."""
    
    name = "mid"
    
    def compute_batch(self, df: pd.DataFrame) -> pd.Series:
        return (df["bid_price1"] + df["ask_price1"]) / 2
    
    def compute_incremental(self, state: dict[str, Any], tick: dict[str, Any]) -> float:
        return (tick["bid_price1"] + tick["ask_price1"]) / 2
    
    def init_state(self) -> dict[str, Any]:
        return {}


@register_factor
class MicropriceFactor(Factor):
    """Microprice (volume-weighted mid): (bid1*ask_vol1 + ask1*bid_vol1) / (bid_vol1 + ask_vol1)."""
    
    name = "microprice"
    
    def compute_batch(self, df: pd.DataFrame) -> pd.Series:
        bid_vol = df["bid_volume1"]
        ask_vol = df["ask_volume1"]
        total_vol = bid_vol + ask_vol
        return np.where(
            total_vol > 0,
            (df["bid_price1"] * ask_vol + df["ask_price1"] * bid_vol) / total_vol,
            (df["bid_price1"] + df["ask_price1"]) / 2,
        )
    
    def compute_incremental(self, state: dict[str, Any], tick: dict[str, Any]) -> float:
        bid_vol = tick["bid_volume1"]
        ask_vol = tick["ask_volume1"]
        total = bid_vol + ask_vol
        if total > 0:
            return (tick["bid_price1"] * ask_vol + tick["ask_price1"] * bid_vol) / total
        return (tick["bid_price1"] + tick["ask_price1"]) / 2
    
    def init_state(self) -> dict[str, Any]:
        return {}


@register_factor
class ImbalanceFactor(Factor):
    """Order book imbalance at level 1: (bid_vol1 - ask_vol1) / (bid_vol1 + ask_vol1)."""
    
    name = "imbalance_1"
    
    def compute_batch(self, df: pd.DataFrame) -> pd.Series:
        bid_vol = df["bid_volume1"]
        ask_vol = df["ask_volume1"]
        total = bid_vol + ask_vol
        return np.where(total > 0, (bid_vol - ask_vol) / total, 0)
    
    def compute_incremental(self, state: dict[str, Any], tick: dict[str, Any]) -> float:
        bid_vol = tick["bid_volume1"]
        ask_vol = tick["ask_volume1"]
        total = bid_vol + ask_vol
        return (bid_vol - ask_vol) / total if total > 0 else 0.0
    
    def init_state(self) -> dict[str, Any]:
        return {}


@register_factor
class TotalImbalanceFactor(Factor):
    """Total order book imbalance across all 5 levels."""
    
    name = "imbalance_total"
    
    def compute_batch(self, df: pd.DataFrame) -> pd.Series:
        bid_total = sum(df[f"bid_volume{i}"] for i in range(1, 6))
        ask_total = sum(df[f"ask_volume{i}"] for i in range(1, 6))
        total = bid_total + ask_total
        return np.where(total > 0, (bid_total - ask_total) / total, 0)
    
    def compute_incremental(self, state: dict[str, Any], tick: dict[str, Any]) -> float:
        bid_total = sum(tick.get(f"bid_volume{i}", 0) for i in range(1, 6))
        ask_total = sum(tick.get(f"ask_volume{i}", 0) for i in range(1, 6))
        total = bid_total + ask_total
        return (bid_total - ask_total) / total if total > 0 else 0.0
    
    def init_state(self) -> dict[str, Any]:
        return {}


@register_factor
class DepthWeightedPriceFactor(Factor):
    """Depth-weighted average price across 5 levels."""
    
    name = "depth_weighted_price"
    
    def compute_batch(self, df: pd.DataFrame) -> pd.Series:
        bid_sum = sum(df[f"bid_price{i}"] * df[f"bid_volume{i}"] for i in range(1, 6))
        ask_sum = sum(df[f"ask_price{i}"] * df[f"ask_volume{i}"] for i in range(1, 6))
        vol_sum = sum(df[f"bid_volume{i}"] + df[f"ask_volume{i}"] for i in range(1, 6))
        return np.where(vol_sum > 0, (bid_sum + ask_sum) / vol_sum, np.nan)
    
    def compute_incremental(self, state: dict[str, Any], tick: dict[str, Any]) -> float:
        bid_sum = sum(tick.get(f"bid_price{i}", 0) * tick.get(f"bid_volume{i}", 0) for i in range(1, 6))
        ask_sum = sum(tick.get(f"ask_price{i}", 0) * tick.get(f"ask_volume{i}", 0) for i in range(1, 6))
        vol_sum = sum(tick.get(f"bid_volume{i}", 0) + tick.get(f"ask_volume{i}", 0) for i in range(1, 6))
        return (bid_sum + ask_sum) / vol_sum if vol_sum > 0 else float("nan")
    
    def init_state(self) -> dict[str, Any]:
        return {}


# ---------------------------------------------------------------------------
# Dynamics factors (require lookback)
# ---------------------------------------------------------------------------

class RollingFactor(Factor):
    """Base class for factors that need rolling windows."""
    
    window_size: int = 10
    
    def init_state(self) -> dict[str, Any]:
        return {"buffer": deque(maxlen=self.window_size)}


@register_factor
class ReturnFactor(RollingFactor):
    """Short-term return: (mid[t] - mid[t-N]) / mid[t-N]."""
    
    name = "return_10"
    window_size = 10
    
    def compute_batch(self, df: pd.DataFrame) -> pd.Series:
        mid = (df["bid_price1"] + df["ask_price1"]) / 2
        # Avoid deprecated default fill_method='pad' (keep NaNs as NaNs).
        return mid.pct_change(periods=self.window_size, fill_method=None)
    
    def compute_incremental(self, state: dict[str, Any], tick: dict[str, Any]) -> float:
        mid = (tick["bid_price1"] + tick["ask_price1"]) / 2
        buf = state["buffer"]
        buf.append(mid)
        if len(buf) < self.window_size:
            return float("nan")
        old_mid = buf[0]
        return (mid - old_mid) / old_mid if old_mid != 0 else 0.0


@register_factor
class VolatilityFactor(RollingFactor):
    """Rolling volatility of mid price returns."""
    
    name = "volatility_20"
    window_size = 20
    
    def compute_batch(self, df: pd.DataFrame) -> pd.Series:
        mid = (df["bid_price1"] + df["ask_price1"]) / 2
        # Avoid deprecated default fill_method='pad' (keep NaNs as NaNs).
        returns = mid.pct_change(fill_method=None)
        return returns.rolling(window=self.window_size).std()
    
    def compute_incremental(self, state: dict[str, Any], tick: dict[str, Any]) -> float:
        mid = (tick["bid_price1"] + tick["ask_price1"]) / 2
        buf = state["buffer"]
        
        if "prev_mid" in state:
            ret = (mid - state["prev_mid"]) / state["prev_mid"] if state["prev_mid"] != 0 else 0
            buf.append(ret)
        state["prev_mid"] = mid
        
        if len(buf) < self.window_size:
            return float("nan")
        return float(np.std(list(buf)))


@register_factor
class VolumeDeltaFactor(RollingFactor):
    """Change in cumulative volume over window."""
    
    name = "volume_delta_10"
    window_size = 10
    
    def compute_batch(self, df: pd.DataFrame) -> pd.Series:
        return df["volume"].diff(periods=self.window_size)
    
    def compute_incremental(self, state: dict[str, Any], tick: dict[str, Any]) -> float:
        vol = tick["volume"]
        buf = state["buffer"]
        buf.append(vol)
        if len(buf) < self.window_size:
            return float("nan")
        return vol - buf[0]


@register_factor
class OIDeltaFactor(RollingFactor):
    """Change in open interest over window."""
    
    name = "oi_delta_10"
    window_size = 10
    
    def compute_batch(self, df: pd.DataFrame) -> pd.Series:
        return df["open_interest"].diff(periods=self.window_size)
    
    def compute_incremental(self, state: dict[str, Any], tick: dict[str, Any]) -> float:
        oi = tick["open_interest"]
        buf = state["buffer"]
        buf.append(oi)
        if len(buf) < self.window_size:
            return float("nan")
        return oi - buf[0]


@register_factor
class BookSlopeFactor(Factor):
    """Order book slope: measures depth distribution across levels."""
    
    name = "book_slope"
    
    def _compute_slope(self, bid_vols: list[float], ask_vols: list[float]) -> float:
        # Simple slope: weighted sum where deeper levels have higher weight
        weights = [1, 2, 3, 4, 5]
        bid_weighted = sum(v * w for v, w in zip(bid_vols, weights))
        ask_weighted = sum(v * w for v, w in zip(ask_vols, weights))
        total = bid_weighted + ask_weighted
        return (bid_weighted - ask_weighted) / total if total > 0 else 0.0
    
    def compute_batch(self, df: pd.DataFrame) -> pd.Series:
        result = []
        for _, row in df.iterrows():
            bid_vols = [row[f"bid_volume{i}"] for i in range(1, 6)]
            ask_vols = [row[f"ask_volume{i}"] for i in range(1, 6)]
            result.append(self._compute_slope(bid_vols, ask_vols))
        return pd.Series(result, index=df.index)
    
    def compute_incremental(self, state: dict[str, Any], tick: dict[str, Any]) -> float:
        bid_vols = [tick.get(f"bid_volume{i}", 0) for i in range(1, 6)]
        ask_vols = [tick.get(f"ask_volume{i}", 0) for i in range(1, 6)]
        return self._compute_slope(bid_vols, ask_vols)
    
    def init_state(self) -> dict[str, Any]:
        return {}


# ---------------------------------------------------------------------------
# FactorEngine
# ---------------------------------------------------------------------------

# Default factors to compute
DEFAULT_FACTORS = [
    "spread",
    "mid",
    "microprice",
    "imbalance_1",
    "imbalance_total",
    "depth_weighted_price",
    "return_10",
    "volatility_20",
    "volume_delta_10",
    "oi_delta_10",
    "book_slope",
]


@dataclass
class FactorEngine:
    """
    Engine for computing factors in batch or incrementally.
    
    Supports a registry of factors that can be enabled/disabled by config.
    """
    
    enabled_factors: list[str] = field(default_factory=lambda: DEFAULT_FACTORS.copy())
    _instances: dict[str, Factor] = field(default_factory=dict, init=False)
    _states: dict[str, dict[str, Any]] = field(default_factory=dict, init=False)
    
    def __post_init__(self) -> None:
        # Instantiate enabled factors
        for name in self.enabled_factors:
            self._instances[name] = get_factor(name)
            self._states[name] = self._instances[name].init_state()
    
    def compute_batch(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute all enabled factors for a DataFrame."""
        result = pd.DataFrame(index=df.index)
        
        for name, factor in self._instances.items():
            try:
                result[name] = factor.compute_batch(df)
            except Exception as e:
                log.warning("factor.compute_failed", factor=name, error=str(e))
                result[name] = np.nan
        
        return result
    
    def compute_incremental(self, tick: dict[str, Any]) -> dict[str, float]:
        """Compute all enabled factors for a single tick (online mode)."""
        result: dict[str, float] = {}
        
        for name, factor in self._instances.items():
            try:
                result[name] = factor.compute_incremental(self._states[name], tick)
            except Exception as e:
                log.warning("factor.incremental_failed", factor=name, error=str(e))
                result[name] = float("nan")
        
        return result
    
    def reset_states(self) -> None:
        """Reset all incremental states."""
        for name in self._instances:
            self._states[name] = self._instances[name].init_state()
    
    def build_features_for_symbol(
        self,
        symbol: str,
        data_dir: Path,
        *,
        ticks_lake: TicksLake = "raw",
        overwrite: bool = False,
        lake_version: LakeVersion = "v1",
    ) -> Path:
        """
        Build features for a symbol from Parquet lake.
        
        Args:
            symbol: Instrument code
            data_dir: Data directory root
        
        Returns:
            Path to output Parquet file
        """
        log.info("features.build_start", symbol=symbol, factors=self.enabled_factors)

        # Discover available tick partitions
        dates = list_available_dates_in_lake(data_dir, symbol, ticks_lake=ticks_lake, lake_version=lake_version)
        if not dates:
            raise ValueError(f"No tick data found for {symbol}")

        # Output root
        out_root = data_dir / "features" / f"symbol={symbol}"
        if overwrite and out_root.exists():
            shutil.rmtree(out_root)
        out_root.mkdir(parents=True, exist_ok=True)

        # Determine a conservative lookback to preserve rolling continuity across day boundaries.
        # (Factors without window_size are treated as 0 lookback.)
        lookback = 0
        for f in self._instances.values():
            lookback = max(lookback, int(getattr(f, "window_size", 0) or 0))

        # Canonical schema for partitions
        schema_fields: list[pa.Field] = [pa.field("symbol", pa.string()), pa.field("datetime", pa.int64())]
        if ticks_lake == "main_l5":
            schema_fields += [pa.field("underlying_contract", pa.string()), pa.field("segment_id", pa.int64())]
        schema_fields += [pa.field(name, pa.float64()) for name in self.enabled_factors]
        schema = pa.schema(schema_fields)

        # For derived ticks, we need schedule provenance + roll-boundary metadata.
        underlying_by_date: dict[date, str] = {}
        segment_id_by_date: dict[date, int] = {}
        schedule_prov: dict[str, Any] = {}
        if ticks_lake == "main_l5":
            schedule_path = (
                ticks_symbol_dir(
                    data_dir,
                    symbol,
                    ticks_lake="main_l5",
                    lake_version=lake_version,
                )
                / "schedule.parquet"
            )
            if not schedule_path.exists():
                raise ValueError(
                    f"Missing schedule.parquet for derived ticks: {schedule_path}. "
                    "Run `ghtrader main-l5` (or `main-depth`) first."
                )
            try:
                sched = pd.read_parquet(schedule_path)
                if "date" not in sched.columns or "main_contract" not in sched.columns:
                    raise ValueError(f"Unexpected schedule schema: {list(sched.columns)}")
                sched = sched.copy()
                sched["date"] = pd.to_datetime(sched["date"], errors="coerce").dt.date
                sched = sched.dropna(subset=["date", "main_contract"]).sort_values("date").reset_index(drop=True)

                # Ensure segment_id exists for schedule copy (older versions may not include it).
                if "segment_id" not in sched.columns:
                    seg_ids: list[int] = []
                    seg = 0
                    prev: str | None = None
                    for mc in sched["main_contract"].astype(str).tolist():
                        if prev is None:
                            seg = 0
                        elif mc != prev:
                            seg += 1
                        seg_ids.append(int(seg))
                        prev = mc
                    sched["segment_id"] = seg_ids

                for _, r in sched.iterrows():
                    d = r["date"]
                    underlying_by_date[d] = str(r["main_contract"])
                    try:
                        segment_id_by_date[d] = int(r.get("segment_id", 0) or 0)
                    except Exception:
                        segment_id_by_date[d] = 0

                schedule_hash = _stable_hash_df(sched[["date", "main_contract", "segment_id"]])
                d0 = sched["date"].min()
                d1 = sched["date"].max()
                schedule_prov = {
                    "path": str(schedule_path),
                    "hash": str(schedule_hash),
                    "l5_start_date": (d0.isoformat() if hasattr(d0, "isoformat") else str(d0)),
                    "end_date": (d1.isoformat() if hasattr(d1, "isoformat") else str(d1)),
                    "rows": int(len(sched)),
                }
            except Exception as e:
                raise ValueError(f"Failed to load schedule.parquet for derived ticks ({e})") from e

        # Manifest safety gate: do not mix outputs across config/schema changes unless overwrite is explicit.
        if not overwrite:
            manifest_path = out_root / "manifest.json"
            if manifest_path.exists():
                try:
                    import hashlib
                    import json

                    cols_str = ",".join(f"{f.name}:{f.type}" for f in schema)
                    expected_schema_hash = hashlib.sha256(cols_str.encode()).hexdigest()[:16]
                    existing = json.loads(manifest_path.read_text())
                    if str(existing.get("dataset")) != "features":
                        raise ValueError("dataset mismatch")
                    if str(existing.get("symbol")) != str(symbol):
                        raise ValueError("symbol mismatch")
                    if str(existing.get("ticks_lake")) != str(ticks_lake):
                        raise ValueError("ticks_lake mismatch")
                    if str(existing.get("lake_version") or "v1") != str(lake_version):
                        raise ValueError("lake_version mismatch")
                    if list(existing.get("enabled_factors") or []) != list(self.enabled_factors):
                        raise ValueError("enabled_factors mismatch")
                    if str(existing.get("schema_hash") or "") != expected_schema_hash:
                        raise ValueError("schema_hash mismatch")
                    if ticks_lake == "main_l5" and schedule_prov:
                        ex_sched = dict(existing.get("schedule") or {})
                        if str(ex_sched.get("hash") or "") and str(ex_sched.get("hash")) != str(schedule_prov.get("hash") or ""):
                            raise ValueError("schedule_hash mismatch")
                        if str(ex_sched.get("l5_start_date") or "") and str(ex_sched.get("l5_start_date")) != str(schedule_prov.get("l5_start_date") or ""):
                            raise ValueError("l5_start_date mismatch")
                except Exception as e:
                    raise ValueError(
                        f"Existing features manifest does not match requested build ({e}). "
                        f"Refusing to mix outputs. Re-run with overwrite=True to rebuild."
                    )

        # Determine which dates need work (incremental default).
        existing_dates: set[date] = set()
        for ddir in out_root.iterdir():
            if not ddir.is_dir() or not ddir.name.startswith("date="):
                continue
            try:
                dt = date.fromisoformat(ddir.name.split("=", 1)[1])
            except Exception:
                continue
            # consider built if any parquet exists
            if any(ddir.glob("*.parquet")):
                existing_dates.add(dt)

        missing_dates = [d for d in dates if d not in existing_dates]

        dates_to_process: set[date] = set()
        if overwrite:
            dates_to_process = set(dates)
        else:
            # Backfill guard: if a day is missing, also recompute the next available tick day.
            idx = {d: i for i, d in enumerate(dates)}
            for d in missing_dates:
                dates_to_process.add(d)
                j = idx.get(d)
                if j is not None and j + 1 < len(dates):
                    dates_to_process.add(dates[j + 1])

        # (Schedule already loaded above when ticks_lake == "main_l5".)

        # Process only dates that require work.
        idx = {d: i for i, d in enumerate(dates)}
        for dt in sorted(dates_to_process):
            df_day = read_ticks_for_symbol(
                data_dir,
                symbol,
                start_date=dt,
                end_date=dt,
                ticks_lake=ticks_lake,
                lake_version=lake_version,
            )
            if df_day.empty:
                continue

            # Build tail from previous available tick day (lookback only).
            tail_ticks: pd.DataFrame | None = None
            if lookback > 0:
                i = idx.get(dt)
                if i is not None and i - 1 >= 0:
                    prev_dt = dates[i - 1]
                    if ticks_lake == "main_l5":
                        seg_prev = segment_id_by_date.get(prev_dt)
                        seg_cur = segment_id_by_date.get(dt)
                        allow = (seg_prev is not None and seg_cur is not None and seg_prev == seg_cur)
                        if not allow:
                            u_prev = underlying_by_date.get(prev_dt)
                            u_cur = underlying_by_date.get(dt)
                            allow = bool(u_prev and u_cur and u_prev == u_cur)

                        if allow:
                            df_prev = read_ticks_for_symbol(
                                data_dir,
                                symbol,
                                start_date=prev_dt,
                                end_date=prev_dt,
                                ticks_lake=ticks_lake,
                                lake_version=lake_version,
                            )
                            if not df_prev.empty:
                                tail_ticks = df_prev.tail(lookback).copy()
                    else:
                        df_prev = read_ticks_for_symbol(
                            data_dir,
                            symbol,
                            start_date=prev_dt,
                            end_date=prev_dt,
                            ticks_lake=ticks_lake,
                            lake_version=lake_version,
                        )
                        if not df_prev.empty:
                            tail_ticks = df_prev.tail(lookback).copy()

            if tail_ticks is not None and not tail_ticks.empty:
                df_in = pd.concat([tail_ticks, df_day], ignore_index=True)
                tail_len = len(tail_ticks)
            else:
                df_in = df_day
                tail_len = 0

            features_in = self.compute_batch(df_in)
            features_day = features_in.iloc[tail_len:].reset_index(drop=True)

            features_day["datetime"] = df_day["datetime"].values
            features_day["symbol"] = symbol
            if ticks_lake == "main_l5":
                u = underlying_by_date.get(dt) or ""
                seg_id = int(segment_id_by_date.get(dt, 0) or 0)
                features_day["underlying_contract"] = u
                features_day["segment_id"] = int(seg_id)

            cols = ["symbol", "datetime"]
            if ticks_lake == "main_l5":
                cols += ["underlying_contract", "segment_id"]
            cols += self.enabled_factors
            features_day = features_day[cols]

            out_dir = out_root / f"date={dt.isoformat()}"
            out_dir.mkdir(parents=True, exist_ok=True)

            # Deterministic per-date output name; remove any previous parquet(s) for this date.
            for p in out_dir.glob("*.parquet*"):
                try:
                    p.unlink()
                except Exception:
                    pass

            out_path = out_dir / "part.parquet"
            tmp_path = out_dir / "part.parquet.tmp"
            table = pa.Table.from_pandas(features_day, schema=schema, preserve_index=False)
            pq.write_table(table, tmp_path, compression="zstd")
            tmp_path.replace(out_path)
            try:
                from ghtrader.integrity import write_sha256_sidecar

                write_sha256_sidecar(out_path)
            except Exception as e:
                log.warning("features.checksum_failed", path=str(out_path), error=str(e))

        # Rebuild symbol-level manifest from on-disk partitions (covers incremental runs).
        total_rows = 0
        row_counts: dict[str, int] = {}
        files: list[dict[str, Any]] = []
        try:
            import hashlib

            from ghtrader.integrity import now_utc_iso, parquet_num_rows, read_sha256_sidecar, write_json_atomic

            for ddir in sorted([p for p in out_root.iterdir() if p.is_dir() and p.name.startswith("date=")], key=lambda p: p.name):
                dt_s = ddir.name.split("=", 1)[1]
                pq_files = sorted(ddir.glob("*.parquet"))
                if not pq_files:
                    continue
                p = pq_files[0]  # should be part.parquet
                rows = parquet_num_rows(p)
                sha = read_sha256_sidecar(p) or ""
                row_counts[dt_s] = int(rows)
                total_rows += int(rows)
                files.append({"date": dt_s, "file": f"{ddir.name}/{p.name}", "rows": int(rows), "sha256": sha})

            cols_str = ",".join(f"{f.name}:{f.type}" for f in schema)
            schema_hash = hashlib.sha256(cols_str.encode()).hexdigest()[:16]
            manifest = {
                "created_at": now_utc_iso(),
                "dataset": "features",
                "symbol": symbol,
                "ticks_lake": ticks_lake,
                "lake_version": lake_version,
                "enabled_factors": list(self.enabled_factors),
                "schema_hash": schema_hash,
                "rows_total": int(total_rows),
                "row_counts": row_counts,
                "files": files,
            }
            if ticks_lake == "main_l5" and schedule_prov:
                manifest["schedule"] = schedule_prov
            write_json_atomic(out_root / "manifest.json", manifest)
        except Exception as e:
            log.warning("features.manifest_failed", symbol=symbol, error=str(e))

        log.info("features.build_done", symbol=symbol, rows=total_rows, root=str(out_root))
        return out_root


def read_features_for_symbol(data_dir: Path, symbol: str) -> pd.DataFrame:
    """Read features for a symbol."""
    base_dir = data_dir / "features" / f"symbol={symbol}"
    legacy_path = base_dir / "features.parquet"
    if legacy_path.exists():
        legacy_schema = pq.read_schema(legacy_path)
        fields = []
        for f in legacy_schema:
            if f.name == "symbol":
                fields.append(pa.field("symbol", pa.string()))
            else:
                fields.append(f)
        schema = pa.schema(fields)
        return pq.read_table(legacy_path, schema=schema).to_pandas()

    if not base_dir.exists():
        raise FileNotFoundError(f"Features not found for {symbol}: {base_dir}")

    partitions: list[Path] = []
    for date_dir in sorted(base_dir.iterdir()):
        if not date_dir.is_dir() or not date_dir.name.startswith("date="):
            continue
        partitions.extend(sorted(date_dir.glob("*.parquet")))

    if not partitions:
        raise FileNotFoundError(f"No feature partitions found for {symbol}: {base_dir}")

    schema = pq.read_schema(partitions[0])
    tables = [pq.read_table(p, schema=schema) for p in partitions]
    combined = pa.concat_tables(tables)
    df = combined.to_pandas()
    return df.sort_values("datetime").reset_index(drop=True)


def read_features_manifest(data_dir: Path, symbol: str) -> dict[str, Any]:
    """
    Read the features manifest for a symbol (if present).

    This is the canonical source of truth for which factor columns are model inputs
    (see `enabled_factors`).
    """
    p = data_dir / "features" / f"symbol={symbol}" / "manifest.json"
    if not p.exists():
        return {}
    try:
        import json

        return dict(json.loads(p.read_text()))
    except Exception:
        return {}
