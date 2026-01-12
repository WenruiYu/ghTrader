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

from ghtrader.lake import list_available_dates, read_ticks_for_symbol

log = structlog.get_logger()


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
    
    def build_features_for_symbol(self, symbol: str, data_dir: Path) -> Path:
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
        dates = list_available_dates(data_dir, symbol)
        if not dates:
            raise ValueError(f"No tick data found for {symbol}")

        # Overwrite derived features for idempotency
        out_root = data_dir / "features" / f"symbol={symbol}"
        if out_root.exists():
            shutil.rmtree(out_root)
        out_root.mkdir(parents=True, exist_ok=True)

        # Determine a conservative lookback to preserve rolling continuity across day boundaries.
        # (Factors without window_size are treated as 0 lookback.)
        lookback = 0
        for f in self._instances.values():
            lookback = max(lookback, int(getattr(f, "window_size", 0) or 0))

        # Canonical schema for partitions
        schema = pa.schema(
            [("symbol", pa.string()), ("datetime", pa.int64())]
            + [(name, pa.float64()) for name in self.enabled_factors]
        )

        tail_ticks: pd.DataFrame | None = None
        total_rows = 0

        for dt in dates:
            df_day = read_ticks_for_symbol(data_dir, symbol, start_date=dt, end_date=dt)
            if df_day.empty:
                continue

            if tail_ticks is not None and not tail_ticks.empty:
                df_in = pd.concat([tail_ticks, df_day], ignore_index=True)
                tail_len = len(tail_ticks)
            else:
                df_in = df_day
                tail_len = 0

            features_in = self.compute_batch(df_in)
            features_day = features_in.iloc[tail_len:].reset_index(drop=True)

            # Add datetime and symbol for current day rows
            features_day["datetime"] = df_day["datetime"].values
            features_day["symbol"] = symbol

            cols = ["symbol", "datetime"] + self.enabled_factors
            features_day = features_day[cols]

            # Write one partition file per date
            part_id = uuid.uuid4().hex[:8]
            out_dir = out_root / f"date={dt.isoformat()}"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"part-{part_id}.parquet"

            table = pa.Table.from_pandas(features_day, schema=schema, preserve_index=False)
            pq.write_table(table, out_path, compression="zstd")

            total_rows += len(features_day)

            # Update tail buffer for next day
            if lookback > 0:
                tail_ticks = df_in.tail(lookback).copy()
            else:
                tail_ticks = None

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
