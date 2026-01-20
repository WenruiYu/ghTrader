"""
FactorEngine: registry-based feature computation with incremental updates.

Supports two modes:
- Offline batch: compute full factor matrix from QuestDB ticks (canonical)
- Online RT: compute a thin subset for real-time signals (using ring buffers)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
import os
from pathlib import Path
import time
from typing import Any, Callable

import numpy as np
import pandas as pd
import structlog

from ghtrader.data.ticks_schema import DatasetVersion, TicksKind, row_hash_from_ticks_df

log = structlog.get_logger()


from ghtrader.util.hash import hash_csv as _hash_csv, stable_hash_df as _stable_hash_df


# ---------------------------------------------------------------------------
# Factor base class and registry
# ---------------------------------------------------------------------------

class Factor(ABC):
    """
    Base class for all factors.

    Attributes:
        name: Factor name (must be unique)
        input_columns: Tick columns this factor depends on (for lineage tracking)
        lookback_ticks: Number of historical ticks required (0 = point-in-time)
        ttl_seconds: Time-to-live for online serving (default: 1800 = 30 min)
        output_dtype: Output data type (default: 'float64')
    """

    name: str
    input_columns: tuple[str, ...] = ()  # Columns this factor depends on
    lookback_ticks: int = 0  # Lookback window size (0 = no lookback needed)
    ttl_seconds: int = 1800  # Default 30 minutes for online serving
    output_dtype: str = "float64"

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

    def get_metadata(self) -> dict[str, Any]:
        """Get factor metadata for feature registry."""
        return {
            "name": self.name,
            "input_columns": list(self.input_columns),
            "lookback_ticks": self.lookback_ticks,
            "ttl_seconds": self.ttl_seconds,
            "output_dtype": self.output_dtype,
        }


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


def get_factor_metadata(name: str) -> dict[str, Any]:
    """Get factor metadata by name."""
    return get_factor(name).get_metadata()


def list_factors() -> list[str]:
    """List all registered factor names."""
    return list(_FACTOR_REGISTRY.keys())


def list_factors_with_metadata() -> list[dict[str, Any]]:
    """List all registered factors with their metadata."""
    return [get_factor(name).get_metadata() for name in _FACTOR_REGISTRY.keys()]


# ---------------------------------------------------------------------------
# Book shape factors
# ---------------------------------------------------------------------------

@register_factor
class SpreadFactor(Factor):
    """Bid-ask spread (ask1 - bid1)."""

    name = "spread"
    input_columns = ("ask_price1", "bid_price1")
    lookback_ticks = 0
    ttl_seconds = 30  # Microstructure - very short TTL

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
    input_columns = ("bid_price1", "ask_price1")
    lookback_ticks = 0
    ttl_seconds = 30  # Microstructure - very short TTL

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
    input_columns = ("bid_price1", "ask_price1", "bid_volume1", "ask_volume1")
    lookback_ticks = 0
    ttl_seconds = 30  # Microstructure - very short TTL

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
    input_columns = ("bid_volume1", "ask_volume1")
    lookback_ticks = 0
    ttl_seconds = 30  # Microstructure - very short TTL
    
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
    ttl_seconds: int = 300  # 5 minutes default for rolling factors

    @property
    def lookback_ticks(self) -> int:
        """Rolling factors require lookback equal to window_size."""
        return self.window_size

    def init_state(self) -> dict[str, Any]:
        return {"buffer": deque(maxlen=self.window_size)}


@register_factor
class ReturnFactor(RollingFactor):
    """Short-term return: (mid[t] - mid[t-N]) / mid[t-N]."""

    name = "return_10"
    window_size = 10
    input_columns = ("bid_price1", "ask_price1")
    ttl_seconds = 300  # 5 minutes

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
    input_columns = ("bid_price1", "ask_price1")
    ttl_seconds = 300  # 5 minutes

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
    input_columns = ("volume",)
    ttl_seconds = 300  # 5 minutes

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

    # Parquet-based feature building has been removed (QuestDB-only system).

    def build_features_for_symbol(
        self,
        *,
        symbol: str,
        data_dir: Path,
        ticks_kind: TicksKind = "raw",
        overwrite: bool = False,
        dataset_version: DatasetVersion = "v2",
    ) -> dict[str, Any]:
        """
        Build features from canonical ticks and store them in QuestDB (`ghtrader_features_v2`).

        This is the canonical QuestDB-first workflow for feature building.

        - Primary storage: QuestDB (QuestDB-first).
        """
        from ghtrader.config import (
            get_questdb_host,
            get_questdb_ilp_port,
            get_questdb_pg_dbname,
            get_questdb_pg_password,
            get_questdb_pg_port,
            get_questdb_pg_user,
        )
        from ghtrader.questdb.features_labels import (
            FEATURES_TABLE_V2,
            ensure_features_tables,
            insert_feature_build,
        )
        from ghtrader.questdb.client import make_questdb_query_config_from_env
        from ghtrader.questdb.queries import (
            fetch_ticks_for_symbol_day,
            list_trading_days_for_symbol,
            query_symbol_day_bounds,
        )
        from ghtrader.questdb.serving_db import ServingDBConfig, make_serving_backend

        dv = str(dataset_version).lower().strip() or "v2"
        tk = str(ticks_kind).lower().strip() or "main_l5"
        if tk != "main_l5":
            raise ValueError("ticks_kind raw is deferred (Phase-1/2)")

        # Derived ticks require schedule provenance to prevent roll-boundary leakage.
        underlying_by_date: dict[date, str] = {}
        segment_id_by_date: dict[date, int] = {}
        schedule_hash: str | None = None

        cfg = make_questdb_query_config_from_env()
        backend = make_serving_backend(
            ServingDBConfig(
                backend="questdb",
                host=str(get_questdb_host()),
                questdb_ilp_port=int(get_questdb_ilp_port()),
                questdb_pg_port=int(get_questdb_pg_port()),
                questdb_pg_user=str(get_questdb_pg_user()),
                questdb_pg_password=str(get_questdb_pg_password()),
                questdb_pg_dbname=str(get_questdb_pg_dbname()),
            )
        )

        ticks_table = "ghtrader_ticks_main_l5_v2"
        bounds = query_symbol_day_bounds(
            cfg=cfg,
            table=ticks_table,
            symbols=[symbol],
            dataset_version=dv,
            ticks_kind=tk,
            l5_only=False,
        )
        b = bounds.get(symbol) or {}
        d0s = str(b.get("first_day") or "").strip()
        d1s = str(b.get("last_day") or "").strip()
        if not d0s or not d1s:
            raise ValueError(f"No tick data found for {symbol} (ticks_kind={ticks_kind}, dataset_version={dataset_version}) in QuestDB")
        start_d = date.fromisoformat(d0s)
        end_d = date.fromisoformat(d1s)
        dates = list_trading_days_for_symbol(
            cfg=cfg,
            table=ticks_table,
            symbol=str(symbol),
            start_day=start_d,
            end_day=end_d,
            dataset_version=dv,
            ticks_kind=tk,
        )
        if not dates:
            raise ValueError(f"No tick data found for {symbol} (ticks_kind={ticks_kind}, dataset_version={dataset_version}) in QuestDB")
        if tk == "main_l5" and schedule_hash is None:
            # Best-effort: pull schedule_hash from derived tick rows (preferred over recomputing).
            try:
                from ghtrader.questdb.queries import _connect

                with _connect(cfg, connect_timeout_s=2) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            f"SELECT schedule_hash FROM {ticks_table} "
                            "WHERE symbol=%s AND ticks_kind=%s AND dataset_version=%s LIMIT 1",
                            [str(symbol), str(tk), str(dv)],
                        )
                        r = cur.fetchone()
                if r and r[0] is not None:
                    schedule_hash = str(r[0])
            except Exception:
                schedule_hash = None
        if tk == "main_l5" and schedule_hash is None and dates:
            # Fallback: read a single row from the first available day.
            try:
                df0 = fetch_ticks_for_symbol_day(
                    cfg=cfg,
                    table=ticks_table,
                    symbol=symbol,
                    trading_day=dates[0].isoformat(),
                    dataset_version=dv,
                    ticks_kind=tk,
                    limit=1,
                    order="asc",
                    include_provenance=True,
                    connect_timeout_s=2,
                )
                if not df0.empty and "schedule_hash" in df0.columns:
                    v0 = df0["schedule_hash"].iloc[0]
                    if not pd.isna(v0):
                        schedule_hash = str(v0)
            except Exception:
                schedule_hash = None

        build_id = _hash_csv([symbol, tk, dv, str(schedule_hash or ""), ",".join(self.enabled_factors)])
        factors_hash = _hash_csv(list(self.enabled_factors))
        schema_hash = _hash_csv(["symbol", "datetime_ns", "trading_day", "row_hash"] + list(self.enabled_factors))
        # QuestDB ILP expects tz-naive timestamps.
        build_ts = datetime.now(timezone.utc).replace(tzinfo=None)

        ensure_features_tables(cfg=cfg, factor_columns=list(self.enabled_factors), connect_timeout_s=2)

        # Conservative lookback to preserve rolling continuity across day boundaries.
        lookback = 0
        for f in self._instances.values():
            lookback = max(lookback, int(getattr(f, "window_size", 0) or 0))

        idx_map = {d: i for i, d in enumerate(dates)}
        rows_total = 0
        days_done = 0
        days_total = int(len(dates))
        started_at = time.time()
        last_progress_ts = started_at
        try:
            progress_every_s = float(os.environ.get("GHTRADER_BUILD_PROGRESS_EVERY_S", "30") or "30")
        except Exception:
            progress_every_s = 30.0
        progress_every_s = max(5.0, float(progress_every_s))
        try:
            progress_every_n = int(os.environ.get("GHTRADER_BUILD_PROGRESS_EVERY_N", "5") or "5")
        except Exception:
            progress_every_n = 5
        progress_every_n = max(1, int(progress_every_n))
        log.info(
            "features.build_start",
            symbol=str(symbol),
            ticks_kind=str(tk),
            dataset_version=str(dv),
            days_total=int(days_total),
            factors=int(len(self.enabled_factors)),
        )

        for dt in dates:
            # Fetch day ticks (QuestDB-first).
            df_day = fetch_ticks_for_symbol_day(
                cfg=cfg,
                table=ticks_table,
                symbol=symbol,
                trading_day=dt.isoformat(),
                dataset_version=dv,
                ticks_kind=tk,
                limit=None,
                order="asc",
                include_provenance=(tk == "main_l5"),
                connect_timeout_s=2,
            )
            if df_day.empty:
                continue
            df_day = df_day.copy()
            if "row_hash" not in df_day.columns or pd.to_numeric(df_day["row_hash"], errors="coerce").isna().all():
                df_day["row_hash"] = row_hash_from_ticks_df(df_day)
            if tk == "main_l5":
                try:
                    if "underlying_contract" in df_day.columns and not df_day["underlying_contract"].empty:
                        u0 = df_day["underlying_contract"].iloc[0]
                        underlying_by_date[dt] = "" if pd.isna(u0) else str(u0)
                except Exception:
                    pass
                try:
                    if "segment_id" in df_day.columns and not df_day["segment_id"].empty:
                        s0 = pd.to_numeric(df_day["segment_id"].iloc[0], errors="coerce")
                        segment_id_by_date[dt] = int(0 if pd.isna(s0) else s0)
                except Exception:
                    segment_id_by_date[dt] = 0
                try:
                    if schedule_hash is None and "schedule_hash" in df_day.columns and not df_day["schedule_hash"].empty:
                        sh0 = df_day["schedule_hash"].iloc[0]
                        if not pd.isna(sh0):
                            schedule_hash = str(sh0)
                except Exception:
                    pass

            # Build tail from previous available tick day (lookback only).
            tail_ticks: pd.DataFrame | None = None
            if lookback > 0:
                i = idx_map.get(dt)
                if i is not None and i - 1 >= 0:
                    prev_dt = dates[i - 1]
                    allow = True
                    if tk == "main_l5":
                        seg_prev = segment_id_by_date.get(prev_dt)
                        seg_cur = segment_id_by_date.get(dt)
                        allow = (seg_prev is not None and seg_cur is not None and seg_prev == seg_cur)
                        if not allow:
                            u_prev = underlying_by_date.get(prev_dt)
                            u_cur = underlying_by_date.get(dt)
                            allow = bool(u_prev and u_cur and u_prev == u_cur)
                    if allow:
                        df_prev_tail = fetch_ticks_for_symbol_day(
                            cfg=cfg,
                            table=ticks_table,
                            symbol=symbol,
                            trading_day=prev_dt.isoformat(),
                            dataset_version=dv,
                            ticks_kind=tk,
                            limit=int(lookback),
                            order="desc",
                            include_provenance=False,
                            connect_timeout_s=2,
                        )
                        if not df_prev_tail.empty:
                            tail_ticks = df_prev_tail.iloc[::-1].reset_index(drop=True)
                    # If no tail from QuestDB, tail_ticks remains None

            if tail_ticks is not None and not tail_ticks.empty:
                df_in = pd.concat([tail_ticks, df_day], ignore_index=True)
                tail_len = len(tail_ticks)
            else:
                df_in = df_day
                tail_len = 0

            feats_in = self.compute_batch(df_in)
            feats_day = feats_in.iloc[tail_len:].reset_index(drop=True)

            datetime_ns = pd.to_numeric(df_day["datetime"], errors="coerce").fillna(0).astype("int64").values
            row_hash = pd.to_numeric(df_day.get("row_hash"), errors="coerce").fillna(0).astype("int64").values

            out = pd.DataFrame(
                {
                    "symbol": str(symbol),
                    "ts": pd.to_datetime(datetime_ns, unit="ns"),
                    "datetime_ns": datetime_ns,
                    "trading_day": str(dt.isoformat()),
                    "row_hash": row_hash,
                    "ticks_kind": str(tk),
                    "dataset_version": str(dv),
                    "build_id": str(build_id),
                    "build_ts": build_ts,
                    "schedule_hash": str(schedule_hash or ""),
                }
            )
            if tk == "main_l5":
                out["underlying_contract"] = str(underlying_by_date.get(dt) or "")
                out["segment_id"] = int(segment_id_by_date.get(dt, 0) or 0)
            else:
                out["underlying_contract"] = ""
                out["segment_id"] = 0

            # Wide factor columns
            for c in self.enabled_factors:
                out[c] = pd.to_numeric(feats_day.get(c), errors="coerce")

            # Ensure SYMBOL-like columns are sent as categories for QuestDB.
            for c in ["symbol", "trading_day", "ticks_kind", "dataset_version", "build_id", "schedule_hash", "underlying_contract"]:
                if c in out.columns:
                    try:
                        out[c] = out[c].astype("category")
                    except Exception:
                        pass

            # Ingest to QuestDB.
            backend.ingest_df(table=FEATURES_TABLE_V2, df=out)

            rows_total += int(len(out))
            days_done += 1
            if (
                days_done == 1
                or days_done == days_total
                or days_done % progress_every_n == 0
                or (time.time() - last_progress_ts) >= progress_every_s
            ):
                log.info(
                    "features.build_progress",
                    symbol=str(symbol),
                    trading_day=str(dt.isoformat()),
                    days_done=int(days_done),
                    days_total=int(days_total),
                    rows_total=int(rows_total),
                    last_rows=int(len(out)),
                    elapsed_s=int(time.time() - started_at),
                )
                last_progress_ts = time.time()
            else:
                log.debug(
                    "features.build_day",
                    symbol=str(symbol),
                    trading_day=str(dt.isoformat()),
                    rows=int(len(out)),
                    days_done=int(days_done),
                    days_total=int(days_total),
                )

        insert_feature_build(
            cfg=cfg,
            symbol=str(symbol),
            ticks_kind=str(tk),
            dataset_version=str(dv),
            build_id=str(build_id),
            factors_hash=str(factors_hash),
            factors=",".join(list(self.enabled_factors)),
            schema_hash=str(schema_hash),
            schedule_hash=str(schedule_hash or ""),
            rows_total=int(rows_total),
            first_day=(dates[0].isoformat() if dates else ""),
            last_day=(dates[-1].isoformat() if dates else ""),
            connect_timeout_s=2,
        )
        log.info(
            "features.build_done",
            symbol=str(symbol),
            ticks_kind=str(tk),
            dataset_version=str(dv),
            rows_total=int(rows_total),
            days=int(days_done),
            elapsed_s=int(time.time() - started_at),
        )

        return {
            "ok": True,
            "symbol": str(symbol),
            "ticks_kind": str(tk),
            "dataset_version": str(dv),
            "build_id": str(build_id),
            "rows_total": int(rows_total),
            "days": int(days_done),
            "schedule_hash": str(schedule_hash or ""),
        }


def read_features_for_symbol(data_dir: Path, symbol: str) -> pd.DataFrame:
    """Read features for a symbol from QuestDB (canonical source)."""
    from ghtrader.questdb.client import connect_pg as _connect, make_questdb_query_config_from_env
    from ghtrader.questdb.features_labels import FEATURES_TABLE_V2, get_latest_feature_build

    _ = data_dir  # unused; QuestDB is the canonical source

    dv = "v2"
    # Best-effort: infer ticks_kind from symbol (continuous symbols must be built on main_l5).
    pref = "main_l5" if str(symbol).startswith("KQ.m@") else "raw"
    alt = "raw" if pref == "main_l5" else "main_l5"
    cfg = make_questdb_query_config_from_env()
    b_pref = get_latest_feature_build(cfg=cfg, symbol=symbol, ticks_kind=pref, dataset_version=dv)
    b_alt = get_latest_feature_build(cfg=cfg, symbol=symbol, ticks_kind=alt, dataset_version=dv)
    b = b_pref or b_alt
    if not b:
        raise FileNotFoundError(f"Features not found for {symbol} in QuestDB")

    tk = pref if b_pref else alt
    build_id = str(b.get("build_id") or "").strip()
    if not build_id:
        raise FileNotFoundError(f"QuestDB features build not found for {symbol}")

    factors_s = str(b.get("factors") or "").strip()
    factor_cols = [c.strip() for c in factors_s.split(",") if c.strip()] if factors_s else []
    if not factor_cols:
        factor_cols = list(DEFAULT_FACTORS)

    cols = ["datetime_ns AS datetime", "underlying_contract", "segment_id"] + factor_cols
    sql = (
        f"SELECT {', '.join(cols)} FROM {FEATURES_TABLE_V2} "
        "WHERE symbol=%s AND ticks_kind=%s AND dataset_version=%s AND build_id=%s "
        "ORDER BY datetime ASC"
    )
    with _connect(cfg, connect_timeout_s=2) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [str(symbol), str(tk), str(dv), str(build_id)])
            rows = cur.fetchall()
    if not rows:
        raise FileNotFoundError(f"No QuestDB features rows for {symbol}")

    df = pd.DataFrame(rows, columns=["datetime", "underlying_contract", "segment_id"] + factor_cols)
    df.insert(0, "symbol", str(symbol))
    df["datetime"] = pd.to_numeric(df["datetime"], errors="coerce").fillna(0).astype("int64")
    if "segment_id" in df.columns:
        df["segment_id"] = pd.to_numeric(df["segment_id"], errors="coerce").fillna(0).astype("int64")
    return df.sort_values("datetime").reset_index(drop=True)


def read_features_manifest(data_dir: Path, symbol: str) -> dict[str, Any]:
    """
    Read the features manifest for a symbol (if present).

    This is the canonical source of truth for which factor columns are model inputs
    (see `enabled_factors`).
    """
    _ = data_dir  # unused; QuestDB is the canonical source

    # QuestDB-first: read the most recent build record for this symbol.
    try:
        from ghtrader.questdb.client import make_questdb_query_config_from_env
        from ghtrader.questdb.features_labels import FEATURES_TABLE_V2, FEATURE_BUILDS_TABLE_V2, get_latest_feature_build

        dv = "v2"
        pref = "main_l5" if str(symbol).startswith("KQ.m@") else "raw"
        alt = "raw" if pref == "main_l5" else "main_l5"
        cfg = make_questdb_query_config_from_env()
        b_pref = get_latest_feature_build(cfg=cfg, symbol=symbol, ticks_kind=pref, dataset_version=dv)
        b_alt = get_latest_feature_build(cfg=cfg, symbol=symbol, ticks_kind=alt, dataset_version=dv)
        b = b_pref or b_alt
        if not b:
            return {}
        tk = pref if b_pref else alt

        factors_s = str(b.get("factors") or "").strip()
        enabled = [c.strip() for c in factors_s.split(",") if c.strip()] if factors_s else []
        if not enabled:
            enabled = list(DEFAULT_FACTORS)

        m: dict[str, Any] = {
            "created_at": str(b.get("ts") or ""),
            "dataset": "features",
            "symbol": str(symbol),
            "ticks_kind": str(tk),
            "dataset_version": str(dv),
            "enabled_factors": enabled,
            "schema_hash": str(b.get("schema_hash") or ""),
            "rows_total": b.get("rows_total"),
            "questdb": {
                "table": FEATURES_TABLE_V2,
                "builds_table": FEATURE_BUILDS_TABLE_V2,
                "build_id": str(b.get("build_id") or ""),
            },
        }
        if str(tk) == "main_l5":
            m["schedule"] = {
                "hash": str(b.get("schedule_hash") or ""),
                "l5_start_date": str(b.get("first_day") or ""),
                "end_date": str(b.get("last_day") or ""),
            }
        return m
    except Exception:
        return {}
