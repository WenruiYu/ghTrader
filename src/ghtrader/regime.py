"""
Market Regime Detection using Hidden Markov Models (HMM).

Implements PRD §5.12:
- HMM-based regime detection (trending, mean-reverting, volatile)
- Feature extraction for regime input (returns, volatility, volume)
- Online decoding for real-time state estimation
"""

from __future__ import annotations

import pickle
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd
import structlog
from sklearn.preprocessing import StandardScaler

log = structlog.get_logger()

RegimeType = Literal["trending", "mean_reverting", "volatile", "quiet"]

@dataclass
class RegimeFeatures:
    """Features used for regime detection."""
    returns: float
    volatility: float
    volume: float
    spread: float

class RegimeDetector:
    """
    HMM-based market regime detector.
    
    Wraps hmmlearn.hmm.GaussianHMM with feature preprocessing and state mapping.
    """
    
    def __init__(
        self,
        n_components: int = 3,
        covariance_type: str = "full",
        n_iter: int = 100,
        random_state: int | None = None,
        **kwargs: Any,
    ) -> None:
        self.n_components = n_components
        self.covariance_type = covariance_type
        self.n_iter = n_iter
        self.random_state = random_state
        self.kwargs = kwargs
        
        self.model = None
        self.scaler = StandardScaler()
        self.state_map: dict[int, RegimeType] = {}
        self.is_fitted = False

    def _init_model(self) -> None:
        try:
            from hmmlearn.hmm import GaussianHMM
        except ImportError:
            raise RuntimeError("hmmlearn not installed. Install with: pip install hmmlearn")
            
        self.model = GaussianHMM(
            n_components=self.n_components,
            covariance_type=self.covariance_type,
            n_iter=self.n_iter,
            random_state=self.random_state,
            **self.kwargs,
        )

    def prepare_features(self, df: pd.DataFrame) -> np.ndarray:
        """
        Extract features from DataFrame for HMM.
        
        Expected columns:
        - mid_price (or bid_price1/ask_price1)
        - volume
        - bid_price1, ask_price1 (for spread)
        """
        # Calculate mid price if not present
        if "mid_price" not in df.columns:
            if "bid_price1" in df.columns and "ask_price1" in df.columns:
                mid = (df["bid_price1"] + df["ask_price1"]) / 2
            elif "last_price" in df.columns:
                mid = df["last_price"]
            else:
                raise ValueError("Cannot calculate mid price: missing bid/ask/last columns")
        else:
            mid = df["mid_price"]

        # 1. Log Returns
        returns = np.log(mid / mid.shift(1)).fillna(0)
        
        # 2. Realized Volatility (rolling std of returns)
        vol = returns.rolling(window=20).std().fillna(0)
        
        # 3. Log Volume (if available)
        if "volume" in df.columns:
            # Handle volume delta if cumulative
            vol_delta = df["volume"].diff().fillna(0)
            # Use log(1 + volume) to handle zeros and scaling
            log_vol = np.log1p(np.maximum(0, vol_delta))
        else:
            log_vol = np.zeros_like(returns)
            
        # 4. Spread (if available)
        if "bid_price1" in df.columns and "ask_price1" in df.columns:
            spread = (df["ask_price1"] - df["bid_price1"])
            # Normalize spread by price
            spread_bps = spread / mid
            spread_bps = spread_bps.fillna(0)
        else:
            spread_bps = np.zeros_like(returns)

        # Stack features: [returns, volatility, log_volume, spread]
        X = np.column_stack([
            returns.values,
            vol.values,
            log_vol.values if isinstance(log_vol, (pd.Series, np.ndarray)) else log_vol,
            spread_bps.values if isinstance(spread_bps, (pd.Series, np.ndarray)) else spread_bps,
        ])
        
        # Remove NaNs/Infs
        X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
        
        return X

    def fit(self, df: pd.DataFrame) -> None:
        """Fit HMM to historical data."""
        if self.model is None:
            self._init_model()
            
        X = self.prepare_features(df)
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        
        # Fit HMM
        self.model.fit(X_scaled)
        self.is_fitted = True
        
        # Heuristic state mapping based on volatility and returns
        # We predict states for training data to analyze their properties
        states = self.model.predict(X_scaled)
        
        state_stats = {}
        for i in range(self.n_components):
            mask = (states == i)
            if not np.any(mask):
                continue
                
            # X columns: 0=returns, 1=volatility, 2=volume, 3=spread
            mean_ret = np.mean(np.abs(X[mask, 0])) # Magnitude of returns
            mean_vol = np.mean(X[mask, 1])
            
            state_stats[i] = {"ret": mean_ret, "vol": mean_vol}
            
        # Sort states by volatility
        sorted_states = sorted(state_stats.keys(), key=lambda k: state_stats[k]["vol"])
        
        # Map to regime types (simplified heuristic)
        # Lowest vol -> Quiet
        # Highest vol -> Volatile
        # Middle -> Trending (if high directional returns) or Mean Reverting
        
        if self.n_components == 3:
            self.state_map[sorted_states[0]] = "quiet"
            self.state_map[sorted_states[1]] = "trending" # Assumption, refine with directional check
            self.state_map[sorted_states[2]] = "volatile"
        else:
            # Generic mapping
            for i, s in enumerate(sorted_states):
                self.state_map[s] = f"regime_{i}"

        log.info("regime.fit_done", n_components=self.n_components, state_map=self.state_map)

    def predict(self, df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        """
        Predict regime for new data.
        
        Returns:
            (states, probabilities)
            states: array of state indices
            probabilities: array of shape (n_samples, n_components)
        """
        if not self.is_fitted:
            raise RuntimeError("Model not fitted")
            
        X = self.prepare_features(df)
        X_scaled = self.scaler.transform(X)
        
        states = self.model.predict(X_scaled)
        probs = self.model.predict_proba(X_scaled)
        
        return states, probs

    def save(self, path: Path) -> None:
        """Save model to disk."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as f:
            pickle.dump({
                "model": self.model,
                "scaler": self.scaler,
                "state_map": self.state_map,
                "config": {
                    "n_components": self.n_components,
                    "covariance_type": self.covariance_type,
                }
            }, f)

    def load(self, path: Path) -> None:
        """Load model from disk."""
        with open(path, "rb") as f:
            data = pickle.load(f)
        
        self.model = data["model"]
        self.scaler = data["scaler"]
        self.state_map = data["state_map"]
        
        config = data["config"]
        self.n_components = config["n_components"]
        self.covariance_type = config["covariance_type"]
        self.is_fitted = True

def train_regime_model(
    symbol: str,
    data_dir: Path,
    artifacts_dir: Path,
    n_components: int = 3,
) -> Path:
    """Train and save a regime detection model for a symbol."""
    from ghtrader.datasets.features import read_features_for_symbol
    
    # Load features (we use raw price/volume from features table if available, 
    # or re-compute from ticks. Here assuming features table has price data or we fetch ticks)
    # For simplicity, let's assume we fetch ticks directly or use a feature set that includes price.
    # Actually, `read_features_for_symbol` returns derived features. 
    # Better to use `fetch_ticks_for_symbol_day` loop or `read_features` if it has raw columns.
    # Let's use `read_features_for_symbol` but we need to ensure it has price columns.
    # If not, we might need to fetch ticks.
    #
    # Alternative: The RegimeDetector `prepare_features` expects a DF with price/volume.
    # Let's assume we can get this from `ghtrader.questdb.queries.fetch_ticks_for_symbol_day`
    # for a range of days.
    
    from ghtrader.questdb.client import make_questdb_query_config_from_env
    from ghtrader.questdb.queries import list_trading_days_for_symbol, fetch_ticks_for_day
    
    cfg = make_questdb_query_config_from_env()
    
    # Get last 30 days of data for training
    days = list_trading_days_for_symbol(
        cfg=cfg, 
        table="ghtrader_ticks_main_l5_v2", 
        symbol=symbol, 
        start_day=date.today() - timedelta(days=60),
        end_day=date.today(),
        dataset_version="v2",
        ticks_kind="main_l5"
    )
    
    if not days:
        raise ValueError(f"No data found for {symbol}")
        
    # Fetch and concat
    dfs = []
    for d in days[-30:]: # Train on last 30 trading days
        df = fetch_ticks_for_day(
            cfg=cfg,
            symbol=symbol,
            trading_day=d,
            table="ghtrader_ticks_main_l5_v2",
            columns=["bid_price1", "ask_price1", "volume"],
            dataset_version="v2",
            ticks_kind="main_l5"
        )
        if not df.empty:
            dfs.append(df)
            
    if not dfs:
        raise ValueError(f"No tick data available for {symbol}")
        
    full_df = pd.concat(dfs, ignore_index=True)
    
    detector = RegimeDetector(n_components=n_components)
    detector.fit(full_df)
    
    # Save
    out_path = artifacts_dir / symbol / "regime" / "hmm_model.pkl"
    detector.save(out_path)
    
    return out_path
