"""Tabular baseline models."""

from __future__ import annotations

import pickle
from pathlib import Path
from typing import Any

import numpy as np
import structlog
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

from .models_base import BaseModel

log = structlog.get_logger()


class LogisticModel(BaseModel):
    """Logistic regression baseline."""

    name = "logistic"

    def __init__(self, n_classes: int = 3, **kwargs: Any) -> None:
        self.n_classes = n_classes
        self.scaler = StandardScaler()
        self.model = LogisticRegression(
            solver="lbfgs",
            max_iter=1000,
            **kwargs,
        )

    def fit(self, X: np.ndarray, y: np.ndarray, **kwargs: Any) -> None:
        mask = ~(np.isnan(X).any(axis=1) | np.isnan(y))
        X_clean = X[mask]
        y_clean = y[mask].astype(int)
        X_scaled = self.scaler.fit_transform(X_clean)
        self.model.fit(X_scaled, y_clean)
        log.info("logistic.fit_done", n_samples=len(y_clean))

    def predict_proba(self, X: np.ndarray, **kwargs: Any) -> np.ndarray:
        X_scaled = self.scaler.transform(X)
        return self.model.predict_proba(X_scaled)

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as f:
            pickle.dump({"scaler": self.scaler, "model": self.model}, f)

    def load(self, path: Path) -> None:
        with open(path, "rb") as f:
            data = pickle.load(f)
        self.scaler = data["scaler"]
        self.model = data["model"]


class LightGBMModel(BaseModel):
    """LightGBM baseline."""

    name = "lightgbm"

    def __init__(self, n_classes: int = 3, **kwargs: Any) -> None:
        self.n_classes = n_classes
        self.params = {
            "objective": "multiclass",
            "num_class": n_classes,
            "metric": "multi_logloss",
            "boosting_type": "gbdt",
            "num_leaves": 63,
            "learning_rate": 0.05,
            "feature_fraction": 0.8,
            "bagging_fraction": 0.8,
            "bagging_freq": 5,
            "verbose": -1,
            "n_jobs": -1,
            **kwargs,
        }
        self.model = None

    def fit(self, X: np.ndarray, y: np.ndarray, n_rounds: int = 500, **kwargs: Any) -> None:
        import lightgbm as lgb

        mask = ~(np.isnan(X).any(axis=1) | np.isnan(y))
        X_clean = X[mask]
        y_clean = y[mask].astype(int)
        train_data = lgb.Dataset(X_clean, label=y_clean)
        self.model = lgb.train(
            self.params,
            train_data,
            num_boost_round=n_rounds,
        )
        log.info("lightgbm.fit_done", n_samples=len(y_clean), n_rounds=n_rounds)

    def predict_proba(self, X: np.ndarray, **kwargs: Any) -> np.ndarray:
        if self.model is None:
            raise RuntimeError("Model not fitted")
        return self.model.predict(X)

    def save(self, path: Path) -> None:
        if self.model is None:
            raise RuntimeError("Model not fitted")
        path.parent.mkdir(parents=True, exist_ok=True)
        self.model.save_model(str(path))

    def load(self, path: Path) -> None:
        import lightgbm as lgb

        self.model = lgb.Booster(model_file=str(path))


class XGBoostModel(BaseModel):
    """XGBoost baseline."""

    name = "xgboost"

    def __init__(self, n_classes: int = 3, **kwargs: Any) -> None:
        self.n_classes = n_classes
        self.params = {
            "objective": "multi:softprob",
            "num_class": n_classes,
            "eval_metric": "mlogloss",
            "max_depth": 6,
            "learning_rate": 0.05,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "n_jobs": -1,
            **kwargs,
        }
        self.model = None

    def fit(self, X: np.ndarray, y: np.ndarray, n_rounds: int = 500, **kwargs: Any) -> None:
        import xgboost as xgb

        mask = ~(np.isnan(X).any(axis=1) | np.isnan(y))
        X_clean = X[mask]
        y_clean = y[mask].astype(int)
        dtrain = xgb.DMatrix(X_clean, label=y_clean)
        self.model = xgb.train(self.params, dtrain, num_boost_round=n_rounds)
        log.info("xgboost.fit_done", n_samples=len(y_clean), n_rounds=n_rounds)

    def predict_proba(self, X: np.ndarray, **kwargs: Any) -> np.ndarray:
        import xgboost as xgb

        if self.model is None:
            raise RuntimeError("Model not fitted")
        dtest = xgb.DMatrix(X)
        return self.model.predict(dtest)

    def save(self, path: Path) -> None:
        if self.model is None:
            raise RuntimeError("Model not fitted")
        path.parent.mkdir(parents=True, exist_ok=True)
        self.model.save_model(str(path))

    def load(self, path: Path) -> None:
        import xgboost as xgb

        self.model = xgb.Booster()
        self.model.load_model(str(path))
