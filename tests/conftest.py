from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

from ghtrader.lake import TICK_COLUMN_NAMES, write_ticks_partition


def _has_tqsdk_creds() -> bool:
    return bool(os.environ.get("TQSDK_USER") and os.environ.get("TQSDK_PASSWORD"))


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "integration: requires network/credentials")
    config.addinivalue_line("markers", "ddp_integration: requires torchrun multi-process")


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if not _has_tqsdk_creds():
        skip_integration = pytest.mark.skip(reason="Missing TQSDK_USER/TQSDK_PASSWORD (.env not configured)")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_integration)

    if os.environ.get("GHTRADER_RUN_DDP_TESTS", "false").lower() != "true":
        skip_ddp = pytest.mark.skip(reason="Set GHTRADER_RUN_DDP_TESTS=true to enable DDP integration tests")
        for item in items:
            if "ddp_integration" in item.keywords:
                item.add_marker(skip_ddp)


@pytest.fixture()
def small_synthetic_tick_df() -> pd.DataFrame:
    """
    Small synthetic tick DataFrame with all canonical columns.

    - L5 columns are present (may be NaN depending on level).
    - datetime is epoch-nanoseconds (int64).
    """
    n = 50
    base_dt = datetime(2025, 1, 1, 9, 0, 0)
    times = np.array(
        [int((base_dt + timedelta(milliseconds=200 * i)).timestamp() * 1_000_000_000) for i in range(n)],
        dtype="int64",
    )

    base_price = 70000.0
    # Oscillatory price path so label generation produces multiple classes.
    osc = np.sin(np.linspace(0, 6 * np.pi, n)) * 25.0

    df: dict[str, Any] = {
        "symbol": ["SHFE.cu2501"] * n,
        "datetime": times,
        "last_price": base_price + osc,
        "average": base_price + osc,
        "highest": base_price + osc + 1.0,
        "lowest": base_price + osc - 1.0,
        "volume": np.linspace(1, n, n),
        "amount": np.linspace(1e6, 1e6 + n, n),
        "open_interest": np.linspace(100000, 100000 + n, n),
    }

    # L5 book
    for lvl in range(1, 6):
        df[f"bid_price{lvl}"] = base_price + osc - float(lvl)
        df[f"ask_price{lvl}"] = base_price + osc + float(lvl)
        df[f"bid_volume{lvl}"] = np.linspace(10 * lvl, 10 * lvl + n, n)
        df[f"ask_volume{lvl}"] = np.linspace(12 * lvl, 12 * lvl + n, n)

    out = pd.DataFrame(df)
    out = out[TICK_COLUMN_NAMES]
    return out


@pytest.fixture()
def synthetic_tick_df(small_synthetic_tick_df: pd.DataFrame) -> pd.DataFrame:
    # Slightly larger than small_synthetic_tick_df for training tests
    df = pd.concat([small_synthetic_tick_df] * 6, ignore_index=True)
    df["datetime"] = df["datetime"].astype("int64") + np.arange(len(df), dtype="int64")
    return df


@pytest.fixture()
def synthetic_tick_dict(small_synthetic_tick_df: pd.DataFrame) -> dict[str, Any]:
    return small_synthetic_tick_df.iloc[-1].to_dict()


@pytest.fixture()
def synthetic_data_dir(tmp_path: Path) -> Path:
    return tmp_path / "data"


@pytest.fixture()
def synthetic_lake(synthetic_data_dir: Path, small_synthetic_tick_df: pd.DataFrame) -> tuple[Path, str, list[date]]:
    """
    Create a tiny Parquet lake with 2 day partitions for one symbol.
    """
    symbol = "SHFE.cu2501"
    d0 = date(2025, 1, 1)
    d1 = date(2025, 1, 2)
    data_dir = synthetic_data_dir

    df0 = small_synthetic_tick_df.copy()
    df0["symbol"] = symbol
    write_ticks_partition(df0, data_dir=data_dir, symbol=symbol, dt=d0, part_id="d0")

    df1 = small_synthetic_tick_df.copy()
    df1["symbol"] = symbol
    df1["datetime"] = df1["datetime"].astype("int64") + 10_000_000_000  # shift 10s

    # Make day-2 prices differ from day-1 so horizon=50 labels aren't a single class.
    # For horizon=50 with 50 ticks/day, labels compare day1[i] to day2[i].
    offset = np.sin(np.linspace(0, 4 * np.pi, len(df1))) * 20.0
    for lvl in range(1, 6):
        df1[f"bid_price{lvl}"] = df1[f"bid_price{lvl}"] + offset
        df1[f"ask_price{lvl}"] = df1[f"ask_price{lvl}"] + offset
    for col in ["last_price", "average", "highest", "lowest"]:
        df1[col] = df1[col] + offset
    write_ticks_partition(df1, data_dir=data_dir, symbol=symbol, dt=d1, part_id="d1")

    return data_dir, symbol, [d0, d1]

