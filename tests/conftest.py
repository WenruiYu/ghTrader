from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

from ghtrader.data.ticks_schema import TICK_COLUMN_NAMES


# ---------------------------------------------------------------------------
# QuestDB test utilities
# ---------------------------------------------------------------------------

def _has_questdb() -> bool:
    """Check if QuestDB is available for integration tests."""
    return bool(os.environ.get("QUESTDB_HOST") or os.environ.get("GHTRADER_RUN_QUESTDB_TESTS", "").lower() == "true")


@pytest.fixture
def questdb_config():
    """
    QuestDB configuration for integration tests.
    
    Returns None if QuestDB is not available (tests should be skipped).
    For unit tests that need to mock QuestDB, use the mock_questdb_* fixtures instead.
    """
    from ghtrader.questdb.client import QuestDBQueryConfig
    
    host = os.environ.get("QUESTDB_HOST", "localhost")
    pg_port = int(os.environ.get("QUESTDB_PG_PORT", "8812"))
    pg_user = os.environ.get("QUESTDB_PG_USER", "admin")
    pg_password = os.environ.get("QUESTDB_PG_PASSWORD", "quest")
    pg_dbname = os.environ.get("QUESTDB_PG_DBNAME", "qdb")
    
    return QuestDBQueryConfig(
        host=host,
        pg_port=pg_port,
        pg_user=pg_user,
        pg_password=pg_password,
        pg_dbname=pg_dbname,
    )


@pytest.fixture
def mock_questdb_connection(monkeypatch: pytest.MonkeyPatch):
    """
    Mock QuestDB connection for unit tests.
    
    This fixture patches the _connect function to return a mock connection
    that can be used for unit testing without a real QuestDB instance.
    """
    from unittest.mock import MagicMock

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)

    monkeypatch.setattr("ghtrader.questdb.queries._connect", lambda *args, **kwargs: mock_conn)
    return mock_conn, mock_cursor


def _has_tqsdk_creds() -> bool:
    return bool(os.environ.get("TQSDK_USER") and os.environ.get("TQSDK_PASSWORD"))


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "integration: requires network/credentials")
    config.addinivalue_line("markers", "ddp_integration: requires torchrun multi-process")
    config.addinivalue_line("markers", "questdb: requires QuestDB instance")


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

    if not _has_questdb():
        skip_questdb = pytest.mark.skip(reason="Set GHTRADER_RUN_QUESTDB_TESTS=true or QUESTDB_HOST to enable QuestDB tests")
        for item in items:
            if "questdb" in item.keywords:
                item.add_marker(skip_questdb)


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

