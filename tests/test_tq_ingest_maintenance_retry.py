from __future__ import annotations

import sys
from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest


def _sample_tick_df() -> pd.DataFrame:
    from ghtrader.data.ticks_schema import TICK_COLUMN_NAMES

    row: dict[str, list[float | int]] = {
        "id": [1],
        "datetime": [int(pd.Timestamp("2025-01-02T09:00:00Z").value)],
    }
    for col in TICK_COLUMN_NAMES:
        if col in {"symbol", "datetime"}:
            continue
        row[col] = [1.0]
    return pd.DataFrame(row)


def test_download_main_l5_maintenance_retry_exhausted(monkeypatch, tmp_path) -> None:
    import ghtrader.tq.ingest as ingest

    class _Backend:
        def ensure_table(self, *, table: str, include_segment_metadata: bool) -> None:
            _ = (table, include_segment_metadata)

        def ingest_df(self, *, table: str, df) -> None:
            _ = (table, df)

    class _FakeTqApi:
        def __init__(self, auth=None) -> None:
            self.auth = auth

        def get_tick_data_series(self, *, symbol: str, start_dt: date, end_dt: date):
            _ = (symbol, start_dt, end_dt)
            raise RuntimeError("maintenance window")

        def close(self) -> None:
            return None

    monkeypatch.setattr(ingest, "_make_questdb_backend_required", lambda: _Backend())
    monkeypatch.setattr(ingest, "get_tqsdk_auth", lambda: object())
    monkeypatch.setattr(ingest.time, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setitem(sys.modules, "tqsdk", SimpleNamespace(TqApi=_FakeTqApi))
    monkeypatch.setenv("GHTRADER_INGEST_WORKERS", "1")
    monkeypatch.setenv("GHTRADER_TQ_RETRY_MAX", "0")
    monkeypatch.setenv("GHTRADER_TQ_MAINTENANCE_WAIT_S", "1")
    monkeypatch.setenv("GHTRADER_TQ_MAINTENANCE_MAX_RETRIES", "1")

    with pytest.raises(RuntimeError, match="maintenance retry exhausted"):
        ingest.download_main_l5_for_days(
            underlying_symbol="SHFE.cu2501",
            derived_symbol="KQ.m@SHFE.cu",
            trading_days=[date(2025, 1, 2)],
            segment_id=0,
            schedule_hash="hash-x",
            data_dir=tmp_path,
            dataset_version="v2",
        )


def test_download_main_l5_reuses_tqapi_per_worker(monkeypatch, tmp_path) -> None:
    import ghtrader.tq.ingest as ingest

    tick_df = _sample_tick_df()

    class _Backend:
        def __init__(self) -> None:
            self.calls = 0

        def ensure_table(self, *, table: str, include_segment_metadata: bool) -> None:
            _ = (table, include_segment_metadata)

        def ingest_df(self, *, table: str, df) -> None:
            _ = table
            self.calls += int(len(df))

    backend = _Backend()

    class _FakeTqApi:
        created = 0
        closed = 0

        def __init__(self, auth=None, disable_print=False) -> None:
            _ = (auth, disable_print)
            type(self).created += 1

        def get_tick_data_series(self, *, symbol: str, start_dt: date, end_dt: date):
            _ = (symbol, start_dt, end_dt)
            out = tick_df.copy()
            out["datetime"] = int(pd.Timestamp(f"{start_dt.isoformat()}T02:00:00Z").value)
            return out

        def close(self) -> None:
            type(self).closed += 1

    monkeypatch.setattr(ingest, "_make_questdb_backend_required", lambda: backend)
    monkeypatch.setattr(ingest, "get_tqsdk_auth", lambda: object())
    monkeypatch.setitem(sys.modules, "tqsdk", SimpleNamespace(TqApi=_FakeTqApi))
    monkeypatch.setenv("GHTRADER_TQ_RETRY_MAX", "0")

    res = ingest.download_main_l5_for_days(
        underlying_symbol="SHFE.cu2501",
        derived_symbol="KQ.m@SHFE.cu",
        trading_days=[date(2025, 1, 2), date(2025, 1, 3), date(2025, 1, 6)],
        segment_id=0,
        schedule_hash="hash-x",
        data_dir=tmp_path,
        dataset_version="v2",
        workers=1,
    )

    assert _FakeTqApi.created == 1
    assert _FakeTqApi.closed == 1
    assert backend.calls == 3
    assert int(res.get("rows_total") or 0) == 3


def test_download_main_l5_timeout_retry_avoids_immediate_reconnect(monkeypatch, tmp_path) -> None:
    import ghtrader.tq.ingest as ingest

    tick_df = _sample_tick_df()

    class _Backend:
        def ensure_table(self, *, table: str, include_segment_metadata: bool) -> None:
            _ = (table, include_segment_metadata)

        def ingest_df(self, *, table: str, df) -> None:
            _ = (table, df)

    class _FakeTqApi:
        created = 0

        def __init__(self, auth=None, disable_print=False) -> None:
            _ = (auth, disable_print)
            type(self).created += 1
            self.calls = 0

        def get_tick_data_series(self, *, symbol: str, start_dt: date, end_dt: date):
            _ = (symbol, start_dt, end_dt)
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("get_tick_data_series timeout")
            return tick_df.copy()

        def close(self) -> None:
            return None

    monkeypatch.setattr(ingest, "_make_questdb_backend_required", lambda: _Backend())
    monkeypatch.setattr(ingest, "get_tqsdk_auth", lambda: object())
    monkeypatch.setattr(ingest.time, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setitem(sys.modules, "tqsdk", SimpleNamespace(TqApi=_FakeTqApi))
    monkeypatch.setenv("GHTRADER_TQ_RETRY_MAX", "1")
    monkeypatch.setenv("GHTRADER_TQ_RETRY_BASE_S", "0")
    monkeypatch.setenv("GHTRADER_TQ_RETRY_MAX_S", "0")
    monkeypatch.setenv("GHTRADER_TQ_TIMEOUT_RESET_AFTER", "2")

    res = ingest.download_main_l5_for_days(
        underlying_symbol="SHFE.cu2501",
        derived_symbol="KQ.m@SHFE.cu",
        trading_days=[date(2025, 1, 2)],
        segment_id=0,
        schedule_hash="hash-x",
        data_dir=tmp_path,
        dataset_version="v2",
        workers=1,
    )

    assert _FakeTqApi.created == 1
    assert int(res.get("rows_total") or 0) == 1


def test_download_main_l5_trims_cross_day_rows(monkeypatch, tmp_path) -> None:
    import ghtrader.tq.ingest as ingest

    from ghtrader.data.ticks_schema import TICK_COLUMN_NAMES

    rows: dict[str, list[float | int]] = {
        "id": [1, 2, 3, 4],
        "datetime": [
            int(pd.Timestamp("2025-01-01T13:00:00Z").value),  # 21:00 CST -> trading_day 2025-01-02 (keep)
            int(pd.Timestamp("2025-01-02T02:00:00Z").value),  # 10:00 CST -> trading_day 2025-01-02 (keep)
            int(pd.Timestamp("2025-01-01T09:00:00Z").value),  # 17:00 CST -> trading_day 2025-01-01 (drop)
            int(pd.Timestamp("2025-01-03T02:00:00Z").value),  # 10:00 CST -> trading_day 2025-01-03 (drop)
        ],
    }
    for col in TICK_COLUMN_NAMES:
        if col in {"symbol", "datetime"}:
            continue
        rows[col] = [1.0, 1.0, 1.0, 1.0]
    tick_df = pd.DataFrame(rows)

    class _Backend:
        def __init__(self) -> None:
            self.calls = 0
            self.last_df = None

        def ensure_table(self, *, table: str, include_segment_metadata: bool) -> None:
            _ = (table, include_segment_metadata)

        def ingest_df(self, *, table: str, df) -> None:
            _ = table
            self.calls += int(len(df))
            self.last_df = df.copy()

    backend = _Backend()

    class _FakeTqApi:
        def __init__(self, auth=None, disable_print=False) -> None:
            _ = (auth, disable_print)

        def get_tick_data_series(self, *, symbol: str, start_dt: date, end_dt: date):
            _ = (symbol, start_dt, end_dt)
            return tick_df.copy()

        def close(self) -> None:
            return None

    monkeypatch.setattr(ingest, "_make_questdb_backend_required", lambda: backend)
    monkeypatch.setattr(ingest, "get_tqsdk_auth", lambda: object())
    monkeypatch.setitem(sys.modules, "tqsdk", SimpleNamespace(TqApi=_FakeTqApi))
    monkeypatch.setenv("GHTRADER_TQ_RETRY_MAX", "0")

    res = ingest.download_main_l5_for_days(
        underlying_symbol="SHFE.cu2502",
        derived_symbol="KQ.m@SHFE.cu",
        trading_days=[date(2025, 1, 2)],
        segment_id=0,
        schedule_hash="hash-trim",
        data_dir=tmp_path,
        dataset_version="v2",
        workers=1,
    )

    assert int(res.get("rows_total") or 0) == 2
    assert backend.calls == 2
    assert backend.last_df is not None
    assert set(backend.last_df["trading_day"].astype(str).tolist()) == {"2025-01-02"}


def test_download_main_l5_friday_night_post_midnight_kept_for_monday(monkeypatch, tmp_path) -> None:
    """
    Regression: Friday night session 21:00-02:30 crosses midnight into Saturday.
    Ticks at Saturday 00:00-02:30 must be kept for Monday's trading day when
    session windows are available.
    """
    import json
    from datetime import datetime, timedelta, timezone

    import ghtrader.tq.ingest as ingest
    from ghtrader.data.ticks_schema import TICK_COLUMN_NAMES
    from ghtrader.data.trading_sessions import write_trading_sessions_cache, normalize_trading_sessions

    monday = date(2025, 1, 13)
    friday = date(2025, 1, 10)

    ag_trading_time = {
        "day": [["09:00:00", "10:15:00"], ["10:30:00", "11:30:00"], ["13:30:00", "15:00:00"]],
        "night": [["21:00:00", "26:30:00"]],
    }
    payload = normalize_trading_sessions(trading_time=ag_trading_time, exchange="SHFE", variety="ag")
    write_trading_sessions_cache(data_dir=tmp_path, exchange="SHFE", variety="ag", payload=payload)

    cal_path = tmp_path / "trading_calendar" / "calendar.json"
    cal_path.parent.mkdir(parents=True, exist_ok=True)
    cal_days = [
        date(2025, 1, 6), date(2025, 1, 7), date(2025, 1, 8), date(2025, 1, 9), friday,
        monday, date(2025, 1, 14), date(2025, 1, 15), date(2025, 1, 16), date(2025, 1, 17),
    ]
    cal_path.write_text(json.dumps([d.isoformat() for d in cal_days]))

    tick_rows: dict[str, list[float | int]] = {"id": [], "datetime": []}
    for col in TICK_COLUMN_NAMES:
        if col in {"symbol", "datetime"}:
            continue
        tick_rows[col] = []

    def _add_tick(ts_utc: str) -> None:
        ns = int(pd.Timestamp(ts_utc).value)
        tick_rows["id"].append(len(tick_rows["id"]) + 1)
        tick_rows["datetime"].append(ns)
        for col in TICK_COLUMN_NAMES:
            if col in {"symbol", "datetime"}:
                continue
            tick_rows[col].append(1.0)

    _add_tick("2025-01-10T13:00:00.500Z")  # Fri 21:00 CST (night, keep)
    _add_tick("2025-01-10T14:30:00.500Z")  # Fri 22:30 CST (night, keep)
    _add_tick("2025-01-10T16:00:00.500Z")  # Sat 00:00 CST (night post-midnight, MUST KEEP)
    _add_tick("2025-01-10T17:30:00.500Z")  # Sat 01:30 CST (night post-midnight, MUST KEEP)
    _add_tick("2025-01-10T18:29:00.500Z")  # Sat 02:29 CST (night post-midnight, MUST KEEP)
    _add_tick("2025-01-13T01:00:00.500Z")  # Mon 09:00 CST (day, keep)
    _add_tick("2025-01-13T05:30:00.500Z")  # Mon 13:30 CST (day, keep)
    _add_tick("2025-01-10T08:00:00.500Z")  # Fri 16:00 CST (outside sessions, drop)
    _add_tick("2025-01-13T09:00:00.500Z")  # Mon 17:00 CST (outside sessions, drop)

    tick_df = pd.DataFrame(tick_rows)

    ingested_dfs: list[pd.DataFrame] = []

    class _Backend:
        def ensure_table(self, *, table: str, include_segment_metadata: bool) -> None:
            pass

        def ingest_df(self, *, table: str, df) -> None:
            ingested_dfs.append(df.copy())

    class _FakeTqApi:
        def __init__(self, auth=None, disable_print=False) -> None:
            pass

        def get_tick_data_series(self, *, symbol: str, start_dt: date, end_dt: date):
            return tick_df.copy()

        def close(self) -> None:
            pass

    monkeypatch.setattr(ingest, "_make_questdb_backend_required", lambda: _Backend())
    monkeypatch.setattr(ingest, "get_tqsdk_auth", lambda: object())
    monkeypatch.setitem(sys.modules, "tqsdk", SimpleNamespace(TqApi=_FakeTqApi))
    monkeypatch.setenv("GHTRADER_TQ_RETRY_MAX", "0")

    res = ingest.download_main_l5_for_days(
        underlying_symbol="SHFE.ag2502",
        derived_symbol="KQ.m@SHFE.ag",
        trading_days=[friday, monday],
        segment_id=0,
        schedule_hash="hash-night-fix",
        data_dir=tmp_path,
        exchange="SHFE",
        variety="ag",
        dataset_version="v2",
        workers=1,
    )

    assert len(ingested_dfs) >= 1
    monday_dfs = [df for df in ingested_dfs if set(df["trading_day"].astype(str).unique()) == {monday.isoformat()}]
    assert len(monday_dfs) == 1, f"Expected exactly one Monday ingest batch, got {len(monday_dfs)}"
    monday_df = monday_dfs[0]

    monday_ns = set(monday_df["datetime_ns"].astype("int64").tolist())
    sat_post_midnight_ticks = [
        int(pd.Timestamp("2025-01-10T16:00:00.500Z").value),  # Sat 00:00 CST
        int(pd.Timestamp("2025-01-10T17:30:00.500Z").value),  # Sat 01:30 CST
        int(pd.Timestamp("2025-01-10T18:29:00.500Z").value),  # Sat 02:29 CST
    ]
    for ns in sat_post_midnight_ticks:
        assert ns in monday_ns, f"Saturday post-midnight tick {ns} missing from Monday ingest"

    fri_evening_ticks = [
        int(pd.Timestamp("2025-01-10T13:00:00.500Z").value),  # Fri 21:00 CST
        int(pd.Timestamp("2025-01-10T14:30:00.500Z").value),  # Fri 22:30 CST
    ]
    for ns in fri_evening_ticks:
        assert ns in monday_ns, f"Friday evening tick {ns} missing from Monday ingest"

    outside_ticks = [
        int(pd.Timestamp("2025-01-10T08:00:00.500Z").value),  # Fri 16:00 CST
        int(pd.Timestamp("2025-01-13T09:00:00.500Z").value),  # Mon 17:00 CST
    ]
    for ns in outside_ticks:
        assert ns not in monday_ns, f"Outside-session tick {ns} should not be in Monday ingest"

    assert int(len(monday_df)) == 7
