from __future__ import annotations

from datetime import date, datetime, timezone


class _FakeApiNone:
    def get_tick_data_series(self, **kwargs):  # noqa: ANN003
        _ = kwargs
        return None


class _FakeApiRaise:
    def get_tick_data_series(self, **kwargs):  # noqa: ANN003
        _ = kwargs
        raise RuntimeError("boom")


class _FakeDF:
    def __init__(self, datetimes: list[int]) -> None:
        self._datetimes = list(datetimes)
        self.empty = False

    def get(self, key: str):
        if str(key) == "datetime":
            return list(self._datetimes)
        return None


class _FakeApiDF:
    def __init__(self, datetimes: list[int]) -> None:
        self._datetimes = list(datetimes)

    def get_tick_data_series(self, **kwargs):  # noqa: ANN003
        _ = kwargs
        return _FakeDF(self._datetimes)


def test_verify_missing_segments_with_tqsdk_provider_empty() -> None:
    from ghtrader.data.main_l5_validation_tqsdk import verify_missing_segments_with_tqsdk

    segs = [
        {"start_ts": "2026-01-01T00:00:00+00:00", "end_ts": "2026-01-01T00:00:01+00:00", "tqsdk_status": "unchecked"},
        {"start_ts": "2026-01-01T00:00:05+00:00", "end_ts": "2026-01-01T00:00:05+00:00", "tqsdk_status": "unchecked"},
    ]
    out = verify_missing_segments_with_tqsdk(
        tqsdk_api=_FakeApiNone(),
        symbol="SHFE.cu2602",
        day=date(2026, 1, 1),
        day_missing_segments=segs,
        tqsdk_check_max_segments=1,
    )
    assert out["checked_days"] == 1
    assert out["checked_segments"] == 1
    assert out["provider_missing_segments"] == 1
    assert out["provider_has_data_segments"] == 0
    assert out["errors"] == 0
    assert segs[0]["tqsdk_status"] == "provider_missing"
    assert segs[1]["tqsdk_status"] == "unchecked"


def test_verify_missing_segments_with_tqsdk_has_data_and_missing(monkeypatch) -> None:
    import ghtrader.data.main_l5_validation_tqsdk as mod

    # Force the code path where l5_mask_df failure falls back silently.
    def _raise_mask(df):  # noqa: ANN001
        _ = df
        raise RuntimeError("mask fail")

    monkeypatch.setattr(mod, "l5_mask_df", _raise_mask)

    tick_ns = int(datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc).timestamp() * 1_000_000_000) + 500_000_000
    segs = [
        {"start_ts": "2026-01-01T00:00:00+00:00", "end_ts": "2026-01-01T00:00:01+00:00", "tqsdk_status": "unchecked"},
        {"start_ts": "2026-01-01T00:00:05+00:00", "end_ts": "2026-01-01T00:00:05+00:00", "tqsdk_status": "unchecked"},
    ]
    out = mod.verify_missing_segments_with_tqsdk(
        tqsdk_api=_FakeApiDF([tick_ns]),
        symbol="SHFE.cu2602",
        day=date(2026, 1, 1),
        day_missing_segments=segs,
        tqsdk_check_max_segments=5,
    )
    assert out["checked_days"] == 1
    assert out["checked_segments"] == 2
    assert out["provider_has_data_segments"] == 1
    assert out["provider_missing_segments"] == 1
    assert out["errors"] == 0
    assert segs[0]["tqsdk_status"] == "provider_has_data"
    assert segs[1]["tqsdk_status"] == "provider_missing"


def test_verify_missing_segments_with_tqsdk_error() -> None:
    from ghtrader.data.main_l5_validation_tqsdk import verify_missing_segments_with_tqsdk

    segs = [{"start_ts": "2026-01-01T00:00:00+00:00", "end_ts": "2026-01-01T00:00:01+00:00", "tqsdk_status": "unchecked"}]
    out = verify_missing_segments_with_tqsdk(
        tqsdk_api=_FakeApiRaise(),
        symbol="SHFE.cu2602",
        day=date(2026, 1, 1),
        day_missing_segments=segs,
        tqsdk_check_max_segments=1,
    )
    assert out["checked_days"] == 0
    assert out["checked_segments"] == 0
    assert out["provider_has_data_segments"] == 0
    assert out["provider_missing_segments"] == 0
    assert out["errors"] == 1
    assert "boom" in str(out["error"])

