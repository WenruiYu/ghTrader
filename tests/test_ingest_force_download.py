from __future__ import annotations

from datetime import date


def test_compute_missing_dates_force_bypasses_existing_and_no_data() -> None:
    from ghtrader.tq.ingest import _compute_missing_dates

    day = date(2026, 1, 16)
    all_dates = [day]
    existing_dates = {day}
    no_data_dates = {day}

    missing = _compute_missing_dates(
        all_dates=all_dates,
        existing_dates=existing_dates,
        no_data_dates=no_data_dates,
        force_dates={day},
    )
    assert missing == [day]

    missing_no_force = _compute_missing_dates(
        all_dates=all_dates,
        existing_dates=existing_dates,
        no_data_dates=no_data_dates,
        force_dates=None,
    )
    assert missing_no_force == []
