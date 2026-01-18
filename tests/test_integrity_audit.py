from __future__ import annotations

import pytest

from ghtrader.data.audit import audit_ticks_index_vs_table


def test_audit_ticks_index_vs_table_no_findings_when_match(monkeypatch: pytest.MonkeyPatch) -> None:
    import ghtrader.data.audit as aud
    import ghtrader.questdb.index as qix

    monkeypatch.setattr(qix, "ensure_index_tables", lambda **_kwargs: None)

    computed = {
        ("SHFE.cu2501", "2025-01-02"): {
            "rows_total": 10,
            "first_datetime_ns": 1,
            "last_datetime_ns": 10,
            "l5_present": True,
            "row_hash_min": 1,
            "row_hash_max": 10,
            "row_hash_sum": 55,
            "row_hash_sum_abs": 55,
        }
    }
    index_rows = {
        ("SHFE.cu2501", "2025-01-02"): dict(computed[("SHFE.cu2501", "2025-01-02")]),
    }

    monkeypatch.setattr(aud, "_compute_day_aggregates_from_ticks_table", lambda **_kwargs: computed)
    monkeypatch.setattr(aud, "_fetch_index_rows", lambda **_kwargs: index_rows)

    findings = audit_ticks_index_vs_table(
        ticks_table="ghtrader_ticks_raw_v2",
        ticks_kind="raw",
        dataset_version="v2",
        symbols=["SHFE.cu2501"],
        index_table="ghtrader_symbol_day_index_v2",
    )
    assert findings == []


def test_audit_ticks_index_vs_table_reports_checksum_mismatch(monkeypatch: pytest.MonkeyPatch) -> None:
    import ghtrader.data.audit as aud
    import ghtrader.questdb.index as qix

    monkeypatch.setattr(qix, "ensure_index_tables", lambda **_kwargs: None)

    computed = {
        ("SHFE.cu2501", "2025-01-02"): {
            "rows_total": 10,
            "first_datetime_ns": 1,
            "last_datetime_ns": 10,
            "l5_present": False,
            "row_hash_min": 1,
            "row_hash_max": 10,
            "row_hash_sum": 55,
            "row_hash_sum_abs": 55,
        }
    }
    index_rows = {
        ("SHFE.cu2501", "2025-01-02"): {
            **computed[("SHFE.cu2501", "2025-01-02")],
            "row_hash_sum": 56,
        }
    }

    monkeypatch.setattr(aud, "_compute_day_aggregates_from_ticks_table", lambda **_kwargs: computed)
    monkeypatch.setattr(aud, "_fetch_index_rows", lambda **_kwargs: index_rows)

    findings = audit_ticks_index_vs_table(
        ticks_table="ghtrader_ticks_raw_v2",
        ticks_kind="raw",
        dataset_version="v2",
        symbols=["SHFE.cu2501"],
        index_table="ghtrader_symbol_day_index_v2",
    )
    assert any(f.code == "checksum_mismatch" for f in findings)


def test_audit_ticks_index_vs_table_reports_index_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    import ghtrader.data.audit as aud
    import ghtrader.questdb.index as qix

    monkeypatch.setattr(qix, "ensure_index_tables", lambda **_kwargs: None)

    computed = {
        ("SHFE.cu2501", "2025-01-02"): {
            "rows_total": 10,
            "first_datetime_ns": 1,
            "last_datetime_ns": 10,
            "l5_present": False,
            "row_hash_min": None,
            "row_hash_max": None,
            "row_hash_sum": None,
            "row_hash_sum_abs": None,
        }
    }
    index_rows: dict[tuple[str, str], dict] = {}

    monkeypatch.setattr(aud, "_compute_day_aggregates_from_ticks_table", lambda **_kwargs: computed)
    monkeypatch.setattr(aud, "_fetch_index_rows", lambda **_kwargs: index_rows)

    findings = audit_ticks_index_vs_table(
        ticks_table="ghtrader_ticks_raw_v2",
        ticks_kind="raw",
        dataset_version="v2",
        symbols=["SHFE.cu2501"],
        index_table="ghtrader_symbol_day_index_v2",
    )
    assert any(f.code == "index_missing" for f in findings)

