from __future__ import annotations

import json

from click.testing import CliRunner
import pandas as pd

from ghtrader.cli import main


def test_evaluate_tick_data_contract_passes_on_synthetic_ticks() -> None:
    from ghtrader.data.contract_checks import build_synthetic_contract_ticks, evaluate_tick_data_contract

    df = build_synthetic_contract_ticks(rows=96)
    report = evaluate_tick_data_contract(df=df)

    assert report["ok"] is True
    assert report["checks"]["schema_columns_present"] is True
    assert report["checks"]["row_hash_integrity"] is True
    assert report["checks"]["null_rates_within_threshold"] is True


def test_evaluate_tick_data_contract_detects_missing_columns() -> None:
    from ghtrader.data.contract_checks import build_synthetic_contract_ticks, evaluate_tick_data_contract

    df = build_synthetic_contract_ticks(rows=96).drop(columns=["bid_price3"])
    report = evaluate_tick_data_contract(df=df)

    assert report["ok"] is False
    assert report["checks"]["schema_columns_present"] is False
    assert "bid_price3" in report["schema"]["missing_columns"]


def test_evaluate_tick_data_contract_detects_null_rate_breach() -> None:
    from ghtrader.data.contract_checks import (
        DataContractThresholds,
        build_synthetic_contract_ticks,
        evaluate_tick_data_contract,
    )

    df = build_synthetic_contract_ticks(rows=96)
    df["last_price"] = float("nan")
    report = evaluate_tick_data_contract(
        df=df,
        thresholds=DataContractThresholds(max_core_null_rate=0.01),
    )

    assert report["ok"] is False
    assert any(v["metric"] == "last_price_null_rate" for v in report["violations"])


def test_cli_data_contract_check_json_passes() -> None:
    runner = CliRunner()
    res = runner.invoke(main, ["data", "contract-check", "--rows", "96", "--json"])
    assert res.exit_code == 0, res.output
    start = res.output.find("{")
    assert start >= 0, res.output
    payload = json.loads(res.output[start:])
    assert payload["ok"] is True
    assert payload["source"] == "synthetic"


def test_evaluate_tick_data_contract_detects_rows_below_minimum() -> None:
    from ghtrader.data.contract_checks import (
        DataContractThresholds,
        build_synthetic_contract_ticks,
        evaluate_tick_data_contract,
    )

    df = build_synthetic_contract_ticks(rows=10)
    report = evaluate_tick_data_contract(
        df=df,
        thresholds=DataContractThresholds(min_rows=32),
    )

    assert report["ok"] is False
    assert report["checks"]["rows_minimum"] is False
    assert any(v["type"] == "rows_below_minimum" for v in report["violations"])


def test_evaluate_tick_data_contract_detects_l5_null_rate_breach() -> None:
    from ghtrader.data.contract_checks import (
        DataContractThresholds,
        build_synthetic_contract_ticks,
        evaluate_tick_data_contract,
    )

    df = build_synthetic_contract_ticks(rows=96)
    for col in df.columns:
        if col.startswith(("bid_price", "ask_price", "bid_volume", "ask_volume")) and col[-1] != "1":
            df[col] = float("nan")

    report = evaluate_tick_data_contract(
        df=df,
        thresholds=DataContractThresholds(max_l5_null_rate=0.05),
    )

    assert report["ok"] is False
    assert any(v["type"] == "l5_null_rate_exceeded" for v in report["violations"])


def test_evaluate_tick_data_contract_reports_extra_columns() -> None:
    from ghtrader.data.contract_checks import build_synthetic_contract_ticks, evaluate_tick_data_contract

    df = build_synthetic_contract_ticks(rows=64)
    df["bonus_col"] = 42
    report = evaluate_tick_data_contract(df=df)

    assert report["ok"] is True
    assert "bonus_col" in report["schema"]["extra_columns"]


def test_shared_null_rate_default_on_error_parameter() -> None:
    from ghtrader.data.ticks_schema import null_rate

    series = pd.Series([1.0, float("nan"), 3.0])
    assert null_rate(series, 3) == 1.0 / 3.0
    assert null_rate(series, 0) == 0.0
    assert null_rate(series, 3, default_on_error=1.0) == 1.0 / 3.0


def test_cli_data_contract_check_fails_on_invalid_input_schema(tmp_path) -> None:
    bad = tmp_path / "bad_ticks.csv"
    pd.DataFrame({"foo": [1], "bar": [2]}).to_csv(bad, index=False)

    runner = CliRunner()
    res = runner.invoke(main, ["data", "contract-check", "--input-path", str(bad)])
    assert res.exit_code != 0
    assert "data contract check failed" in res.output
