from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from ghtrader.data.ticks_schema import (
    L5_BOOK_COLS,
    TICK_COLUMN_NAMES,
    null_rate as _null_rate_shared,
    row_hash_aggregates,
    row_hash_algorithm_version,
    row_hash_from_ticks_df,
    ticks_schema_hash,
)

CORE_NULL_RATE_COLUMNS: tuple[str, ...] = (
    "last_price",
    "volume",
    "open_interest",
    "bid_price1",
    "ask_price1",
)


@dataclass(frozen=True)
class DataContractThresholds:
    min_rows: int = 32
    max_core_null_rate: float = 0.01
    max_l5_null_rate: float = 0.05


def _null_rate(series: pd.Series, *, total_rows: int) -> float:
    return _null_rate_shared(series, total_rows, default_on_error=1.0)


def _coerce_threshold(value: Any, *, default: float) -> float:
    try:
        out = float(value)
    except Exception:
        out = float(default)
    return max(0.0, min(1.0, float(out)))


def _coerce_min_rows(value: Any, *, default: int) -> int:
    try:
        out = int(value)
    except Exception:
        out = int(default)
    return max(1, int(out))


def build_synthetic_contract_ticks(*, rows: int = 64) -> pd.DataFrame:
    """
    Build a deterministic canonical tick frame for CI preflight checks.
    """
    n = max(1, int(rows))
    base_ns = 1_735_689_600_000_000_000
    base_price = 70_000.0
    payload: dict[str, list[Any]] = {
        "symbol": ["SHFE.cu2505"] * n,
        "datetime": [base_ns + (i * 500_000_000) for i in range(n)],
        "last_price": [base_price + (i * 0.1) for i in range(n)],
        "average": [base_price + (i * 0.1) for i in range(n)],
        "highest": [base_price + 1.0 + (i * 0.1) for i in range(n)],
        "lowest": [base_price - 1.0 + (i * 0.1) for i in range(n)],
        "volume": [float(i + 1) for i in range(n)],
        "amount": [1_000_000.0 + float(i * 10) for i in range(n)],
        "open_interest": [100_000.0 + float(i) for i in range(n)],
    }

    for level in range(1, 6):
        payload[f"bid_price{level}"] = [base_price - float(level) + (i * 0.1) for i in range(n)]
        payload[f"ask_price{level}"] = [base_price + float(level) + (i * 0.1) for i in range(n)]
        payload[f"bid_volume{level}"] = [float(10 * level + i) for i in range(n)]
        payload[f"ask_volume{level}"] = [float(12 * level + i) for i in range(n)]

    return pd.DataFrame(payload)[TICK_COLUMN_NAMES]


def load_tick_frame_for_contract_check(
    *,
    path: str | Path | None = None,
    synthetic_rows: int = 64,
) -> tuple[pd.DataFrame, str]:
    """
    Load tick data from file, or generate deterministic synthetic ticks.
    """
    raw_path = str(path or "").strip()
    if not raw_path:
        return build_synthetic_contract_ticks(rows=int(synthetic_rows)), "synthetic"

    input_path = Path(raw_path).expanduser().resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"input path does not exist: {input_path}")

    suffix = input_path.suffix.lower()
    if suffix in {".csv"}:
        df = pd.read_csv(input_path)
    elif suffix in {".parquet", ".pq"}:
        df = pd.read_parquet(input_path)
    elif suffix in {".json"}:
        df = pd.read_json(input_path)
    elif suffix in {".jsonl", ".ndjson"}:
        df = pd.read_json(input_path, lines=True)
    else:
        raise ValueError(f"unsupported input format: {suffix}")
    return df, str(input_path)


def evaluate_tick_data_contract(
    *,
    df: pd.DataFrame,
    thresholds: DataContractThresholds | None = None,
) -> dict[str, Any]:
    """
    Validate schema/null-rate/hash contract for a tick dataframe.
    """
    if thresholds is None:
        thresholds = DataContractThresholds()

    min_rows = _coerce_min_rows(thresholds.min_rows, default=32)
    max_core_null_rate = _coerce_threshold(thresholds.max_core_null_rate, default=0.01)
    max_l5_null_rate = _coerce_threshold(thresholds.max_l5_null_rate, default=0.05)

    rows_total = int(len(df))
    missing_columns = [c for c in TICK_COLUMN_NAMES if c not in df.columns]
    extra_columns = [c for c in df.columns if c not in TICK_COLUMN_NAMES]
    canonical = df.reindex(columns=TICK_COLUMN_NAMES)

    row_hash_first = row_hash_from_ticks_df(canonical)
    row_hash_second = row_hash_from_ticks_df(canonical)
    row_hash_deterministic = bool(row_hash_first.equals(row_hash_second))
    row_hash_non_null_rows = int(row_hash_first.notna().sum())
    row_hash_integrity = bool(row_hash_non_null_rows == rows_total and row_hash_deterministic)
    row_hash_stats = row_hash_aggregates(row_hash_first)

    core_null_rates: dict[str, float] = {}
    for col in CORE_NULL_RATE_COLUMNS:
        series = pd.to_numeric(canonical[col], errors="coerce")
        core_null_rates[col] = _null_rate(series, total_rows=rows_total)

    l5_null_rates: dict[str, float] = {}
    for col in L5_BOOK_COLS:
        series = pd.to_numeric(canonical[col], errors="coerce")
        l5_null_rates[col] = _null_rate(series, total_rows=rows_total)
    l5_null_rate_avg = float(sum(l5_null_rates.values()) / float(len(l5_null_rates) or 1))

    violations: list[dict[str, Any]] = []
    if rows_total < min_rows:
        violations.append(
            {
                "type": "rows_below_minimum",
                "metric": "rows_total",
                "value": int(rows_total),
                "threshold": int(min_rows),
            }
        )
    if missing_columns:
        violations.append(
            {
                "type": "missing_columns",
                "metric": "schema_columns",
                "value": list(missing_columns),
                "threshold": [],
            }
        )
    for col, rate in core_null_rates.items():
        if float(rate) > max_core_null_rate:
            violations.append(
                {
                    "type": "core_null_rate_exceeded",
                    "metric": f"{col}_null_rate",
                    "value": float(rate),
                    "threshold": float(max_core_null_rate),
                }
            )
    if l5_null_rate_avg > max_l5_null_rate:
        violations.append(
            {
                "type": "l5_null_rate_exceeded",
                "metric": "l5_null_rate_avg",
                "value": float(l5_null_rate_avg),
                "threshold": float(max_l5_null_rate),
            }
        )

    checks = {
        "rows_minimum": bool(rows_total >= min_rows),
        "schema_columns_present": bool(not missing_columns),
        "row_hash_deterministic": bool(row_hash_deterministic),
        "row_hash_integrity": bool(row_hash_integrity),
        "null_rates_within_threshold": bool(
            all(float(v) <= max_core_null_rate for v in core_null_rates.values())
            and float(l5_null_rate_avg) <= max_l5_null_rate
        ),
    }
    ok = bool(all(checks.values()))

    return {
        "ok": ok,
        "rows_total": int(rows_total),
        "checks": checks,
        "schema": {
            "expected_columns": int(len(TICK_COLUMN_NAMES)),
            "expected_schema_hash": ticks_schema_hash(),
            "missing_columns": missing_columns,
            "extra_columns": extra_columns,
        },
        "row_hash": {
            "algorithm": row_hash_algorithm_version(),
            "deterministic": bool(row_hash_deterministic),
            "non_null_rows": int(row_hash_non_null_rows),
            "aggregates": row_hash_stats,
        },
        "null_rates": {
            "core": core_null_rates,
            "l5_avg": float(l5_null_rate_avg),
        },
        "thresholds": {
            "min_rows": int(min_rows),
            "max_core_null_rate": float(max_core_null_rate),
            "max_l5_null_rate": float(max_l5_null_rate),
        },
        "violations": violations,
    }
